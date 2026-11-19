"""Frozen, model-free capacity inputs; not an agent SQL generator/fallback.

There are no expected answers here. Evaluation computes those from source rows
in a separate test-side oracle after freezing the actual inputs.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec, Grain, QueryPlan, QuerySpec, Scope, TrustedContext
from datapilot.specification import confirm_spec
from datapilot.sql_guard import add_probe_limit, guard_plan

SUITE_ID = "DP-CAP-SQL-v1"
ROOT = Path(__file__).resolve().parents[1] / "fixtures/datapilot/capacity"
DEFINITIONS: tuple[tuple[str, str, str, str | None], ...] = (
    ("DP-SQL-01", "paid_goods_amount", "total", None),
    ("DP-SQL-02", "paid_order_count", "total", None),
    ("DP-SQL-03", "paid_goods_amount", "total", "category"),
    ("DP-SQL-04", "paid_aov", "month", "sales_region"),
    ("DP-SQL-05", "refund_goods_amount", "month", "category"),
    ("DP-SQL-06", "period_net_goods_amount", "total", None),
)
WINDOWS = {
    "narrow": ("2026-10-01T00:00:00+08:00", "2026-10-08T00:00:00+08:00"),
    "wide": ("2026-04-01T00:00:00+08:00", "2026-11-01T00:00:00+08:00"),
}


def capacity_case(
    sql_id: str, window: str, data_version: str, scope: Scope = Scope.ALL
) -> tuple[ConfirmedSpec, QueryPlan]:
    """Bind the registered SQL to an actual immutable dataset and trusted scope."""
    definition = next((row for row in DEFINITIONS if row[0] == sql_id), None)
    if definition is None or window not in WINDOWS:
        raise ValueError("CAPACITY_CASE_NOT_REGISTERED")
    _, metric, grain, group = definition
    lower, upper = WINDOWS[window]
    catalog = Catalog()
    spec = QuerySpec.model_validate(
        {
            "schema_version": "1",
            "metric_id": metric,
            "time_range": {
                "start_at": lower,
                "end_at": upper,
                "timezone": "Asia/Shanghai",
                "reference_time": "2026-11-04T12:00:00+08:00",
            },
            "grain": grain,
            "group_by": group,
            "filters": {"sales_regions": [], "category_codes": []},
            "comparison": None,
            "top_k": None,
            "result_limit": 200,
        }
    )
    context = TrustedContext(
        effective_scope=scope,
        authz_version=1,
        data_version=data_version,
        source_query_id=SUITE_ID + ":" + sql_id + ":" + window,
    )
    confirmed = confirm_spec(spec, context, catalog)
    # Scope is a closed server enum; no caller-provided identifier or SQL text.
    sql = (ROOT / (sql_id + ".sql")).read_text(encoding="utf8")
    if scope == Scope.EAST:
        sql = sql.replace("_all", "_east")
        for predicate in ("o.payment_status = 'paid'", "r.status = 'succeeded'"):
            sql = sql.replace(
                "WHERE " + predicate + " AND ",
                "WHERE " + predicate + " AND o.sales_region=ANY(%(sales_regions)s) AND ",
            )
    plan = QueryPlan(
        current_sql=sql,
        baseline_sql=None,
        expected_columns={  # type: ignore[arg-type]
            "current": catalog.get_metric(metric, scope).result_contract,
            "baseline": None,
        },
    )
    return confirmed, plan


def frozen_case(
    sql_id: str, window: str, data_version: str, scope: Scope = Scope.ALL
) -> dict[str, Any]:
    confirmed, plan = capacity_case(sql_id, window, data_version, scope)
    checked = tuple(
        add_probe_limit(sql, confirmed, Catalog()) for sql in guard_plan(plan, confirmed, Catalog())
    )
    return {
        "suite_id": SUITE_ID,
        "sql_id": sql_id,
        "window": window,
        "confirmed": confirmed.model_dump(mode="json"),
        "plan": plan.model_dump(mode="json"),
        "input_sql_sha256": hashlib.sha256(plan.current_sql.encode()).hexdigest(),
        "executed_sql": [sql.sql for sql in checked],
        "executed_sql_sha256": [sql.sql_sha256 for sql in checked],
        "bindings": {
            "start_at": confirmed.spec.time_range.start_at.isoformat(),
            "end_at": confirmed.spec.time_range.end_at.isoformat(),
            **(
                {"sales_regions": [r.value for r in confirmed.spec.filters.sales_regions]}
                if confirmed.spec.filters.sales_regions
                else {}
            ),
        },
    }


def overflow_case(data_version: str) -> tuple[ConfirmedSpec, QueryPlan]:
    """Separately registered daily/category wide probe; never a matrix cell."""
    confirmed, plan = capacity_case("DP-SQL-03", "wide", data_version)
    spec = confirmed.spec.model_copy(update={"grain": Grain.DAY})
    context = TrustedContext(
        effective_scope=Scope.ALL,
        authz_version=1,
        data_version=data_version,
        source_query_id=SUITE_ID + ":daily-category-overflow-probe",
    )
    probe = confirm_spec(spec, context, Catalog())
    sql = plan.current_sql.replace(
        "NULL::timestamptz",
        "DATE_TRUNC('day', o.paid_at AT TIME ZONE 'Asia/Shanghai') AT TIME ZONE 'Asia/Shanghai'",
    )
    return probe, plan.model_copy(update={"current_sql": sql})
