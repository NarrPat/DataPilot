"""Fixed October/September inputs shared by the public tests."""

from __future__ import annotations

from typing import Any

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec, QueryPlan, QuerySpec, Scope, TrustedContext
from datapilot.specification import confirm_spec
from tests.datapilot.contract_sql import contract_sql

METRICS = (
    "paid_goods_amount",
    "paid_order_count",
    "refund_goods_amount",
    "period_net_goods_amount",
    "paid_aov",
)
BASELINE = {
    "baseline_start_at": "2026-09-01T00:00:00+08:00",
    "baseline_end_at": "2026-10-01T00:00:00+08:00",
    "mode": "period_total",
}


def candidate(metric: str = "paid_goods_amount", **changes: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "schema_version": "1",
        "metric_id": metric,
        "time_range": {
            "start_at": "2026-10-01T00:00:00+08:00",
            "end_at": "2026-11-01T00:00:00+08:00",
            "timezone": "Asia/Shanghai",
            "reference_time": "2026-11-04T12:00:00+08:00",
        },
        "grain": "total",
        "group_by": None,
        "filters": {"sales_regions": ["east"], "category_codes": []},
        "comparison": None,
        "top_k": None,
        "result_limit": 200,
    }
    data.update(changes)
    return data


def context(scope: str = "east") -> TrustedContext:
    return TrustedContext(
        effective_scope=Scope(scope),
        authz_version=1,
        data_version="demo_v1",
        source_query_id="contract-test",
    )


def envelope(metric: str, scope: str = "east", **changes: Any) -> ConfirmedSpec:
    data = candidate(metric, **changes)
    if scope == "all":
        data["filters"]["sales_regions"] = []
    return confirm_spec(QuerySpec.model_validate(data), context(scope), Catalog())


def plan_for(
    confirmed: ConfirmedSpec, current: str | None = None, baseline: str | None = None
) -> QueryPlan:
    columns = (
        Catalog().get_metric(confirmed.spec.metric_id, confirmed.effective_scope).result_contract
    )
    return QueryPlan.model_validate(
        {
            "current_sql": current or contract_sql(confirmed),
            "baseline_sql": (baseline or contract_sql(confirmed, baseline=True))
            if confirmed.spec.comparison
            else None,
            "expected_columns": {
                "current": columns,
                "baseline": columns if confirmed.spec.comparison else None,
            },
        }
    )
