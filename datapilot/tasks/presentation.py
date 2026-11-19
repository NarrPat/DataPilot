"""Finite charts describe persisted result fields; no amount is recalculated."""

from __future__ import annotations

from typing import Any

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec, Grain, QueryPlan, QueryResult
from datapilot.sql_guard import guard_plan


def guard_summary(
    confirmed: ConfirmedSpec, plan: QueryPlan, catalog: Catalog
) -> list[dict[str, Any]]:
    """Recheck the final projection, without claiming the original check time.

    This structural boundary cannot prove metric correctness or SQL execution.
    It also runs on recovered outcomes, whose final plan must remain admissible.
    """
    statements = ("current", "baseline") if plan.baseline_sql is not None else ("current",)
    return [
        {
            "source": "projection_revalidation",
            "statement": statement,
            "allowed": True,
            "input_sql_hash": checked.input_sha256,
            "sql_hash": checked.sql_sha256,
            "complexity": checked.complexity,
            "canonical_spec_hash": confirmed.canonical_spec_hash,
            "spec_version": confirmed.spec_version,
        }
        for statement, checked in zip(statements, guard_plan(plan, confirmed, catalog), strict=True)
    ]


def chart_spec(row: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("status") != "completed" or not row.get("result_ref"):
        return None
    try:
        confirmed = ConfirmedSpec.model_validate(row.get("confirmed_spec"))
        result = QueryResult.model_validate(row.get("result_snapshot"))
        if result.metric_id != confirmed.spec.metric_id:
            return None
    except ValueError:
        return None
    kind = "table"
    x_column = None
    if confirmed.spec.grain != Grain.TOTAL and result.series:
        kind, x_column = "line", "bucket_start"
    elif confirmed.spec.group_by is not None and result.periods:
        kind, x_column = "bar", "group_key"
    return {
        "result_ref": str(row["result_ref"]),
        "kind": kind,
        "x_column": x_column,
        "series_column": "group_key" if confirmed.spec.group_by is not None else None,
        "y_column": "value",
        "unit": result.unit,
    }
