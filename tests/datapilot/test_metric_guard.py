"""Metric SQL contracts for both periods, with business-semantic counterexamples."""

import json

import pytest

from datapilot.catalog import Catalog
from datapilot.model.prompts import plan_messages
from datapilot.sql_guard import GuardError, guard_plan, guard_sql
from tests.datapilot.cases import BASELINE, METRICS, envelope, plan_for
from tests.datapilot.contract_sql import contract_sql


@pytest.mark.parametrize("metric", METRICS)
@pytest.mark.parametrize("scope", ["east", "all"])
@pytest.mark.parametrize("grain", ["total", "day", "week", "month"])
@pytest.mark.parametrize("group", [None, "category", "sales_region"])
def test_each_metric_scope_grain_and_separate_baseline_contract(
    metric: str, scope: str, grain: str, group: str | None
) -> None:
    confirmed = envelope(
        metric,
        scope,
        grain=grain,
        group_by=group,
        comparison={
            "baseline_start_at": "2026-09-01T00:00:00+08:00",
            "baseline_end_at": "2026-10-01T00:00:00+08:00",
            "mode": "period_total",
        },
    )
    assert len(guard_plan(plan_for(confirmed), confirmed, Catalog())) == 2


def test_net_month_baseline_keeps_null_time_slot_out_of_join_keys() -> None:
    confirmed = envelope(
        "period_net_goods_amount", grain="week", group_by="category", comparison=BASELINE
    )
    linked = Catalog().link_schema(
        confirmed.spec.metric_id, confirmed.effective_scope, category=True
    )
    messages = plan_messages(confirmed, linked)
    shape = json.loads(messages[1].text)["sql_shape_contract"]
    assert shape["current"]["aggregate_join_keys"] == ["bucket_start", "group_key"]
    assert shape["baseline"]["aggregate_join_keys"] == ["group_key"]
    assert shape["baseline"]["aggregate_group_keys"] == ["group_key"]
    assert shape["baseline"]["required_null_projections"] == {
        "bucket_start": "NULL::timestamptz AS bucket_start"
    }


@pytest.mark.parametrize(
    ("metric", "before", "after", "reason"),
    [
        (
            "paid_goods_amount",
            "SUM(i.paid_amount_cents)",
            "SUM(i.paid_amount_cents)/2",
            "SQL_METRIC_COMPONENT_MISMATCH",
        ),
        (
            "paid_goods_amount",
            "SUM(i.paid_amount_cents)",
            "SUM(DISTINCT i.paid_amount_cents)",
            "SQL_METRIC_COMPONENT_MISMATCH",
        ),
        (
            "paid_goods_amount",
            "SUM(i.paid_amount_cents)",
            "SUM(o.paid_goods_amount_cents)",
            "SQL_METRIC_COMPONENT_MISMATCH",
        ),
        ("paid_goods_amount", "o.payment_status='paid' AND ", "", "SQL_METRIC_PREDICATE_MISMATCH"),
        ("paid_goods_amount", "o.paid_at>=", "o.created_at>=", "SQL_METRIC_PREDICATE_MISMATCH"),
        ("paid_goods_amount", "o.paid_at<", "o.paid_at<=", "SQL_METRIC_PREDICATE_MISMATCH"),
        (
            "paid_goods_amount",
            " AND o.sales_region=ANY(%(sales_regions)s)",
            " OR o.sales_region=ANY(%(sales_regions)s)",
            "SQL_METRIC_PREDICATE_MISMATCH",
        ),
        (
            "paid_aov",
            "COUNT(DISTINCT o.order_id)",
            "COUNT(o.order_id)",
            "SQL_METRIC_COMPONENT_MISMATCH",
        ),
        ("refund_goods_amount", "r.succeeded_at>=", "o.paid_at>=", "SQL_METRIC_PREDICATE_MISMATCH"),
        ("refund_goods_amount", "r.status='succeeded' AND ", "", "SQL_METRIC_PREDICATE_MISMATCH"),
        (
            "period_net_goods_amount",
            "FULL OUTER JOIN",
            "LEFT JOIN",
            "SQL_METRIC_COMBINATION_MISMATCH",
        ),
        (
            "period_net_goods_amount",
            "COALESCE(p.group_key,r.group_key)",
            "p.group_key",
            "SQL_METRIC_GRAIN_MISMATCH",
        ),
    ],
)
def test_structurally_legal_business_wrong_sql_cannot_execute(
    metric: str, before: str, after: str, reason: str
) -> None:
    confirmed = envelope(metric, group_by="category")
    sql = contract_sql(confirmed)
    assert before in sql
    mutated = sql.replace(before, after)
    # These are specifically beyond the old AST/object boundary.
    guard_sql(mutated, confirmed, Catalog())
    with pytest.raises(GuardError) as caught:
        guard_plan(plan_for(confirmed, mutated), confirmed, Catalog())
    assert caught.value.code == reason


def test_wrong_baseline_grain_is_rejected_before_any_database_work() -> None:
    confirmed = envelope(
        "paid_goods_amount",
        grain="week",
        group_by="category",
        comparison={
            "baseline_start_at": "2026-09-01T00:00:00+08:00",
            "baseline_end_at": "2026-10-01T00:00:00+08:00",
            "mode": "period_total",
        },
    )
    with pytest.raises(GuardError, match="SQL_METRIC_GRAIN_MISMATCH"):
        guard_plan(plan_for(confirmed, baseline=contract_sql(confirmed)), confirmed, Catalog())


def test_detail_refund_fanout_is_not_a_valid_paid_amount_axis() -> None:
    confirmed = envelope("paid_goods_amount")
    sql = contract_sql(confirmed).replace(
        " WHERE ",
        " LEFT JOIN analytics_views.refunds_east r ON r.order_item_id=i.order_item_id WHERE ",
    )
    guard_sql(sql, confirmed, Catalog())
    with pytest.raises(GuardError, match="SQL_METRIC_RELATION_MISMATCH"):
        guard_plan(plan_for(confirmed, sql), confirmed, Catalog())


@pytest.mark.parametrize(
    "expression", ["COUNT(*)", "COALESCE(COUNT(*),0)", "CAST(COALESCE(COUNT(*),0) AS BIGINT)"]
)
def test_order_grain_count_star_wrappers_preserve_count_contract(expression: str) -> None:
    confirmed = envelope("paid_order_count")
    sql = contract_sql(confirmed).replace("COUNT(DISTINCT o.order_id)", expression)
    assert guard_plan(plan_for(confirmed, sql), confirmed, Catalog())
    fanout = sql.replace(
        " WHERE ", " JOIN analytics_views.order_items_east i ON i.order_id=o.order_id WHERE "
    )
    with pytest.raises(GuardError):
        guard_plan(plan_for(confirmed, fanout), confirmed, Catalog())
