"""Validate exact driver rows, then calculate presentation values with Decimal.

Time components may be rolled up within one group. Category order counts are
never summed into a global distinct count. No frontend floating-point math.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Any
from zoneinfo import ZoneInfo

from datapilot.catalog import Catalog, MetricDefinition
from datapilot.contracts import (
    Category,
    ConfirmedSpec,
    Dimension,
    Grain,
    QueryResult,
    Region,
    Scope,
)

ZONE = ZoneInfo("Asia/Shanghai")


class ResultError(Exception):
    pass


def calendar_bucket(instant: datetime, grain: Grain) -> datetime | None:
    local = instant.astimezone(ZONE)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    if grain == Grain.TOTAL:
        return None
    if grain == Grain.WEEK:
        return midnight - timedelta(days=midnight.weekday())
    if grain == Grain.MONTH:
        return midnight.replace(day=1)
    return midnight


def _integer(value: object) -> int:
    if type(value) is int:
        return value
    if isinstance(value, Decimal) and value.is_finite() and value == value.to_integral_value():
        return int(value)
    raise ResultError("RESULT_TYPE_MISMATCH")


def validate_rows(
    rows: list[dict[str, Any]],
    confirmed: ConfirmedSpec,
    metric: MetricDefinition,
    *,
    baseline: bool = False,
) -> list[dict[str, Any]]:
    spec = confirmed.spec
    if len(rows) > min(200, spec.result_limit):
        raise ResultError("RESULT_TOO_LARGE")
    # The executor also counts serialized driver bytes while reading, before this conversion.
    if (
        len(json.dumps(rows, default=str, ensure_ascii=False, separators=(",", ":")).encode())
        > 512 * 1024
    ):
        raise ResultError("RESULT_TOO_LARGE")
    grain = Grain.TOTAL if baseline else spec.grain
    if baseline:
        if spec.comparison is None:
            raise ResultError("RESULT_UNEXPECTED_BASELINE")
        start, end = spec.comparison.baseline_start_at, spec.comparison.baseline_end_at
    else:
        start, end = spec.time_range.start_at, spec.time_range.end_at
    if grain == Grain.TOTAL and spec.group_by is None and len(rows) != 1:
        raise ResultError("RESULT_TOTAL_REQUIRES_ONE_ROW")
    result = []
    previous: tuple[str, str] | None = None
    for raw in rows:
        if set(raw) != set(metric.result_contract) or len(raw) > 12:
            raise ResultError("RESULT_COLUMNS_MISMATCH")
        bucket = raw["bucket_start"]
        if grain == Grain.TOTAL:
            if bucket is not None:
                raise ResultError("RESULT_BUCKET_MISMATCH")
        else:
            if (
                not isinstance(bucket, datetime)
                or bucket.tzinfo is None
                or bucket.utcoffset() is None
            ):
                raise ResultError("RESULT_BUCKET_MISMATCH")
            if bucket != calendar_bucket(bucket, grain) or not (
                calendar_bucket(start, grain) <= bucket < end  # type: ignore[operator]
            ):
                raise ResultError("RESULT_BUCKET_MISMATCH")
            bucket = bucket.astimezone(ZONE)
        group = raw["group_key"]
        allowed: set[str] = set()
        if spec.group_by == Dimension.CATEGORY:
            allowed = set(spec.filters.category_codes) or set(Category)
        elif spec.group_by == Dimension.SALES_REGION:
            allowed = set(spec.filters.sales_regions) or set(Region)
            if confirmed.effective_scope == Scope.EAST:
                allowed &= {"east"}
        if (spec.group_by is None and group is not None) or (
            spec.group_by is not None and (type(group) is not str or group not in allowed)
        ):
            raise ResultError("RESULT_GROUP_MISMATCH")
        order = (bucket.isoformat() if bucket else "", group or "")
        if previous is not None and order <= previous:
            raise ResultError("RESULT_ORDER_OR_DUPLICATE")
        previous = order
        row = {"bucket_start": bucket, "group_key": group}
        for column in metric.result_contract[2:]:
            row[column] = _integer(raw[column])
            if row[column] < 0:
                raise ResultError("RESULT_NEGATIVE_COMPONENT")
        if metric.denominator_key and not row["order_count"] and row["amount_cents"]:
            raise ResultError("RESULT_INCONSISTENT_COMPONENTS")
        result.append(row)
    return result


def _exact(row: dict[str, Any], metric: MetricDefinition) -> Decimal | None:
    if metric.denominator_key == "distinct_paid_orders":
        count = row["order_count"]
        return Decimal(row["amount_cents"]) / Decimal(count) / 100 if count else None
    if metric.numerator_key == "distinct_paid_orders":
        return Decimal(row["order_count"])
    if metric.numerator_key == "paid_minus_succeeded_refund_cents":
        return (Decimal(row["paid_amount_cents"]) - Decimal(row["refund_amount_cents"])) / 100
    return Decimal(row["amount_cents"]) / 100


def _display(value: Decimal | None, metric: MetricDefinition) -> str | int | None:
    if value is None:
        return None
    if metric.unit == "orders":
        return int(value)
    return format(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), ".2f")


def _period(
    rows: list[dict[str, Any]], metric: MetricDefinition
) -> dict[str | None, dict[str, Any]]:
    groups: dict[str | None, dict[str, Any]] = {}
    for row in rows:
        key = row["group_key"]
        total = groups.setdefault(key, {name: 0 for name in metric.result_contract[2:]})
        for column in total:
            total[column] += row[column]
    return groups


def present_results(
    confirmed: ConfirmedSpec,
    catalog: Catalog,
    current_rows: list[dict[str, Any]],
    baseline_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if (baseline_rows is not None) != (confirmed.spec.comparison is not None):
        raise ResultError("RESULT_BASELINE_MISMATCH")
    metric = catalog.get_metric(confirmed.spec.metric_id, confirmed.effective_scope)
    current = validate_rows(current_rows, confirmed, metric)
    baseline = (
        validate_rows(baseline_rows, confirmed, metric, baseline=True)
        if baseline_rows is not None
        else []
    )
    # Precision scales with bounded integer input, preserving large integer cents.
    digits = max(
        (len(str(abs(row[c]))) for row in current + baseline for c in metric.result_contract[2:]),
        default=1,
    )
    with localcontext() as ctx:
        ctx.prec = max(50, digits * 2 + 30)
        current_period, baseline_period = _period(current, metric), _period(baseline, metric)
        keys = set(current_period) | set(baseline_period)
        if confirmed.spec.top_k is not None:
            rankable = [(key, _exact(row, metric)) for key, row in current_period.items()]
            ranked = sorted(
                ((k, v) for k, v in rankable if v is not None), key=lambda x: (-x[1], x[0] or "")
            )
            selected = [key for key, _ in ranked[: confirmed.spec.top_k.count]]
        else:
            selected = sorted(keys, key=lambda key: key or "")
        zero = {name: 0 for name in metric.result_contract[2:]}
        periods = []
        for key in selected:
            curr = _exact(current_period.get(key, zero), metric)
            item: dict[str, Any] = {
                "group_key": key,
                "value": _display(curr, metric),
                "null_reason": "NO_PAID_ORDERS" if curr is None else None,
            }
            if baseline_rows is not None:
                base = _exact(baseline_period.get(key, zero), metric)
                delta = curr - base if curr is not None and base is not None else None
                pct = delta / base if base is not None and base > 0 and delta is not None else None
                item.update(
                    baseline_value=_display(base, metric),
                    delta=_display(delta, metric),
                    pct_change=format(
                        pct.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP), ".6f"
                    )
                    if pct is not None
                    else None,
                    comparison_reason=None
                    if pct is not None
                    else (
                        "NO_PAID_ORDERS"
                        if curr is None or base is None
                        else "BASELINE_NON_POSITIVE"
                    ),
                    baseline_null_reason="NO_PAID_ORDERS" if base is None else None,
                )
            periods.append(item)
        series = [
            {
                "bucket_start": row["bucket_start"].isoformat() if row["bucket_start"] else None,
                "group_key": row["group_key"],
                "value": _display(_exact(row, metric), metric),
                "null_reason": "NO_PAID_ORDERS" if _exact(row, metric) is None else None,
            }
            for row in current
            if row["group_key"] in selected
        ]
        answer = {
            "metric_id": metric.metric_id.value,
            "unit": metric.unit,
            "periods": periods,
            "series": series,
            "ranked_groups": selected if confirmed.spec.top_k is not None else None,
            "calendar": {
                "timezone": "Asia/Shanghai",
                "week_start": "Monday",
                "partial_buckets_clipped_to_window": True,
                "missing_buckets_filled": False,
            },
        }
        if confirmed.spec.grain != Grain.TOTAL:
            coverage = []
            for bucket in sorted({row["bucket_start"] for row in current}):
                if confirmed.spec.grain == Grain.MONTH:
                    following = (
                        bucket.replace(year=bucket.year + 1, month=1)
                        if bucket.month == 12
                        else bucket.replace(month=bucket.month + 1)
                    )
                else:
                    following = bucket + timedelta(
                        days=7 if confirmed.spec.grain == Grain.WEEK else 1
                    )
                coverage.append(
                    {
                        "bucket_start": bucket.isoformat(),
                        "effective_start_at": max(
                            bucket, confirmed.spec.time_range.start_at
                        ).isoformat(),
                        "effective_end_at": min(
                            following, confirmed.spec.time_range.end_at
                        ).isoformat(),
                    }
                )
            answer["bucket_coverage"] = coverage
        QueryResult.model_validate(answer)
        return answer
