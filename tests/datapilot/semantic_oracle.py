"""OR-DP-v2 independent event-row oracle, including the fifth metric.

Uses Decimal source cents and sets of order identities. No product import, SQL,
Pydantic model, SQL result or repair feedback is used to compute an answer.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Any
from zoneinfo import ZoneInfo

ORACLE_VERSION = "OR-DP-v2"


def instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Oracle requires offset-aware timestamps")
    return parsed.astimezone(ZoneInfo("Asia/Shanghai"))


def components(
    source: dict[str, Any], spec: dict[str, Any], scope: str, *, baseline: bool = False
) -> list[dict[str, Any]]:
    if baseline:
        lower = instant(spec["comparison"]["baseline_start_at"])
        upper = instant(spec["comparison"]["baseline_end_at"])
    else:
        lower, upper = (
            instant(spec["time_range"]["start_at"]),
            instant(spec["time_range"]["end_at"]),
        )
    grain = "total" if baseline else spec["grain"]
    regions, categories = spec["filters"]["sales_regions"], spec["filters"]["category_codes"]
    group = spec["group_by"]
    orders = {r["order_id"]: r for r in source["tables"]["orders"]}
    items = {r["order_item_id"]: r for r in source["tables"]["order_items"]}
    products = {r["product_id"]: r for r in source["tables"]["products"]}
    items_by_order: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for item in items.values():
        items_by_order[item["order_id"]].append(item)
    # Each event ledger entry is [paid cents, refund cents, distinct paid identities].
    ledger: dict[tuple[datetime | None, str | None], list[Any]] = defaultdict(
        lambda: [Decimal(0), Decimal(0), set()]
    )

    def key(
        at: str, order: dict[str, Any], category: str | None
    ) -> tuple[datetime | None, str | None]:
        time = instant(at)
        if grain == "total":
            bucket = None
        elif grain == "day":
            bucket = time.replace(hour=0, minute=0, second=0, microsecond=0)
        elif grain == "week":
            bucket = (time - timedelta(days=time.isoweekday() - 1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        else:
            bucket = time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        dimension = (
            category
            if group == "category"
            else order["sales_region"]
            if group == "sales_region"
            else None
        )
        return bucket, dimension

    def visible(order: dict[str, Any]) -> bool:
        return (scope == "all" or order["sales_region"] == "east") and (
            not regions or order["sales_region"] in regions
        )

    metric = spec["metric_id"]
    paid_metric = metric in {
        "paid_goods_amount",
        "paid_order_count",
        "period_net_goods_amount",
        "paid_aov",
    }
    if paid_metric:
        for order_id, order in orders.items():
            if (
                not visible(order)
                or order["payment_status"] != "paid"
                or not lower <= instant(order["paid_at"]) < upper
            ):
                continue
            matching = [
                item
                for item in items_by_order[order_id]
                if not categories or products[item["product_id"]]["category_code"] in categories
            ]
            if group != "category" and not categories:
                ledger[key(order["paid_at"], order, None)][2].add(order_id)
            for item in matching:
                category = products[item["product_id"]]["category_code"]
                entry = ledger[key(order["paid_at"], order, category)]
                entry[0] += Decimal(item["paid_amount_cents"])
                entry[2].add(order_id)
    if metric in {"refund_goods_amount", "period_net_goods_amount"}:
        for refund in source["tables"]["refunds"]:
            if (
                refund["status"] != "succeeded"
                or not lower <= instant(refund["succeeded_at"]) < upper
            ):
                continue
            item = items[refund["order_item_id"]]
            order = orders[item["order_id"]]
            category = products[item["product_id"]]["category_code"]
            if visible(order) and (not categories or category in categories):
                ledger[key(refund["succeeded_at"], order, category)][1] += Decimal(
                    refund["amount_cents"]
                )
    if grain == "total" and group is None and not ledger:
        ledger[(None, None)]
    rows = []
    for (bucket, dimension), (paid, refunded, identities) in sorted(
        ledger.items(), key=lambda pair: (str(pair[0][0] or ""), pair[0][1] or "")
    ):
        row: dict[str, Any] = {"bucket_start": bucket, "group_key": dimension}
        if metric == "paid_order_count":
            row["order_count"] = len(identities)
        elif metric == "paid_aov":
            row.update(amount_cents=int(paid), order_count=len(identities))
        elif metric == "period_net_goods_amount":
            row.update(paid_amount_cents=int(paid), refund_amount_cents=int(refunded))
        else:
            row["amount_cents"] = int(refunded if metric == "refund_goods_amount" else paid)
        rows.append(row)
    return rows


def answer(source: dict[str, Any], spec: dict[str, Any], scope: str) -> dict[str, Any]:
    current = components(source, spec, scope)
    baseline = components(source, spec, scope, baseline=True) if spec["comparison"] else []
    metric = spec["metric_id"]

    def value(rows: list[dict[str, Any]]) -> Decimal | None:
        if metric == "paid_aov":
            count = sum(row["order_count"] for row in rows)
            return (
                sum(Decimal(row["amount_cents"]) for row in rows) / (100 * count) if count else None
            )
        if metric == "paid_order_count":
            return Decimal(sum(row["order_count"] for row in rows))
        if metric == "period_net_goods_amount":
            return (
                sum(
                    (
                        (Decimal(row["paid_amount_cents"]) - Decimal(row["refund_amount_cents"]))
                        for row in rows
                    ),
                    Decimal(0),
                )
                / 100
            )
        return sum((Decimal(row["amount_cents"]) for row in rows), Decimal(0)) / 100

    def show(number: Decimal | None) -> str | int | None:
        if number is None:
            return None
        return (
            int(number)
            if metric == "paid_order_count"
            else str(number.quantize(Decimal(".01"), rounding=ROUND_HALF_UP))
        )

    with localcontext() as ctx:
        ctx.prec = 80
        groups = {row["group_key"] for row in current + baseline}
        scores = {g: value([r for r in current if r["group_key"] == g]) for g in groups}
        if spec["top_k"]:
            observed = {r["group_key"] for r in current}
            rankable = [(g, v) for g in observed if (v := scores[g]) is not None]
            ranked = sorted(rankable, key=lambda pair: (-pair[1], pair[0] or ""))
            selected = [g for g, _ in ranked[: spec["top_k"]["count"]]]
        else:
            selected = sorted(groups, key=lambda g: g or "")
        periods = []
        for group in selected:
            curr = scores[group]
            period = {
                "group_key": group,
                "value": show(curr),
                "null_reason": "NO_PAID_ORDERS" if curr is None else None,
            }
            if spec["comparison"]:
                base = value([r for r in baseline if r["group_key"] == group])
                difference = curr - base if curr is not None and base is not None else None
                ratio = (
                    difference / base
                    if difference is not None and base is not None and base > 0
                    else None
                )
                period.update(
                    baseline_value=show(base),
                    delta=show(difference),
                    pct_change=str(ratio.quantize(Decimal(".000001"), rounding=ROUND_HALF_UP))
                    if ratio is not None
                    else None,
                    comparison_reason=None
                    if ratio is not None
                    else "NO_PAID_ORDERS"
                    if curr is None or base is None
                    else "BASELINE_NON_POSITIVE",
                    baseline_null_reason="NO_PAID_ORDERS" if base is None else None,
                )
            periods.append(period)
        series = [
            {
                "bucket_start": r["bucket_start"].isoformat() if r["bucket_start"] else None,
                "group_key": r["group_key"],
                "value": show(value([r])),
                "null_reason": "NO_PAID_ORDERS" if value([r]) is None else None,
            }
            for r in current
            if r["group_key"] in selected
        ]
        return {
            "periods": periods,
            "series": series,
            "ranked_groups": selected if spec["top_k"] else None,
        }
