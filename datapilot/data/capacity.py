"""Deterministic synthetic five-table capacity data, never metric answers.

The seed, allocation rule, paid-order denominator and odd-count rounding are
part of this version. 10k/100k describe actual orders, not multiplied seed rows.
Generation metadata belongs in a separate manifest, outside the source schema.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta
from typing import Any

from datapilot.data.fixtures import projection_hash

SEED = 20261104
GENERATOR_VERSION = "dp-cap-generator-v1"
REGIONS = ("east", "north", "south", "west")
CATEGORIES = ("books", "electronics", "apparel", "food", "home", "sports", "beauty", "toys")
PAID_CELLS = tuple((region, month) for region in REGIONS for month in range(4, 11))
HOTSPOT = ("east", 10)
OTHER_CELLS = tuple(cell for cell in PAID_CELLS if cell != HOTSPOT)
LOWER = datetime.fromisoformat("2026-04-01T00:00:00+08:00")
UPPER = datetime.fromisoformat("2026-11-01T00:00:00+08:00")


def generate_capacity(order_count: int, distribution: str) -> dict[str, Any]:
    """Generate within 100k orders; uniform cells differ by at most one paid row.

    Every fifth order is unpaid (every tenth canceled). For skew50, exactly
    floor(paid_count / 2) paid orders occupy east + October; no other paid order
    occupies that pair. Regions/months individually are not claimed 50 percent.
    """
    if (
        type(order_count) is not int
        or not 1 <= order_count <= 100000
        or distribution not in {"uniform", "skew50"}
    ):
        raise ValueError("CAPACITY_PROFILE_INVALID")
    rng = random.Random(SEED)
    size = {10000: "10k", 100000: "100k"}.get(order_count, str(order_count))
    fixture_id = f"dp-cap-orders-{size}-v1"
    customer_count = max(4, order_count // 8)
    tables: dict[str, list[dict[str, Any]]] = {
        "customers": [
            {
                "customer_id": i,
                "registered_at": LOWER.isoformat(),
                "home_region": REGIONS[(i - 1) % 4],
                "is_active": i % 13 != 0,
            }
            for i in range(1, customer_count + 1)
        ],
        "products": [
            {
                "product_id": i,
                "category_code": CATEGORIES[(i - 1) % 8],
                "product_name": f"Synthetic {CATEGORIES[(i - 1) % 8]} {i}",
                "status": "active" if i % 17 else "inactive",
            }
            for i in range(1, 81)
        ],
        "orders": [],
        "order_items": [],
        "refunds": [],
    }
    paid_ids = [i for i in range(1, order_count + 1) if i % 5]
    rng.shuffle(paid_ids)
    assignments = {}
    hotspot_count = len(paid_ids) // 2
    for index, identity in enumerate(paid_ids):
        if distribution == "uniform":
            cell = PAID_CELLS[index % len(PAID_CELLS)]
        elif index < hotspot_count:
            cell = HOTSPOT
        else:
            cell = OTHER_CELLS[(index - hotspot_count) % len(OTHER_CELLS)]
        assignments[identity] = cell
    for identity in range(1, order_count + 1):
        paid = identity % 5 != 0
        region, month = assignments.get(identity, (REGIONS[identity % 4], 4 + identity % 7))
        instant = LOWER.replace(
            month=month,
            day=1 + rng.randrange(28),
            hour=1 + rng.randrange(22),
            minute=rng.randrange(60),
            second=rng.randrange(60),
        )
        created = max(LOWER, instant - timedelta(minutes=15 + rng.randrange(2880)))
        amount = (99 + 123 * (identity % 97)) if paid and identity % 101 else 0
        tables["orders"].append(
            {
                "order_id": identity,
                "customer_id": 1 + rng.randrange(customer_count),
                "created_at": created.isoformat(),
                "paid_at": instant.isoformat() if paid else None,
                "payment_status": "paid"
                if paid
                else "canceled_unpaid"
                if identity % 10 == 0
                else "unpaid",
                "sales_region": region,
                "paid_goods_amount_cents": amount,
                "currency": "CNY",
            }
        )
        item_count = 1 + identity % 4
        # Integral equal shares plus remainder retain the exact order total.
        shares = [amount // item_count + (slot < amount % item_count) for slot in range(item_count)]
        first_item = len(tables["order_items"]) + 1
        for slot, cents in enumerate(shares):
            tables["order_items"].append(
                {
                    "order_item_id": first_item + slot,
                    "order_id": identity,
                    "product_id": 1 + rng.randrange(80),
                    "quantity": 1 + rng.randrange(3),
                    "paid_amount_cents": int(cents),
                }
            )
        if not paid or identity % 7 or shares[0] < 4:
            continue
        # Distinct pending/failed records plus partial and split successful refunds.
        status = (
            "pending" if identity % 11 == 0 else "failed" if identity % 13 == 0 else "succeeded"
        )
        requested = min(instant + timedelta(days=1 + identity % 20), UPPER - timedelta(seconds=3))
        succeeded = min(requested + timedelta(hours=6), UPPER - timedelta(seconds=2))
        parts = [shares[0] // 4, shares[0] // 5] if identity % 3 == 0 else [shares[0] // 3]
        for part_index, cents in enumerate(parts):
            tables["refunds"].append(
                {
                    "refund_id": len(tables["refunds"]) + 1,
                    "order_item_id": first_item,
                    "requested_at": requested.isoformat(),
                    "succeeded_at": (succeeded + timedelta(microseconds=part_index)).isoformat()
                    if status == "succeeded"
                    else None,
                    "status": status,
                    "amount_cents": cents,
                }
            )
    source: dict[str, Any] = {
        "fixture_id": fixture_id,
        "schema_version": "dp-schema-v1",
        "catalog_version": "dp-catalog-v2-five",
        "data_version": fixture_id + "-" + distribution,
        "seed": SEED,
        "oracle_version": "OR-DP-v2",
        "human_reviewed": False,
        "ready": False,
        "tables": tables,
    }
    source["business_projection_hash"] = projection_hash(source)
    return source
