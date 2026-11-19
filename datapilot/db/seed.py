"""Load a validated source fixture into an empty analytics database atomically."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from datapilot.data.fixtures import TABLES, FixtureInvalid, projection_hash
from datapilot.data.validation import validate_fixture

KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "refunds": "refund_id",
}
COLUMNS = {
    "customers": ("customer_id", "registered_at", "home_region", "is_active"),
    "products": ("product_id", "category_code", "product_name", "status"),
    "orders": (
        "order_id",
        "customer_id",
        "created_at",
        "paid_at",
        "payment_status",
        "sales_region",
        "paid_goods_amount_cents",
        "currency",
    ),
    "order_items": ("order_item_id", "order_id", "product_id", "quantity", "paid_amount_cents"),
    "refunds": (
        "refund_id",
        "order_item_id",
        "requested_at",
        "succeeded_at",
        "status",
        "amount_cents",
    ),
}


class SeedConflict(Exception):
    """A different frozen dataset requires the admin-only, manifest-bound reset protocol."""


def export_tables(connection: psycopg.Connection[dict[str, Any]]) -> dict[str, Any]:
    tables = {}
    for table in TABLES:
        rows = connection.execute(
            sql.SQL("SELECT {} FROM raw.{} ORDER BY {}").format(
                sql.SQL(", ").join(map(sql.Identifier, COLUMNS[table])),
                sql.Identifier(table),
                sql.Identifier(KEYS[table]),
            )
        ).fetchall()
        tables[table] = [
            {
                key: value.isoformat() if isinstance(value, datetime) else value
                for key, value in row.items()
            }
            for row in rows
        ]
    return {"tables": tables}


def load_seed(
    connection: psycopg.Connection[dict[str, Any]],
    source: dict[str, Any],
    *,
    control: Callable[[], None] | None = None,
) -> dict[str, Any]:
    errors = validate_fixture(source)
    if errors:
        raise FixtureInvalid(errors)
    expected = projection_hash(source)
    counts = {table: len(source["tables"][table]) for table in TABLES}
    with connection.transaction():
        connection.execute("LOCK TABLE analytics_meta.dataset_state IN EXCLUSIVE MODE")
        previous = connection.execute("SELECT * FROM analytics_meta.dataset_state").fetchone()
        if previous:
            if (
                previous["projection_hash"] != expected
                or previous["fixture_id"] != source["fixture_id"]
                or previous["data_version"] != source["data_version"]
                or previous["row_counts"] != counts
                or projection_hash(export_tables(connection)) != expected
            ):
                raise SeedConflict("FROZEN_DATA_VERSION_CONFLICT")
            return {"projection_hash": expected, "row_counts": counts, "reused": True}
        if any(export_tables(connection)["tables"].values()):
            raise SeedConflict("UNREGISTERED_EXISTING_BUSINESS_ROWS")
        for table in TABLES:
            columns = COLUMNS[table]
            with connection.cursor() as cursor:
                statement = sql.SQL("INSERT INTO raw.{} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(map(sql.Identifier, columns)),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                )
                rows = source["tables"][table]
                for offset in range(0, len(rows), 1000):
                    if control is not None:
                        control()
                    cursor.executemany(
                        statement,
                        [
                            tuple(row[column] for column in columns)
                            for row in rows[offset : offset + 1000]
                        ],
                    )
        if projection_hash(export_tables(connection)) != expected:
            raise SeedConflict("DATABASE_ROUNDTRIP_HASH_MISMATCH")
        connection.execute(
            "INSERT INTO analytics_meta.dataset_state "
            "(id, fixture_id, data_version, projection_hash, row_counts) VALUES (true,%s,%s,%s,%s)",
            (source["fixture_id"], source["data_version"], expected, Jsonb(counts)),
        )
    return {"projection_hash": expected, "row_counts": counts, "reused": False}
