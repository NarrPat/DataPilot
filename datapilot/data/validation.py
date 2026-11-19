"""Validate five-table synthetic source facts, without SQL or metric/oracle logic.

Temporal ordering is nondecreasing: registered <= created <= paid <= refund
requested <= succeeded. Timestamps must carry an offset. Canonical projections
retain source facts but express their instants in UTC with microsecond precision.
``ready`` is an audit/assessment flag and is not a database readiness decision.
"""

import hashlib
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any, TypeGuard, cast

TABLES = ("customers", "products", "orders", "order_items", "refunds")
BIGINT_MIN = -(2**63)
BIGINT_MAX = 2**63 - 1
INTEGER_MAX = 2**31 - 1

_PRIMARY_KEYS = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "refunds": "refund_id",
}
_FIELDS = {
    "customers": {"customer_id", "registered_at", "home_region", "is_active"},
    "products": {"product_id", "category_code", "product_name", "status"},
    "orders": {
        "order_id",
        "customer_id",
        "created_at",
        "paid_at",
        "payment_status",
        "sales_region",
        "paid_goods_amount_cents",
        "currency",
    },
    "order_items": {"order_item_id", "order_id", "product_id", "quantity", "paid_amount_cents"},
    "refunds": {
        "refund_id",
        "order_item_id",
        "requested_at",
        "succeeded_at",
        "status",
        "amount_cents",
    },
}
_FOREIGN_KEYS = {
    "orders": {"customer_id": "customers"},
    "order_items": {"order_id": "orders", "product_id": "products"},
    "refunds": {"order_item_id": "order_items"},
}
_ENUMS = {
    ("customers", "home_region"): {"east", "north", "south", "west"},
    ("products", "category_code"): {
        "books",
        "electronics",
        "apparel",
        "food",
        "home",
        "sports",
        "beauty",
        "toys",
    },
    ("orders", "sales_region"): {"east", "north", "south", "west"},
    ("orders", "payment_status"): {"unpaid", "paid", "canceled_unpaid"},
    ("refunds", "status"): {"pending", "succeeded", "failed"},
}
_TIMESTAMPS = {
    "customers": {"registered_at": False},
    "orders": {"created_at": False, "paid_at": True},
    "refunds": {"requested_at": False, "succeeded_at": True},
}
_AMOUNTS = {
    "orders": "paid_goods_amount_cents",
    "order_items": "paid_amount_cents",
    "refunds": "amount_cents",
}
_METADATA_FIELDS = {
    "fixture_id",
    "schema_version",
    "catalog_version",
    "data_version",
    "seed",
    "business_projection_hash",
    "oracle_version",
    "human_reviewed",
    "ready",
}

type Row = dict[str, Any]
type Tables = dict[str, list[Row]]
type Error = dict[str, str]
type Indexes = dict[str, dict[int, Row]]


class FixtureInvalid(ValueError):
    """Reject an invalid source while keeping error text free of source values."""

    def __init__(self, errors: list[Error]) -> None:
        self.errors = [dict(error) for error in errors]
        super().__init__(f"Fixture validation failed ({len(errors)} error(s)).")


def _error(errors: list[Error], code: str, table: str, row_id: str = "-") -> None:
    errors.append({"code": code, "table": table, "row_id": row_id})


def _integer(value: object, minimum: int = BIGINT_MIN, maximum: int = BIGINT_MAX) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool) and minimum <= value <= maximum


def _timestamp(value: object, nullable: bool = False) -> tuple[datetime | None, str | None]:
    if value is None and nullable:
        return None, None
    if not isinstance(value, str):
        return None, "INVALID_TIMESTAMP"
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None, "TIMEZONE_REQUIRED"
        return parsed.astimezone(UTC), None
    except (ValueError, OverflowError):
        return None, "INVALID_TIMESTAMP"


def _date(row: Row, field: str) -> datetime | None:
    parsed, error = _timestamp(row[field], nullable=True)
    if error is not None:
        raise AssertionError("Only validated timestamps may enter relationship checks.")
    return parsed


def _row_id(row: Row, primary_key: str, position: int) -> str:
    value = row.get(primary_key)
    return str(value) if _integer(value) else f"#{position}"


def _row_errors(table: str, row: Row, row_id: str) -> list[Error]:
    errors: list[Error] = []
    if set(row) != _FIELDS[table]:
        _error(errors, "ROW_FIELDS_INVALID", table, row_id)
        return errors
    if not _integer(row[_PRIMARY_KEYS[table]]):
        _error(errors, "INVALID_PK", table, row_id)
    for field in _FOREIGN_KEYS.get(table, {}):
        if not _integer(row[field]):
            _error(errors, "INVALID_FK", table, row_id)
    for (enum_table, field), allowed in _ENUMS.items():
        if enum_table == table and (not isinstance(row[field], str) or row[field] not in allowed):
            _error(errors, "INVALID_ENUM", table, row_id)
    for field, nullable in _TIMESTAMPS.get(table, {}).items():
        _, timestamp_error = _timestamp(row[field], nullable)
        if timestamp_error is not None:
            _error(errors, timestamp_error, table, row_id)
    if table in _AMOUNTS:
        minimum = 1 if table == "refunds" else 0
        if not _integer(row[_AMOUNTS[table]], minimum, BIGINT_MAX):
            _error(errors, "INVALID_AMOUNT", table, row_id)
    if table == "customers" and not isinstance(row["is_active"], bool):
        _error(errors, "INVALID_BOOLEAN", table, row_id)
    if table == "products":
        # The central contract does not define a products.status enum.
        for field in ("product_name", "status"):
            if not isinstance(row[field], str) or not row[field].strip():
                _error(errors, "INVALID_TEXT", table, row_id)
    if table == "order_items" and not _integer(row["quantity"], 1, INTEGER_MAX):
        _error(errors, "INVALID_QUANTITY", table, row_id)
    if table == "orders":
        if row["currency"] != "CNY":
            _error(errors, "INVALID_CURRENCY", table, row_id)
        if (row["payment_status"] == "paid") != (row["paid_at"] is not None):
            _error(errors, "PAYMENT_TIME_STATE_MISMATCH", table, row_id)
        if row["payment_status"] in ("unpaid", "canceled_unpaid") and (
            _integer(row["paid_goods_amount_cents"]) and row["paid_goods_amount_cents"] != 0
        ):
            _error(errors, "UNPAID_AMOUNT_NONZERO", table, row_id)
    if table == "refunds" and (row["status"] == "succeeded") != (row["succeeded_at"] is not None):
        _error(errors, "REFUND_TIME_STATE_MISMATCH", table, row_id)
    return errors


def _extract_tables(document: object, errors: list[Error]) -> Tables:
    tables: Tables = {table: [] for table in TABLES}
    if not isinstance(document, dict):
        _error(errors, "DOCUMENT_SHAPE_INVALID", "_document")
        return tables
    raw_tables = document.get("tables")
    if not isinstance(raw_tables, dict):
        _error(errors, "TABLES_SHAPE_INVALID", "_document")
        return tables
    if set(raw_tables) != set(TABLES):
        _error(errors, "TABLES_FIELDS_INVALID", "_document")
    for table in TABLES:
        source_rows = raw_tables.get(table)
        if not isinstance(source_rows, list):
            _error(errors, "TABLE_NOT_LIST", table)
            continue
        for position, value in enumerate(source_rows, start=1):
            if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
                _error(errors, "ROW_SHAPE_INVALID", table, f"#{position}")
                continue
            row = cast(Row, value)
            row_id = _row_id(row, _PRIMARY_KEYS[table], position)
            local_errors = _row_errors(table, row, row_id)
            errors.extend(local_errors)
            if not local_errors:
                tables[table].append(row)
    return tables


def _indexes(tables: Tables, errors: list[Error]) -> Indexes:
    indexes: Indexes = {table: {} for table in TABLES}
    for table in TABLES:
        primary_key = _PRIMARY_KEYS[table]
        for row in tables[table]:
            row_id = cast(int, row[primary_key])
            if row_id in indexes[table]:
                _error(errors, "DUPLICATE_PK", table, str(row_id))
            else:
                indexes[table][row_id] = row
    return indexes


def _relationships(tables: Tables, indexes: Indexes, errors: list[Error]) -> None:
    for table, references in _FOREIGN_KEYS.items():
        for row in tables[table]:
            row_id = str(row[_PRIMARY_KEYS[table]])
            for field, target in references.items():
                if row[field] not in indexes[target]:
                    _error(errors, "FK_NOT_FOUND", table, row_id)

    for order in tables["orders"]:
        row_id = str(order["order_id"])
        created_at, paid_at = _date(order, "created_at"), _date(order, "paid_at")
        customer = indexes["customers"].get(order["customer_id"])
        if customer is not None:
            registered_at = _date(customer, "registered_at")
            if registered_at is not None and created_at is not None and registered_at > created_at:
                _error(errors, "REGISTRATION_AFTER_ORDER", "orders", row_id)
        if created_at is not None and paid_at is not None and created_at > paid_at:
            _error(errors, "PAYMENT_BEFORE_ORDER", "orders", row_id)

    order_totals: defaultdict[int, int] = defaultdict(int)
    for item in tables["order_items"]:
        order_totals[item["order_id"]] += item["paid_amount_cents"]
    for order in tables["orders"]:
        if order_totals[order["order_id"]] != order["paid_goods_amount_cents"]:
            _error(errors, "ORDER_ITEM_AMOUNT_MISMATCH", "orders", str(order["order_id"]))

    succeeded_totals: defaultdict[int, int] = defaultdict(int)
    for refund in tables["refunds"]:
        row_id = str(refund["refund_id"])
        requested_at, succeeded_at = _date(refund, "requested_at"), _date(refund, "succeeded_at")
        if requested_at is not None and succeeded_at is not None and requested_at > succeeded_at:
            _error(errors, "SUCCESS_BEFORE_REFUND_REQUEST", "refunds", row_id)
        refund_item = indexes["order_items"].get(refund["order_item_id"])
        if refund_item is None:
            continue
        refund_order = indexes["orders"].get(refund_item["order_id"])
        if refund_order is None:
            continue
        if refund_order["payment_status"] != "paid":
            _error(errors, "REFUND_REQUIRES_PAID_ORDER", "refunds", row_id)
        paid_at = _date(refund_order, "paid_at")
        if requested_at is not None and paid_at is not None and requested_at < paid_at:
            _error(errors, "REFUND_BEFORE_PAYMENT", "refunds", row_id)
        if refund["status"] == "succeeded":
            succeeded_totals[refund["order_item_id"]] += refund["amount_cents"]
    for item_id, amount in succeeded_totals.items():
        if amount > indexes["order_items"][item_id]["paid_amount_cents"]:
            _error(errors, "SUCCESSFUL_REFUND_EXCEEDS_PAYMENT", "order_items", str(item_id))


def _business(document: object) -> tuple[Tables, list[Error]]:
    errors: list[Error] = []
    tables = _extract_tables(document, errors)
    indexes = _indexes(tables, errors)
    _relationships(tables, indexes, errors)
    return tables, errors


def _canonical_projection(tables: Tables) -> Tables:
    result: Tables = {}
    for table in TABLES:
        rows: list[Row] = []
        for source_row in sorted(tables[table], key=lambda row: row[_PRIMARY_KEYS[table]]):
            row = {key: source_row[key] for key in sorted(source_row)}
            for key, value in row.items():
                if key.endswith("_at") and value is not None:
                    parsed, error = _timestamp(value)
                    if error is not None or parsed is None:
                        raise AssertionError("Only validated timestamps may be canonicalized.")
                    row[key] = parsed.isoformat(timespec="microseconds")
            rows.append(row)
        result[table] = rows
    return result


def _projection_digest(projection: Tables) -> str:
    encoded = json.dumps(
        projection, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_errors(errors: list[Error]) -> list[Error]:
    triples = sorted({(error["table"], error["row_id"], error["code"]) for error in errors})
    return [{"code": code, "table": table, "row_id": row_id} for table, row_id, code in triples]


def validate_fixture(doc: object) -> list[Error]:
    """Return stable errors; an empty list means data/metadata are structurally valid.

    ``human_reviewed=False`` and ``ready=False`` are valid and are retained.
    Neither a successful result here nor a projection hash proves metric answers.
    """
    tables, errors = _business(doc)
    if not isinstance(doc, dict):
        return _stable_errors(errors)
    if set(doc) != _METADATA_FIELDS | {"tables"}:
        _error(errors, "DOCUMENT_FIELDS_INVALID", "_metadata")
    for field in (
        "fixture_id",
        "schema_version",
        "catalog_version",
        "data_version",
        "oracle_version",
    ):
        value = doc.get(field)
        if not isinstance(value, str) or not value.strip():
            _error(errors, "INVALID_METADATA", "_metadata")
    if not _integer(doc.get("seed"), 0, BIGINT_MAX):
        _error(errors, "INVALID_METADATA", "_metadata")
    for field in ("human_reviewed", "ready"):
        if not isinstance(doc.get(field), bool):
            _error(errors, "INVALID_METADATA", "_metadata")
    if doc.get("ready") is True and doc.get("human_reviewed") is not True:
        _error(errors, "READY_REQUIRES_HUMAN_REVIEW", "_metadata")
    supplied_hash = doc.get("business_projection_hash")
    if not isinstance(supplied_hash, str) or re.fullmatch("[0-9a-f]{64}", supplied_hash) is None:
        _error(errors, "INVALID_PROJECTION_HASH", "_metadata")
    elif not errors and supplied_hash != _projection_digest(_canonical_projection(tables)):
        _error(errors, "PROJECTION_HASH_MISMATCH", "_metadata")
    return _stable_errors(errors)


def business_projection(doc: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Copy valid source rows in PK order, canonicalizing instants; exclude metadata.

    A database export may supply only ``{"tables": ...}``; metadata does not enter
    this function's business validation, projection, or digest.
    """
    tables, errors = _business(doc)
    if errors:
        raise FixtureInvalid(_stable_errors(errors))
    return _canonical_projection(tables)


def projection_hash(doc: dict[str, Any]) -> str:
    """SHA256 of UTF-8 canonical business JSON, independent of source metadata."""
    return _projection_digest(business_projection(doc))
