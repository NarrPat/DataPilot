"""Read immutable fixture JSON; validation never writes to a database or source file."""

import json
from pathlib import Path
from typing import Any, cast

from datapilot.data.validation import (
    TABLES,
    FixtureInvalid,
    business_projection,
    projection_hash,
    validate_fixture,
)

__all__ = [
    "TABLES",
    "FixtureInvalid",
    "business_projection",
    "load_fixture",
    "projection_hash",
    "validate_fixture",
]


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key.")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise ValueError("Non-standard JSON numeric constant.")


def load_fixture(path: Path) -> dict[str, Any]:
    """Load and validate JSON without changing the input or persisting business rows.

    ``FixtureInvalid.errors`` contains only stable code/table/row_id triples.
    Parsing failures deliberately omit raw JSON, duplicate key names, and values.
    Filesystem failures retain their normal ``OSError`` behavior.
    """
    try:
        with path.open("r", encoding="utf-8") as source:
            document: object = json.load(
                source,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_json_constant,
            )
    except (ValueError, UnicodeError):
        raise FixtureInvalid(
            [{"code": "INVALID_JSON", "table": "_document", "row_id": "-"}]
        ) from None
    errors = validate_fixture(document)
    if errors:
        raise FixtureInvalid(errors)
    return cast(dict[str, Any], document)
