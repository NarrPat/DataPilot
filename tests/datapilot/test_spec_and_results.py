"""A user's requested time, scope and metric survive confirmation and presentation."""

from copy import deepcopy
from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError

from datapilot.catalog import Catalog
from datapilot.contracts import QuerySpec
from datapilot.results import ResultError, present_results
from datapilot.specification import SpecError, confirm_spec
from tests.datapilot.cases import candidate, context, envelope


@pytest.mark.parametrize("variant", ["reverse", "367_days", "wrong_timezone", "naive", "numeric"])
def test_time_bounds(variant: str) -> None:
    data = candidate()
    changes = {
        "reverse": {"end_at": "2026-09-01T00:00:00+08:00"},
        "367_days": {"end_at": "2027-10-03T00:00:00+08:00"},
        "wrong_timezone": {"timezone": "UTC"},
        "naive": {"start_at": "2026-10-01T00:00:00"},
        "numeric": {"start_at": 0},
    }
    data["time_range"].update(changes[variant])
    with pytest.raises(ValidationError):
        QuerySpec.model_validate(data)


def test_confirmation_scope_and_canonical_utc_equivalence() -> None:
    data = candidate(filters={"sales_regions": [], "category_codes": []})
    first = confirm_spec(QuerySpec.model_validate(data), context(), Catalog())
    data["time_range"]["start_at"] = "2026-09-30T16:00:00+00:00"
    second = confirm_spec(QuerySpec.model_validate(data), context(), Catalog())
    assert first.canonical_spec_hash == second.canonical_spec_hash
    assert first.spec.filters.sales_regions == ("east",)
    assert first.spec.time_range.start_at.isoformat() == "2026-10-01T00:00:00+08:00"
    with pytest.raises(SpecError, match="SCOPE_FORBIDDEN"):
        confirm_spec(
            QuerySpec.model_validate(
                candidate(filters={"sales_regions": ["north"], "category_codes": []})
            ),
            context(),
            Catalog(),
        )


@pytest.mark.parametrize(
    ("metric", "components", "value"),
    [
        ("paid_goods_amount", {"amount_cents": 20000}, "200.00"),
        ("paid_order_count", {"order_count": 2}, 2),
        ("refund_goods_amount", {"amount_cents": 2500}, "25.00"),
        (
            "period_net_goods_amount",
            {"paid_amount_cents": 0, "refund_amount_cents": 2000},
            "-20.00",
        ),
    ],
)
def test_first_four_hand_values(metric: str, components: dict[str, int], value: str | int) -> None:
    result = present_results(
        envelope(metric), Catalog(), [{"bucket_start": None, "group_key": None, **components}]
    )
    assert result["periods"][0]["value"] == value


@pytest.mark.parametrize(
    "change",
    [
        {"amount_cents": 2.0},
        {"amount_cents": True},
        {"amount_cents": -1},
        {"group_key": "east"},
        {"bucket_start": datetime.fromisoformat("2026-10-01T00:00:00+08:00")},
        {"extra": 1},
    ],
)
def test_result_boundary_rejects_bad_driver_rows(change: dict[str, Any]) -> None:
    row = {"bucket_start": None, "group_key": None, "amount_cents": 20000, **change}
    with pytest.raises(ResultError):
        present_results(envelope("paid_goods_amount"), Catalog(), [row])


def test_input_candidate_unchanged() -> None:
    data = candidate()
    before = deepcopy(data)
    confirm_spec(QuerySpec.model_validate(data), context(), Catalog())
    assert before == data
