"""Human-readable hand totals checked independently from the SQL and result code."""

from pathlib import Path
from typing import Any

import pytest

from datapilot.data.fixtures import load_fixture
from tests.datapilot.cases import METRICS, envelope
from tests.datapilot.semantic_oracle import answer

SOURCE = Path(__file__).resolve().parents[2] / "fixtures/datapilot/demo_v1.source.json"


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ("east", ["214.69", 7, "136.00", "78.69", "30.67"]),
        ("all", ["634.69", 11, "148.00", "486.69", "57.70"]),
    ],
)
def test_five_metric_october_hand_totals(scope: str, expected: list[Any]) -> None:
    source = load_fixture(SOURCE)
    actual = []
    for metric in METRICS:
        confirmed = envelope(metric, scope)
        result = answer(source, confirmed.spec.model_dump(mode="json"), scope)
        actual.append(result["periods"][0]["value"])
    assert actual == expected


def test_refund_only_groups_zero_price_and_round_half_up() -> None:
    source = load_fixture(SOURCE)
    net = envelope("period_net_goods_amount", group_by="category")
    amounts = {
        row["group_key"]: row["value"]
        for row in answer(source, net.spec.model_dump(mode="json"), "east")["periods"]
    }
    assert amounts["home"] == "-80.00" and amounts["apparel"] == "-30.00"
    aov = envelope("paid_aov", group_by="category")
    prices = {
        row["group_key"]: row["value"]
        for row in answer(source, aov.spec.model_dump(mode="json"), "east")["periods"]
    }
    assert prices["food"] == "0.00" and prices["sports"] == "2.35" and prices["books"] == "42.50"
    # This is a category-weighted ratio of sums, not an average of bucket AOVs.
    assert prices["books"] != "30.67"
