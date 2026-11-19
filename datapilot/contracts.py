"""Public candidate contracts. Trusted execution fields have a separate type.

DP-I003 froze four metrics; DP-I004 adds the planned fifth in a new snapshot.
No SQL, role, scope or connection input is accepted by QuerySpec.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MetricId(StrEnum):
    PAID_GOODS_AMOUNT = "paid_goods_amount"
    PAID_ORDER_COUNT = "paid_order_count"
    REFUND_GOODS_AMOUNT = "refund_goods_amount"
    PERIOD_NET_GOODS_AMOUNT = "period_net_goods_amount"
    PAID_AOV = "paid_aov"


class Grain(StrEnum):
    TOTAL = "total"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


class Dimension(StrEnum):
    CATEGORY = "category"
    SALES_REGION = "sales_region"


class Region(StrEnum):
    EAST = "east"
    NORTH = "north"
    SOUTH = "south"
    WEST = "west"


class Category(StrEnum):
    BOOKS = "books"
    ELECTRONICS = "electronics"
    APPAREL = "apparel"
    FOOD = "food"
    HOME = "home"
    SPORTS = "sports"
    BEAUTY = "beauty"
    TOYS = "toys"


class Scope(StrEnum):
    ALL = "all"
    EAST = "east"


def aware_datetime(value: object) -> datetime:
    if not isinstance(value, (str, datetime)):
        raise ValueError("ISO8601_TIMESTAMP_REQUIRED")
    try:
        parsed = (
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            if isinstance(value, str)
            else value
        )
    except ValueError as exc:
        raise ValueError("ISO8601_TIMESTAMP_REQUIRED") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("TIMEZONE_OFFSET_REQUIRED")
    return parsed


def check_window(start: datetime, end: datetime) -> None:
    if not start < end or end - start > timedelta(days=366):
        raise ValueError("WINDOW_MUST_BE_POSITIVE_AND_AT_MOST_366_DAYS")


class TimeRange(ClosedModel):
    start_at: datetime
    end_at: datetime
    timezone: Literal["Asia/Shanghai"]
    reference_time: datetime

    _aware = field_validator("start_at", "end_at", "reference_time", mode="before")(aware_datetime)

    @model_validator(mode="after")
    def window(self) -> Self:
        check_window(self.start_at, self.end_at)
        return self


class Filters(ClosedModel):
    sales_regions: tuple[Region, ...] = ()
    category_codes: tuple[Category, ...] = ()

    @model_validator(mode="after")
    def unique(self) -> Self:
        if len(set(self.sales_regions)) != len(self.sales_regions):
            raise ValueError("DUPLICATE_REGION")
        if len(set(self.category_codes)) != len(self.category_codes):
            raise ValueError("DUPLICATE_CATEGORY")
        return self


class Comparison(ClosedModel):
    baseline_start_at: datetime
    baseline_end_at: datetime
    mode: Literal["period_total"]

    _aware = field_validator("baseline_start_at", "baseline_end_at", mode="before")(aware_datetime)

    @model_validator(mode="after")
    def window(self) -> Self:
        check_window(self.baseline_start_at, self.baseline_end_at)
        return self


class TopK(ClosedModel):
    count: Annotated[int, Field(strict=True, ge=1, le=10)]
    scope: Literal["whole_current_period"]
    order: Literal["desc"]


class QuerySpec(ClosedModel):
    schema_version: Literal["1"]
    metric_id: MetricId
    time_range: TimeRange
    grain: Grain
    group_by: Dimension | None
    filters: Filters
    comparison: Comparison | None
    top_k: TopK | None
    result_limit: Annotated[int, Field(strict=True, ge=1, le=200)]

    @model_validator(mode="after")
    def top_k_requires_group(self) -> Self:
        if self.top_k is not None and self.group_by is None:
            raise ValueError("TOP_K_REQUIRES_GROUP")
        return self


class ExpectedColumns(ClosedModel):
    current: tuple[str, ...]
    baseline: tuple[str, ...] | None


class QueryPlan(ClosedModel):
    current_sql: Annotated[str, Field(strict=True, min_length=1, max_length=8192)]
    baseline_sql: Annotated[str, Field(strict=True, min_length=1, max_length=8192)] | None
    expected_columns: ExpectedColumns


class TrustedContext(ClosedModel):
    """Created by the server after authorization, never a public request DTO."""

    effective_scope: Scope
    authz_version: Annotated[int, Field(strict=True, ge=1)]
    data_version: Annotated[str, Field(min_length=1, max_length=80)]
    source_query_id: Annotated[str, Field(min_length=1, max_length=80)]
    spec_version: Annotated[int, Field(strict=True, ge=1)] = 1


class ConfirmedSpec(ClosedModel):
    spec: QuerySpec
    spec_version: int
    metric_version: str
    catalog_version: str
    data_version: str
    effective_scope: Scope
    authz_version: int
    source_query_id: str
    canonical_spec_hash: str


MoneyString = Annotated[str, Field(strict=True, pattern=r"^-?[0-9]+\.[0-9]{2}$")]
ResultValue = MoneyString | Annotated[int, Field(strict=True)] | None


class ResultPoint(ClosedModel):
    bucket_start: str | None
    group_key: str | None
    value: ResultValue
    null_reason: Literal["NO_PAID_ORDERS"] | None


class ResultPeriod(ClosedModel):
    group_key: str | None
    value: ResultValue
    null_reason: Literal["NO_PAID_ORDERS"] | None
    baseline_value: ResultValue = None
    baseline_null_reason: Literal["NO_PAID_ORDERS"] | None = None
    delta: ResultValue = None
    pct_change: Annotated[str, Field(strict=True, pattern=r"^-?[0-9]+\.[0-9]{6}$")] | None = None
    comparison_reason: Literal["NO_PAID_ORDERS", "BASELINE_NON_POSITIVE"] | None = None


class CalendarPolicy(ClosedModel):
    timezone: Literal["Asia/Shanghai"]
    week_start: Literal["Monday"]
    partial_buckets_clipped_to_window: Literal[True]
    missing_buckets_filled: Literal[False]


class BucketCoverage(ClosedModel):
    bucket_start: str
    effective_start_at: str
    effective_end_at: str


class QueryResult(ClosedModel):
    metric_id: MetricId
    unit: Literal["CNY", "orders"]
    periods: tuple[ResultPeriod, ...]
    series: tuple[ResultPoint, ...]
    ranked_groups: tuple[str | None, ...] | None
    calendar: CalendarPolicy
    bucket_coverage: tuple[BucketCoverage, ...] = ()
