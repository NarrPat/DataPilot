"""Closed display contracts; no endpoint accepts SQL, scope or model state."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from datapilot.catalog import MetricDefinition
from datapilot.contracts import (
    ClosedModel,
    ConfirmedSpec,
    MetricId,
    QueryPlan,
    QueryResult,
    Scope,
    aware_datetime,
)
from datapilot.tasks.contracts import Status

NonNegative = Annotated[int, Field(strict=True, ge=0)]


class ChartSpec(ClosedModel):
    result_ref: str
    kind: Literal["table", "bar", "line"]
    x_column: Literal["bucket_start", "group_key"] | None
    series_column: Literal["group_key"] | None
    y_column: Literal["value"]
    unit: Literal["CNY", "orders"]


class ClarificationSnapshot(ClosedModel):
    clarification_id: UUID
    spec_version: int
    missing_fields: tuple[Literal["metric_id"], ...]
    question: str
    options: tuple[MetricId, ...]
    expires_at: datetime


class PrincipalPublic(ClosedModel):
    user_id: UUID
    username: str
    role: Literal["analyst_east", "analyst_all", "report_admin"]
    effective_scope: Scope | None
    authz_version: int
    capabilities: tuple[str, ...]


class CsrfResponse(ClosedModel):
    request_id: str
    csrf_token: str


class MeResponse(ClosedModel):
    request_id: str
    user: PrincipalPublic


class LoginResponse(MeResponse):
    csrf_token: str


class GuardComplexity(ClosedModel):
    bytes: NonNegative
    nodes: NonNegative
    ctes: NonNegative
    joins: NonNegative
    subquery_depth: NonNegative


class GuardSummary(ClosedModel):
    source: Literal["projection_revalidation"]
    statement: Literal["current", "baseline"]
    allowed: Literal[True]
    input_sql_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sql_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    complexity: GuardComplexity
    canonical_spec_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    spec_version: Annotated[int, Field(strict=True, ge=1)]


class QuerySnapshot(ClosedModel):
    request_id: str
    query_id: UUID
    thread_id: UUID
    question: str
    base_query_id: UUID | None
    base_spec_version: int | None
    api_received_at: datetime
    reference_time: datetime
    finished_at: datetime | None
    status: Status
    status_version: int
    spec_version: int
    effective_scope: Scope
    execution_generation: int
    cancel_requested: bool
    last_event_seq: int
    catalog_version: str
    data_version: str
    current_version_matches: bool
    reason_code: str | None
    spec: ConfirmedSpec | None
    query_plan: QueryPlan | None
    guard_summary: tuple[GuardSummary, ...] | None
    result: QueryResult | None
    result_ref: UUID | None
    chart_spec: ChartSpec | None
    clarification: ClarificationSnapshot | None
    events_url: str


class HistoryItem(ClosedModel):
    query_id: UUID
    question: str
    status: Status
    status_version: int
    spec_version: int
    effective_scope: Scope
    catalog_version: str
    data_version: str
    current_version_matches: bool
    api_received_at: datetime
    finished_at: datetime | None


class HistoryResponse(ClosedModel):
    request_id: str
    items: tuple[HistoryItem, ...]
    next_cursor: str | None


class CatalogReadiness(ClosedModel):
    state: Literal["unknown", "ready", "not_ready"]
    code: str


class MetricReadiness(ClosedModel):
    metric_id: str
    ready: bool | None
    code: str


class CatalogEnvelope(ClosedModel):
    request_id: str
    catalog_version: str
    definition_hash: str
    schema_fingerprint: str
    effective_scope: Scope
    readiness: CatalogReadiness
    metric_readiness: tuple[MetricReadiness, ...]


class MetricsResponse(CatalogEnvelope):
    metrics: tuple[MetricDefinition, ...]


class SchemaResponse(CatalogEnvelope):
    views: dict[str, dict[str, str]]
    join_paths: tuple[tuple[str, str], ...]
    allowed_features: tuple[str, ...]


class ReportConfiguration(ClosedModel):
    code_revision: str = Field(min_length=1, max_length=128)
    model_mode: Literal["none", "stub", "replay", "live"]
    model: str | None = Field(max_length=120)
    prompt_version: str | None = Field(max_length=120)
    graph_version: str = Field(min_length=1, max_length=120)
    catalog_version: str = Field(min_length=1, max_length=120)
    data_version: str = Field(min_length=1, max_length=120)
    dataset_id: str = Field(min_length=1, max_length=120)


class ReportMetric(ClosedModel):
    key: str = Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")
    numerator: NonNegative
    denominator: NonNegative

    @model_validator(mode="after")
    def fraction(self) -> Self:
        if self.numerator > self.denominator:
            raise ValueError("REPORT_FRACTION_INVALID")
        return self


class ReportUsage(ClosedModel):
    model_calls: NonNegative | None
    input_tokens: NonNegative | None
    output_tokens: NonNegative | None
    cost_cny: Annotated[str, Field(strict=True, pattern=r"^[0-9]+\.[0-9]{2,8}$")] | None


class ReportFailure(ClosedModel):
    case_id: str = Field(min_length=1, max_length=120)
    stage: str = Field(min_length=1, max_length=80)
    code: str = Field(min_length=1, max_length=120)
    message: str = Field(min_length=1, max_length=800)


class ReportDocument(ClosedModel):
    report_id: UUID
    title: str = Field(min_length=1, max_length=200)
    report_type: Literal["synthetic_test_report", "formal_evaluation"]
    status: Literal["design_only", "not_run", "scored", "failed"]
    ready: bool = Field(strict=True)
    gold_reviewed: bool = Field(strict=True)
    created_at: datetime
    configuration: ReportConfiguration
    metrics: tuple[ReportMetric, ...] = Field(max_length=30)
    usage: ReportUsage
    failures: tuple[ReportFailure, ...] = Field(max_length=100)

    _aware = field_validator("created_at", mode="before")(aware_datetime)

    @model_validator(mode="after")
    def provenance(self) -> Self:
        if len({m.key for m in self.metrics}) != len(self.metrics):
            raise ValueError("REPORT_METRIC_KEYS_DUPLICATED")
        if self.status in {"design_only", "not_run"} and (self.ready or self.metrics):
            raise ValueError("UNRUN_REPORT_HAS_NO_SCORE")
        if self.status == "scored" and not self.ready:
            raise ValueError("SCORED_REPORT_NOT_READY")
        if (
            self.report_type == "formal_evaluation"
            and (self.status == "scored" or bool(self.metrics))
            and (
                not self.ready or not self.gold_reviewed or self.configuration.model_mode != "live"
            )
        ):
            raise ValueError("FORMAL_SCORE_REQUIRES_REVIEWED_LIVE_EVIDENCE")
        return self


class ReportSummary(ClosedModel):
    report_id: UUID
    title: str
    report_type: Literal["synthetic_test_report", "formal_evaluation"]
    status: Literal["design_only", "not_run", "scored", "failed"]
    ready: bool
    gold_reviewed: bool
    created_at: datetime


class ReportsResponse(ClosedModel):
    request_id: str
    items: tuple[ReportSummary, ...]
    next_cursor: str | None


class ReportResponse(ClosedModel):
    request_id: str
    report: ReportDocument
