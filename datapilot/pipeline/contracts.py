"""Trusted invocation and closed model output contracts for one graph fragment.

The request is assembled after application authorization, never from HTTP JSON.
Counts carry already-consumed budget through clarification and recovery. HTTP,
dispatch and application projection remain outside these invocation contracts.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, Protocol

from pydantic import Field, field_validator

from datapilot.catalog import LinkedSchema
from datapilot.config_versions import MODEL_CONFIG_VERSION
from datapilot.contracts import (
    ClosedModel,
    ConfirmedSpec,
    MetricId,
    QueryPlan,
    TrustedContext,
    aware_datetime,
)
from datapilot.executor import Cancellation, Deadline, ExecutedAttempt
from datapilot.tasks.contracts import Status

GRAPH_VERSION = "query-pipeline-v3"
Control = Callable[[str], None]
PhaseSink = Callable[[Status], None]
ResultSink = Callable[[ExecutedAttempt], str]
ClarificationSink = Callable[[dict[str, Any]], None]
PhaseMeasure = Callable[[str], AbstractContextManager[None]]


class PipelineRequest(ClosedModel):
    question: Annotated[str, Field(strict=True, min_length=1, max_length=2000)]
    reference_time: datetime
    context: TrustedContext
    previous_confirmed: ConfirmedSpec | None = None
    model_config_version: str = MODEL_CONFIG_VERSION
    model_call_count: Annotated[int, Field(strict=True, ge=0, le=4)] = 0
    repair_count: Annotated[int, Field(strict=True, ge=0, le=1)] = 0
    graph_step_count: Annotated[int, Field(strict=True, ge=0, le=24)] = 0

    _aware = field_validator("reference_time", mode="before")(aware_datetime)

    @field_validator("question")
    @classmethod
    def question_text(cls, value: str) -> str:
        value.encode("utf-8")
        if not value.strip() or "\x00" in value:
            raise ValueError("QUESTION_REQUIRED")
        return value.strip()


class ClarificationNeeded(ClosedModel):
    """Finite metric ambiguity output, not a persisted/resumable waiting object."""

    kind: Literal["clarification"]
    missing_fields: tuple[Literal["metric_id"], ...] = Field(min_length=1, max_length=1)
    question: str = Field(min_length=1, max_length=800)
    options: tuple[MetricId, ...] = Field(min_length=2, max_length=5)

    @field_validator("options")
    @classmethod
    def unique_options(cls, value: tuple[MetricId, ...]) -> tuple[MetricId, ...]:
        if len(value) != len(set(value)):
            raise ValueError("CLARIFICATION_OPTIONS_MUST_BE_UNIQUE")
        return value


class ModelGateway(Protocol):
    """No credentials, execution authority or gold are gateway arguments.

    Implementations enforce the supplied timeout at their transport and disable
    hidden retries. Parse returns a QuerySpec-shaped value or ClarificationNeeded;
    generate/repair return a QueryPlan-shaped value, all validated locally.
    """

    def parse(
        self, request: PipelineRequest, vocabulary: dict[str, Any], *, timeout: float
    ) -> object: ...

    def generate(
        self, confirmed: ConfirmedSpec, linked: LinkedSchema, *, timeout: float
    ) -> object: ...

    def repair(
        self,
        confirmed: ConfirmedSpec,
        linked: LinkedSchema,
        initial_plan: QueryPlan | None,
        error_code: str,
        *,
        timeout: float,
    ) -> object: ...


class ClarificationGateway(ModelGateway, Protocol):
    """The one bounded semantic parse after an accepted finite answer."""

    def parse_clarification(
        self,
        request: PipelineRequest,
        vocabulary: dict[str, Any],
        answer: MetricId,
        *,
        timeout: float,
    ) -> object: ...


class Executor(Protocol):
    def execute(
        self,
        confirmed: ConfirmedSpec,
        plan: QueryPlan,
        *,
        deadline: Deadline,
        cancellation: Cancellation | None = None,
    ) -> ExecutedAttempt: ...


@dataclass(frozen=True)
class PipelineOutcome:
    status: Status
    reason: str | None
    confirmed: ConfirmedSpec | None
    plan: QueryPlan | None
    result: dict[str, Any] | None
    attempt_id: str | None
    result_ref: str | None
    clarification: dict[str, Any] | None
    model_call_count: int
    repair_count: int
    graph_step_count: int
    trace: tuple[dict[str, Any], ...]
    checkpoint_id: str | None
