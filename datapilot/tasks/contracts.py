"""Closed public commands and a hand-enumerated DataPilot lifecycle boundary."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from datapilot.auth.errors import AppError
from datapilot.contracts import ClosedModel


class QueryInput(ClosedModel):
    question: Annotated[str, Field(strict=True, min_length=1, max_length=2000)]
    thread_id: UUID | None = None
    base_query_id: UUID | None = None
    base_spec_version: Annotated[int, Field(strict=True, ge=1)] | None = None

    @field_validator("question")
    @classmethod
    def normalize(cls, value: str) -> str:
        value = value.strip()
        value.encode("utf-8")
        if not value or "\x00" in value:
            raise ValueError("QUESTION_REQUIRED")
        return value

    @model_validator(mode="after")
    def base_pair(self) -> Self:
        if (self.base_query_id is None) != (self.base_spec_version is None):
            raise ValueError("BASE_REFERENCE_PAIR_REQUIRED")
        return self


class CancelInput(ClosedModel):
    expected_status_version: Annotated[int, Field(strict=True, ge=1)]


class Status(StrEnum):
    QUEUED = "queued"
    PARSING = "parsing"
    GENERATING = "generating"
    GUARDING = "guarding"
    PLANNING = "planning"
    EXECUTING = "executing"
    VALIDATING = "validating"
    REPAIRING = "repairing"
    WAITING = "needs_clarification"
    COMPLETED = "completed"
    REJECTED = "rejected"
    NEEDS_REFINE = "needs_refine"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"


TERMINAL = frozenset(
    {
        Status.COMPLETED,
        Status.REJECTED,
        Status.NEEDS_REFINE,
        Status.FAILED,
        Status.TIMED_OUT,
        Status.CANCELED,
    }
)
RUNNING = frozenset(set(Status) - TERMINAL - {Status.QUEUED, Status.WAITING})
FAILURES = frozenset(
    {Status.REJECTED, Status.NEEDS_REFINE, Status.FAILED, Status.TIMED_OUT, Status.CANCELED}
)
NEXT = {
    Status.QUEUED: frozenset(
        {Status.PARSING, Status.REJECTED, Status.TIMED_OUT, Status.FAILED, Status.CANCELED}
    ),
    Status.PARSING: FAILURES | {Status.GENERATING, Status.WAITING},
    Status.GENERATING: FAILURES | {Status.GUARDING},
    Status.GUARDING: FAILURES | {Status.PLANNING, Status.REPAIRING},
    Status.PLANNING: FAILURES | {Status.EXECUTING, Status.REPAIRING},
    Status.EXECUTING: FAILURES | {Status.VALIDATING, Status.REPAIRING},
    Status.VALIDATING: FAILURES | {Status.COMPLETED},
    Status.REPAIRING: FAILURES | {Status.GUARDING},
    Status.WAITING: frozenset({Status.QUEUED, Status.CANCELED, Status.NEEDS_REFINE}),
    **{state: frozenset() for state in TERMINAL},
}


def phase(status: Status) -> str:
    status = Status(status)
    return (
        "closed"
        if status in TERMINAL
        else "waiting"
        if status == Status.WAITING
        else "queued"
        if status == Status.QUEUED
        else "running"
    )


def payload_hash(model: ClosedModel) -> str:
    return hashlib.sha256(model.model_dump_json().encode()).hexdigest()


def validate_key(key: str | None) -> str:
    if not isinstance(key, str) or not re.fullmatch(r"[\x20-\x7e]{1,128}", key):
        raise AppError("IDEMPOTENCY_KEY_INVALID", 422)
    return key


def parse_cursor(header: str | None, query: str | None) -> int:
    def parse(value: str | None) -> int | None:
        if value is None:
            return None
        if not re.fullmatch(r"[0-9]{1,19}", value) or int(value) > 2**63 - 1:
            raise AppError("CURSOR_INVALID", 422)
        return int(value)

    left, right = parse(header), parse(query)
    if left is not None and right is not None and left != right:
        raise AppError("CURSOR_CONFLICT", 409)
    return left if left is not None else right if right is not None else 0


def elapsed_ms(origin: datetime, now: datetime, intervals: list[tuple[datetime, datetime]]) -> int:
    if origin.tzinfo is None or now.tzinfo is None:
        raise ValueError("AWARE_BUDGET_CLOCK_REQUIRED")
    union: list[list[datetime]] = []
    for start, end in sorted(intervals):
        if start.tzinfo is None or end.tzinfo is None or end < start:
            raise ValueError("WAIT_INTERVAL_INVALID")
        start, end = max(start, origin), min(end, now)
        if end <= start:
            continue
        if union and start <= union[-1][1]:
            union[-1][1] = max(union[-1][1], end)
        else:
            union.append([start, end])
    elapsed = max(now - origin, now - now)
    for start, end in union:
        elapsed -= end - start
    return max(0, elapsed // (now - now).resolution // 1000)


def processing_elapsed_ms(row: dict[str, Any], now: datetime) -> int:
    """Trusted UTC processing budget, excluding persisted waiting interval union."""
    intervals = [
        (datetime.fromisoformat(item["start_at"]), datetime.fromisoformat(item["end_at"]))
        for item in row.get("waiting_intervals", [])
    ]
    if row["status"] == Status.WAITING.value and row.get("waiting_since") is not None:
        intervals.append((row["waiting_since"], now))
    return elapsed_ms(row["api_received_at"], now, intervals)
