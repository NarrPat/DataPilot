"""Real server clock facts and conservative reconstruction, separate from budgets."""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from datapilot.contracts import ClosedModel
from datapilot.tasks.contracts import TERMINAL, Status
from datapilot.telemetry import measure_elapsed

ClockSource = Literal[
    "api_receipt",
    "service_entry",
    "state_projection",
    "answer_accepted",
    "post_commit_verification",
    "wait_commit_return",
    "wait_closed",
    "phase_start",
    "phase_finish",
]
PRODUCERS: dict[int, UUID] = {}


class ClockPoint(ClosedModel):
    schema_version: Literal[1]
    source: ClockSource
    wall_at: datetime
    monotonic_ns: Annotated[int, Field(strict=True, ge=0)]
    monotonic_lower_ns: Annotated[int, Field(strict=True, ge=0)]
    monotonic_upper_ns: Annotated[int, Field(strict=True, ge=0)]
    producer_pid: Annotated[int, Field(strict=True, ge=1)]
    producer_id: UUID
    clock_implementation: str = Field(min_length=1, max_length=128)
    clock_resolution_seconds: float = Field(gt=0, le=1, allow_inf_nan=False)

    @field_validator("schema_version", mode="before")
    @classmethod
    def exact_version(cls, value: Any) -> Any:
        if type(value) is not int or value != 1:
            raise ValueError("CLOCK_SCHEMA_VERSION_UNSUPPORTED")
        return value

    @field_validator("wall_at")
    @classmethod
    def aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("AWARE_REAL_CLOCK_REQUIRED")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def capture_bounds(self) -> Self:
        if not self.monotonic_lower_ns <= self.monotonic_ns <= self.monotonic_upper_ns:
            raise ValueError("CLOCK_CAPTURE_BOUNDS_INVALID")
        return self


def capture_clock(source: ClockSource) -> ClockPoint:
    pid = os.getpid()
    producer = PRODUCERS.setdefault(pid, uuid4())
    clock = time.get_clock_info("monotonic")
    lower = time.monotonic_ns()
    wall = datetime.now(UTC)
    upper = time.monotonic_ns()
    return ClockPoint(
        schema_version=1,
        source=source,
        wall_at=wall,
        monotonic_ns=(lower + upper) // 2,
        monotonic_lower_ns=lower,
        monotonic_upper_ns=upper,
        producer_pid=pid,
        producer_id=producer,
        clock_implementation=clock.implementation,
        clock_resolution_seconds=clock.resolution,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
            default=str,
        ).encode()
    ).hexdigest()


def input_digest(origin: dict[str, Any], events: list[dict[str, Any]]) -> str:
    # JSONB point strings are kept byte-equivalent semantically across DB/export;
    # private DB insertion timestamps are observations, not the input identity.
    return _digest(
        {
            "origin": {key: origin[key] for key in ("query_id", "accepted_request_id", "point")},
            "events": [
                {
                    key: event[key]
                    for key in (
                        "query_id",
                        "seq",
                        "status_version",
                        "spec_version",
                        "execution_generation",
                        "status",
                        "point",
                    )
                }
                for event in events
            ],
        }
    )


def projection_digest(row: dict[str, Any]) -> str:
    names = (
        "query_id",
        "owner_user_id",
        "accepted_request_id",
        "status",
        "status_version",
        "spec_version",
        "last_event_seq",
        "execution_generation",
        "catalog_version",
        "data_version",
        "graph_version",
        "model_config_version",
        "confirmed_spec",
        "query_plan",
        "result_ref",
        "result_snapshot",
    )
    return _digest({key: row[key] for key in names})


def elapsed_utc(
    start: datetime, end: datetime, waits: list[tuple[datetime, datetime]]
) -> dict[str, int]:
    """Exact microsecond UTC arithmetic. Fixed inputs alone are not real samples."""
    for value in [start, end, *(point for pair in waits for point in pair)]:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("AWARE_REAL_CLOCK_REQUIRED")
    unit = timedelta(microseconds=1)
    # Normalize away the absolute epoch, avoiding datetime.timestamp() float loss.
    ends = (end - start) // unit * 1000
    if ends < 0:
        raise ValueError("SERVER_CLOCK_REVERSED")
    # measure_elapsed accepts nonnegative positions. Absolute waits outside the
    # interval are clipped here; reversed intervals are still rejected first.
    clipped = []
    for left, right in waits:
        if right < left:
            raise ValueError("WAIT_INTERVAL_REVERSED")
        clipped.append(
            [
                max(0, min(ends, (left - start) // unit * 1000)),
                max(0, min(ends, (right - start) // unit * 1000)),
            ]
        )
    return measure_elapsed(0, ends, clipped)


def validate_journal(
    origin: dict[str, Any], events: list[dict[str, Any]], row: dict[str, Any], end: ClockPoint
) -> tuple[ClockPoint, list[tuple[datetime, datetime]]]:
    first = ClockPoint.model_validate(origin["point"])
    if (
        first.source not in {"api_receipt", "service_entry"}
        or end.source != "post_commit_verification"
    ):
        raise ValueError("TIMING_SOURCE_INVALID")
    if str(origin["query_id"]) != str(row["query_id"]) or (
        str(origin["accepted_request_id"]) != str(row["accepted_request_id"])
    ):
        raise ValueError("TIMING_QUERY_IDENTITY_MISMATCH")
    if not events or len(events) != row["last_event_seq"] or len(events) > 4096:
        raise ValueError("TIMING_EVENT_GAP")
    previous_wall = first.wall_at
    producers = {first.producer_id: first}
    last_version, last_spec, last_generation = 0, 0, 0
    waits = []
    waiting_since = None
    for index, event in enumerate(events, 1):
        point = ClockPoint.model_validate(event["point"])
        if point.source not in {"state_projection", "answer_accepted", "wait_closed"}:
            raise ValueError("TIMING_SOURCE_INVALID")
        status = Status(event["status"])
        if (
            str(event["query_id"]) != str(row["query_id"])
            or event["seq"] != index
            or event["status_version"] < last_version
            or event["spec_version"] < last_spec
            or event["execution_generation"] < last_generation
        ):
            raise ValueError("TIMING_EVENT_GAP")
        if point.wall_at < previous_wall:
            raise ValueError("SERVER_CLOCK_ORDER_UNKNOWN")
        _same_producer_clock(producers.get(point.producer_id), point)
        if waiting_since is not None and status != Status.WAITING:
            waits.append((waiting_since, point.wall_at))
            waiting_since = None
        if status == Status.WAITING and waiting_since is None:
            waiting_since = point.wall_at
        previous_wall = point.wall_at
        producers[point.producer_id] = point
        last_version, last_spec, last_generation = (
            event["status_version"],
            event["spec_version"],
            event["execution_generation"],
        )
    final = events[-1]
    if (
        Status(row["status"]) not in TERMINAL
        or final["status"] != row["status"]
        or final["status_version"] != row["status_version"]
        or final["spec_version"] != row["spec_version"]
        or final["execution_generation"] != row["execution_generation"]
        or waiting_since is not None
    ):
        raise ValueError("TIMING_TERMINAL_MISMATCH")
    if end.wall_at < previous_wall:
        raise ValueError("SERVER_CLOCK_ORDER_UNKNOWN")
    _same_producer_clock(producers.get(end.producer_id), end)
    return first, waits


def _same_producer_clock(previous: ClockPoint | None, point: ClockPoint) -> None:
    if previous is None:
        return  # Never subtract monotonic values from different producers.
    if previous.producer_pid != point.producer_pid or point.monotonic_ns < previous.monotonic_ns:
        raise ValueError("SERVER_CLOCK_ORDER_UNKNOWN")
    # UTC is quantized to microseconds; monotonic capture brackets and actual
    # advertised resolution bound local sampling uncertainty without a SLA.
    tolerance = 2000 + 2 * math.ceil(
        max(previous.clock_resolution_seconds, point.clock_resolution_seconds) * 1_000_000_000
    )
    wall_delta = (point.wall_at - previous.wall_at) // timedelta(microseconds=1) * 1000
    lower = point.monotonic_lower_ns - previous.monotonic_upper_ns - tolerance
    upper = point.monotonic_upper_ns - previous.monotonic_lower_ns + tolerance
    if not lower <= wall_delta <= upper:
        raise ValueError("UTC_MONOTONIC_DIVERGENCE")


def summarize_server_timing(
    origin: dict[str, Any] | None,
    events: list[dict[str, Any]],
    check: dict[str, Any] | None,
    row: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": 1,
        "query_id": str(row["query_id"]),
        "clock_kind": "server_system_utc_with_per_producer_monotonic",
        "timing_ns": None,
        "unknown_reason": None,
        "clock_alignment_attested": False,
        "formal_P95_eligible": False,
        "business_clock_used": False,
        "not_observer_processing": True,
        "definition": None,
        "standard_origin": False,
        "measurement_kind": "server_boundary_candidate_v1",
        "wait_start_semantics": "same_transaction_state_sample_not_commit_return",
        "all_persistence_overhead_accounted": False,
    }
    if origin is None:
        result["unknown_reason"] = "TIMING_ORIGIN_MISSING"
        return result
    if check is None:
        result["unknown_reason"] = "POST_COMMIT_VERIFICATION_MISSING"
        return result
    if check["outcome"] != "verified":
        result["unknown_reason"] = check["reason_code"]
        return result
    try:
        if check["inputs_sha256"] != input_digest(origin, events):
            raise ValueError("TIMING_INPUT_HASH_MISMATCH")
        if check["projection_sha256"] != projection_digest(row):
            raise ValueError("TIMING_PROJECTION_CHANGED")
        if (
            str(check["query_id"]) != str(row["query_id"])
            or check["last_event_seq"] != row["last_event_seq"]
            or check["final_status"] != row["status"]
            or str(check["result_ref"]) != str(row["result_ref"])
        ):
            raise ValueError("TIMING_TERMINAL_MISMATCH")
        endpoint = ClockPoint.model_validate(check["point"])
        first, waits = validate_journal(origin, events, row, endpoint)
        result.update(
            timing_ns=elapsed_utc(first.wall_at, endpoint.wall_at, waits),
            accepted_request_id=str(origin["accepted_request_id"])
            if origin["accepted_request_id"]
            else None,
            standard_origin=first.source == "api_receipt",
            definition=first.source + "_to_post_commit_readback_v1",
            human_wait_intervals=[
                {"start_at": a.isoformat(), "end_at": b.isoformat()} for a, b in waits
            ],
            lifecycle_endpoint="durable_projection_readback",
            business_outcome=row["status"],
            producer_count=len(
                {first.producer_id, endpoint.producer_id}
                | {ClockPoint.model_validate(e["point"]).producer_id for e in events}
            ),
            wall_clock_resolution="microseconds; not a latency/SLA guarantee",
        )
    except (ValueError, KeyError, TypeError):
        result["unknown_reason"] = "TIMING_RECORD_INVALID"
    return result


def summarize_committed_timing(
    origin: dict[str, Any] | None,
    events: list[dict[str, Any]],
    commits: list[dict[str, Any]],
    check: dict[str, Any] | None,
    row: dict[str, Any],
) -> dict[str, Any]:
    from datapilot.wait_timing import summarize

    return summarize(origin, events, commits, check, row)
