"""Exact wait unions and one-process, real query observation timing.

Business clocks never enter this module. Observer times include HTTP observation
overhead and are deliberately distinct from server processing/P95 measurements.
"""

from __future__ import annotations

import os
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID, uuid4

from datapilot.tasks.contracts import TERMINAL, Status


def _nanoseconds(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("NONNEGATIVE_INTEGER_NANOSECONDS_REQUIRED")
    return value


def measure_elapsed(start_ns: int, end_ns: int, waits: Sequence[Sequence[int]]) -> dict[str, int]:
    """Pure integer arithmetic; supplied timestamps alone are not real telemetry."""
    start_ns, end_ns = _nanoseconds(start_ns), _nanoseconds(end_ns)
    if end_ns < start_ns:
        raise ValueError("TIMING_END_BEFORE_START")
    clipped = []
    for interval in waits:
        if len(interval) != 2:
            raise ValueError("WAIT_INTERVAL_PAIR_REQUIRED")
        left, right = (_nanoseconds(value) for value in interval)
        if right < left:
            raise ValueError("WAIT_INTERVAL_REVERSED")
        left, right = max(left, start_ns), min(right, end_ns)
        if left < right:
            clipped.append((left, right))
    union: list[tuple[int, int]] = []
    for left, right in sorted(clipped):
        if union and left <= union[-1][1]:
            union[-1] = (union[-1][0], max(right, union[-1][1]))
        else:
            union.append((left, right))
    human_wait = sum(right - left for left, right in union)
    lifecycle = end_ns - start_ns
    return {
        "lifecycle_ns": lifecycle,
        "human_wait_ns": human_wait,
        "processing_ns": lifecycle - human_wait,
    }


def _positive(value: object) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("POSITIVE_OBSERVATION_VERSION_REQUIRED")
    return value


class ObservedQueryTimer:
    """Start before submission, observe accepted snapshots, finish after durable GET.

    No clock injection, endpoint, disk writer or raw response body is provided.
    Snapshot observation cannot certify that an unobserved transition never ran.
    """

    def __init__(self, *, model_mode: Literal["none", "stub", "replay", "live"]) -> None:
        if model_mode not in {"none", "stub", "replay", "live"}:
            raise ValueError("TIMING_MODEL_MODE_REQUIRED")
        self._pid = os.getpid()
        self._observer_id = str(uuid4())
        self._model_mode = model_mode
        self._clock_info = time.get_clock_info("monotonic")
        self._query_id: str | None = None
        self._request_id: str | None = None
        self._version = 0
        self._seq = 0
        self._status: Status | None = None
        self._generation: int | None = None
        self._wait_start: int | None = None
        self._waits: list[tuple[int, int]] = []
        self._end: int | None = None
        self._invalid: str | None = None
        self._observations: list[dict[str, Any]] = []
        self._start = self._sample()
        self._record({"kind": "request_started", **self._start})

    def _record(self, observation: dict[str, Any]) -> None:
        if len(self._observations) >= 4096:
            self._invalid = "observation_limit"
            raise ValueError("TIMING_OBSERVATION_LIMIT")
        self._observations.append(observation)

    def _sample(self) -> dict[str, Any]:
        if os.getpid() != self._pid:
            self._invalid = "clock_process_changed"
            raise ValueError("TIMING_CLOCK_PROCESS_CHANGED")
        stamp = {
            "monotonic_ns": time.monotonic_ns(),
            "observed_at": datetime.now(UTC).isoformat(timespec="microseconds"),
        }
        if self._observations and stamp["monotonic_ns"] < self._observations[-1]["monotonic_ns"]:
            self._invalid = "monotonic_clock_reversed"
            raise ValueError("TIMING_MONOTONIC_CLOCK_REVERSED")
        return stamp

    def bind(self, accepted_snapshot: Mapping[str, Any]) -> None:
        if self._query_id is not None:
            raise ValueError("TIMING_ALREADY_BOUND")
        try:
            request_id = str(UUID(accepted_snapshot["request_id"]))
            query_id = str(UUID(accepted_snapshot["query_id"]))
        except (KeyError, TypeError, ValueError, AttributeError):
            self._invalid = "acceptance_identity_invalid"
            raise ValueError("TIMING_ACCEPTANCE_IDENTITY_REQUIRED") from None
        self._query_id, self._request_id = query_id, request_id
        self.observe(accepted_snapshot)

    def observe(self, snapshot: Mapping[str, Any]) -> None:
        if self._query_id is None:
            raise ValueError("TIMING_ACCEPTANCE_REQUIRED")
        try:
            query_id = str(UUID(snapshot["query_id"]))
            version = _positive(snapshot["status_version"])
            seq = _positive(snapshot["last_event_seq"])
            generation = _positive(snapshot["execution_generation"])
            status = Status(snapshot["status"])
        except (KeyError, TypeError, ValueError, AttributeError):
            self._invalid = "snapshot_contract_invalid"
            raise ValueError("TIMING_SNAPSHOT_INVALID") from None
        if query_id != self._query_id:
            self._invalid = "query_identity_changed"
            raise ValueError("TIMING_QUERY_IDENTITY_CHANGED")
        stamp = self._sample()
        observation = {
            "kind": "snapshot",
            **stamp,
            "query_id": query_id,
            "status": status.value,
            "status_version": version,
            "event_seq": seq,
            "execution_generation": generation,
        }
        if version < self._version or seq < self._seq:
            observation["kind"] = "stale_snapshot_ignored"
        elif version == self._version:
            if status != self._status or generation != self._generation:
                self._invalid = "snapshot_version_conflict"
                raise ValueError("TIMING_SNAPSHOT_VERSION_CONFLICT")
            if seq > self._seq:
                self._seq = seq
                observation["kind"] = "same_status_sequence_advanced"
            else:
                observation["kind"] = "duplicate_snapshot_ignored"
        elif seq == self._seq:
            self._invalid = "event_version_conflict"
            raise ValueError("TIMING_EVENT_VERSION_CONFLICT")
        elif self._end is not None:
            self._invalid = "terminal_version_changed"
            raise ValueError("TIMING_TERMINAL_VERSION_CHANGED")
        else:
            current = stamp["monotonic_ns"]
            if self._wait_start is not None and status != Status.WAITING:
                self._waits.append((self._wait_start, current))
                self._wait_start = None
            if status == Status.WAITING and self._wait_start is None:
                self._wait_start = current
            self._status, self._version, self._seq, self._generation = (
                status,
                version,
                seq,
                generation,
            )
            if status in TERMINAL:
                self._end = current
        self._record(observation)

    def invalidate(
        self,
        reason: Literal["observation_disconnected", "protocol_invalid", "observation_incomplete"],
    ) -> None:
        if reason not in {"observation_disconnected", "protocol_invalid", "observation_incomplete"}:
            raise ValueError("TIMING_INVALIDATION_REASON_REQUIRED")
        self._invalid = reason
        self._record({"kind": reason, **self._sample()})

    def summary(self) -> dict[str, Any]:
        self._sample()  # Fail closed on a fork even when the timer had finished.
        reason = self._invalid
        if reason is None and self._query_id is None:
            reason = "not_accepted"
        elif reason is None and self._end is None:
            reason = "terminal_not_observed"
        timing = (
            measure_elapsed(self._start["monotonic_ns"], self._end, self._waits)
            if self._end is not None and reason is None
            else None
        )
        return {
            "schema_version": 1,
            "definition": "single_observer_request_to_durable_terminal_response_v1",
            "clock_kind": "real_single_process_monotonic",
            "observer_id": self._observer_id,
            "process_id": self._pid,
            "clock_implementation": self._clock_info.implementation,
            "clock_resolution_seconds": self._clock_info.resolution,
            "clock_adjustable": self._clock_info.adjustable,
            "UTC_used_for_duration": False,
            "business_clock_used": False,
            "model_mode": self._model_mode,
            "query_id": self._query_id,
            "accepted_request_id": self._request_id,
            "business_outcome": self._status.value if self._status in TERMINAL else None,
            "timing_ns": timing,
            "unknown_reason": reason,
            "wait_intervals_ns": [list(interval) for interval in self._waits],
            "open_wait_start_ns": self._wait_start,
            "observations": [dict(item) for item in self._observations],
            "observation_completeness": "observed_snapshots_only",
            "unobserved_transitions_excluded": False,
            "server_processing_P95_eligible": False,
            "formal_quality_or_capacity_claim": False,
        }
