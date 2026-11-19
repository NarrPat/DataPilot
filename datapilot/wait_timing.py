"""Version2 timing uses original COMMIT returns, never recovered substitute times."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from datapilot.server_timing import (
    ClockPoint,
    _digest,
    _same_producer_clock,
    elapsed_utc,
    summarize_server_timing,
)


class WaitTimingUnavailable(ValueError):
    """Business projection can be valid even when this timing fact is absent."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def waits_digest(commits: list[dict[str, Any]]) -> str:
    return _digest(
        [
            {
                key: row[key]
                for key in (
                    "query_id",
                    "waiting_event_seq",
                    "status_version",
                    "spec_version",
                    "execution_generation",
                    "point",
                )
            }
            for row in commits
        ]
    )


def committed_waits(
    origin: dict[str, Any],
    events: list[dict[str, Any]],
    commits: list[dict[str, Any]],
    endpoint: ClockPoint,
) -> list[tuple[datetime, datetime]]:
    waiting = [e for e in events if e["status"] == "needs_clarification"]
    if len(commits) < len(waiting):
        raise WaitTimingUnavailable("WAIT_COMMIT_POINT_MISSING")
    if len(commits) != len(waiting):
        raise ValueError("TIMING_WAIT_FACT_INVALID")
    result = []
    all_points = [ClockPoint.model_validate(origin["point"]), endpoint]
    all_points.extend(ClockPoint.model_validate(e["point"]) for e in events)
    for event, fact in zip(waiting, commits, strict=True):
        point = ClockPoint.model_validate(fact["point"])
        state = ClockPoint.model_validate(event["point"])
        if (
            str(fact["query_id"]) != str(event["query_id"])
            or fact["waiting_event_seq"] != event["seq"]
            or any(
                fact[k] != event[k]
                for k in ("status_version", "spec_version", "execution_generation")
            )
            or point.source != "wait_commit_return"
            or point.producer_id != state.producer_id
            or point.producer_pid != state.producer_pid
            or point.wall_at < state.wall_at
        ):
            raise ValueError("TIMING_WAIT_FACT_INVALID")
        end = next(
            (e for e in events if e["seq"] > event["seq"] and e["status"] != "needs_clarification"),
            None,
        )
        if end is None:
            raise WaitTimingUnavailable("WAIT_END_POINT_MISSING")
        accepted = ClockPoint.model_validate(end["point"])
        if accepted.source not in {"answer_accepted", "wait_closed"}:
            raise WaitTimingUnavailable("WAIT_END_POINT_MISSING")
        if point.wall_at > accepted.wall_at:
            raise WaitTimingUnavailable("WAIT_COMMIT_AFTER_END")
        _same_producer_clock(state, point)
        all_points.append(point)
        result.append((point.wall_at, accepted.wall_at))
    producers: dict[Any, ClockPoint] = {}
    for point in sorted(all_points, key=lambda p: (p.wall_at, p.monotonic_ns)):
        _same_producer_clock(producers.get(point.producer_id), point)
        producers[point.producer_id] = point
    return result


def summarize(
    origin: dict[str, Any] | None,
    events: list[dict[str, Any]],
    commits: list[dict[str, Any]],
    check: dict[str, Any] | None,
    row: dict[str, Any],
) -> dict[str, Any]:
    result = summarize_server_timing(origin, events, check, row)
    result.update(
        schema_version=2,
        timing_ns=None,
        measurement_kind="server_commit_boundaries_candidate_v2",
        wait_start_semantics="original_transaction_commit_return",
        wait_commit_tail_in_processing=False,
        human_wait_intervals=None,
        waiting_commit_points=None,
        definition=None,
        producer_count=None,
    )
    if result["unknown_reason"] is not None:
        return result
    try:
        if (
            not check
            or type(check.get("timing_version")) is not int
            or check["timing_version"] != 2
        ):
            raise ValueError("TIMING_VERSION_UNSUPPORTED")
        if check["waits_sha256"] != waits_digest(commits):
            raise ValueError("TIMING_WAIT_INPUT_CHANGED")
        if check["timing_reason_code"] is not None:
            result["unknown_reason"] = check["timing_reason_code"]
            return result
        if origin is None:
            raise ValueError("TIMING_ORIGIN_MISSING")
        endpoint = ClockPoint.model_validate(check["point"])
        waits = committed_waits(origin, events, commits, endpoint)
        first = ClockPoint.model_validate(origin["point"])
        result.update(
            timing_ns=elapsed_utc(first.wall_at, endpoint.wall_at, waits),
            definition=first.source + "_to_post_commit_readback_v2",
            wait_commit_tail_in_processing=True,
            human_wait_intervals=[
                dict(start_at=a.isoformat(), end_at=b.isoformat()) for a, b in waits
            ],
            waiting_commit_points=len(commits),
            producer_count=len(
                {
                    ClockPoint.model_validate(p["point"]).producer_id
                    for p in [origin, *events, *commits, check]
                }
            ),
        )
    except WaitTimingUnavailable as exc:
        result["unknown_reason"] = exc.code
    except (KeyError, ValueError, TypeError):
        result["unknown_reason"] = "TIMING_RECORD_INVALID"
    return result
