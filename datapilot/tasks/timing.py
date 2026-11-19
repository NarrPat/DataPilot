"""Atomic private clock journal and lease-fenced post-commit verification."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from datapilot.server_timing import (
    ClockPoint,
    capture_clock,
    input_digest,
    projection_digest,
    summarize_committed_timing,
    summarize_server_timing,
    validate_journal,
)
from datapilot.tasks.contracts import TERMINAL, Status
from datapilot.wait_timing import WaitTimingUnavailable, committed_waits, waits_digest

if TYPE_CHECKING:
    from datapilot.tasks.service import Connect, RunLease, TaskService

LOGGER = logging.getLogger("datapilot.server_timing")
DIAGNOSTIC_CODES = frozenset(
    {
        "UTC_MONOTONIC_DIVERGENCE",
        "SERVER_CLOCK_ORDER_UNKNOWN",
        "TIMING_SOURCE_INVALID",
        "TIMING_QUERY_IDENTITY_MISMATCH",
        "TIMING_EVENT_GAP",
        "TIMING_TERMINAL_MISMATCH",
        "TIMING_WAIT_FACT_INVALID",
        "DURABLE_PROJECTION_INVALID",
    }
)


def invalid_diagnostic(query_id: UUID, error: Exception) -> None:
    """Only fixed codes escape: exception text may contain credentials or state."""
    code = error.args[0] if len(error.args) == 1 else None
    if not isinstance(code, str) or code not in DIAGNOSTIC_CODES:
        code = "UNCLASSIFIED_TIMING_OR_PROJECTION_INVALID"
    try:
        LOGGER.warning(
            json.dumps(
                {
                    "event": "timing.invalid_diagnostic",
                    "query_id": str(query_id),
                    "code": code,
                    "changes_business_result": False,
                }
            )
        )
    except Exception:
        # A diagnostic sink must not prevent the original private check INSERT.
        pass


def record_origin(conn: Any, row: dict[str, Any], point: ClockPoint) -> None:
    if point.source not in {"api_receipt", "service_entry"}:
        raise ValueError("TIMING_ORIGIN_SOURCE_INVALID")
    conn.execute(
        "INSERT INTO app.query_timing_origins(query_id,accepted_request_id,point) VALUES(%s,%s,%s)",
        (row["query_id"], row["accepted_request_id"], Jsonb(point.model_dump(mode="json"))),
    )


def record_event(conn: Any, row: dict[str, Any], point: ClockPoint | None = None) -> None:
    point = point or capture_clock("state_projection")
    if point.source not in {"state_projection", "answer_accepted", "wait_closed"}:
        raise ValueError("TIMING_EVENT_SOURCE_INVALID")
    conn.execute(
        "INSERT INTO app.query_timing_events(query_id,seq,status_version,spec_version,"
        "execution_generation,status,point) VALUES(%s,%s,%s,%s,%s,%s,%s)",
        (
            row["query_id"],
            row["last_event_seq"],
            row["status_version"],
            row["spec_version"],
            row["execution_generation"],
            row["status"],
            Jsonb(point.model_dump(mode="json")),
        ),
    )


def _read(conn: Any, query_id: UUID) -> tuple[Any, list[dict[str, Any]], Any, Any]:
    row = conn.execute("SELECT * FROM app.query_runs WHERE query_id=%s", (query_id,)).fetchone()
    origin = conn.execute(
        "SELECT * FROM app.query_timing_origins WHERE query_id=%s", (query_id,)
    ).fetchone()
    events = conn.execute(
        "SELECT * FROM app.query_timing_events WHERE query_id=%s ORDER BY seq", (query_id,)
    ).fetchall()
    check = conn.execute(
        "SELECT * FROM app.query_timing_checks WHERE query_id=%s", (query_id,)
    ).fetchone()
    return origin, list(events), check, row


def read_server_timing(conn: Any, query_id: UUID) -> dict[str, Any]:
    from datapilot.phases import read_phase_breakdown

    origin, events, check, row = _read(conn, query_id)
    if row is None:
        raise ValueError("TIMING_QUERY_NOT_FOUND")
    if check and check.get("timing_version", 1) == 1:
        result = summarize_server_timing(origin, events, check, row)
    else:
        result = summarize_committed_timing(
            origin, events, _wait_commits(conn, query_id), check, row
        )
    try:
        # A savepoint keeps an optional telemetry read failure from poisoning
        # the caller's transaction or hiding the original timing verdict.
        with conn.transaction():
            result["phase_breakdown"] = read_phase_breakdown(conn, query_id)
    except (psycopg.Error, ValueError, KeyError, TypeError):
        result["phase_breakdown"] = {
            "schema_version": 1,
            "available": False,
            "unknown_reason": "PHASE_READ_UNAVAILABLE",
            "all_persistence_overhead_accounted": False,
            "formal_P95_eligible": False,
        }
    return result


def _wait_commits(conn: Any, query_id: UUID) -> list[dict[str, Any]]:
    return list(
        conn.execute(
            "SELECT * FROM app.query_wait_commits WHERE query_id=%s ORDER BY waiting_event_seq",
            (query_id,),
        ).fetchall()
    )


def record_wait_commit_after_commit(
    tasks: TaskService, lease: RunLease, row: dict[str, Any]
) -> None:
    try:
        if (
            lease.connection.info.transaction_status.name != "IDLE"
            or row["status"] != "needs_clarification"
        ):
            raise ValueError("ORIGINAL_OUTERMOST_WAIT_COMMIT_REQUIRED")
        point = capture_clock("wait_commit_return")
        lease.check()
        with lease.connection.transaction():
            lease.connection.execute(
                "INSERT INTO app.query_wait_commits(query_id,waiting_event_seq,status_version,"
                "spec_version,execution_generation,point) VALUES(%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(query_id,waiting_event_seq) DO NOTHING",
                (
                    row["query_id"],
                    row["last_event_seq"],
                    row["status_version"],
                    row["spec_version"],
                    row["execution_generation"],
                    Jsonb(point.model_dump(mode="json")),
                ),
            )
    except Exception:
        _deferred(lease.query_id, code="WAIT_COMMIT_RECORD_DEFERRED")


def _verify_projection(conn: Any, row: dict[str, Any]) -> None:
    if row["status"] != Status.COMPLETED.value:
        if row["result_snapshot"] is not None:
            raise ValueError("DURABLE_PROJECTION_INVALID")
        return
    from datapilot.artifacts import verify_binding
    from datapilot.contracts import ConfirmedSpec, QueryPlan, QueryResult
    from datapilot.worker.fragment import trusted_request

    saved = conn.execute(
        "SELECT * FROM app.query_attempt_results WHERE attempt_id=%s", (row["result_ref"],)
    ).fetchone()
    if saved is None:
        raise ValueError("DURABLE_PROJECTION_INVALID")
    actual = verify_binding(
        saved,
        row,
        trusted_request(row),
        ConfirmedSpec.model_validate(row["confirmed_spec"]),
        QueryPlan.model_validate(row["query_plan"]),
    )
    if actual != QueryResult.model_validate(row["result_snapshot"]):
        raise ValueError("DURABLE_PROJECTION_INVALID")


class ServerTimingVerifier:
    def __init__(self, tasks: TaskService, fixed_connect: Connect | None = None) -> None:
        self.tasks, self.fixed_connect = tasks, fixed_connect

    def verify(self, lease: RunLease) -> str | None:
        if lease.connection.info.transaction_status.name != "IDLE":
            raise ValueError("POST_COMMIT_CONNECTION_IDLE_REQUIRED")
        lease.check()
        conn = lease.connection
        with conn.transaction():
            row = conn.execute(
                "SELECT * FROM app.query_runs WHERE query_id=%s FOR SHARE", (lease.query_id,)
            ).fetchone()
            if not row or Status(row["status"]) not in TERMINAL:
                return None
            origin, events, check, _ = _read(conn, lease.query_id)
            if origin is None or check is not None:
                return None
            outcome: str = "verified"
            reason: str | None = None
            timing_reason: str | None = None
            commits = _wait_commits(conn, lease.query_id)
            point: ClockPoint | None = None
            try:
                _verify_projection(conn, row)
                point = capture_clock("post_commit_verification")
                validate_journal(origin, events, row, point)
                committed_waits(origin, events, commits, point)
            except WaitTimingUnavailable as exc:
                timing_reason = exc.code
            except (ValueError, KeyError, TypeError) as exc:
                outcome, reason = "invalid", "TIMING_OR_PROJECTION_INVALID"
                invalid_diagnostic(lease.query_id, exc)
            if point is None:
                # A projection failure can precede clock capture. Once captured,
                # keep that original point even when its clock check is invalid.
                point = capture_clock("post_commit_verification")
            inserted = conn.execute(
                "INSERT INTO app.query_timing_checks(query_id,last_event_seq,"
                "final_status,result_ref,outcome,reason_code,"
                "inputs_sha256,projection_sha256,point,timing_version,"
                "waits_sha256,timing_reason_code) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,2,%s,%s) ON CONFLICT(query_id) DO NOTHING "
                "RETURNING query_id",
                (
                    lease.query_id,
                    row["last_event_seq"],
                    row["status"],
                    row["result_ref"],
                    outcome,
                    reason,
                    input_digest(origin, events),
                    projection_digest(row),
                    Jsonb(point.model_dump(mode="json")),
                    waits_digest(commits),
                    timing_reason,
                ),
            ).fetchone()
            return outcome if inserted else None

    def sweep(self, *, limit: int = 100) -> dict[str, int]:
        if type(limit) is not int or not 1 <= limit <= 100 or self.fixed_connect is None:
            raise ValueError("BOUNDED_TIMING_SWEEP_CONNECTION_REQUIRED")
        from datapilot.tasks.service import run_lease

        result = dict(checked=0, verified=0, invalid=0, blocked=0, errors=0)
        with self.tasks.connect() as conn:
            rows = conn.execute(
                "SELECT q.query_id FROM app.query_runs q "
                "JOIN app.query_timing_origins o USING(query_id) "
                "WHERE q.status=ANY(%s) AND NOT EXISTS(SELECT 1 FROM app.query_timing_checks c "
                "WHERE c.query_id=q.query_id) ORDER BY o.db_recorded_at,q.query_id LIMIT %s",
                ([s.value for s in TERMINAL], limit),
            ).fetchall()
        for row in rows:
            try:
                with self.fixed_connect() as conn:
                    with run_lease(conn, row["query_id"]) as lease:
                        if lease is None:
                            result["blocked"] += 1
                            continue
                        outcome = self.verify(lease)
                        if outcome is not None:
                            result["checked"] += 1
                            result[outcome] += 1
            except (psycopg.Error, ValueError):
                result["errors"] += 1
                _deferred(row["query_id"])
        return result


def _deferred(query_id: UUID, *, code: str = "TIMING_VERIFY_DEFERRED") -> None:
    LOGGER.warning(
        json.dumps(
            {
                "event": "timing.verification",
                "code": code,
                "query_id": str(query_id),
            }
        )
    )


def verify_after_commit(tasks: TaskService, lease: RunLease) -> None:
    try:
        ServerTimingVerifier(tasks).verify(lease)
    except Exception:
        # A telemetry outage cannot alter an already committed business result.
        _deferred(lease.query_id)
