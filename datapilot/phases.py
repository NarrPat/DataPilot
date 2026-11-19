"""Private phase envelopes, separate from business budgets and qualified P95.

Start is committed before an operation. A killed process leaves an open span;
recovery never invents its end. Child spans are included in parent durations.
The next append records the previous append's observed cost, leaving an honest
unmeasured tail rather than recursively claiming to measure its own final write.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from threading import RLock
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from datapilot.server_timing import ClockPoint
from datapilot.tasks.contracts import Status

if TYPE_CHECKING:
    from datapilot.worker.fragment import FragmentStore

LOGGER = logging.getLogger("datapilot.phases")
MAX_GENERATION_EVENTS = 192
MAX_QUERY_EVENTS = 4096
SQL_COMPONENTS = ("pool_wait", "guard", "plan", "execute", "fetch_serialize")
PHASES = frozenset(
    {
        "worker_fragment",
        "sql_attempt",
        "sql_components",
        "persist_result",
        "persist_clarification",
        "persist_projection",
        "post_commit_verification",
    }
    | {
        "node:" + name
        for name in (
            "parse_spec",
            "clarify",
            "validate_spec",
            "link_schema",
            "generate_sql",
            "guard_plan",
            "execute_plan",
            "validate_result",
            "repair",
            "present",
        )
    }
    | {"model:" + name for name in ("parse", "parse_clarification", "generate", "repair")}
)


def _deferred(query_id: UUID, generation: int) -> None:
    try:
        LOGGER.warning(
            json.dumps(
                {
                    "event": "phase.record_deferred",
                    "query_id": str(query_id),
                    "execution_generation": generation,
                    "code": "PHASE_RECORD_UNAVAILABLE",
                }
            )
        )
    except Exception:
        pass


class PhaseJournal:
    def __init__(self, store: FragmentStore) -> None:
        self.store = store
        self.available = False
        self.root: UUID | None = None
        self.current: ContextVar[UUID | None] = ContextVar(
            "datapilot_phase_" + uuid4().hex, default=None
        )
        self._record_lock = RLock()
        self._previous_write: dict[str, Any] | None = None
        try:
            with store.serialized_transaction():
                row = store.lease.connection.execute(
                    "SELECT to_regclass('app.query_phase_events') AS name"
                ).fetchone()
                self.available = row is not None and row["name"] is not None
        except Exception:
            _deferred(store.lease.query_id, store.generation)

    def _write(
        self,
        span: UUID,
        parent: UUID | None,
        phase: str,
        boundary: str,
        point: ClockPoint,
        *,
        outcome: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if not self.available:
            return
        try:
            with self._record_lock:
                event_id = uuid4()
                payload = dict(details or {})
                if self._previous_write is not None:
                    payload["previous_append"] = self._previous_write
                if phase not in PHASES or len(json.dumps(payload).encode()) > 16384:
                    raise ValueError("PHASE_RECORD_INVALID")
                started = time.monotonic_ns()
                with self.store.serialized_transaction():
                    conn = self.store.lease.connection
                    row = conn.execute(
                        "SELECT accepted_request_id,execution_generation FROM app.query_runs "
                        "WHERE query_id=%s FOR SHARE",
                        (self.store.lease.query_id,),
                    ).fetchone()
                    if not row:
                        raise ValueError("PHASE_GENERATION_CHANGED")
                    if row["execution_generation"] != self.store.generation:
                        # A fast accepted clarification can advance the generation
                        # while this lease still closes its original measured call.
                        # Only finish an already durable span; never start old work
                        # or let recovery manufacture an end for a killed process.
                        start = None
                        if (
                            boundary == "finish"
                            and row["execution_generation"] > self.store.generation
                        ):
                            start = conn.execute(
                                "SELECT event_id FROM app.query_phase_events WHERE query_id=%s "
                                "AND execution_generation=%s AND span_id=%s AND phase=%s "
                                "AND parent_span_id IS NOT DISTINCT FROM %s AND boundary='start'",
                                (
                                    self.store.lease.query_id,
                                    self.store.generation,
                                    span,
                                    phase,
                                    parent,
                                ),
                            ).fetchone()
                        if start is None:
                            raise ValueError("PHASE_GENERATION_CHANGED")
                    counts = conn.execute(
                        "SELECT count(*) AS total,count(*) FILTER(WHERE execution_generation=%s) "
                        "AS generation FROM app.query_phase_events WHERE query_id=%s",
                        (self.store.generation, self.store.lease.query_id),
                    ).fetchone()
                    if (
                        not counts
                        or counts["total"] >= MAX_QUERY_EVENTS
                        or counts["generation"] >= MAX_GENERATION_EVENTS
                    ):
                        raise ValueError("PHASE_RECORD_LIMIT")
                    conn.execute(
                        "INSERT INTO app.query_phase_events(event_id,query_id,accepted_request_id,"
                        "execution_generation,span_id,parent_span_id,phase,boundary,"
                        "outcome,point,details) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            event_id,
                            self.store.lease.query_id,
                            row["accepted_request_id"],
                            self.store.generation,
                            span,
                            parent,
                            phase,
                            boundary,
                            outcome,
                            Jsonb(point.model_dump(mode="json")),
                            Jsonb(payload),
                        ),
                    )
                self._previous_write = {
                    "event_id": str(event_id),
                    "duration_ns": time.monotonic_ns() - started,
                    "definition": "store_mutex_lease_transaction_and_commit_return",
                }
        except Exception:
            # An optional observation never repeats a model/SQL operation.
            _deferred(self.store.lease.query_id, self.store.generation)

    @contextmanager
    def measure(self, phase: str) -> Iterator[None]:
        if not self.available or phase not in PHASES:
            yield
            return
        from datapilot.server_timing import capture_clock

        try:
            start = capture_clock("phase_start")
        except Exception:
            _deferred(self.store.lease.query_id, self.store.generation)
            yield
            return
        span = uuid4()
        parent = self.current.get() or self.root
        if phase == "worker_fragment":
            self.root = span
        token = self.current.set(span)
        self._write(span, parent, phase, "start", start)
        outcome = "returned"
        try:
            yield
        except BaseException:
            outcome = "raised"
            raise
        finally:
            try:
                self._write(
                    span, parent, phase, "finish", capture_clock("phase_finish"), outcome=outcome
                )
            except Exception:
                _deferred(self.store.lease.query_id, self.store.generation)
            finally:
                self.current.reset(token)

    def record_sql(self, record: dict[str, Any]) -> None:
        """Called only after the analyst transaction and pool return have ended."""
        if not self.available:
            return
        from datapilot.server_timing import capture_clock

        try:
            if str(record["source_query_id"]) != str(self.store.lease.query_id):
                raise ValueError("PHASE_QUERY_CHANGED")
            components = {key: record["phases_ns"][key] for key in SQL_COMPONENTS}
            numbers = [
                *components.values(),
                record["other_ns"],
                record["total_ns"],
                record["started_ns"],
                record["finished_ns"],
            ]
            if any(type(v) is not int or v < 0 for v in numbers):
                raise ValueError("PHASE_SQL_TIMING_INVALID")
            if (
                sum(components.values()) + record["other_ns"] != record["total_ns"]
                or record["finished_ns"] - record["started_ns"] != record["total_ns"]
                or record["status"] not in {"completed", "failed"}
            ):
                raise ValueError("PHASE_SQL_TIMING_INVALID")
            attempt = str(UUID(record["attempt_id"])) if record["attempt_id"] is not None else None
            self._write(
                uuid4(),
                self.current.get() or self.root,
                "sql_components",
                "summary",
                capture_clock("phase_finish"),
                details={
                    "components_ns": components,
                    "other_ns": record["other_ns"],
                    "total_ns": record["total_ns"],
                    "started_ns": record["started_ns"],
                    "finished_ns": record["finished_ns"],
                    "attempt_id": attempt,
                    "status": record["status"],
                    "definition": "one_executor_call_including_failed_attempts",
                },
            )
        except Exception:
            _deferred(self.store.lease.query_id, self.store.generation)


def _duration(start: dict[str, Any], finish: dict[str, Any]) -> int | None:
    first = ClockPoint.model_validate(start["point"])
    last = ClockPoint.model_validate(finish["point"])
    if (
        first.source != "phase_start"
        or last.source != "phase_finish"
        or first.producer_id != last.producer_id
        or first.producer_pid != last.producer_pid
        or first.clock_implementation != last.clock_implementation
        or first.monotonic_ns > last.monotonic_ns
    ):
        return None
    return last.monotonic_ns - first.monotonic_ns


def _queue_intervals(conn: Any, query_id: UUID) -> dict[str, Any]:
    """Existing durable state samples, not invented queue COMMIT-return times."""
    rows = list(
        conn.execute(
            "SELECT seq,status,point FROM app.query_timing_events WHERE query_id=%s "
            "ORDER BY seq LIMIT %s",
            (query_id, MAX_QUERY_EVENTS + 1),
        ).fetchall()
    )
    result: dict[str, Any] = {
        "definition": "queued_state_sample_to_first_exit_state_sample",
        "includes_acceptance_and_claim_commit_uncertainty": True,
        "clock_alignment_attested": False,
        "intervals": [],
        "unknown_reason": None,
    }
    waiting = None
    try:
        if len(rows) > MAX_QUERY_EVENTS or any(
            type(row["seq"]) is not int or row["seq"] != n for n, row in enumerate(rows, 1)
        ):
            raise ValueError("QUEUE_TIMING_EVENT_GAP")
        for row in rows:
            Status(row["status"])
            point = ClockPoint.model_validate(row["point"])
            if point.source not in {"state_projection", "answer_accepted", "wait_closed"}:
                raise ValueError("QUEUE_TIMING_SOURCE_INVALID")
            if row["status"] == "queued":
                if waiting is None:
                    waiting = (row["seq"], point)
            elif waiting is not None:
                seq, first = waiting
                delta = point.wall_at - first.wall_at
                candidate = (
                    delta.days * 86400_000000 + delta.seconds * 1000000 + delta.microseconds
                ) * 1000
                same = (
                    first.producer_id == point.producer_id
                    and first.producer_pid == point.producer_pid
                    and first.clock_implementation == point.clock_implementation
                    and point.monotonic_ns >= first.monotonic_ns
                )
                result["intervals"].append(
                    {
                        "start_seq": seq,
                        "end_seq": row["seq"],
                        "end_status": row["status"],
                        "duration_ns": point.monotonic_ns - first.monotonic_ns if same else None,
                        "utc_candidate_ns": candidate if candidate >= 0 else None,
                        "clock_kind": "single_producer_monotonic"
                        if same
                        else "unattested_cross_producer_UTC",
                        "unknown_reason": None if same else "CLOCK_ALIGNMENT_NOT_ATTESTED",
                    }
                )
                waiting = None
        if waiting is not None:
            result["intervals"].append(
                {
                    "start_seq": waiting[0],
                    "end_seq": None,
                    "duration_ns": None,
                    "utc_candidate_ns": None,
                    "unknown_reason": "QUEUE_STILL_OPEN",
                }
            )
        if not rows:
            result["unknown_reason"] = "QUEUE_TIMING_NOT_OBSERVED"
    except (ValueError, KeyError, TypeError):
        result.update(intervals=[], unknown_reason="QUEUE_TIMING_INVALID")
    return result


def read_phase_breakdown(conn: Any, query_id: UUID) -> dict[str, Any]:
    """Private bounded projection; no SQL text, model messages or raw clock export."""
    result: dict[str, Any] = {
        "schema_version": 1,
        "measurement_kind": "phase_envelopes_v1",
        "available": False,
        "unknown_reason": "PHASE_SCHEMA_NOT_INSTALLED",
        "spans": [],
        "by_phase": [],
        "sql_attempts": [],
        "observed_records": 0,
        "durations_are_inclusive": True,
        "sum_across_phases_is_processing_time": False,
        "all_persistence_overhead_accounted": False,
        "formal_P95_eligible": False,
        "coverage_gaps": [
            "resource_setup_before_claim",
            "dispatcher_broker_and_recovery_scheduler_not_separately_measured",
            "checkpoint_background_writes_only_inside_fragment_envelope",
            "append_failures_and_final_append_tail_not_fully_measured",
            "lease_release_and_resource_close_after_fragment",
            "missing_or_unobserved_phase_boundaries",
        ],
    }
    result["queue"] = _queue_intervals(conn, query_id)
    table = conn.execute("SELECT to_regclass('app.query_phase_events') AS name").fetchone()
    if not table or table["name"] is None:
        return result
    rows = list(
        conn.execute(
            "SELECT * FROM app.query_phase_events WHERE query_id=%s "
            "ORDER BY db_recorded_at,event_id LIMIT %s",
            (query_id, MAX_QUERY_EVENTS + 1),
        ).fetchall()
    )
    result["available"] = True
    result["observed_records"] = len(rows)
    if len(rows) > MAX_QUERY_EVENTS:
        result["unknown_reason"] = "PHASE_RECORD_LIMIT"
        return result
    if not rows:
        result["unknown_reason"] = "PHASE_NOT_OBSERVED"
        return result
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    acknowledged: dict[str, int] = {}
    event_ids = {str(row["event_id"]) for row in rows}
    totals: dict[str, dict[str, Any]] = {}
    try:
        for row in rows:
            if str(row["query_id"]) != str(query_id) or row["phase"] not in PHASES:
                raise ValueError("PHASE_RECORD_INVALID")
            observed_point = ClockPoint.model_validate(row["point"])
            if row["boundary"] not in {"start", "finish", "summary"}:
                raise ValueError("PHASE_RECORD_INVALID")
            previous = row["details"].get("previous_append")
            if previous is not None:
                identity, duration = str(UUID(previous["event_id"])), previous["duration_ns"]
                if (
                    identity not in event_ids
                    or identity == str(row["event_id"])
                    or type(duration) is not int
                    or duration < 0
                ):
                    raise ValueError("PHASE_RECORD_INVALID")
                if identity in acknowledged and acknowledged[identity] != duration:
                    raise ValueError("PHASE_RECORD_INVALID")
                acknowledged[identity] = duration
            if row["boundary"] == "summary":
                if row["phase"] != "sql_components":
                    raise ValueError("PHASE_RECORD_INVALID")
                values = row["details"]
                components = values["components_ns"]
                if (
                    observed_point.source != "phase_finish"
                    or set(components) != set(SQL_COMPONENTS)
                    or any(type(v) is not int or v < 0 for v in components.values())
                    or any(
                        type(values[k]) is not int or values[k] < 0
                        for k in ("other_ns", "total_ns", "started_ns", "finished_ns")
                    )
                    or sum(components.values()) + values["other_ns"] != values["total_ns"]
                    or values["finished_ns"] - values["started_ns"] != values["total_ns"]
                    or values["status"] not in {"completed", "failed"}
                ):
                    raise ValueError("PHASE_RECORD_INVALID")
                if values["attempt_id"] is not None:
                    UUID(values["attempt_id"])
                result["sql_attempts"].append(
                    {
                        "execution_generation": row["execution_generation"],
                        **{
                            k: row["details"][k]
                            for k in (
                                "attempt_id",
                                "status",
                                "components_ns",
                                "other_ns",
                                "total_ns",
                            )
                        },
                    }
                )
            else:
                grouped[str(row["span_id"])].append(row)
        for span, events in grouped.items():
            starts = [r for r in events if r["boundary"] == "start"]
            finishes = [r for r in events if r["boundary"] == "finish"]
            sample = events[0]
            if any(
                (r["phase"], r["execution_generation"], r["parent_span_id"])
                != (sample["phase"], sample["execution_generation"], sample["parent_span_id"])
                for r in events
            ):
                raise ValueError("PHASE_RECORD_INVALID")
            duration = (
                _duration(starts[0], finishes[0]) if len(starts) == len(finishes) == 1 else None
            )
            result["spans"].append(
                {
                    "span_id": span,
                    "parent_span_id": str(sample["parent_span_id"])
                    if sample["parent_span_id"]
                    else None,
                    "phase": sample["phase"],
                    "execution_generation": sample["execution_generation"],
                    "duration_ns": duration,
                    "outcome": finishes[0]["outcome"] if len(finishes) == 1 else None,
                    "unknown_reason": None
                    if duration is not None
                    else "PHASE_BOUNDARY_MISSING_OR_INVALID",
                }
            )
            total = totals.setdefault(
                sample["phase"],
                {
                    "phase": sample["phase"],
                    "observed_spans": 0,
                    "known_duration_ns": 0,
                    "unknown_spans": 0,
                },
            )
            total["observed_spans"] += 1
            if duration is None:
                total["unknown_spans"] += 1
            else:
                total["known_duration_ns"] += duration
        result["by_phase"] = [
            {
                **value,
                "duration_ns": value["known_duration_ns"] if not value["unknown_spans"] else None,
            }
            for _, value in sorted(totals.items())
        ]
        result["persistence"] = {
            "observed_append_count": len(rows),
            "measured_append_count": len(acknowledged),
            "known_append_duration_ns": sum(acknowledged.values()) if acknowledged else None,
            "unmeasured_append_count": len(rows) - len(acknowledged),
            "definition": "append cost is included in enclosing spans; do not add it again",
            "tail_not_self_measured": True,
        }
        result["unknown_reason"] = (
            "PHASE_BOUNDARIES_INCOMPLETE"
            if any(r["duration_ns"] is None for r in result["spans"])
            else None
        )
    except (ValueError, KeyError, TypeError):
        result.update(unknown_reason="PHASE_RECORD_INVALID", spans=[], by_phase=[], sql_attempts=[])
    return result
