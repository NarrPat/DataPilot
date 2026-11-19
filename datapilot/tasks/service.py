"""Atomic app facts: authentication precedes deduplication; DB owns admission.

No model, scheduler or test fixture is imported. Worker primitives require a
fixed advisory-lock connection and generation; production dispatch/recovery is
deliberately not supplied by this foundation slice.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb

from datapilot.auth.errors import AppError
from datapilot.auth.models import Principal, principal_from_user
from datapilot.config_versions import MODEL_CONFIG_VERSION
from datapilot.server_timing import ClockPoint, capture_clock
from datapilot.tasks.contracts import (
    NEXT,
    TERMINAL,
    CancelInput,
    QueryInput,
    Status,
    payload_hash,
    processing_elapsed_ms,
    validate_key,
)
from datapilot.tasks.followup import validated_base
from datapilot.tasks.presentation import chart_spec
from datapilot.tasks.timing import record_event, record_origin

Connect = Callable[[], AbstractContextManager[psycopg.Connection[dict[str, Any]]]]


def _required(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        raise AppError("APP_STATE_UNAVAILABLE", 503, retryable=True)
    return dict(row)


@dataclass
class RunLease:
    connection: psycopg.Connection[dict[str, Any]]
    query_id: UUID
    backend_pid: int
    active: bool = True

    def check(self) -> None:
        if (
            not self.active
            or self.connection.closed
            or self.connection.info.backend_pid != self.backend_pid
        ):
            raise AppError("EXECUTION_LOCK_LOST", 409)
        self.connection.execute("SELECT 1")


@contextmanager
def run_lease(
    connection: psycopg.Connection[dict[str, Any]], query_id: UUID
) -> Iterator[RunLease | None]:
    if not connection.autocommit or connection.info.user != "app_runtime":
        raise ValueError("FIXED_AUTOCOMMIT_APP_RUNTIME_CONNECTION_REQUIRED")
    key = int.from_bytes(
        hashlib.sha256(("datapilot:" + str(query_id)).encode()).digest()[:8], "big", signed=True
    )
    row = connection.execute("SELECT pg_try_advisory_lock(%s) AS acquired", (key,)).fetchone()
    if not row or not row["acquired"]:
        yield None
        return
    lease = RunLease(connection, query_id, connection.info.backend_pid)
    try:
        yield lease
    finally:
        lease.active = False
        if not connection.closed:
            connection.execute("SELECT pg_advisory_unlock(%s)", (key,))


class TaskService:
    def __init__(
        self,
        connect: Connect,
        *,
        catalog_version: str = "dp-catalog-v2-five",
        data_version: str = "tiny_v1",
        graph_version: str = "not_connected_i006",
        model_config_version: str = MODEL_CONFIG_VERSION,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.connect, self.catalog_version, self.data_version, self.clock = (
            connect,
            catalog_version,
            data_version,
            clock,
        )
        self.graph_version = graph_version
        if not isinstance(model_config_version, str) or not 1 <= len(model_config_version) <= 160:
            raise ValueError("MODEL_CONFIGURATION_VERSION_REQUIRED")
        self.model_config_version = model_config_version

    def now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("AWARE_TASK_CLOCK_REQUIRED")
        return now

    def _actor(self, conn: Any, principal: Principal, capability: str) -> Principal:
        row = conn.execute(
            (
                "SELECT user_id,username,role,authz_version,is_active FROM ap"
                "p.users WHERE user_id=%s"
            ),
            (principal.user_id,),
        ).fetchone()
        if not row or not row["is_active"]:
            raise AppError("AUTH_REQUIRED", 401)
        current = principal_from_user(row)
        if (
            current.authz_version != principal.authz_version
            or capability not in current.capabilities
        ):
            raise AppError("FORBIDDEN", 403)
        return current

    def _owned(
        self, conn: Any, principal: Principal, query_id: UUID, *, lock: bool = False
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM app.query_runs WHERE query_id=%s" + (" FOR UPDATE" if lock else ""),
            (query_id,),
        ).fetchone()
        if not row or str(row["owner_user_id"]) != principal.user_id:
            raise AppError("NOT_FOUND", 404)
        if principal.scope is None or (
            principal.scope.value != "all" and row["effective_scope"] != principal.scope.value
        ):
            raise AppError("FORBIDDEN_SCOPE", 403)
        return dict(row)

    def current_data_version(self, connection: Any | None = None) -> str:
        if connection is None:
            with self.connect() as current:
                return self.current_data_version(current)
        row = _required(
            connection.execute(
                "SELECT published_data_version FROM app.runtime_limits WHERE id=1"
            ).fetchone()
        )
        return str(row["published_data_version"] or self.data_version)

    def snapshot(self, row: dict[str, Any], *, connection: Any | None = None) -> dict[str, Any]:
        names = (
            "query_id",
            "thread_id",
            "question",
            "base_query_id",
            "base_spec_version",
            "api_received_at",
            "finished_at",
            "status",
            "status_version",
            "spec_version",
            "effective_scope",
            "execution_generation",
            "cancel_requested",
            "last_event_seq",
            "catalog_version",
            "data_version",
            "reason_code",
        )
        result = {
            name: row[name].isoformat()
            if isinstance(row[name], datetime)
            else str(row[name])
            if isinstance(row[name], UUID)
            else row[name]
            for name in names
        }
        result.update(
            reference_time=(
                row.get("processing_reference_time") or row["api_received_at"]
            ).isoformat(),
            current_version_matches=row["catalog_version"] == self.catalog_version
            and row["data_version"] == self.current_data_version(connection),
            spec=row.get("confirmed_spec"),
            query_plan=row.get("query_plan"),
            guard_summary=row.get("guard_summary"),
            result=row.get("result_snapshot") if row["status"] == "completed" else None,
            result_ref=str(row["result_ref"])
            if row["status"] == "completed" and row.get("result_ref")
            else None,
            chart_spec=chart_spec(row),
            clarification=row.get("clarification_snapshot"),
        )
        result["events_url"] = f"/api/v1/queries/{row['query_id']}/events"
        return result

    def _event(
        self,
        conn: Any,
        row: dict[str, Any],
        event_type: str = "query.updated",
        *,
        timing_point: ClockPoint | None = None,
    ) -> None:
        seq = row["last_event_seq"] + 1
        payload = {
            "status": row["status"],
            "reason_code": row["reason_code"],
            "cancel_requested": row["cancel_requested"],
        }
        conn.execute(
            (
                "INSERT INTO app.query_events(query_id,seq,event_type,status_version,"
                "spec_version,occurred_at,payload) VALUES(%s,%s,%s,%s,%s,%s,%s)"
            ),
            (
                row["query_id"],
                seq,
                event_type,
                row["status_version"],
                row["spec_version"],
                self.now(),
                Jsonb(payload),
            ),
        )
        conn.execute(
            "UPDATE app.query_runs SET last_event_seq=%s WHERE query_id=%s",
            (seq, row[("query_id")]),
        )
        row["last_event_seq"] = seq
        record_event(conn, row, timing_point)

    def _key(
        self, conn: Any, actor: Principal, method: str, resource: str, key: str, digest: str
    ) -> dict[str, Any] | None:
        row = conn.execute(
            (
                "SELECT payload_hash,query_id FROM app.request_keys WHERE actor_id=%s AND "
                "method=%s AND resource=%s AND request_key=%s"
            ),
            (actor.user_id, method, resource, key),
        ).fetchone()
        if row is None:
            return None
        # The referenced query must still be authorized on every replay.
        query = self._owned(conn, actor, row["query_id"])
        if row["payload_hash"] != digest:
            raise AppError("IDEMPOTENCY_CONFLICT", 409)
        return query

    @staticmethod
    def _remember(
        conn: Any, actor: Principal, resource: str, key: str, digest: str, query_id: UUID
    ) -> None:
        conn.execute(
            (
                "INSERT INTO app.request_keys(actor_id,method,resource,request_key,"
                "payload_hash,query_id) VALUES(%s,'POST',%s,%s,%s,%s)"
            ),
            (actor.user_id, resource, key, digest, query_id),
        )

    def create(
        self,
        actor: Principal,
        command: QueryInput,
        key: str | None,
        *,
        received_at: datetime | None = None,
        request_id: UUID | None = None,
        received_clock: ClockPoint | None = None,
    ) -> dict[str, Any]:
        timing_origin = received_clock or capture_clock("service_entry")
        if received_clock is not None and received_clock.source != "api_receipt":
            raise ValueError("API_RECEIPT_POINT_REQUIRED")
        key, digest, now = validate_key(key), payload_hash(command), self.now()
        with self.connect() as conn:
            current = self._actor(conn, actor, "queries:create")
            if current.scope is None:
                raise AppError("FORBIDDEN", 403)
            if command.thread_id:
                thread = conn.execute(
                    "SELECT owner_user_id FROM app.analysis_threads WHERE thread_id=%s",
                    (command.thread_id,),
                ).fetchone()
                if not thread or str(thread["owner_user_id"]) != current.user_id:
                    raise AppError("NOT_FOUND", 404)
            base = (
                self._owned(conn, current, command.base_query_id) if command.base_query_id else None
            )
            # Global row serializes admission AND same-key creation across API processes.
            limits = conn.execute(
                "SELECT * FROM app.runtime_limits WHERE id=1 FOR UPDATE"
            ).fetchone()
            previous = self._key(conn, current, "POST", "/api/v1/queries", key, digest)
            if previous is not None:
                return self.snapshot(previous, connection=conn)
            if limits is None:
                raise AppError("APP_STATE_UNAVAILABLE", 503, retryable=True)
            if limits["maintenance"]:
                raise AppError("SERVICE_MAINTENANCE", 503, retryable=True)
            data_version = limits["published_data_version"] or self.data_version
            if base and (
                base["spec_version"] != command.base_spec_version or base["spec_version"] < 1
            ):
                raise AppError("SPEC_VERSION_CONFLICT", 409)
            if base and command.thread_id and base["thread_id"] != command.thread_id:
                raise AppError("BASE_THREAD_CONFLICT", 409)
            base_snapshot = (
                validated_base(base, self.catalog_version, data_version) if base else None
            )
            if limits is None or limits["logical_queued"] >= 10:
                raise AppError("QUEUE_FULL", 429, retryable=True)
            thread_id = command.thread_id or (base["thread_id"] if base else uuid4())
            if command.thread_id is None and base is None:
                conn.execute(
                    "INSERT INTO app.analysis_threads VALUES(%s,%s,%s)",
                    (thread_id, current.user_id, now),
                )
            query_id = uuid4()
            row = conn.execute(
                (
                    "INSERT INTO app.query_runs(query_id,thread_id,owner_user_id,question,"
                    "base_query_id,base_spec_version,effective_scope,authz_version,status,"
                    "admission_state,api_received_at,catalog_version,data_version,"
                    "graph_version,model_config_version,base_confirmed_snapshot,"
                    "accepted_request_id) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'queued',"
                    "'queued',%s,%s,%s,%s,%s,%s,%s) RETURNING *"
                ),
                (
                    query_id,
                    thread_id,
                    current.user_id,
                    command.question,
                    command.base_query_id,
                    command.base_spec_version,
                    current.scope.value,
                    current.authz_version,
                    received_at or now,
                    self.catalog_version,
                    data_version,
                    self.graph_version,
                    self.model_config_version,
                    Jsonb(base_snapshot.model_dump(mode="json")) if base_snapshot else None,
                    request_id,
                ),
            ).fetchone()
            record_origin(conn, _required(row), timing_origin)
            conn.execute("UPDATE app.runtime_limits SET logical_queued=logical_queued+1 WHERE id=1")
            conn.execute(
                (
                    "INSERT INTO app.query_outbox(outbox_id,query_id,execution_generation,kind,"
                    "created_at) VALUES(%s,%s,1,'create',%s)"
                ),
                (uuid4(), query_id, now),
            )
            self._remember(conn, current, "/api/v1/queries", key, digest, query_id)
            row = _required(row)
            self._event(conn, row)
            return self.snapshot(row, connection=conn)

    def get(self, actor: Principal, query_id: UUID) -> dict[str, Any]:
        with self.connect() as conn:
            actor = self._actor(conn, actor, "queries:read_self")
            return self.snapshot(self._owned(conn, actor, query_id), connection=conn)

    def history(self, actor: Principal, *, limit: int = 50) -> list[dict[str, Any]]:
        if not 1 <= limit <= 50:
            raise AppError("LIMIT_INVALID", 422)
        with self.connect() as conn:
            current = self._actor(conn, actor, "history:read_self")
            if current.scope is None:
                raise AppError("FORBIDDEN", 403)
            rows = conn.execute(
                (
                    "SELECT * FROM app.query_runs WHERE owner_user_id=%s AND "
                    "(effective_scope=%s OR %s='all') ORDER BY api_received_at DESC,query_id "
                    "DESC LIMIT %s"
                ),
                (current.user_id, current.scope.value, current.scope.value, limit),
            ).fetchall()
            return [self.snapshot(dict(row), connection=conn) for row in rows]

    def cancel(
        self, actor: Principal, query_id: UUID, command: CancelInput, key: str | None
    ) -> tuple[int, dict[str, Any]]:
        key, digest = validate_key(key), payload_hash(command)
        resource = f"/api/v1/queries/{query_id}/cancel"
        with self.connect() as conn:
            current = self._actor(conn, actor, "queries:cancel_self")
            self._owned(conn, current, query_id)
            conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
            row = self._owned(conn, current, query_id, lock=True)
            previous = self._key(conn, current, "POST", resource, key, digest)
            if previous is None:
                if Status(row["status"]) not in TERMINAL:
                    if row["status_version"] != command.expected_status_version:
                        raise AppError("STATUS_VERSION_CONFLICT", 409)
                    if row["admission_state"] == "active":
                        if not row["cancel_requested"]:
                            row = _required(
                                conn.execute(
                                    (
                                        "UPDATE app.query_runs SET cancel_requested=true,"
                                        "status_version=status_version+1 "
                                        "WHERE query_id=%s RETURNING *"
                                    ),
                                    (query_id,),
                                ).fetchone()
                            )
                            self._event(conn, row)
                    else:
                        row = self._close(conn, row, Status.CANCELED, "CANCELED_BY_USER")
                self._remember(conn, current, resource, key, digest, query_id)
            else:
                row = previous
            return (202 if row["admission_state"] == "active" else 200), self.snapshot(
                row, connection=conn
            )

    def _close(
        self,
        conn: Any,
        row: dict[str, Any],
        status: Status,
        reason: str,
        *,
        timing_point: ClockPoint | None = None,
    ) -> dict[str, Any]:
        if row["status"] == Status.WAITING.value and timing_point is None:
            timing_point = capture_clock("wait_closed")
        if row.get("clarification_ref") is not None:
            clarification_status = (
                "expired"
                if reason == "CLARIFICATION_EXPIRED"
                else "incompatible"
                if reason
                in {"CONFIG_VERSION_UNSUPPORTED", "CONFIG_INCOMPATIBLE", "DATA_VERSION_CHANGED"}
                else "canceled"
            )
            conn.execute(
                "UPDATE app.query_clarifications SET status=%s WHERE clarification_id=%s "
                "AND status IN ('prepared','waiting')",
                (clarification_status, row["clarification_ref"]),
            )
        if row.get("waiting_since") is not None:
            intervals = [
                *row["waiting_intervals"],
                {
                    "start_at": row["waiting_since"].isoformat(),
                    "end_at": self.now().isoformat(),
                },
            ]
            conn.execute(
                "UPDATE app.query_runs SET waiting_intervals=%s,waiting_since=NULL,"
                "wait_expires_at=NULL WHERE query_id=%s",
                (Jsonb(intervals), row["query_id"]),
            )
        if row["admission_state"] == "queued":
            conn.execute("UPDATE app.runtime_limits SET logical_queued=logical_queued-1 WHERE id=1")
        elif row["admission_state"] == "active":
            conn.execute(
                "UPDATE app.runtime_limits SET active_reserved=active_reserved-1 WHERE id=1"
            )
        row = _required(
            conn.execute(
                (
                    "UPDATE app.query_runs SET status=%s,status_version=status_version+1,"
                    "admission_state='none',recovery_pending=false,reason_code=%s,finished_at=%s "
                    "WHERE query_id=%s "
                    "RETURNING *"
                ),
                (status.value, reason, self.now(), row["query_id"]),
            ).fetchone()
        )
        conn.execute(
            (
                "UPDATE app.query_outbox SET canceled_at=%s WHERE query_id=%s"
                " AND published_at IS NULL AND canceled_at IS NULL"
            ),
            (self.now(), row["query_id"]),
        )
        self._event(
            conn,
            row,
            "query.canceled" if status == Status.CANCELED else "query.failed",
            timing_point=timing_point,
        )
        return row

    def claim(self, lease: RunLease, generation: int) -> dict[str, Any] | None:
        lease.check()
        with lease.connection.transaction():
            conn = lease.connection
            limits = conn.execute(
                "SELECT * FROM app.runtime_limits WHERE id=1 FOR UPDATE"
            ).fetchone()
            row = conn.execute(
                "SELECT * FROM app.query_runs WHERE query_id=%s FOR UPDATE", (lease.query_id,)
            ).fetchone()
            if not row or row["execution_generation"] != generation or row["status"] != "queued":
                return None
            row = dict(row)
            if limits is None or limits["maintenance"]:
                return None
            if row["data_version"] != (limits["published_data_version"] or self.data_version):
                self._close(conn, row, Status.NEEDS_REFINE, "DATA_VERSION_CHANGED")
                return None
            if processing_elapsed_ms(row, self.now()) >= row["budget_ms"]:
                self._close(conn, row, Status.TIMED_OUT, "PROCESSING_DEADLINE")
                return None
            owner = conn.execute(
                "SELECT * FROM app.users WHERE user_id=%s", (row["owner_user_id"],)
            ).fetchone()
            owner_scope = principal_from_user(owner).scope if owner and owner["is_active"] else None
            if owner_scope is None or (
                owner_scope.value != "all" and row["effective_scope"] != owner_scope.value
            ):
                self._close(conn, row, Status.REJECTED, "AUTHORIZATION_REVOKED")
                return None
            if limits is None or limits["active_reserved"] >= 2:
                # A delivery is not an execution permit. Yield this worker so
                # priority recovery messages can take over their existing slot.
                conn.execute(
                    "UPDATE app.query_outbox SET next_attempt_at=%s,"
                    "last_dispatch_code='ACTIVE_LIMIT' WHERE query_id=%s "
                    "AND execution_generation=%s AND claimed_at IS NULL "
                    "AND canceled_at IS NULL",
                    (self.now() + timedelta(seconds=1), lease.query_id, generation),
                )
                return None
            conn.execute(
                "UPDATE app.runtime_limits SET logical_queued=logical_queued-"
                "1,active_reserved=active_reserved+1 WHERE id=1"
            )
            row = _required(
                conn.execute(
                    (
                        "UPDATE app.query_runs SET status='parsing',"
                        "status_version=status_version+1,admission_state='active',"
                        "claimed_generation=%s,claimed_at=%s,recovery_pending=false WHERE "
                        "query_id=%s RETURNING *"
                    ),
                    (generation, self.now(), lease.query_id),
                ).fetchone()
            )
            conn.execute(
                (
                    "UPDATE app.query_outbox SET claimed_at=%s WHERE query_id=%s "
                    "AND execution_generation=%s AND kind IN ('create','resume')"
                ),
                (self.now(), lease.query_id, generation),
            )
            self._event(conn, row)
            return self.snapshot(row, connection=conn)

    def claim_recovery(self, lease: RunLease, generation: int) -> dict[str, Any] | None:
        """Claim a pending recovery without acquiring or releasing any permit."""
        lease.check()
        with lease.connection.transaction():
            conn = lease.connection
            conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
            row = conn.execute(
                "SELECT * FROM app.query_runs WHERE query_id=%s FOR UPDATE", (lease.query_id,)
            ).fetchone()
            if (
                not row
                or row["execution_generation"] != generation
                or row["admission_state"] != "active"
                or Status(row["status"]) in TERMINAL | {Status.WAITING, Status.QUEUED}
                or not row["recovery_pending"]
                or row["claimed_generation"] == generation
            ):
                return None
            outbox = conn.execute(
                "SELECT outbox_id FROM app.query_outbox WHERE query_id=%s "
                "AND execution_generation=%s AND kind='recover_fragment' "
                "AND claimed_at IS NULL AND canceled_at IS NULL FOR UPDATE",
                (lease.query_id, generation),
            ).fetchone()
            if outbox is None:
                return None
            # This fact, rather than publish/sent, lets the scanner distinguish
            # a lost delivery from a second genuinely claimed fragment crash.
            claimed = _required(
                conn.execute(
                    "UPDATE app.query_runs SET claimed_generation=%s,claimed_at=%s,"
                    "recovery_pending=false WHERE query_id=%s RETURNING *",
                    (generation, self.now(), lease.query_id),
                ).fetchone()
            )
            conn.execute(
                "UPDATE app.query_outbox SET claimed_at=%s WHERE outbox_id=%s",
                (self.now(), outbox["outbox_id"]),
            )
            return self.snapshot(claimed, connection=conn)

    def transition(
        self, lease: RunLease, generation: int, expected_version: int, target: Status
    ) -> dict[str, Any]:
        lease.check()
        with lease.connection.transaction():
            conn = lease.connection
            conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
            row = conn.execute(
                "SELECT * FROM app.query_runs WHERE query_id=%s FOR UPDATE", (lease.query_id,)
            ).fetchone()
            if (
                not row
                or row["execution_generation"] != generation
                or row["status_version"] != expected_version
                or row["admission_state"] != "active"
            ):
                raise AppError("EXECUTION_VERSION_CONFLICT", 409)
            row = dict(row)
            owner = conn.execute(
                "SELECT * FROM app.users WHERE user_id=%s", (row["owner_user_id"],)
            ).fetchone()
            owner_scope = principal_from_user(owner).scope if owner and owner["is_active"] else None
            if owner_scope is None or (
                owner_scope.value != "all" and row["effective_scope"] != owner_scope.value
            ):
                return self.snapshot(
                    self._close(conn, row, Status.REJECTED, "AUTHORIZATION_REVOKED"),
                    connection=conn,
                )
            deadline_triggered = processing_elapsed_ms(row, self.now()) >= row["budget_ms"]
            if row["cancel_requested"]:
                target = Status.CANCELED
            elif deadline_triggered:
                target = Status.TIMED_OUT
            if target not in NEXT[Status(row["status"])]:
                raise AppError("STATE_CONFLICT", 409)
            if target in {Status.WAITING, Status.QUEUED, Status.COMPLETED}:
                # These require the later graph/checkpoint/result adapter.
                raise AppError("GRAPH_ADAPTER_NOT_READY", 503)
            if target in TERMINAL:
                row = self._close(
                    conn,
                    row,
                    target,
                    "CANCELED_BY_USER"
                    if target == Status.CANCELED
                    else "PROCESSING_DEADLINE"
                    if deadline_triggered
                    else "WORKER_TERMINAL",
                )
            else:
                row = _required(
                    conn.execute(
                        (
                            "UPDATE app.query_runs SET status=%s,status_version=status_ve"
                            "rsion+1 WHERE query_id=%s RETURNING *"
                        ),
                        (target.value, lease.query_id),
                    ).fetchone()
                )
                self._event(conn, row)
            return self.snapshot(row, connection=conn)

    def expire_queued(self) -> int:
        count = 0
        with self.connect() as conn:
            conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
            rows = conn.execute(
                "SELECT * FROM app.query_runs WHERE admission_state='queued' "
                "ORDER BY api_received_at,query_id FOR UPDATE"
            ).fetchall()
            for row in rows:
                if processing_elapsed_ms(row, self.now()) >= row["budget_ms"]:
                    self._close(conn, dict(row), Status.TIMED_OUT, "PROCESSING_DEADLINE")
                    count += 1
        return count

    def events(self, actor: Principal, query_id: UUID, after: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            actor = self._actor(conn, actor, "queries:read_self")
            row = self._owned(conn, actor, query_id)
            if after > row["last_event_seq"]:
                raise AppError("CURSOR_AHEAD", 409)
            if after < row["min_event_seq"] - 1:
                raise AppError(
                    "CURSOR_EXPIRED", 410, details={"reset_url": f"/api/v1/queries/{query_id}"}
                )
            events = conn.execute(
                (
                    "SELECT * FROM app.query_events WHERE query_id=%s AND seq>%s "
                    "ORDER BY seq LIMIT 100"
                ),
                (query_id, after),
            ).fetchall()
            return [
                {
                    "schema_version": "1",
                    "app": "datapilot",
                    "query_id": str(query_id),
                    "seq": event["seq"],
                    "type": event["event_type"],
                    "status_version": event["status_version"],
                    "spec_version": event["spec_version"],
                    "occurred_at": event["occurred_at"].isoformat(),
                    "payload": event["payload"],
                }
                for event in events
            ]
