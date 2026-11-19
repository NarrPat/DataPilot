"""One stable metric clarification and its atomic wait/resume transitions.

Worker callbacks must serialize this service with their PostgresSaver mutex.
The fixed run lease still supplies cross-process ownership. Public answer
commands never accept checkpoint state, scope, SQL or runtime configuration.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb
from pydantic import Field, field_validator, model_validator

from datapilot.auth.errors import AppError
from datapilot.auth.models import Principal, principal_from_user
from datapilot.contracts import ClosedModel, MetricId, Scope
from datapilot.server_timing import capture_clock
from datapilot.tasks.contracts import (
    TERMINAL,
    Status,
    payload_hash,
    processing_elapsed_ms,
    validate_key,
)
from datapilot.tasks.service import RunLease, TaskService, _required

WAIT_TTL = timedelta(minutes=15)


class ClarificationPayload(ClosedModel):
    clarification_id: UUID
    spec_version: Annotated[int, Field(strict=True, ge=1, le=1)]
    missing_fields: tuple[Literal["metric_id"], ...] = Field(min_length=1, max_length=1)
    question: Annotated[str, Field(strict=True, min_length=1, max_length=800)]
    options: tuple[MetricId, ...] = Field(min_length=2, max_length=5)

    @field_validator("question")
    @classmethod
    def question_text(cls, value: str) -> str:
        value.encode("utf8")
        if not value.strip() or "\x00" in value:
            raise ValueError("CLARIFICATION_QUESTION_REQUIRED")
        return value.strip()

    @model_validator(mode="after")
    def distinct_options(self) -> Self:
        if len(set(self.options)) != len(self.options):
            raise ValueError("CLARIFICATION_OPTIONS_MUST_BE_UNIQUE")
        return self


class ClarificationAnswer(ClosedModel):
    metric_id: MetricId


class ClarifyInput(ClosedModel):
    clarification_id: UUID
    expected_spec_version: Annotated[int, Field(strict=True, ge=1)]
    answers: ClarificationAnswer


class WaitingCheckpoint(ClosedModel):
    """An internal proof read by the graph adapter from a saved interrupt.

    The projection additionally verifies this is the latest physical checkpoint
    and has an interrupt pending write; a fabricated ID cannot create waiting.
    """

    query_id: UUID
    checkpoint_id: Annotated[str, Field(strict=True, min_length=1, max_length=200)]
    graph_version: Annotated[str, Field(strict=True, min_length=1, max_length=80)]
    catalog_version: Annotated[str, Field(strict=True, min_length=1, max_length=80)]
    data_version: Annotated[str, Field(strict=True, min_length=1, max_length=80)]
    clarification: ClarificationPayload
    model_call_count: Annotated[int, Field(strict=True, ge=0, le=4)]
    repair_count: Annotated[int, Field(strict=True, ge=0, le=1)]
    graph_step_count: Annotated[int, Field(strict=True, ge=0, le=24)]


class ClarificationService:
    def __init__(self, tasks: TaskService) -> None:
        self.tasks = tasks

    def _worker_row(self, lease: RunLease, generation: int) -> dict[str, Any]:
        conn = lease.connection
        conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
        row = conn.execute(
            "SELECT * FROM app.query_runs WHERE query_id=%s FOR UPDATE", (lease.query_id,)
        ).fetchone()
        if not row or row["execution_generation"] != generation:
            raise AppError("EXECUTION_VERSION_CONFLICT", 409)
        owner = conn.execute(
            "SELECT * FROM app.users WHERE user_id=%s", (row["owner_user_id"],)
        ).fetchone()
        actor = principal_from_user(owner) if owner and owner["is_active"] else None
        if (
            not actor
            or actor.scope is None
            or actor.authz_version != row["authz_version"]
            or "queries:clarify_self" not in actor.capabilities
            or (actor.scope != Scope.ALL and actor.scope.value != row["effective_scope"])
        ):
            raise AppError("AUTHORIZATION_REVOKED", 403)
        return dict(row)

    def _compatible(
        self,
        row: dict[str, Any],
        other: dict[str, Any] | None = None,
        *,
        connection: Any | None = None,
    ) -> bool:
        expected = {
            "graph_version": self.tasks.graph_version,
            "catalog_version": self.tasks.catalog_version,
            "data_version": self.tasks.current_data_version(connection),
        }
        return all(
            row[key] == value and (other is None or other[key] == value)
            for key, value in expected.items()
        )

    def prepare(self, lease: RunLease, generation: int, source: object) -> dict[str, Any]:
        data = ClarificationPayload.model_validate(source)
        body = data.model_dump(mode="json")
        lease.check()
        with lease.connection.transaction():
            conn = lease.connection
            row = self._worker_row(lease, generation)
            if row["admission_state"] != "active" or row["status"] != "parsing":
                raise AppError("CLARIFICATION_STATE_CONFLICT", 409)
            if row["cancel_requested"]:
                raise AppError("CANCELED_BY_USER", 409)
            if processing_elapsed_ms(row, self.tasks.now()) >= row["budget_ms"]:
                raise AppError("PROCESSING_TIMEOUT", 409)
            if not self._compatible(row, connection=conn):
                raise AppError("CONFIG_VERSION_UNSUPPORTED", 409)
            existing = conn.execute(
                "SELECT * FROM app.query_clarifications WHERE query_id=%s FOR UPDATE",
                (lease.query_id,),
            ).fetchone()
            if existing:
                if existing["payload"] != body:
                    raise AppError("CLARIFICATION_VERSION_CONFLICT", 409)
                # interrupt nodes re-enter after accepted answer. The stable
                # input remains the same; do not reset its answer, TTL or wait.
                return dict(existing["payload"])
            if row["spec_version"] not in {0, data.spec_version}:
                raise AppError("SPEC_VERSION_CONFLICT", 409)
            conn.execute(
                "INSERT INTO app.query_clarifications"
                "(clarification_id,query_id,spec_version,payload,graph_version,"
                "catalog_version,data_version,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    data.clarification_id,
                    lease.query_id,
                    data.spec_version,
                    Jsonb(body),
                    row["graph_version"],
                    row["catalog_version"],
                    row["data_version"],
                    self.tasks.now(),
                ),
            )
            row = _required(
                conn.execute(
                    "UPDATE app.query_runs SET clarification_ref=%s,"
                    "spec_version=%s,status_version=status_version+1 WHERE query_id=%s RETURNING *",
                    (data.clarification_id, data.spec_version, lease.query_id),
                ).fetchone()
            )
            self.tasks._event(conn, row)
            return body

    def project_waiting(
        self, lease: RunLease, generation: int, source: WaitingCheckpoint | dict[str, Any]
    ) -> dict[str, Any]:
        proof = WaitingCheckpoint.model_validate(source)
        if proof.query_id != lease.query_id:
            raise AppError("CHECKPOINT_QUERY_MISMATCH", 409)
        lease.check()
        with lease.connection.transaction():
            conn = lease.connection
            row = self._worker_row(lease, generation)
            clarification = conn.execute(
                "SELECT * FROM app.query_clarifications WHERE query_id=%s FOR UPDATE",
                (lease.query_id,),
            ).fetchone()
            if not clarification or clarification["payload"] != proof.clarification.model_dump(
                mode="json"
            ):
                raise AppError("CLARIFICATION_VERSION_CONFLICT", 409)
            if row["status"] in TERMINAL:
                return self.tasks.snapshot(row, connection=conn)
            if row["status"] == Status.WAITING.value:
                if row["waiting_checkpoint_id"] != proof.checkpoint_id:
                    raise AppError("CHECKPOINT_VERSION_CONFLICT", 409)
                return self.tasks.snapshot(row, connection=conn)
            if (
                row["admission_state"] != "active"
                or row["status"] != "parsing"
                or clarification["answer"] is not None
            ):
                raise AppError("CLARIFICATION_STATE_CONFLICT", 409)
            if not self._compatible(row, proof.model_dump(mode="json"), connection=conn):
                raise AppError("CONFIG_VERSION_UNSUPPORTED", 409)
            if any(
                getattr(proof, key) > row[key]
                for key in ("model_call_count", "repair_count", "graph_step_count")
            ):
                raise AppError("CHECKPOINT_COUNTER_AHEAD", 409)
            latest = conn.execute(
                "SELECT checkpoint_id FROM query_checkpoints.checkpoints "
                "WHERE thread_id=%s AND checkpoint_ns='' ORDER BY checkpoint_id DESC LIMIT 1",
                (str(lease.query_id),),
            ).fetchone()
            pending = conn.execute(
                "SELECT 1 FROM query_checkpoints.checkpoint_writes "
                "WHERE thread_id=%s AND checkpoint_ns='' AND checkpoint_id=%s "
                "AND channel='__interrupt__' LIMIT 1",
                (str(lease.query_id), proof.checkpoint_id),
            ).fetchone()
            if not latest or latest["checkpoint_id"] != proof.checkpoint_id or not pending:
                raise AppError("CHECKPOINT_INTERRUPT_REQUIRED", 409)
            now = self.tasks.now()
            if row["cancel_requested"]:
                return self.tasks.snapshot(
                    self.tasks._close(conn, row, Status.CANCELED, "CANCELED_BY_USER"),
                    connection=conn,
                )
            if processing_elapsed_ms(row, now) >= row["budget_ms"]:
                return self.tasks.snapshot(
                    self.tasks._close(conn, row, Status.TIMED_OUT, "PROCESSING_DEADLINE"),
                    connection=conn,
                )
            expires = now + WAIT_TTL
            snapshot = {
                **proof.clarification.model_dump(mode="json"),
                "expires_at": expires.isoformat(),
            }
            conn.execute(
                "UPDATE app.query_clarifications SET status=%s,checkpoint_id=%s,"
                "wait_started_at=%s,expires_at=%s WHERE clarification_id=%s",
                (
                    "waiting",
                    proof.checkpoint_id,
                    now,
                    expires,
                    proof.clarification.clarification_id,
                ),
            )
            row = _required(
                conn.execute(
                    "UPDATE app.query_runs SET status='needs_clarification',"
                    "admission_state='none',status_version=status_version+1,waiting_since=%s,"
                    "wait_expires_at=%s,waiting_checkpoint_id=%s,clarification_snapshot=%s "
                    "WHERE query_id=%s RETURNING *",
                    (now, expires, proof.checkpoint_id, Jsonb(snapshot), lease.query_id),
                ).fetchone()
            )
            conn.execute(
                "UPDATE app.runtime_limits SET active_reserved=active_reserved-1 WHERE id=1"
            )
            self.tasks._event(conn, row, "clarification.required")
        from datapilot.tasks.timing import record_wait_commit_after_commit

        record_wait_commit_after_commit(self.tasks, lease, row)
        return self.tasks.snapshot(row, connection=conn)

    def _close_wait(self, conn: Any, row: dict[str, Any], reason: str) -> dict[str, Any]:
        closed_clock = capture_clock("wait_closed")
        state = "expired" if reason == "CLARIFICATION_EXPIRED" else "incompatible"
        conn.execute(
            "UPDATE app.query_clarifications SET status=%s WHERE query_id=%s AND answer IS NULL",
            (state, row["query_id"]),
        )
        return self.tasks._close(conn, row, Status.NEEDS_REFINE, reason, timing_point=closed_clock)

    def submit(
        self, actor: Principal, query_id: UUID, command: ClarifyInput, key: str | None
    ) -> dict[str, Any]:
        key, digest = validate_key(key), payload_hash(command)
        resource = f"/api/v1/queries/{query_id}/clarify"
        failure: AppError | None = None
        result: dict[str, Any]
        with self.tasks.connect() as conn:
            current = self.tasks._actor(conn, actor, "queries:clarify_self")
            self.tasks._owned(conn, current, query_id)
            limits = conn.execute(
                "SELECT * FROM app.runtime_limits WHERE id=1 FOR UPDATE"
            ).fetchone()
            row = self.tasks._owned(conn, current, query_id, lock=True)
            clarification = conn.execute(
                "SELECT * FROM app.query_clarifications WHERE query_id=%s FOR UPDATE", (query_id,)
            ).fetchone()
            previous = self.tasks._key(conn, current, "POST", resource, key, digest)
            if previous is not None:
                return self.tasks.snapshot(previous, connection=conn)
            if row["reason_code"] == "DATA_VERSION_CHANGED":
                raise AppError("DATA_VERSION_CHANGED", 409)
            if limits is None or limits["maintenance"]:
                raise AppError("SERVICE_MAINTENANCE", 503, retryable=True)
            if (
                not clarification
                or clarification["clarification_id"] != command.clarification_id
                or row["clarification_ref"] != command.clarification_id
                or row["spec_version"] != command.expected_spec_version
                or clarification["spec_version"] != command.expected_spec_version
            ):
                raise AppError("CLARIFICATION_VERSION_CONFLICT", 409)
            if clarification["answer"] is not None:
                if clarification["answer"] != command.answers.model_dump(mode="json"):
                    raise AppError("CLARIFICATION_ANSWER_CONFLICT", 409)
                # Same already-accepted answer, possibly under a new key. This
                # reads the current state without reviving it or getting a slot.
                self.tasks._remember(conn, current, resource, key, digest, query_id)
                return self.tasks.snapshot(row, connection=conn)
            if row["reason_code"] == "CLARIFICATION_EXPIRED":
                raise AppError("CLARIFICATION_EXPIRED", 409)
            if row["status"] != Status.WAITING.value:
                raise AppError("CLARIFICATION_STATE_CONFLICT", 409)
            now = self.tasks.now()
            if row["wait_expires_at"] is None or now >= row["wait_expires_at"]:
                result = self.tasks.snapshot(
                    self._close_wait(conn, row, "CLARIFICATION_EXPIRED"), connection=conn
                )
                failure = AppError("CLARIFICATION_EXPIRED", 409)
            elif not self._compatible(row, dict(clarification), connection=conn):
                result = self.tasks.snapshot(
                    self._close_wait(conn, row, "CONFIG_VERSION_UNSUPPORTED"), connection=conn
                )
                failure = AppError("CONFIG_VERSION_UNSUPPORTED", 409)
            else:
                if command.answers.metric_id.value not in clarification["payload"]["options"]:
                    raise AppError("CLARIFICATION_ANSWER_INVALID", 422)
                if limits is None:
                    raise AppError("APP_STATE_UNAVAILABLE", 503, retryable=True)
                if limits["logical_queued"] >= 10:
                    raise AppError("QUEUE_FULL", 429, retryable=True)
                if row["waiting_since"] is None or now < row["waiting_since"]:
                    raise AppError("WAIT_STATE_INVALID", 503)
                accepted_clock = capture_clock("answer_accepted")
                intervals = [
                    *row["waiting_intervals"],
                    {"start_at": row["waiting_since"].isoformat(), "end_at": now.isoformat()},
                ]
                generation = row["execution_generation"] + 1
                conn.execute(
                    "UPDATE app.query_clarifications SET status='answered',answer=%s,"
                    "answered_at=%s,resume_generation=%s WHERE clarification_id=%s",
                    (
                        Jsonb(command.answers.model_dump(mode="json")),
                        now,
                        generation,
                        command.clarification_id,
                    ),
                )
                row = _required(
                    conn.execute(
                        "UPDATE app.query_runs SET status='queued',admission_state='queued',"
                        "execution_generation=%s,status_version=status_version+1,waiting_since=NULL,"
                        "wait_expires_at=NULL,waiting_intervals=%s WHERE query_id=%s RETURNING *",
                        (generation, Jsonb(intervals), query_id),
                    ).fetchone()
                )
                conn.execute(
                    "UPDATE app.runtime_limits SET logical_queued=logical_queued+1 WHERE id=1"
                )
                conn.execute(
                    "INSERT INTO app.query_outbox"
                    "(outbox_id,query_id,execution_generation,kind,created_at) "
                    "VALUES(%s,%s,%s,'resume',%s)",
                    (uuid4(), query_id, generation, now),
                )
                self.tasks._remember(conn, current, resource, key, digest, query_id)
                self.tasks._event(conn, row, timing_point=accepted_clock)
                result = self.tasks.snapshot(row, connection=conn)
        # These controlled closures must COMMIT before the client sees 409.
        if failure is not None:
            raise failure
        return result

    def expire_waiting(self, *, limit: int = 100) -> int:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("WAIT_SCAN_LIMIT_INVALID")
        count = 0
        with self.tasks.connect() as conn:
            conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
            rows = conn.execute(
                "SELECT * FROM app.query_runs WHERE status='needs_clarification' "
                "AND wait_expires_at<=%s ORDER BY wait_expires_at,query_id LIMIT %s FOR UPDATE",
                (self.tasks.now(), limit),
            ).fetchall()
            for row in rows:
                self._close_wait(conn, dict(row), "CLARIFICATION_EXPIRED")
                count += 1
        return count
