"""Execute one accepted generation with its fixed DB lock/checkpoint connection.

The consumer and metadata inspector share trusted invocation validation. A
process crash retains its active reservation; recovery belongs to the scanner.
Ordinary HTTP acceptance is a separate explicit runtime configuration.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import datetime
from threading import RLock
from typing import Any
from uuid import UUID

import psycopg
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg.types.json import Jsonb

from datapilot.artifacts import create_binding, fingerprint, verify_binding
from datapilot.auth.errors import AppError
from datapilot.auth.models import principal_from_user
from datapilot.catalog import Catalog, CatalogError
from datapilot.contracts import ConfirmedSpec, QueryResult, Scope, TrustedContext
from datapilot.executor import Deadline, ExecutedAttempt, ExecutionError, ReadonlyExecutor
from datapilot.phases import PhaseJournal
from datapilot.pipeline import GRAPH_VERSION, PipelineOutcome, PipelineRequest, QueryPipeline
from datapilot.pipeline.graph import read_checkpoint, validate_checkpoint_clarification
from datapilot.specification import SpecError
from datapilot.sql_guard import GuardError
from datapilot.tasks.clarification import ClarificationService
from datapilot.tasks.contracts import RUNNING, TERMINAL, Status, processing_elapsed_ms
from datapilot.tasks.followup import validated_base
from datapilot.tasks.presentation import guard_summary
from datapilot.tasks.service import RunLease, TaskService, run_lease


def trusted_request(row: dict[str, Any]) -> PipelineRequest:
    """Restore authority from app facts, never from a checkpoint's claimed scope."""
    previous = None
    base_id, base_version, copied = (
        row.get("base_query_id"),
        row.get("base_spec_version"),
        row.get("base_confirmed_snapshot"),
    )
    if any(value is not None for value in (base_id, base_version, copied)):
        # Consumer and metadata inspector share this gate. Legacy refs without
        # a validated copy cannot become a base-free invocation on either path.
        if base_id is None or base_version is None or copied is None:
            raise ExecutionError("CONFIG_INCOMPATIBLE")
        try:
            saved = ConfirmedSpec.model_validate(copied)
            previous = validated_base(
                {
                    "query_id": base_id,
                    "spec_version": base_version,
                    "confirmed_spec": copied,
                    "effective_scope": saved.effective_scope.value,
                    "authz_version": saved.authz_version,
                    "catalog_version": saved.catalog_version,
                    "data_version": saved.data_version,
                },
                row["catalog_version"],
                row["data_version"],
            )
        except (AppError, ValueError, TypeError, KeyError):
            raise ExecutionError("CONFIG_INCOMPATIBLE") from None
    return PipelineRequest(
        question=row["question"],
        reference_time=row.get("processing_reference_time") or row["api_received_at"],
        context=TrustedContext(
            effective_scope=Scope(row["effective_scope"]),
            authz_version=row["authz_version"],
            data_version=row["data_version"],
            source_query_id=str(row["query_id"]),
            spec_version=row["spec_version"] or 1,
        ),
        previous_confirmed=previous,
        model_call_count=row["model_call_count"],
        repair_count=row["repair_count"],
        graph_step_count=row["graph_step_count"],
        model_config_version=row["model_config_version"],
    )


class FragmentStore:
    def __init__(
        self, tasks: TaskService, lease: RunLease, generation: int, *, recovering: bool = False
    ) -> None:
        self.tasks, self.lease, self.generation = tasks, lease, generation
        self.recovering = recovering
        # Share this with PostgresSaver's cursor mutex once the saver is built.
        # Pending-write futures can still be threaded under durability='sync'.
        self.mutex: Any = RLock()

    @contextmanager
    def serialized_transaction(self) -> Iterator[None]:
        with self.mutex:
            self.lease.check()
            with self.lease.connection.transaction():
                yield

    def locked_row(self) -> dict[str, Any]:
        conn = self.lease.connection
        conn.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
        row = conn.execute(
            "SELECT * FROM app.query_runs WHERE query_id=%s FOR UPDATE", (self.lease.query_id,)
        ).fetchone()
        if not row or row["execution_generation"] != self.generation:
            raise AppError("EXECUTION_VERSION_CONFLICT", 409)
        return dict(row)

    def check_row(self, row: dict[str, Any]) -> None:
        if row["admission_state"] != "active" or Status(row["status"]) in TERMINAL:
            raise ExecutionError("FRAGMENT_NO_LONGER_ACTIVE")
        if self.recovering and (
            row["claimed_generation"] != self.generation or row["recovery_pending"]
        ):
            raise ExecutionError("EXECUTION_VERSION_CONFLICT")
        owner = self.lease.connection.execute(
            "SELECT * FROM app.users WHERE user_id=%s", (row["owner_user_id"],)
        ).fetchone()
        actor = principal_from_user(owner) if owner and owner["is_active"] else None
        if (
            actor is None
            or actor.scope is None
            or "queries:create" not in actor.capabilities
            or actor.authz_version != row["authz_version"]
            or (actor.scope != Scope.ALL and actor.scope.value != row["effective_scope"])
        ):
            raise ExecutionError("AUTHORIZATION_REVOKED")
        if row["cancel_requested"]:
            raise ExecutionError("CANCELED")
        if processing_elapsed_ms(row, self.tasks.now()) >= row["budget_ms"]:
            raise ExecutionError("PROCESSING_TIMEOUT")

    def control(self, boundary: str) -> None:
        with self.serialized_transaction():
            row = self.locked_row()
            self.check_row(row)
            if boundary.startswith("before_node:"):
                if row["graph_step_count"] >= 24:
                    raise ExecutionError("GRAPH_STEP_BUDGET")
                self.lease.connection.execute(
                    "UPDATE app.query_runs SET graph_step_count=graph_step_count+1 "
                    "WHERE query_id=%s",
                    (self.lease.query_id,),
                )
            if boundary in {
                "before_parse",
                "before_parse_clarification",
                "before_generate",
                "before_repair",
            }:
                if row["model_call_count"] >= 4:
                    raise ExecutionError("MODEL_CALL_BUDGET_EXHAUSTED")
                if boundary == "before_repair" and row["repair_count"] >= 1:
                    raise ExecutionError("SQL_REPAIR_EXHAUSTED")
                self.lease.connection.execute(
                    "UPDATE app.query_runs SET model_call_count=model_call_count+1,"
                    "repair_count=repair_count+%s WHERE query_id=%s",
                    (1 if boundary == "before_repair" else 0, self.lease.query_id),
                )

    def phase(self, target: Status) -> None:
        self.control("phase")
        with self.serialized_transaction():
            row = self.locked_row()
        if target in TERMINAL or target in {Status.WAITING, Status.PARSING}:
            return  # terminal projection follows checkpoint completion
        if row["status"] == target.value:
            return
        if self.recovering:
            # Only a claimed replacement fragment may replay a registered
            # earlier phase. Ordinary TaskService.NEXT remains unchanged.
            if target not in RUNNING:
                raise ExecutionError("RECOVERY_PHASE_INVALID")
            with self.serialized_transaction():
                row = self.locked_row()
                self.check_row(row)
                changed_row = self.lease.connection.execute(
                    "UPDATE app.query_runs SET status=%s,status_version=status_version+1,"
                    "reason_code='RECOVERING' WHERE query_id=%s RETURNING *",
                    (target.value, self.lease.query_id),
                ).fetchone()
                assert changed_row is not None
                self.tasks._event(self.lease.connection, dict(changed_row))
            return
        if target == Status.REPAIRING and row["status"] == Status.GENERATING.value:
            self.phase(Status.GUARDING)
            with self.serialized_transaction():
                row = self.locked_row()
        with self.mutex:
            changed = self.tasks.transition(
                self.lease, self.generation, row["status_version"], target
            )
        if Status(changed["status"]) in TERMINAL:
            raise ExecutionError(changed["reason_code"] or "FRAGMENT_STOPPED")

    def put_result(self, attempt: ExecutedAttempt) -> str:
        self.control("persist_result")
        result = QueryResult.model_validate(attempt.result).model_dump(mode="json")
        with self.serialized_transaction():
            row = self.locked_row()
            self.check_row(row)
            binding = create_binding(attempt, row, trusted_request(row), Catalog())
            self.lease.connection.execute(
                "INSERT INTO app.query_attempt_results"
                "(attempt_id,query_id,execution_generation,result,created_at,"
                "binding,binding_sha256) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                (
                    UUID(attempt.attempt_id),
                    self.lease.query_id,
                    self.generation,
                    Jsonb(result),
                    self.tasks.now(),
                    Jsonb(binding.model_dump(mode="json")),
                    fingerprint(binding),
                ),
            )
        return attempt.attempt_id

    def put_clarification(self, payload: dict[str, Any]) -> None:
        self.control("persist_clarification")
        with self.mutex:
            ClarificationService(self.tasks).prepare(self.lease, self.generation, payload)

    def finish(self, outcome: PipelineOutcome | None, error: str | None = None) -> dict[str, Any]:
        with self.serialized_transaction():
            row = self.locked_row()
            if Status(row["status"]) in TERMINAL:
                return self.tasks.snapshot(row, connection=self.lease.connection)
            try:
                self.check_row(row)
            except ExecutionError as exc:
                error = exc.code
            status = Status(outcome.status) if outcome and error is None else Status.FAILED
            reason = error or (outcome.reason if outcome else "FRAGMENT_FAILED")
            if reason == "AUTHORIZATION_REVOKED":
                status = Status.REJECTED
            elif reason in {"CANCELED", "CANCELED_BY_USER"}:
                status = Status.CANCELED
            elif reason in {"PROCESSING_TIMEOUT", "PROCESSING_DEADLINE"}:
                status = Status.TIMED_OUT
            elif reason in {"CONFIG_VERSION_UNSUPPORTED", "CONFIG_INCOMPATIBLE"}:
                status = Status.NEEDS_REFINE
            if status == Status.WAITING:
                status, reason = Status.NEEDS_REFINE, "CLARIFICATION_ADAPTER_NOT_READY"
            if status not in TERMINAL:
                status, reason = Status.FAILED, "FRAGMENT_OUTCOME_INVALID"
            if status == Status.COMPLETED:
                if (
                    not outcome
                    or not outcome.attempt_id
                    or outcome.result_ref != outcome.attempt_id
                    or row["status"] != "validating"
                ):
                    raise AppError("FRAGMENT_RESULT_NOT_READY", 503)
                saved = self.lease.connection.execute(
                    "SELECT * FROM app.query_attempt_results WHERE attempt_id=%s "
                    "AND query_id=%s AND (execution_generation=%s OR "
                    "(%s AND execution_generation<%s))",
                    (
                        UUID(outcome.attempt_id),
                        self.lease.query_id,
                        self.generation,
                        self.recovering,
                        self.generation,
                    ),
                ).fetchone()
                if not saved or not outcome.confirmed or not outcome.plan:
                    raise AppError("FRAGMENT_RESULT_NOT_READY", 503)
                try:
                    result = verify_binding(
                        dict(saved), row, trusted_request(row), outcome.confirmed, outcome.plan
                    )
                    definition = Catalog().get_metric(
                        outcome.confirmed.spec.metric_id, outcome.confirmed.effective_scope
                    )
                    if (
                        result.metric_id != outcome.confirmed.spec.metric_id
                        or result.unit != definition.unit
                    ):
                        raise ValueError("DURABLE_RESULT_IDENTITY_MISMATCH")
                    summary = guard_summary(outcome.confirmed, outcome.plan, Catalog())
                except (ValueError, CatalogError, GuardError, SpecError):
                    # Exact query/ref ownership does not prove that damaged
                    # checkpoint metadata still describes this immutable result.
                    # Check identity only, never gold values or monetary answers.
                    row = self.tasks._close(
                        self.lease.connection, row, Status.NEEDS_REFINE, "CONFIG_INCOMPATIBLE"
                    )
                    return self.tasks.snapshot(row, connection=self.lease.connection)
                self.lease.connection.execute(
                    "UPDATE app.query_runs SET confirmed_spec=%s,query_plan=%s,"
                    "result_snapshot=%s,result_ref=%s,spec_version=%s,guard_summary=%s "
                    "WHERE query_id=%s",
                    (
                        Jsonb(outcome.confirmed.model_dump(mode="json")),
                        Jsonb(outcome.plan.model_dump(mode="json")),
                        Jsonb(saved["result"]),
                        UUID(outcome.attempt_id),
                        outcome.confirmed.spec_version,
                        Jsonb(summary),
                        self.lease.query_id,
                    ),
                )
                self.lease.connection.execute(
                    "UPDATE app.runtime_limits SET active_reserved=active_reserved-1 WHERE id=1"
                )
                updated = self.lease.connection.execute(
                    "UPDATE app.query_runs SET status='completed',status_version=status_version+1,"
                    "admission_state='none',reason_code=NULL,recovery_pending=false,finished_at=%s "
                    "WHERE query_id=%s RETURNING *",
                    (self.tasks.now(), self.lease.query_id),
                ).fetchone()
                assert updated is not None
                row = dict(updated)
                self.tasks._event(self.lease.connection, row, "query.completed")
            else:
                row = self.tasks._close(
                    self.lease.connection, row, status, reason or "FRAGMENT_FAILED"
                )
            return self.tasks.snapshot(row, connection=self.lease.connection)


class SingleFragmentWorker:
    def __init__(
        self,
        tasks: TaskService,
        build_pipeline: Callable[[PostgresSaver], QueryPipeline],
        *,
        reference_time: datetime | None = None,
        configure_pipeline: Callable[[QueryPipeline, FragmentStore], None] | None = None,
    ) -> None:
        self.tasks, self.build_pipeline, self.reference_time = tasks, build_pipeline, reference_time
        self.configure_pipeline = configure_pipeline

    def handle(
        self,
        query_id: UUID,
        generation: int,
        connection: psycopg.Connection[dict[str, Any]],
        *,
        kind: str = "create",
    ) -> dict[str, Any] | None:
        if kind not in {"create", "resume", "recover_fragment"}:
            raise ValueError("DISPATCH_MESSAGE_INVALID")
        with run_lease(connection, query_id) as lease, ExitStack() as observations:
            if lease is None:
                return None
            recovering = kind == "recover_fragment"
            claimed = (
                self.tasks.claim_recovery(lease, generation)
                if recovering
                else self.tasks.claim(lease, generation)
            )
            if claimed is None:
                return None
            store = FragmentStore(self.tasks, lease, generation, recovering=recovering)
            journal = PhaseJournal(store)
            observations.enter_context(journal.measure("worker_fragment"))
            previous = connection.execute("SHOW search_path").fetchone()
            previous_prepare = connection.prepare_threshold
            try:
                store.control("start")
                with connection.transaction():
                    row = store.locked_row()
                    if (
                        row["graph_version"] != GRAPH_VERSION
                        or row["catalog_version"] != self.tasks.catalog_version
                        or row["data_version"] != self.tasks.current_data_version(connection)
                        or row["model_config_version"] != self.tasks.model_config_version
                    ):
                        raise ExecutionError("CONFIG_VERSION_UNSUPPORTED")
                    if (
                        row["base_query_id"] is not None
                        and row.get("base_confirmed_snapshot") is None
                    ):
                        # Older rows never recorded a validated immutable semantic base.
                        raise ExecutionError("CONFIG_INCOMPATIBLE")
                    if row.get("processing_reference_time") is None:
                        row["processing_reference_time"] = (
                            self.reference_time or row["api_received_at"]
                        )
                        connection.execute(
                            "UPDATE app.query_runs SET processing_reference_time=%s "
                            "WHERE query_id=%s",
                            (row["processing_reference_time"], query_id),
                        )
                remaining = (row["budget_ms"] - processing_elapsed_ms(row, self.tasks.now())) / 1000
                if remaining <= 0:
                    raise ExecutionError("PROCESSING_TIMEOUT")
                connection.prepare_threshold = 0
                connection.execute("SET search_path TO query_checkpoints, pg_catalog")
                saver = PostgresSaver(connection)
                store.mutex = saver.lock
                pipeline = self.build_pipeline(saver)
                if self.configure_pipeline is not None:
                    self.configure_pipeline(pipeline, store)
                observations.callback(setattr, pipeline, "measure", pipeline.measure)
                pipeline.measure = journal.measure
                if isinstance(pipeline.executor, ReadonlyExecutor):
                    observations.callback(
                        setattr, pipeline.executor, "timing_sink", pipeline.executor.timing_sink
                    )
                    pipeline.executor.timing_sink = journal.record_sql
                request = trusted_request(row)
                callbacks: dict[str, Any] = dict(
                    deadline=Deadline.after(min(120, remaining)),
                    control=store.control,
                    on_phase=store.phase,
                    on_result=store.put_result,
                    on_clarification=store.put_clarification,
                )
                with store.serialized_transaction():
                    clarification = connection.execute(
                        "SELECT * FROM app.query_clarifications WHERE query_id=%s",
                        (query_id,),
                    ).fetchone()
                if clarification is not None:
                    try:
                        validate_checkpoint_clarification(
                            read_checkpoint(saver, str(query_id)),
                            request,
                            pipeline.catalog,
                            dict(clarification),
                        )
                    except ValueError:
                        raise ExecutionError("CONFIG_INCOMPATIBLE") from None
                waiting = pipeline.waiting_snapshot(str(query_id))
                if (
                    clarification is not None
                    and clarification["answer"] is not None
                    and (not recovering or waiting is not None)
                ):
                    from datapilot.contracts import MetricId

                    outcome = pipeline.resume(
                        request,
                        MetricId(clarification["answer"]["metric_id"]),
                        clarification_id=str(clarification["clarification_id"]),
                        expected_spec_version=clarification["spec_version"],
                        **callbacks,
                    )
                elif recovering:
                    outcome = pipeline.recover(request, **callbacks)
                else:
                    outcome = pipeline.run(request, **callbacks)
                if outcome.status == Status.WAITING:
                    proof = pipeline.waiting_snapshot(str(query_id))
                    if proof is None:
                        raise ExecutionError("WAITING_CHECKPOINT_REQUIRED")
                    with journal.measure("persist_projection"), store.mutex:
                        return ClarificationService(self.tasks).project_waiting(
                            lease, generation, proof
                        )

                with journal.measure("persist_projection"):
                    return store.finish(outcome)
            except (ExecutionError, AppError) as exc:
                with journal.measure("persist_projection"):
                    return store.finish(None, exc.code)
            except ValueError:
                with journal.measure("persist_projection"):
                    return store.finish(None, "CONFIG_VERSION_UNSUPPORTED")
            finally:
                if not connection.closed and not connection.broken:
                    connection.prepare_threshold = previous_prepare
                    if previous:
                        connection.execute(
                            "SELECT set_config('search_path',%s,false)", (previous["search_path"],)
                        )
                    from datapilot.tasks.timing import verify_after_commit

                    with journal.measure("post_commit_verification"):
                        verify_after_commit(self.tasks, lease)

    def inspect_recovery(self, lease: RunLease, generation: int) -> dict[str, Any] | None:
        """Compatibility facade; the control process constructs metadata-only inspector."""
        from datapilot.worker.checkpoints import CheckpointInspector

        return CheckpointInspector(self.tasks).inspect_recovery(lease, generation)

    def reconcile_waiting_gap(
        self, query_id: UUID, generation: int, connection: psycopg.Connection[dict[str, Any]]
    ) -> dict[str, Any] | None:
        """Repair only an already durable interrupt's missing app projection.

        A live lock returns immediately. No recover generation, new model call,
        queued slot or replacement state is created for ordinary running work.
        """
        with run_lease(connection, query_id) as lease:
            if lease is None:
                return None
            store = FragmentStore(self.tasks, lease, generation)
            with store.serialized_transaction():
                row = store.locked_row()
                if row["status"] == Status.WAITING.value or Status(row["status"]) in TERMINAL:
                    return self.tasks.snapshot(row, connection=connection)
                if row["admission_state"] != "active":
                    return None
                clarification = connection.execute(
                    "SELECT status,answer,resume_generation FROM app.query_clarifications "
                    "WHERE query_id=%s FOR UPDATE",
                    (query_id,),
                ).fetchone()
                # An answered generation can still expose the old interrupt
                # before Command.resume starts. It belongs to general running
                # recovery, not this unprojected-wait helper. Do not close it.
                if (
                    row["status"] != Status.PARSING.value
                    or not clarification
                    or clarification["status"] != "prepared"
                    or clarification["answer"] is not None
                    or clarification["resume_generation"] is not None
                ):
                    return None
            previous = connection.execute("SHOW search_path").fetchone()
            threshold = connection.prepare_threshold
            try:
                store.control("projection_scan")
                connection.execute("SET search_path TO query_checkpoints,pg_catalog")
                connection.prepare_threshold = 0
                saver = PostgresSaver(connection)
                store.mutex = saver.lock
                pipeline = self.build_pipeline(saver)
                proof = pipeline.waiting_snapshot(str(query_id))
                if proof is None:
                    return None
                saved = pipeline.get_resume_request(str(query_id))
                ignored = {"model_call_count", "repair_count", "graph_step_count"}
                if saved.model_dump(exclude=ignored) != trusted_request(row).model_dump(
                    exclude=ignored
                ):
                    raise ExecutionError("CONFIG_VERSION_UNSUPPORTED")
                with store.mutex:
                    return ClarificationService(self.tasks).project_waiting(
                        lease, generation, proof
                    )
            except (ExecutionError, AppError) as exc:
                return store.finish(None, exc.code)
            except ValueError:
                return store.finish(None, "CONFIG_VERSION_UNSUPPORTED")
            finally:
                if not connection.closed and not connection.broken:
                    connection.prepare_threshold = threshold
                    if previous:
                        connection.execute(
                            "SELECT set_config('search_path',%s,false)", (previous["search_path"],)
                        )
