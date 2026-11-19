"""Metadata-only checkpoint projection; no model factory, key or analytics pool.

The scanner owns the existing fixed run lease. A real saved interrupt or final
checkpoint can repair app projection under that same ownership, before any new
recovery generation is scheduled. All other execution belongs to the consumer.
"""

from __future__ import annotations

from typing import Any

from langgraph.checkpoint.postgres import PostgresSaver

from datapilot.auth.errors import AppError
from datapilot.catalog import Catalog
from datapilot.executor import ExecutionError
from datapilot.pipeline import GRAPH_VERSION
from datapilot.pipeline.graph import (
    read_checkpoint,
    terminal_checkpoint_outcome,
    validate_checkpoint_clarification,
    waiting_checkpoint,
)
from datapilot.tasks.clarification import ClarificationService
from datapilot.tasks.contracts import TERMINAL, Status
from datapilot.tasks.service import RunLease, TaskService
from datapilot.worker.fragment import FragmentStore, trusted_request


class CheckpointInspector:
    def __init__(self, tasks: TaskService) -> None:
        self.tasks = tasks
        self.catalog = Catalog()

    def inspect_recovery(self, lease: RunLease, generation: int) -> dict[str, Any] | None:
        store = FragmentStore(self.tasks, lease, generation)
        connection = lease.connection
        with store.serialized_transaction():
            row = store.locked_row()
            if row["status"] == Status.WAITING.value or Status(row["status"]) in TERMINAL:
                return self.tasks.snapshot(row, connection=connection)
            if row["admission_state"] != "active":
                return None
        previous = connection.execute("SHOW search_path").fetchone()
        threshold = connection.prepare_threshold
        try:
            store.control("checkpoint_inspection")
            if (
                row["graph_version"] != GRAPH_VERSION
                or row["catalog_version"] != self.tasks.catalog_version
                or row["data_version"] != self.tasks.current_data_version(connection)
                or row["model_config_version"] != self.tasks.model_config_version
            ):
                raise ExecutionError("CONFIG_INCOMPATIBLE")
            connection.execute("SET search_path TO query_checkpoints,pg_catalog")
            connection.prepare_threshold = 0
            saver = PostgresSaver(connection)
            store.mutex = saver.lock
            request = trusted_request(row)
            saved = read_checkpoint(saver, str(lease.query_id))
            with store.serialized_transaction():
                clarification = connection.execute(
                    "SELECT * FROM app.query_clarifications WHERE query_id=%s", (lease.query_id,)
                ).fetchone()
            state = validate_checkpoint_clarification(
                saved, request, self.catalog, dict(clarification) if clarification else None
            )
            if state is None:
                return None
            completed = terminal_checkpoint_outcome(saved, request, self.catalog)
            if completed is not None:
                store.recovering = (
                    row["recover_count"] > 0
                    and row["claimed_generation"] == generation
                    and not row["recovery_pending"]
                )
                return store.finish(completed)
            proof = waiting_checkpoint(saved, str(lease.query_id))
            if proof is not None:
                with store.serialized_transaction():
                    current = store.locked_row()
                    store.check_row(current)
                    clarification = connection.execute(
                        "SELECT status,answer,resume_generation FROM app.query_clarifications "
                        "WHERE query_id=%s FOR UPDATE",
                        (lease.query_id,),
                    ).fetchone()
                    # An answered interrupt is ordinary running recovery. It
                    # must be resumed with its accepted answer by a consumer.
                    if (
                        not clarification
                        or clarification["status"] != "prepared"
                        or clarification["answer"] is not None
                        or clarification["resume_generation"] is not None
                    ):
                        return None
                with store.mutex:
                    return ClarificationService(self.tasks).project_waiting(
                        lease, generation, proof
                    )
            return None
        except (ExecutionError, AppError) as exc:
            return store.finish(None, exc.code)
        except ValueError:
            return store.finish(None, "CONFIG_INCOMPATIBLE")
        finally:
            if not connection.closed and not connection.broken:
                connection.prepare_threshold = threshold
                if previous:
                    connection.execute(
                        "SELECT set_config('search_path',%s,false)", (previous["search_path"],)
                    )
