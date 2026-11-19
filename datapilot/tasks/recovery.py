"""Bounded metadata scanner; the advisory lease, not heartbeats, decides death.

Only the dispatcher talks to the broker. A pending recovery keeps its existing
backoff/outbox and reservation until a worker atomically claims that generation.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID, uuid4

import psycopg

from datapilot.auth.errors import AppError
from datapilot.auth.models import principal_from_user
from datapilot.contracts import Scope
from datapilot.tasks.clarification import ClarificationService
from datapilot.tasks.contracts import RUNNING, Status, processing_elapsed_ms
from datapilot.tasks.service import Connect, RunLease, TaskService, _required, run_lease

LOGGER = logging.getLogger("datapilot.recovery")


def recovery_trace(
    code: str, query_id: UUID, generation: int, *, status: str | None = None
) -> None:
    """Finite allowlisted fields; never serialize a row, SQL or exception text."""
    LOGGER.info(
        json.dumps(
            {
                "event": "recovery.scan",
                "code": code,
                "query_id": str(query_id),
                "observed_generation": generation,
                "status": status,
                "process_id": os.getpid(),
                "occurred_at": datetime.now(UTC).isoformat(),
            },
            sort_keys=True,
        )
    )


class RecoveryInspector(Protocol):
    def inspect_recovery(self, lease: RunLease, generation: int) -> dict[str, Any] | None: ...


@dataclass
class ScanReport:
    queued_expired: int = 0
    waiting_expired: int = 0
    inspected: int = 0
    blocked: int = 0
    projected: int = 0
    scheduled: int = 0
    pending: int = 0
    closed: int = 0
    errors: int = 0


class RecoveryScanner:
    def __init__(
        self,
        tasks: TaskService,
        inspector: RecoveryInspector,
        fixed_connect: Connect,
        *,
        stale_after_seconds: float = 5,
    ) -> None:
        if isinstance(stale_after_seconds, bool) or not 0 <= stale_after_seconds <= 60:
            raise ValueError("RECOVERY_SCAN_THRESHOLD_INVALID")
        self.tasks, self.inspector, self.fixed_connect = tasks, inspector, fixed_connect
        self.stale_after = timedelta(seconds=stale_after_seconds)

    def _locked_row(self, lease: RunLease) -> dict[str, Any] | None:
        lease.check()
        lease.connection.execute("SELECT id FROM app.runtime_limits WHERE id=1 FOR UPDATE")
        row = lease.connection.execute(
            "SELECT * FROM app.query_runs WHERE query_id=%s FOR UPDATE", (lease.query_id,)
        ).fetchone()
        if not row or row["admission_state"] != "active" or Status(row["status"]) not in RUNNING:
            return None
        return dict(row)

    def _control(self, lease: RunLease, row: dict[str, Any]) -> bool:
        """Close only after the old lock is absent and this scanner owns it."""
        conn = lease.connection
        owner = conn.execute(
            "SELECT * FROM app.users WHERE user_id=%s", (row["owner_user_id"],)
        ).fetchone()
        actor = principal_from_user(owner) if owner and owner["is_active"] else None
        target: Status | None = None
        reason = ""
        if (
            actor is None
            or actor.scope is None
            or "queries:create" not in actor.capabilities
            or actor.authz_version != row["authz_version"]
            or (actor.scope != Scope.ALL and actor.scope.value != row["effective_scope"])
        ):
            target, reason = Status.REJECTED, "AUTHORIZATION_REVOKED"
        elif row["cancel_requested"]:
            target, reason = Status.CANCELED, "CANCELED_BY_USER"
        elif processing_elapsed_ms(row, self.tasks.now()) >= row["budget_ms"]:
            target, reason = Status.TIMED_OUT, "PROCESSING_DEADLINE"
        elif (
            row["graph_version"] != self.tasks.graph_version
            or row["catalog_version"] != self.tasks.catalog_version
            or row["data_version"] != self.tasks.current_data_version(conn)
            or row["model_config_version"] != self.tasks.model_config_version
        ):
            target, reason = Status.NEEDS_REFINE, "CONFIG_INCOMPATIBLE"
        if target is not None:
            self.tasks._close(conn, row, target, reason)
            return True
        return False

    def scan_once(self, *, limit: int = 100) -> ScanReport:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("RECOVERY_SCAN_LIMIT_INVALID")
        report = ScanReport(
            queued_expired=self.tasks.expire_queued(),
            waiting_expired=ClarificationService(self.tasks).expire_waiting(limit=limit),
        )
        with self.tasks.connect() as conn:
            candidates = conn.execute(
                "SELECT query_id,execution_generation FROM app.query_runs "
                "WHERE admission_state='active' "
                "ORDER BY COALESCE(recovery_scheduled_at,claimed_at,api_received_at),"
                "query_id LIMIT %s",
                (limit,),
            ).fetchall()
        for candidate in candidates:
            try:
                self._scan_run(candidate["query_id"], candidate["execution_generation"], report)
            except (psycopg.Error, AppError):
                # Lost fixed connections remain reserved. No exception message,
                # reconnect or assumed completion is substituted for evidence.
                report.errors += 1
                recovery_trace(
                    "SCAN_DEPENDENCY_ERROR",
                    candidate["query_id"],
                    candidate["execution_generation"],
                )
        return report

    def _scan_run(self, query_id: UUID, observed_generation: int, report: ScanReport) -> None:
        with self.fixed_connect() as conn:
            with run_lease(conn, query_id) as lease:
                if lease is None:
                    report.blocked += 1  # RECOVERY_BLOCKED; do not mutate the live run.
                    recovery_trace("RECOVERY_BLOCKED", query_id, observed_generation)
                    return
                action, generation = self._prepare_candidate(lease)
                if action == "closed":
                    report.closed += 1
                    recovery_trace("RUN_CLOSED", query_id, generation)
                    return
                if action != "inspect":
                    return
                report.inspected += 1
                # No app row transaction spans checkpoint IO. The same session
                # lease still excludes another scanner/worker for this run.
                projected = self.inspector.inspect_recovery(lease, generation)
                if projected is not None:
                    report.projected += 1
                    recovery_trace(
                        "CHECKPOINT_PROJECTED", query_id, generation, status=projected["status"]
                    )
                    return
                action, generation = self._schedule_candidate(lease, generation)
                # Transaction helpers return only after commit (including any
                # deferred constraints). An attempted close is not a fact.
                if action == "closed":
                    report.closed += 1
                    recovery_trace("RUN_CLOSED", query_id, generation)
                elif action == "pending":
                    report.pending += 1
                    recovery_trace("RECOVERY_PENDING", query_id, generation)
                elif action == "scheduled":
                    report.scheduled += 1
                    recovery_trace("RECOVERING", query_id, generation)

    def _prepare_candidate(self, lease: RunLease) -> tuple[str, int]:
        with lease.connection.transaction():
            row = self._locked_row(lease)
            if row is None:
                return "ignored", 0
            generation = row["execution_generation"]
            if self._control(lease, row):
                return "closed", generation
            if (
                not row["recovery_pending"]
                and self.tasks.now() - (row["claimed_at"] or row["api_received_at"])
                < self.stale_after
            ):
                return "fresh", generation
            return "inspect", generation

    def _schedule_candidate(self, lease: RunLease, generation: int) -> tuple[str, int]:
        conn, query_id = lease.connection, lease.query_id
        with conn.transaction():
            row = self._locked_row(lease)
            if row is None or row["execution_generation"] != generation:
                return "ignored", generation
            if self._control(lease, row):
                return "closed", generation
            if row["recovery_pending"] and row["claimed_generation"] != generation:
                outbox = conn.execute(
                    "SELECT outbox_id FROM app.query_outbox WHERE query_id=%s "
                    "AND execution_generation=%s AND kind='recover_fragment' "
                    "AND claimed_at IS NULL AND canceled_at IS NULL FOR UPDATE",
                    (query_id, generation),
                ).fetchone()
                if outbox is None:
                    self.tasks._close(conn, row, Status.FAILED, "RECOVERY_STATE_INVALID")
                    return "closed", generation
                # Sent is not claimed. Keep its existing finite backoff.
                return "pending", generation
            if row["recover_count"] >= 2:
                self.tasks._close(conn, row, Status.FAILED, "RECOVERY_EXHAUSTED")
                return "closed", generation
            current = _required(
                conn.execute(
                    "UPDATE app.query_runs SET execution_generation=execution_generation+1,"
                    "recover_count=recover_count+1,recovery_pending=true,"
                    "recovery_scheduled_at=%s,status_version=status_version+1,"
                    "reason_code='RECOVERING' WHERE query_id=%s "
                    "AND execution_generation=%s RETURNING *",
                    (self.tasks.now(), query_id, generation),
                ).fetchone()
            )
            conn.execute(
                "INSERT INTO app.query_outbox(outbox_id,query_id,execution_generation,"
                "kind,created_at,next_attempt_at) VALUES(%s,%s,%s,'recover_fragment',%s,%s)",
                (
                    uuid4(),
                    query_id,
                    current["execution_generation"],
                    self.tasks.now(),
                    self.tasks.now(),
                ),
            )
            self.tasks._event(conn, current, "query.recovering")
            return "scheduled", current["execution_generation"]
