"""Admin-only, manifest-bound analytics replacement after a maintained drain.

There is no HTTP endpoint. Target validation is pure and precedes connections.
The app/audit database is preserved; publication follows atomic analytics load.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from datapilot.data.fixtures import FixtureInvalid, projection_hash, validate_fixture
from datapilot.db.bootstrap import connect
from datapilot.db.seed import export_tables, load_seed
from datapilot.tasks.contracts import Status
from datapilot.tasks.service import TaskService


class ResetTargetDenied(ValueError):
    pass


class ResetTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")
    app: Literal["datapilot"]
    profile: Literal["eval", "cap"]
    compose_project: Literal["dp-local", "dp-ci-i019-a", "dp-ci-i019-b"]
    host: Literal["postgres"]
    port: Literal[5432]
    namespace: str
    experiment_id: Annotated[str, Field(pattern=r"^DP-B[0-9]{3,6}[-a-z0-9]*$")]
    app_database: str
    analytics_database: str

    @model_validator(mode="after")
    def registered_pair(self) -> Self:
        if (
            re.fullmatch(r"dp_eval_i019_[a-z0-9_]{1,20}", self.namespace) is None
            or self.analytics_database != self.namespace
            or self.app_database != "dp_app_" + self.namespace.removeprefix("dp_")
            or len(self.app_database) > 36
        ):
            raise ValueError("RESET_TARGET_DENIED")
        return self


Version = Annotated[str, Field(strict=True, min_length=1, max_length=80)]


class ResetPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal[1]
    target: ResetTarget
    operation_id: UUID
    expected_data_version: Version
    target_data_version: Version
    source_projection_hash: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    drain_seconds: Annotated[int, Field(strict=True, ge=1, le=120)]

    @field_validator("schema_version", mode="before")
    @classmethod
    def strict_version(cls, value: object) -> object:
        if type(value) is not int:
            raise ValueError("INTEGER_SCHEMA_VERSION_REQUIRED")
        return value

    @model_validator(mode="after")
    def distinct_version(self) -> Self:
        if self.expected_data_version == self.target_data_version:
            raise ValueError("NEW_IMMUTABLE_DATA_VERSION_REQUIRED")
        return self


def validate_reset_plan(payload: object, registration: object) -> ResetPlan:
    try:
        plan = ResetPlan.model_validate(payload)
        registered = ResetTarget.model_validate(registration)
        if plan.target != registered:
            raise ValueError("REGISTERED_TARGET_MISMATCH")
    except (ValidationError, ValueError, TypeError):
        # Never echo input values/DSN-like strings from an untrusted manifest.
        raise ResetTargetDenied("RESET_TARGET_DENIED") from None
    return plan


def plan_digest(plan: ResetPlan) -> str:
    return hashlib.sha256(
        json.dumps(plan.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class ResetService:
    """One admin invocation holds a fixed session lock, never a long app transaction."""

    def __init__(
        self,
        registration: object,
        *,
        connector: Callable[..., Any] = connect,
        observer: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.registration, self.connector, self.observer = registration, connector, observer

    def _connect(self, database: str) -> Any:
        connection = self.connector(database, autocommit=True)
        if connection.info.dbname != database or connection.info.user != "dp_bootstrap":
            connection.close()
            raise ResetTargetDenied("RESET_ADMIN_CONNECTION_REQUIRED")
        connection.execute("SET statement_timeout='15000ms'")
        connection.execute("SET lock_timeout='500ms'")
        return connection

    def _step(self, app: Any, plan: ResetPlan, stage: str, **facts: Any) -> None:
        with app.transaction():
            app.execute(
                "INSERT INTO app.dataset_reset_events(operation_id,stage,document) "
                "VALUES(%s,%s,%s)",
                (plan.operation_id, stage, Jsonb(facts)),
            )
        # Test/telemetry observation happens after COMMIT; no public hook/route.
        if self.observer is not None:
            self.observer(stage, {"operation_id": str(plan.operation_id), **facts})

    def _receipt(self, app: Any, plan: ResetPlan, *, reused: bool = False) -> dict[str, Any]:
        row = app.execute(
            "SELECT status,reason_code,started_at,drain_deadline,finished_at "
            "FROM app.dataset_resets WHERE operation_id=%s",
            (plan.operation_id,),
        ).fetchone()
        limits = app.execute(
            "SELECT published_data_version,maintenance FROM app.runtime_limits WHERE id=1"
        ).fetchone()
        return {
            "operation_id": str(plan.operation_id),
            **dict(row),
            "reused": reused,
            "maintenance": limits["maintenance"],
            "data_version": limits["published_data_version"] or plan.expected_data_version,
        }

    def run(self, payload: object, source: dict[str, Any]) -> dict[str, Any]:
        plan = validate_reset_plan(payload, self.registration)
        lock_key = int.from_bytes(
            hashlib.sha256(("datapilot:reset:" + plan.target.namespace).encode()).digest()[:8],
            "big",
            signed=True,
        )
        with self._connect(plan.target.app_database) as app:
            acquired = app.execute("SELECT pg_try_advisory_lock(%s) AS yes", (lock_key,)).fetchone()
            if not acquired or not acquired["yes"]:
                raise ValueError("RESET_IN_PROGRESS")
            try:
                return self._run(app, plan, source)
            finally:
                if not app.closed:
                    app.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))

    def _prepare(self, app: Any, plan: ResetPlan) -> tuple[dict[str, Any], bool]:
        with app.transaction():
            limits = app.execute(
                "SELECT * FROM app.runtime_limits WHERE id=1 FOR UPDATE"
            ).fetchone()
            previous = app.execute(
                "SELECT * FROM app.dataset_resets WHERE operation_id=%s FOR UPDATE",
                (plan.operation_id,),
            ).fetchone()
            if previous:
                if previous["manifest_hash"] != plan_digest(plan):
                    raise ValueError("RESET_IDEMPOTENCY_CONFLICT")
                return dict(previous), False
            if limits["reset_operation_id"] is not None:
                other = app.execute(
                    "SELECT status FROM app.dataset_resets WHERE operation_id=%s",
                    (limits["reset_operation_id"],),
                ).fetchone()
                if other and other["status"] in {"draining", "loading"}:
                    raise ValueError("RESET_IN_PROGRESS")
            # A version is a stable data identity, including failed reset intents.
            # Same-operation replay was handled above; a failed, unpublished
            # version may be retried only with its original projection hash.
            binding_conflict = app.execute(
                "SELECT 1 FROM app.dataset_resets WHERE target_data_version=%s "
                "AND source_projection_hash<>%s LIMIT 1",
                (plan.target_data_version, plan.source_projection_hash),
            ).fetchone()
            if binding_conflict:
                raise ValueError("RESET_VERSION_BINDING_CONFLICT")
            used_version = app.execute(
                "SELECT 1 FROM app.dataset_resets WHERE expected_data_version=%s "
                "OR (target_data_version=%s AND status='ready') "
                "UNION ALL SELECT 1 FROM app.query_runs WHERE data_version=%s LIMIT 1",
                (plan.target_data_version, plan.target_data_version, plan.target_data_version),
            ).fetchone()
            if used_version:
                raise ValueError("RESET_VERSION_REUSE_DENIED")
            with self._connect(plan.target.analytics_database) as analytics:
                current = analytics.execute("SELECT * FROM analytics_meta.dataset_state").fetchone()
            if (
                not current
                or current["data_version"] != plan.expected_data_version
                or limits["published_data_version"] not in (None, plan.expected_data_version)
            ):
                raise ValueError("RESET_DATA_VERSION_CONFLICT")
            now = datetime.now(UTC)
            deadline = now + timedelta(seconds=plan.drain_seconds)
            app.execute(
                "INSERT INTO app.dataset_resets(operation_id,manifest_hash,target,"
                "expected_data_version,"
                "target_data_version,source_projection_hash,started_at,drain_deadline,status) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'draining')",
                (
                    plan.operation_id,
                    plan_digest(plan),
                    Jsonb(plan.target.model_dump(mode="json")),
                    plan.expected_data_version,
                    plan.target_data_version,
                    plan.source_projection_hash,
                    now,
                    deadline,
                ),
            )
            app.execute(
                "UPDATE app.runtime_limits SET maintenance=true,published_data_version=%s,"
                "published_data_hash=%s,reset_operation_id=%s WHERE id=1",
                (current["data_version"], current["projection_hash"], plan.operation_id),
            )
            self._invalidate(app, plan)
        row = app.execute(
            "SELECT * FROM app.dataset_resets WHERE operation_id=%s", (plan.operation_id,)
        ).fetchone()
        return dict(row), True

    def _invalidate(self, app: Any, plan: ResetPlan) -> None:
        # Same lock order as API/worker. Never touch active reservations.
        tasks = TaskService(
            lambda: self.connector(plan.target.app_database),
            data_version=plan.expected_data_version,
        )
        rows = app.execute(
            "SELECT * FROM app.query_runs WHERE admission_state='queued' OR "
            "status='needs_clarification' ORDER BY api_received_at,query_id LIMIT 100 FOR UPDATE"
        ).fetchall()
        for row in rows:
            queued = row["admission_state"] == "queued"
            tasks._close(
                app,
                dict(row),
                Status.CANCELED if queued else Status.NEEDS_REFINE,
                "DATA_VERSION_REPLACED" if queued else "DATA_VERSION_CHANGED",
            )

    def _drain(self, app: Any, plan: ResetPlan, attempt: dict[str, Any]) -> bool:
        remaining = max(0, (attempt["drain_deadline"] - datetime.now(UTC)).total_seconds())
        cutoff = time.monotonic() + min(plan.drain_seconds, remaining)
        while True:
            with app.transaction():
                limits = app.execute(
                    "SELECT * FROM app.runtime_limits WHERE id=1 FOR UPDATE"
                ).fetchone()
                self._invalidate(app, plan)
                limits = app.execute("SELECT * FROM app.runtime_limits WHERE id=1").fetchone()
                counts = {
                    r["admission_state"]: r["n"]
                    for r in app.execute(
                        "SELECT admission_state,count(*) AS n FROM app.query_runs "
                        "GROUP BY admission_state"
                    ).fetchall()
                }
                waiting = app.execute(
                    "SELECT count(*) AS n FROM app.query_runs WHERE status='needs_clarification'"
                ).fetchone()["n"]
            with self._connect("postgres") as control:
                transactions = control.execute(
                    "SELECT pid,usename,state,xact_start FROM pg_stat_activity "
                    "WHERE datname=%s AND xact_start IS NOT NULL",
                    (plan.target.analytics_database,),
                ).fetchall()
            consistent = (
                counts.get("active", 0) == limits["active_reserved"]
                and counts.get("queued", 0) == limits["logical_queued"]
            )
            drained = consistent and not (
                limits["active_reserved"] or limits["logical_queued"] or waiting or transactions
            )
            self._step(
                app,
                plan,
                "drain_checked",
                active=limits["active_reserved"],
                queued=limits["logical_queued"],
                waiting=waiting,
                admission_consistent=consistent,
                sql_transactions=[
                    {**dict(r), "xact_start": r["xact_start"].isoformat()} for r in transactions
                ],
            )
            if drained:
                return True
            if time.monotonic() >= cutoff:
                return False
            time.sleep(min(0.05, max(0, cutoff - time.monotonic())))

    def _load(self, app: Any, plan: ResetPlan, source: dict[str, Any]) -> None:
        errors = validate_fixture(source)
        if errors:
            raise FixtureInvalid(errors)
        if (
            source["data_version"] != plan.target_data_version
            or projection_hash(source) != plan.source_projection_hash
        ):
            raise ValueError("RESET_SOURCE_BINDING_MISMATCH")
        cutoff = time.monotonic() + 120
        with self._connect(plan.target.analytics_database) as analytics:

            def control() -> None:
                remaining = cutoff - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("RESET_LOAD_DEADLINE")
                analytics.execute(
                    "SELECT set_config('statement_timeout',%s,true)",
                    (str(max(1, min(15000, int(remaining * 1000)))),),
                )

            with analytics.transaction():
                # PG18 bounds the whole transaction as well as each statement.
                analytics.execute("SET LOCAL transaction_timeout='120s'")
                analytics.execute("LOCK TABLE analytics_meta.dataset_state IN EXCLUSIVE MODE")
                current = analytics.execute("SELECT * FROM analytics_meta.dataset_state").fetchone()
                if current and current["data_version"] == plan.target_data_version:
                    if (
                        current["projection_hash"] != plan.source_projection_hash
                        or projection_hash(export_tables(analytics)) != plan.source_projection_hash
                    ):
                        raise ValueError("RESET_RECOVERY_SOURCE_MISMATCH")
                else:
                    if not current or current["data_version"] != plan.expected_data_version:
                        raise ValueError("RESET_SOURCE_VERSION_UNKNOWN")
                    control()
                    analytics.execute(
                        "TRUNCATE raw.refunds,raw.order_items,raw.orders,raw.products,raw.customers"
                    )
                    analytics.execute("DELETE FROM analytics_meta.dataset_state")
                    load_seed(analytics, source, control=control)
                if projection_hash(export_tables(analytics)) != plan.source_projection_hash:
                    raise ValueError("RESET_DATABASE_SELF_CHECK_FAILED")
        self._step(
            app,
            plan,
            "analytics_committed",
            data_version=plan.target_data_version,
            projection_hash=plan.source_projection_hash,
        )

    def _run(self, app: Any, plan: ResetPlan, source: dict[str, Any]) -> dict[str, Any]:
        attempt, created = self._prepare(app, plan)
        if attempt["status"] in {"ready", "failed", "drain_timed_out"}:
            return self._receipt(app, plan, reused=True)
        if created:
            self._step(app, plan, "maintenance", expected_data_version=plan.expected_data_version)
        try:
            if not self._drain(app, plan, attempt):
                with app.transaction():
                    app.execute(
                        "UPDATE app.dataset_resets SET status='drain_timed_out',"
                        "reason_code='RESET_DRAIN_INCOMPLETE',finished_at=clock_timestamp() "
                        "WHERE operation_id=%s",
                        (plan.operation_id,),
                    )
                self._step(app, plan, "drain_timed_out")
                return self._receipt(app, plan)
            with app.transaction():
                app.execute(
                    "UPDATE app.dataset_resets SET status='loading' WHERE operation_id=%s",
                    (plan.operation_id,),
                )
            self._step(app, plan, "loading")
            self._load(app, plan, source)
            with app.transaction():
                limits = app.execute(
                    "SELECT * FROM app.runtime_limits WHERE id=1 FOR UPDATE"
                ).fetchone()
                if (
                    limits["reset_operation_id"] != plan.operation_id
                    or limits["active_reserved"]
                    or limits["logical_queued"]
                ):
                    raise ValueError("RESET_PUBLICATION_GATE_FAILED")
                app.execute(
                    "UPDATE app.runtime_limits SET maintenance=false,published_data_version=%s,"
                    "published_data_hash=%s WHERE id=1",
                    (plan.target_data_version, plan.source_projection_hash),
                )
                app.execute(
                    "UPDATE app.dataset_resets SET status='ready',finished_at=clock_timestamp() "
                    "WHERE operation_id=%s",
                    (plan.operation_id,),
                )
                app.execute(
                    "INSERT INTO app.dataset_reset_events(operation_id,stage,document) "
                    "VALUES(%s,'ready',%s)",
                    (plan.operation_id, Jsonb({"data_version": plan.target_data_version})),
                )
        except Exception as error:
            # Keep maintenance. No counter repair, force DROP or app-history deletion.
            with app.transaction():
                app.execute(
                    "UPDATE app.dataset_resets SET status='failed',reason_code=%s,"
                    "finished_at=clock_timestamp() WHERE operation_id=%s",
                    ("RESET_" + type(error).__name__.upper(), plan.operation_id),
                )
            self._step(app, plan, "failed", error_type=type(error).__name__)
            return self._receipt(app, plan)
        # Publication and its audit event committed together. A telemetry failure
        # after that boundary cannot relabel an already published version failed.
        receipt = self._receipt(app, plan)
        if self.observer is not None:
            try:
                self.observer(
                    "ready",
                    {
                        "operation_id": str(plan.operation_id),
                        "data_version": plan.target_data_version,
                    },
                )
            except Exception as error:
                receipt["observer_error_type"] = type(error).__name__
        return receipt
