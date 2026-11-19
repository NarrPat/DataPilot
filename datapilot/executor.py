"""Trusted, bounded, two-statement attempt; contains no model call or repair loop.

The server injects its pools/catalog and a deadline created before queueing.
The task coordinator persists consumed processing budget across resumes.
This component alone does not certify queue/worker budget enforcement.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.pq import TransactionStatus
from psycopg_pool import PoolTimeout, TooManyRequests

from datapilot.catalog import Catalog, CatalogError
from datapilot.contracts import ConfirmedSpec, QueryPlan, Scope
from datapilot.db.analytics import AnalystPool
from datapilot.results import ResultError, present_results
from datapilot.sql_guard import GuardError, add_probe_limit, guard_plan

MAX_RESULT_BYTES = 512 * 1024
MAX_RESULT_COLUMNS = 12
Observer = Callable[[str, dict[str, Any]], None]


@dataclass
class _PhaseTimings:
    """Disjoint wall intervals on this producer only, including failing phases.

    psycopg execute includes driver receive; fetch_serialize covers bounded
    row materialization and result presentation. Other includes controls,
    catalog readiness, observer work, transaction close and pool return.
    """

    started_ns: int = field(default_factory=time.monotonic_ns)
    attempt_id: str | None = None
    phases_ns: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(
            ("pool_wait", "guard", "plan", "execute", "fetch_serialize"), 0
        )
    )

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = time.monotonic_ns()
        try:
            yield
        finally:
            self.phases_ns[name] += time.monotonic_ns() - started

    def finish(self) -> dict[str, Any]:
        finished = time.monotonic_ns()
        total = finished - self.started_ns
        return {
            **{key + "_ms": value / 1_000_000 for key, value in self.phases_ns.items()},
            "other_ms": (total - sum(self.phases_ns.values())) / 1_000_000,
            "total_ms": total / 1_000_000,
            "phases_ns": dict(self.phases_ns),
            "other_ns": total - sum(self.phases_ns.values()),
            "total_ns": total,
            "started_ns": self.started_ns,
            "finished_ns": finished,
            "attempt_id": self.attempt_id,
            "clock": "producer_monotonic_ns",
            "standard_e2e_p95_eligible": False,
        }


TRANSACTION_COMMANDS = (
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
    "SET LOCAL statement_timeout = '2000ms'",
    "SET LOCAL lock_timeout = '500ms'",
    # Security-barrier views lose join-key statistics. A grouped DISTINCT query
    # plus the probe LIMIT can otherwise choose an early-output nested loop and
    # repeatedly scan every item. Keep this bounded analytical workload on
    # hash/merge alternatives; the setting expires with this transaction.
    "SET LOCAL enable_nestloop = off",
    "SET LOCAL search_path = pg_catalog",
    "SET LOCAL TIME ZONE 'Asia/Shanghai'",
)
SETTINGS_QUERY = (
    "SELECT current_user AS role, current_setting('transaction_isolation') AS isolation, "
    "current_setting('transaction_read_only') AS read_only, "
    "current_setting('statement_timeout') AS statement_timeout, "
    "current_setting('lock_timeout') AS lock_timeout, "
    "current_setting('enable_nestloop') AS enable_nestloop, "
    "current_setting('search_path') AS search_path, "
    "current_setting('TimeZone') AS timezone, pg_current_snapshot()::text AS snapshot, "
    "pg_backend_pid() AS pid"
)


def configure_readonly(execute: Callable[[str], Any]) -> dict[str, Any]:
    """Shared by real attempts and explicitly flagged trusted component probes."""
    for command in TRANSACTION_COMMANDS:
        execute(command)
    settings = execute(SETTINGS_QUERY).fetchone()
    if not isinstance(settings, dict):
        raise ExecutionError("TRANSACTION_SETTINGS_UNAVAILABLE")
    return settings


class ExecutionError(Exception):
    def __init__(self, code: str, *, sqlstate: str | None = None, **details: Any) -> None:
        self.code, self.sqlstate = code, sqlstate
        self.details = details
        super().__init__(code)


@dataclass(frozen=True)
class Deadline:
    expires_at: float

    @classmethod
    def after(cls, seconds: float = 120) -> Deadline:
        if not 0 < seconds <= 120:
            raise ValueError("PROCESSING_BUDGET_OUTSIDE_CONTRACT")
        return cls(time.monotonic() + seconds)

    def remaining(self) -> float:
        remaining = self.expires_at - time.monotonic()
        if remaining <= 0:
            raise ExecutionError("PROCESSING_TIMEOUT")
        return remaining


class Cancellation:
    """Thread-safe cooperative cancellation plus driver cancellation in flight."""

    def __init__(self) -> None:
        self._requested = threading.Event()
        self._lock = threading.Lock()
        self._connection: psycopg.Connection[Any] | None = None

    @property
    def requested(self) -> bool:
        return self._requested.is_set()

    def check(self) -> None:
        if self.requested:
            raise ExecutionError("CANCELED")

    def request(self) -> None:
        self._requested.set()
        # Serialize unbinding with the cancel packet so it cannot hit the next
        # borrower of the same pooled backend.
        with self._lock:
            if self._connection is not None:
                self._connection.cancel_safe(timeout=1)

    @contextmanager
    def bind(self, connection: psycopg.Connection[Any]) -> Iterator[None]:
        with self._lock:
            self.check()
            self._connection = connection
        try:
            yield
        finally:
            with self._lock:
                self._connection = None


@dataclass(frozen=True)
class ExecutedAttempt:
    attempt_id: str
    current: list[dict[str, Any]]
    baseline: list[dict[str, Any]] | None
    result: dict[str, Any]
    settings: dict[str, Any]
    plans: tuple[dict[str, Any], ...]
    # Frozen inputs actually used by this executor; not later checkpoint claims.
    # Explicit control-flow doubles may omit these, but cannot persist artifacts.
    confirmed: ConfirmedSpec | None = None
    query_plan: QueryPlan | None = None


def parameters(confirmed: ConfirmedSpec, *, baseline: bool = False) -> dict[str, Any]:
    """All values come from the confirmed spec, never from QueryPlan."""
    spec = confirmed.spec
    if baseline:
        if spec.comparison is None:
            raise ExecutionError("PLAN_BASELINE_MISMATCH")
        lower, upper = spec.comparison.baseline_start_at, spec.comparison.baseline_end_at
    else:
        lower, upper = spec.time_range.start_at, spec.time_range.end_at
    bindings: dict[str, Any] = {"start_at": lower, "end_at": upper}
    if spec.filters.sales_regions:
        bindings["sales_regions"] = [r.value for r in spec.filters.sales_regions]
    if spec.filters.category_codes:
        bindings["category_codes"] = [c.value for c in spec.filters.category_codes]
    return bindings


def read_bounded(
    cursor: Any, limit: int, *, check: Callable[[], None] = lambda: None
) -> list[dict[str, Any]]:
    """Check driver metadata and COMPLETE bounded rows, without partial success.

    The byte contract is UTF-8 JSON with compact separators and default=str for
    exact Decimal/datetime driver values. Two brackets and separating commas count.
    Metric-specific shape/type validation follows this transport limit.
    """
    columns = [column.name for column in cursor.description or ()]
    if len(columns) > MAX_RESULT_COLUMNS:
        raise ExecutionError(
            "RESULT_TOO_LARGE",
            dimension="columns",
            observed=len(columns),
            maximum=MAX_RESULT_COLUMNS,
        )
    if len(set(columns)) != len(columns):
        raise ExecutionError("RESULT_COLUMNS_MISMATCH")
    rows: list[dict[str, Any]] = []
    size = 2
    while True:
        check()
        batch = cursor.fetchmany(32)
        if not batch:
            break
        for row in batch:
            size += len(
                json.dumps(row, default=str, ensure_ascii=False, separators=(",", ":")).encode()
            ) + bool(rows)
            if len(rows) >= min(limit, 200) or size > MAX_RESULT_BYTES:
                raise ExecutionError(
                    "RESULT_TOO_LARGE",
                    observed_rows=len(rows) + 1,
                    maximum_rows=min(limit, 200),
                    observed_bytes=size,
                    maximum_bytes=MAX_RESULT_BYTES,
                )
            rows.append(row)
    return rows


class ReadonlyExecutor:
    def __init__(
        self,
        pools: Mapping[Scope, AnalystPool],
        catalog: Catalog,
        *,
        observer: Observer | None = None,
        timing_sink: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        if set(pools) != set(Scope):
            raise ValueError("BOTH_REGISTERED_SCOPES_REQUIRED")
        self.pools, self.catalog = pools, catalog
        self.observe = observer or (lambda _event, _details: None)
        self.timing_sink = timing_sink
        self._readiness: dict[Scope, dict[str, Any]] = {}

    def startup(self) -> dict[str, Any]:
        """Call after opening the fixed pools; no model SQL is involved."""
        for scope, pool in self.pools.items():
            with pool.connection(timeout=5) as connection:
                self._readiness[scope] = self.catalog.inspect_schema(connection, scope)
        return {scope.value: state for scope, state in self._readiness.items()}

    def execute(
        self,
        confirmed: ConfirmedSpec,
        plan: QueryPlan,
        *,
        deadline: Deadline,
        cancellation: Cancellation | None = None,
    ) -> ExecutedAttempt:
        timings = _PhaseTimings()
        status, code = "failed", None
        try:
            result = self._execute(
                confirmed, plan, deadline=deadline, cancellation=cancellation, timings=timings
            )
            status = "completed"
            return result
        except BaseException as exc:
            code = getattr(exc, "code", type(exc).__name__)
            raise
        finally:
            record = {
                **timings.finish(),
                "status": status,
                "error_code": code,
                "source_query_id": confirmed.source_query_id,
                "canonical_spec_hash": confirmed.canonical_spec_hash,
                "scope": confirmed.effective_scope.value,
            }
            try:
                self.observe("execution_timing", record)
            finally:
                if self.timing_sink is not None:
                    try:
                        self.timing_sink(record)
                    except Exception:
                        # Optional persistence cannot replace the original SQL result/error.
                        pass

    def _execute(
        self,
        confirmed: ConfirmedSpec,
        plan: QueryPlan,
        *,
        deadline: Deadline,
        cancellation: Cancellation | None,
        timings: _PhaseTimings,
    ) -> ExecutedAttempt:
        cancel = cancellation or Cancellation()
        cancel.check()
        deadline.remaining()
        try:
            with timings.phase("guard"):
                originals = guard_plan(plan, confirmed, self.catalog)
                checked = tuple(add_probe_limit(sql, confirmed, self.catalog) for sql in originals)
        except GuardError as exc:
            self.observe(
                "guard_reject", {"code": exc.code, "details": exc.details, "db_submitted": False}
            )
            raise
        self.observe(
            "guard_allow",
            {
                "statements": len(checked),
                "sql_hashes": [q.sql_sha256 for q in checked],
                "complexity": [q.complexity for q in checked],
                "lineage": [q.lineage for q in checked],
            },
        )
        readiness = self._readiness.get(confirmed.effective_scope)
        if readiness is None:
            raise ExecutionError("CATALOG_NOT_INITIALIZED")
        self.catalog.require_ready(confirmed.spec.metric_id, confirmed.effective_scope, readiness)
        attempt_id = str(uuid.uuid4())
        timings.attempt_id = attempt_id
        pool = self.pools[confirmed.effective_scope]
        rows: list[list[dict[str, Any]]] = []
        summaries = []
        connection = None

        def check() -> None:
            cancel.check()
            deadline.remaining()

        try:
            check()
            with ExitStack() as borrowed:
                with timings.phase("pool_wait"):
                    connection = borrowed.enter_context(
                        pool.connection(timeout=min(5, deadline.remaining()))
                    )
                if (
                    not connection.autocommit
                    or connection.info.transaction_status != TransactionStatus.IDLE
                ):
                    raise ExecutionError("CONNECTION_NOT_CLEAN")
                with cancel.bind(connection), connection.transaction():

                    def control(sql: str) -> Any:
                        check()
                        self.observe("control", {"attempt_id": attempt_id, "sql": sql})
                        return connection.execute(sql)

                    settings = configure_readonly(control)
                    if settings["role"] != "analyst_" + confirmed.effective_scope.value:
                        raise ExecutionError("EXECUTION_ROLE_MISMATCH")
                    self.observe("transaction_settings", {"attempt_id": attempt_id, **settings})
                    for index, sql in enumerate(checked):
                        check()
                        timeout_ms = max(1, min(2000, int(deadline.remaining() * 1000)))
                        control(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
                        bound = parameters(confirmed, baseline=index == 1)
                        metadata = {
                            "attempt_id": attempt_id,
                            "period": "baseline" if index else "current",
                            "sql_hash": sql.sql_sha256,
                            "input_hash": sql.input_sha256,
                            "role": settings["role"],
                            "snapshot": settings["snapshot"],
                            "pid": settings["pid"],
                            "bindings": bound,
                        }
                        self.observe("explain", {**metadata, "analyze": False})
                        with timings.phase("plan"):
                            explained = connection.execute(
                                "EXPLAIN (FORMAT JSON) " + sql.sql, bound
                            ).fetchone()
                        if explained is None:
                            raise ExecutionError("EXPLAIN_RESULT_MISSING")
                        plan_json = explained["QUERY PLAN"]
                        summary = {
                            **metadata,
                            "plan": plan_json,
                            "total_cost": plan_json[0]["Plan"]["Total Cost"],
                            "estimated_rows": plan_json[0]["Plan"]["Plan Rows"],
                            "cost_unit": "optimizer_cost_not_milliseconds",
                            "cost_threshold_calibrated": False,
                        }
                        summaries.append(summary)
                        self.observe("plan_summary", summary)
                        check()
                        # Precheck consumed budget too; each SELECT remains <=2s.
                        timeout_ms = max(1, min(2000, int(deadline.remaining() * 1000)))
                        control(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
                        self.observe("select", metadata)
                        check()
                        with timings.phase("execute"):
                            cursor = connection.execute(sql.sql, bound)
                        with timings.phase("fetch_serialize"):
                            data = read_bounded(cursor, confirmed.spec.result_limit, check=check)
                        if tuple(column.name for column in cursor.description or ()) != sql.columns:
                            raise ExecutionError("RESULT_COLUMNS_MISMATCH")
                        rows.append(data)
                        self.observe("period_read", {**metadata, "rows": data})
                    with timings.phase("fetch_serialize"):
                        result = present_results(
                            confirmed, self.catalog, rows[0], rows[1] if len(rows) == 2 else None
                        )
                    check()
                self.observe(
                    "transaction_finished",
                    {
                        "attempt_id": attempt_id,
                        "transaction_status": connection.info.transaction_status.name,
                    },
                )
            check()
            return ExecutedAttempt(
                attempt_id,
                rows[0],
                rows[1] if len(rows) == 2 else None,
                result,
                settings,
                tuple(summaries),
                confirmed,
                plan,
            )
        except (PoolTimeout, TooManyRequests) as exc:
            raise ExecutionError(
                "PROCESSING_TIMEOUT" if time.monotonic() >= deadline.expires_at else "EXECUTOR_BUSY"
            ) from exc
        except psycopg.Error as exc:
            code = (
                "CANCELED"
                if cancel.requested
                else "STATEMENT_TIMEOUT"
                if exc.sqlstate == "57014"
                else "LOCK_TIMEOUT"
                if exc.sqlstate == "55P03"
                else "SQL_EXECUTION_ERROR"
            )
            self.observe(
                "attempt_failed",
                {
                    "attempt_id": attempt_id,
                    "code": code,
                    "sqlstate": exc.sqlstate,
                    "partial_periods_discarded": len(rows),
                },
            )
            raise ExecutionError(code, sqlstate=exc.sqlstate) from None
        except (ExecutionError, ResultError, CatalogError) as exc:
            self.observe(
                "attempt_failed",
                {
                    "attempt_id": attempt_id,
                    "code": str(exc),
                    "details": getattr(exc, "details", {}),
                    "partial_periods_discarded": len(rows),
                },
            )
            raise
        finally:
            if connection is not None:
                self.observe(
                    "connection_released",
                    {
                        "attempt_id": attempt_id,
                        "closed": connection.closed,
                        "transaction_status": connection.info.transaction_status.name,
                    },
                )
