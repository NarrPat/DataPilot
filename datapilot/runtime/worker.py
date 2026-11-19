"""Explicit prefork consumer with per-task resources; no inherited open pool."""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from datapilot.dispatch.celery import create_celery, register_fragment_task
from datapilot.dispatch.contracts import DispatchMessage
from datapilot.runtime.config import RuntimeConfig

if TYPE_CHECKING:
    import psycopg

    from datapilot.tasks.service import TaskService
    from datapilot.worker.fragment import SingleFragmentWorker

TraceSink = Callable[[dict[str, Any]], None]


def emit_runtime_trace(
    event: str, fields: dict[str, Any], *, sink: TraceSink | None = None
) -> None:
    """Emit a bounded projection, never a model/result/exception source object."""
    record = {
        **fields,
        "schema_version": 1,
        "event": event,
        "occurred_at": datetime.now(UTC).isoformat(),
        "process_id": os.getpid(),
    }
    encoded = json.dumps(record, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("utf8")) > 4096:
        raise ValueError("RUNTIME_TRACE_LIMIT_EXCEEDED")
    if sink is None:
        print(encoded, flush=True)
    else:
        sink(record)


def configure_runtime_logging() -> None:
    """Expose project control logs without enabling third-party INFO/debug."""
    logger = logging.getLogger("datapilot")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def app_connection(
    config: RuntimeConfig, *, autocommit: bool = False
) -> psycopg.Connection[dict[str, Any]]:
    import psycopg
    from psycopg.rows import dict_row

    from datapilot.db.bootstrap import password

    return psycopg.connect(
        host="postgres",
        port=5432,
        dbname=config.app_database,
        user="app_runtime",
        password=password("app_runtime"),
        autocommit=autocommit,
        row_factory=dict_row,
        connect_timeout=5,
        application_name="datapilot-runtime",
        options="-c statement_timeout=5000 -c lock_timeout=1000 -c search_path=pg_catalog",
    )


def task_service(config: RuntimeConfig) -> TaskService:
    from datapilot.pipeline import GRAPH_VERSION
    from datapilot.tasks.service import TaskService

    return TaskService(lambda: app_connection(config), graph_version=GRAPH_VERSION)


@dataclass(frozen=True)
class FragmentResources:
    worker: SingleFragmentWorker
    connection: psycopg.Connection[dict[str, Any]]


ResourcesFactory = Callable[[], AbstractContextManager[FragmentResources]]


@contextmanager
def live_resources(config: RuntimeConfig) -> Iterator[FragmentResources]:
    if config.profile == "test" or config.model_mode != "live":
        raise ValueError("LIVE_RESOURCE_CONFIGURATION_REQUIRED")
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
    from pydantic import SecretStr

    from datapilot.catalog import Catalog
    from datapilot.contracts import Scope
    from datapilot.db.bootstrap import password
    from datapilot.executor import ReadonlyExecutor
    from datapilot.model.audit import ModelAudit
    from datapilot.model.deepseek import DeepSeekGateway
    from datapilot.pipeline import QueryPipeline
    from datapilot.worker.fragment import SingleFragmentWorker

    with ExitStack() as stack:
        gateway = DeepSeekGateway(
            SecretStr(os.environ.get("DEEPSEEK_API_KEY", "")), max_calls=4, capture_content=False
        )
        stack.callback(gateway.close)
        pools = {}
        for scope in Scope:
            role = "analyst_" + scope.value
            pool = ConnectionPool[psycopg.Connection[dict[str, Any]]](
                conninfo="",
                kwargs={
                    "host": "postgres",
                    "port": 5432,
                    "dbname": config.analytics_database,
                    "user": role,
                    "password": password(role),
                    "autocommit": True,
                    "row_factory": dict_row,
                    "connect_timeout": 5,
                    "application_name": "datapilot-analyst",
                },
                min_size=0,
                max_size=1,
                max_waiting=2,
                timeout=5,
                open=False,
            )
            stack.callback(pool.close, timeout=5)
            pool.open()
            pools[scope] = pool
        catalog = Catalog()
        executor = ReadonlyExecutor(pools, catalog)
        executor.startup()
        tasks = task_service(config)
        fixed = stack.enter_context(app_connection(config, autocommit=True))
        worker = SingleFragmentWorker(
            tasks,
            lambda saver: QueryPipeline(gateway, executor, catalog, checkpointer=saver),
            configure_pipeline=lambda _pipeline, store: gateway.bind_audit(
                ModelAudit(store, budget_id=config.live_budget_id)
            ),
        )
        yield FragmentResources(worker, fixed)


class FragmentHandler:
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        resources_factory: ResourcesFactory | None = None,
        trace_sink: TraceSink | None = None,
    ) -> None:
        if resources_factory is not None and config.profile != "test":
            raise ValueError("RESOURCE_INJECTION_REQUIRES_TEST_PROFILE")
        if resources_factory is None and (config.profile == "test" or config.model_mode != "live"):
            raise ValueError("LIVE_WORKER_OR_EXPLICIT_TEST_RESOURCES_REQUIRED")
        self.config = config
        self.resources_factory = resources_factory or (lambda: live_resources(config))
        self.trace_sink = trace_sink

    def __call__(self, message: DispatchMessage) -> dict[str, Any]:
        from datapilot.tasks.contracts import Status

        fields: dict[str, Any] = {
            "query_id": str(message.query_id),
            "execution_generation": message.execution_generation,
            "kind": message.kind,
            "backend_pid": None,
            "status": None,
        }
        try:
            self.config.validate_environment(os.environ)
            with self.resources_factory() as resources:
                backend = getattr(getattr(resources.connection, "info", None), "backend_pid", None)
                fields["backend_pid"] = backend if type(backend) is int and backend > 0 else None
                emit_runtime_trace("fragment.received", fields, sink=self.trace_sink)
                result = resources.worker.handle(
                    message.query_id,
                    message.execution_generation,
                    resources.connection,
                    kind=message.kind,
                )
                status = result.get("status") if result is not None else None
                if result is not None and (
                    not isinstance(status, str) or status not in {s.value for s in Status}
                ):
                    raise ValueError("FRAGMENT_STATUS_INVALID")
                fields["status"] = status
            # Finished means this task's owned resources have also been closed.
            emit_runtime_trace(
                "fragment.finished" if result is not None else "fragment.skipped",
                fields,
                sink=self.trace_sink,
            )
            # The app DB owns the result. Celery never receives its body.
            return {
                "query_id": str(message.query_id),
                "execution_generation": message.execution_generation,
                "handled": result is not None,
                "status": status,
            }
        except Exception:
            emit_runtime_trace(
                "fragment.failed",
                {**fields, "code": "FRAGMENT_EXECUTION_FAILED"},
                sink=self.trace_sink,
            )
            raise


def create_worker_app(
    config: RuntimeConfig,
    *,
    resources_factory: ResourcesFactory | None = None,
    trace_sink: TraceSink | None = None,
) -> Any:
    config.validate_environment(os.environ)
    handler = FragmentHandler(config, resources_factory=resources_factory, trace_sink=trace_sink)
    app = create_celery(namespace=config.namespace)
    register_fragment_task(app, handler)
    return app


def main() -> int:
    app = None
    try:
        config = RuntimeConfig.from_environment()
        if (
            sys.platform != "linux"
            or config.model_mode != "live"
            or not os.environ.get("DEEPSEEK_API_KEY")
        ):
            raise ValueError("LIVE_WORKER_CREDENTIALS_REQUIRED")
        configure_runtime_logging()
        app = create_worker_app(config)
        app.worker_main(
            [
                "worker",
                "--pool=prefork",
                "--concurrency=2",
                "--loglevel=WARNING",
                "--without-gossip",
                "--without-mingle",
                "--hostname=datapilot@%h",
            ]
        )
        return 0
    except Exception:
        emit_runtime_trace("worker.error", {"code": "WORKER_RUNTIME_FAILED"})
        return 2
    finally:
        if app is not None:
            app.close()


if __name__ == "__main__":
    raise SystemExit(main())
