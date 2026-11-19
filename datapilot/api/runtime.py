"""Ordinary API lifecycle; no tests, model client or administrator credential."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import psycopg
import redis
from fastapi import FastAPI
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from datapilot.api.app import create_app
from datapilot.auth.config import AuthConfig
from datapilot.auth.errors import AppError
from datapilot.auth.service import AuthService
from datapilot.db.bootstrap import password
from datapilot.pipeline.contracts import GRAPH_VERSION
from datapilot.tasks.service import TaskService

APP_REVISION = "app_0013_phase_events"
# Migration13 adds optional private telemetry only; the business schema at12
# remains compatible, with phase_breakdown explicitly unavailable until upgrade.
APP_COMPATIBLE_REVISIONS = frozenset({"app_0012_dataset_reset", APP_REVISION})


def processing_enabled(environment: Mapping[str, str]) -> bool:
    """An explicit operator opt-in; it does not claim a live consumer heartbeat."""
    flag = environment.get("DP_QUERY_PROCESSING_ENABLED", "0")
    if flag not in {"0", "1"}:
        raise ValueError("QUERY_PROCESSING_FLAG_INVALID")
    if flag == "1" and environment.get("DP_MODEL_MODE", "none") != "live":
        raise ValueError("QUERY_PROCESSING_REQUIRES_LIVE_WORKER_CONFIGURATION")
    return flag == "1"


def create_runtime_app() -> FastAPI:
    from datapilot.runtime.worker import configure_runtime_logging

    configure_runtime_logging()
    enabled = processing_enabled(os.environ)
    if (
        os.environ.get("DP_APP_DATABASE", "dp_app") != "dp_app"
        or os.environ.get("DP_POSTGRES_HOST", "postgres") != "postgres"
    ):
        raise ValueError("FIXED_API_RUNTIME_TARGET_REQUIRED")
    config = AuthConfig(
        profile=os.environ.get("DP_PROFILE", "dev"),
        session_secret=os.environ.get("DP_SESSION_SECRET", ""),
        allowed_origins=tuple(
            filter(
                None,
                os.environ.get(
                    "DP_APP_ALLOWED_ORIGINS", "http://127.0.0.1:18081,http://localhost:18081"
                ).split(","),
            )
        ),
    )
    pool = ConnectionPool[psycopg.Connection[dict[str, Any]]](
        conninfo="",
        kwargs={
            "host": "postgres",
            "port": 5432,
            "dbname": "dp_app",
            "user": "app_runtime",
            "password": password("app_runtime"),
            "row_factory": dict_row,
            "application_name": "datapilot-api",
            "connect_timeout": 5,
            "options": "-c statement_timeout=5000 -c lock_timeout=1000 -c search_path=pg_catalog",
        },
        min_size=0,
        max_size=4,
        max_waiting=10,
        timeout=5,
        open=False,
    )
    broker = redis.Redis(
        host="redis",
        port=6379,
        db=0,
        socket_timeout=2,
        socket_connect_timeout=2,
        retry_on_timeout=False,
    )

    def ready_check() -> None:
        try:
            with pool.connection() as connection:
                identity = connection.execute("SELECT current_user AS role").fetchone()
                revision = connection.execute(
                    "SELECT version_num FROM app.schema_version"
                ).fetchone()
                if (
                    not identity
                    or identity["role"] != "app_runtime"
                    or not revision
                    or revision["version_num"] not in APP_COMPATIBLE_REVISIONS
                ):
                    raise AppError("APPLICATION_SCHEMA_NOT_READY", 503, retryable=True)
            if broker.ping() is not True:
                raise AppError("BROKER_UNAVAILABLE", 503, retryable=True)
        except AppError:
            raise
        except Exception:
            raise AppError("RUNTIME_DEPENDENCY_UNAVAILABLE", 503, retryable=True) from None

    app = create_app(
        AuthService(pool.connection, config),
        TaskService(pool.connection, graph_version=GRAPH_VERSION),
        queries_ready=enabled,
        ready_check=ready_check,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        pool.open()
        try:
            yield
        finally:
            broker.close()
            pool.close()

    app.router.lifespan_context = lifespan
    return app
