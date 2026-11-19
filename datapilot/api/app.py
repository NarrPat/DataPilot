"""ASGI app factory with explicit server-owned query acceptance configuration.

The server supplies its fixed DB connection/config. No model doubles, admin
password, arbitrary scope or SQL can be supplied through these HTTP routes.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any
from uuid import UUID

import psycopg
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.responses import Response

from datapilot.api.audit import RequestAuditMiddleware, snapshot_reference
from datapilot.api.auth import auth_router
from datapilot.api.read_models import QuerySnapshot
from datapilot.api.read_routes import read_router
from datapilot.api.read_service import CatalogState
from datapilot.auth.errors import AppError, ErrorEnvelope, install_error_handlers
from datapilot.auth.service import AuthService
from datapilot.server_timing import capture_clock
from datapilot.tasks.clarification import ClarificationService, ClarifyInput
from datapilot.tasks.contracts import CancelInput, QueryInput, parse_cursor
from datapilot.tasks.service import TaskService


def create_app(
    auth: AuthService,
    tasks: TaskService,
    *,
    queries_ready: bool = False,
    catalog_state: CatalogState | None = None,
    ready_check: Callable[[], None] | None = None,
) -> FastAPI:
    app = FastAPI(
        title="DataPilot",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        responses={
            code: {"model": ErrorEnvelope}
            for code in (400, 401, 403, 404, 405, 409, 410, 422, 429, 503)
        },
    )
    install_error_handlers(app)
    app.include_router(auth_router(auth))
    app.include_router(read_router(auth, tasks, catalog_state))
    app.add_middleware(RequestAuditMiddleware)

    @app.middleware("http")
    async def received(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request.state.server_clock = capture_clock("api_receipt")
        request.state.api_received_at = request.state.server_clock.wall_at
        return await call_next(request)

    def reply(request: Request, data: dict[str, Any], status: int = 200) -> JSONResponse:
        snapshot_reference(request, data)
        return JSONResponse(
            {"request_id": request.state.request_id, **data},
            status_code=status,
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/health/live")
    def live(request: Request) -> dict[str, str]:
        return {"status": "alive", "request_id": request.state.request_id}

    @app.get("/health/ready")
    def ready(request: Request) -> JSONResponse:
        try:
            with tasks.connect() as conn:
                state = conn.execute(
                    "SELECT id,maintenance FROM app.runtime_limits WHERE id=1"
                ).fetchone()
                if state is None:
                    raise AppError("APP_DATABASE_UNAVAILABLE", 503, retryable=True)
        except Exception:
            raise AppError("APP_DATABASE_UNAVAILABLE", 503, retryable=True) from None
        if state["maintenance"]:
            raise AppError("SERVICE_MAINTENANCE", 503, retryable=True)
        if ready_check is not None:
            ready_check()
        if not queries_ready:
            raise AppError(
                "WORKER_NOT_READY",
                503,
                retryable=True,
                details={"app_database": "ready", "query_processing": False},
            )
        return reply(request, {"status": "ready", "query_processing": True})

    @app.post("/api/v1/queries", status_code=202, response_model=QuerySnapshot)
    def create_query(
        request: Request, command: QueryInput, idempotency_key: str | None = Header(default=None)
    ) -> JSONResponse:
        actor = auth.require_mutation(request)
        if not queries_ready:
            raise AppError("WORKER_NOT_READY", 503, retryable=True)
        result = tasks.create(
            actor,
            command,
            idempotency_key,
            received_at=request.state.api_received_at,
            request_id=UUID(request.state.request_id),
            received_clock=request.state.server_clock,
        )
        return reply(request, result, 202)

    @app.get("/api/v1/queries/{query_id}", response_model=QuerySnapshot)
    def get_query(request: Request, query_id: UUID) -> JSONResponse:
        actor = auth.authenticate(request.cookies.get(auth.config.session_cookie))
        request.state.audit_actor_id = actor.user_id
        return reply(request, tasks.get(actor, query_id))

    @app.post("/api/v1/queries/{query_id}/clarify", status_code=202, response_model=QuerySnapshot)
    def clarify_query(
        request: Request,
        query_id: UUID,
        command: ClarifyInput,
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        actor = auth.require_mutation(request)
        if not queries_ready:
            raise AppError("WORKER_NOT_READY", 503, retryable=True)
        result = ClarificationService(tasks).submit(actor, query_id, command, idempotency_key)
        return reply(request, result, 202)

    @app.post("/api/v1/queries/{query_id}/cancel", response_model=QuerySnapshot)
    def cancel_query(
        request: Request,
        query_id: UUID,
        command: CancelInput,
        idempotency_key: str | None = Header(default=None),
    ) -> JSONResponse:
        actor = auth.require_mutation(request)
        status, result = tasks.cancel(actor, query_id, command, idempotency_key)
        return reply(request, result, status)

    @app.get(
        "/api/v1/queries/{query_id}/events",
        response_class=StreamingResponse,
        responses={200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}}},
    )
    def events(
        request: Request,
        query_id: UUID,
        after_seq: str | None = None,
        last_event_id: str | None = Header(default=None),
    ) -> StreamingResponse:
        after = parse_cursor(last_event_id, after_seq)
        cookie = request.cookies.get(auth.config.session_cookie)
        actor = auth.authenticate(cookie)
        request.state.audit_actor_id = actor.user_id
        tasks.events(actor, query_id, after)  # validate auth/cursor before headers

        async def stream() -> AsyncIterator[str]:
            cursor, heartbeat = after, time.monotonic()
            while not await request.is_disconnected():
                try:
                    # Each operation closes its short DB connection before yielding/sleep.
                    actor = await asyncio.to_thread(auth.authenticate, cookie)
                    records = await asyncio.to_thread(tasks.events, actor, query_id, cursor)
                except AppError as exc:
                    if exc.status_code in {401, 403, 404}:
                        yield 'event: access.revoked\ndata: {"code":"ACCESS_REVOKED"}\n\n'
                    elif exc.code == "CURSOR_EXPIRED":
                        reset = {
                            "code": "CURSOR_EXPIRED",
                            "reset_url": f"/api/v1/queries/{query_id}",
                        }
                        yield f"event: stream.reset\ndata: {json.dumps(reset)}\n\n"
                    else:
                        yield 'event: stream.error\ndata: {"code":"STREAM_ERROR"}\n\n'
                    return
                except psycopg.Error:
                    yield 'event: stream.error\ndata: {"code":"DEPENDENCY_UNAVAILABLE"}\n\n'
                    return
                for event in records:
                    cursor = event["seq"]
                    payload = json.dumps(event, separators=(",", ":"))
                    yield f"id: {cursor}\nevent: {event['type']}\ndata: {payload}\n\n"
                if time.monotonic() - heartbeat >= 15:
                    yield ": heartbeat\n\n"
                    heartbeat = time.monotonic()
                await asyncio.sleep(1)

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    return app


def create_runtime_app() -> FastAPI:
    """Compatibility entry for the fixed-target, least-privilege runtime factory."""
    from datapilot.api.runtime import create_runtime_app as factory

    return factory()
