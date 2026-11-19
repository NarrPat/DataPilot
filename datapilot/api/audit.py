"""Body-free request lifecycle logs, including the real end of an SSE subscription."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from starlette.types import ASGIApp, Message, Receive, Scope, Send

LOGGER = logging.getLogger("datapilot.http")
ROUTES = frozenset(
    {
        "/health/live",
        "/health/ready",
        "/api/v1/auth/csrf",
        "/api/v1/auth/login",
        "/api/v1/auth/logout",
        "/api/v1/me",
        "/api/v1/queries",
        "/api/v1/queries/{query_id}",
        "/api/v1/queries/{query_id}/clarify",
        "/api/v1/queries/{query_id}/cancel",
        "/api/v1/queries/{query_id}/events",
        "/api/v1/catalog/metrics",
        "/api/v1/catalog/schema",
        "/api/v1/history",
        "/api/v1/eval/reports",
        "/api/v1/eval/reports/{report_id}",
    }
)


def safe_uuid(value: object) -> str | None:
    try:
        return str(UUID(str(value)))
    except (ValueError, TypeError, AttributeError):
        return None


def snapshot_reference(request: Any, data: dict[str, Any]) -> None:
    """Take only already-authorized, server-produced scalar snapshot metadata."""
    request.state.audit_query_id = safe_uuid(data.get("query_id"))
    for source, target in (
        ("execution_generation", "audit_generation"),
        ("last_event_seq", "audit_event_seq"),
        ("status_version", "audit_status_version"),
    ):
        value = data.get(source)
        if type(value) is int and 0 <= value < 2**63:
            setattr(request.state, target, value)


class RequestAuditMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.monotonic_ns()
        started_at = datetime.now(UTC).isoformat()
        status = None
        finished = False
        disconnected = False
        raised = False

        async def observed_receive() -> Message:
            nonlocal disconnected
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected = True
            return message

        async def observed_send(message: Message) -> None:
            nonlocal status, finished
            await send(message)
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body" and not message.get("more_body", False):
                finished = True

        try:
            await self.app(scope, observed_receive, observed_send)
        except BaseException:
            raised = True
            raise
        finally:
            try:
                state = scope.get("state", {})
                route = getattr(scope.get("route"), "path", None)
                refs = {
                    "request_id": safe_uuid(state.get("request_id")),
                    "actor_ref": safe_uuid(state.get("audit_actor_id")),
                    "query_id": safe_uuid(state.get("audit_query_id"))
                    or safe_uuid(scope.get("path_params", {}).get("query_id")),
                }
                counters = {
                    key.removeprefix("audit_"): state[key]
                    for key in ("audit_generation", "audit_event_seq", "audit_status_version")
                    if type(state.get(key)) is int and 0 <= state[key] < 2**63
                }
                code = state.get("audit_error_code")
                if (
                    not isinstance(code, str)
                    or not code.isascii()
                    or not code.replace("_", "").isalnum()
                    or len(code) > 80
                ):
                    code = None
                LOGGER.info(
                    json.dumps(
                        {
                            "app": "datapilot",
                            "event": "http.closed",
                            "process_id": os.getpid(),
                            "started_at": started_at,
                            "ended_at": datetime.now(UTC).isoformat(),
                            "duration_ns": time.monotonic_ns() - started,
                            "clock": "producer_monotonic_ns",
                            "route": route if route in ROUTES else "unmatched",
                            "method": scope["method"]
                            if scope["method"] in {"GET", "POST", "HEAD", "OPTIONS"}
                            else "other",
                            "http_status": status,
                            "body_finished": finished,
                            "disconnect_observed": disconnected,
                            "raised": raised,
                            "code": code,
                            **refs,
                            **counters,
                        },
                        separators=(",", ":"),
                    )
                )
            except Exception:
                # Observability failure must not change a response or cancel a query.
                pass
