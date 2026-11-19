"""The three-page application's authenticated, read-only routes."""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response

from datapilot.api.read_models import (
    HistoryResponse,
    MetricsResponse,
    ReportResponse,
    ReportsResponse,
    SchemaResponse,
)
from datapilot.api.read_service import CatalogState, ReadService
from datapilot.auth.errors import request_id
from datapilot.auth.models import Principal
from datapilot.auth.service import AuthService
from datapilot.tasks.contracts import Status
from datapilot.tasks.service import TaskService


def read_router(
    auth: AuthService, tasks: TaskService, catalog_state: CatalogState | None = None
) -> APIRouter:
    router = APIRouter(prefix="/api/v1")
    reads = ReadService(tasks, catalog_state)

    def actor(request: Request, response: Response) -> Principal:
        response.headers["Cache-Control"] = "no-store"
        principal = auth.authenticate(request.cookies.get(auth.config.session_cookie))
        request.state.audit_actor_id = principal.user_id
        return principal

    def reply(request: Request, data: dict[str, Any]) -> dict[str, Any]:
        return {"request_id": request_id(request), **data}

    @router.get("/catalog/metrics", response_model=MetricsResponse)
    def metrics(
        request: Request, response: Response, catalog_version: str | None = None
    ) -> dict[str, Any]:
        return reply(
            request, reads.catalog(actor(request, response), catalog_version, schema=False)
        )

    @router.get("/catalog/schema", response_model=SchemaResponse)
    def schema(
        request: Request, response: Response, catalog_version: str | None = None
    ) -> dict[str, Any]:
        return reply(request, reads.catalog(actor(request, response), catalog_version, schema=True))

    @router.get("/history", response_model=HistoryResponse)
    def history(
        request: Request,
        response: Response,
        limit: Annotated[int, Query(ge=1, le=50)] = 20,
        cursor: str | None = None,
        status: Status | None = None,
    ) -> dict[str, Any]:
        return reply(
            request,
            reads.history(actor(request, response), limit=limit, cursor=cursor, status=status),
        )

    @router.get("/eval/reports", response_model=ReportsResponse)
    def reports(
        request: Request,
        response: Response,
        limit: Annotated[int, Query(ge=1, le=50)] = 20,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return reply(request, reads.reports(actor(request, response), limit=limit, cursor=cursor))

    @router.get("/eval/reports/{report_id}", response_model=ReportResponse)
    def report(request: Request, response: Response, report_id: UUID) -> dict[str, Any]:
        return reply(request, reads.report(actor(request, response), report_id))

    return router
