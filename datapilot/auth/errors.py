"""Five-field safe errors shared by local API adapters."""

from __future__ import annotations

import uuid
from typing import Any

import psycopg
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from datapilot.contracts import ClosedModel


class ErrorEnvelope(ClosedModel):
    code: str
    message: str
    request_id: str
    retryable: bool
    details: dict[str, Any]


MESSAGES = {
    "AUTH_REQUIRED": "A current session is required.",
    "INVALID_CREDENTIALS": "Invalid username or password.",
    "CSRF_FAILED": "Request origin or CSRF validation failed.",
    "FORBIDDEN": "This action is not permitted.",
    "NOT_FOUND": "Resource not found.",
    "DEPENDENCY_UNAVAILABLE": "A required service is unavailable.",
    "SERVICE_MAINTENANCE": "Data maintenance is in progress. Please retry later.",
    "INVALID_REQUEST": "Request does not match the supported contract.",
}


class AppError(Exception):
    def __init__(
        self,
        code: str,
        status_code: int,
        message: str | None = None,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.code, self.status_code = code, status_code
        self.message = message or MESSAGES.get(code, "Request could not be completed.")
        self.retryable, self.details = retryable, details or {}
        super().__init__(code)


def request_id(request: Request) -> str:
    value = getattr(request.state, "request_id", None)
    if not isinstance(value, str):
        value = str(uuid.uuid4())
        request.state.request_id = value
    return value


def install_error_handlers(app: FastAPI) -> None:
    @app.middleware("http")
    async def identify(request: Request, call_next: Any) -> Any:
        request.state.request_id = str(uuid.uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id(request)
        return response

    @app.exception_handler(AppError)
    async def application_error(request: Request, exc: AppError) -> JSONResponse:
        request.state.audit_error_code = exc.code
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "code": exc.code,
                "message": exc.message,
                "request_id": request_id(request),
                "retryable": exc.retryable,
                "details": exc.details,
            },
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Never echo invalid input, password, token or unknown field contents.
        return await application_error(
            request, AppError("INVALID_REQUEST", 422, details={"error_count": len(exc.errors())})
        )

    @app.exception_handler(psycopg.Error)
    async def database_error(request: Request, _exc: psycopg.Error) -> JSONResponse:
        return await application_error(
            request, AppError("DEPENDENCY_UNAVAILABLE", 503, retryable=True)
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = (
            "NOT_FOUND"
            if exc.status_code == 404
            else "METHOD_NOT_ALLOWED"
            if exc.status_code == 405
            else "INVALID_REQUEST"
        )
        return await application_error(request, AppError(code, exc.status_code))
