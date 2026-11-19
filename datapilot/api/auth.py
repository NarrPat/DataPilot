"""Small real auth router; stateful product APIs are owned by the application."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response

from datapilot.api.read_models import CsrfResponse, LoginResponse, MeResponse
from datapilot.auth.errors import request_id
from datapilot.auth.models import LoginRequest
from datapilot.auth.service import AuthService


def auth_router(service: AuthService) -> APIRouter:
    router = APIRouter(prefix="/api/v1")
    config = service.config

    def cookie(response: Response, name: str, value: str, *, httponly: bool, age: int) -> None:
        response.set_cookie(
            name,
            value,
            max_age=age,
            path="/",
            secure=config.secure,
            httponly=httponly,
            samesite="lax",
        )

    @router.get("/auth/csrf", response_model=CsrfResponse)
    def csrf(request: Request, response: Response) -> dict[str, str]:
        token, nonce = service.bootstrap(request)
        response.headers["Cache-Control"] = "no-store"
        if nonce is not None:
            # An expired/revoked cookie must not block anonymous re-login.
            response.delete_cookie(
                config.session_cookie, path="/", secure=config.secure, httponly=True, samesite="lax"
            )
            cookie(
                response,
                config.anonymous_cookie,
                nonce,
                httponly=True,
                age=config.anonymous_seconds,
            )
        cookie(
            response,
            config.csrf_cookie,
            token,
            httponly=False,
            age=config.anonymous_seconds if nonce else config.session_seconds,
        )
        return {"csrf_token": token, "request_id": request_id(request)}

    @router.post("/auth/login", response_model=LoginResponse)
    def login(request: Request, response: Response, body: LoginRequest) -> dict[str, object]:
        principal, token, csrf_token = service.login(request, body)
        request.state.audit_actor_id = principal.user_id
        response.headers["Cache-Control"] = "no-store"
        cookie(response, config.session_cookie, token, httponly=True, age=config.session_seconds)
        cookie(response, config.csrf_cookie, csrf_token, httponly=False, age=config.session_seconds)
        response.delete_cookie(
            config.anonymous_cookie, path="/", secure=config.secure, httponly=True, samesite="lax"
        )
        return {
            "user": principal.public(),
            "csrf_token": csrf_token,
            "request_id": request_id(request),
        }

    @router.post("/auth/logout", status_code=204)
    def logout(request: Request) -> Response:
        service.logout(request)
        response = Response(status_code=204, headers={"Cache-Control": "no-store"})
        for name, httponly in (
            (config.session_cookie, True),
            (config.csrf_cookie, False),
            (config.anonymous_cookie, True),
        ):
            response.delete_cookie(
                name, path="/", secure=config.secure, httponly=httponly, samesite="lax"
            )
        return response

    @router.get("/me", response_model=MeResponse)
    def me(request: Request, response: Response) -> dict[str, object]:
        principal = service.authenticate(request.cookies.get(config.session_cookie))
        request.state.audit_actor_id = principal.user_id
        response.headers["Cache-Control"] = "no-store"
        return {"user": principal.public(), "request_id": request_id(request)}

    return router
