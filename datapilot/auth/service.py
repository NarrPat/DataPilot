"""Persistent opaque sessions and server-checked synchronizer CSRF tokens."""

from __future__ import annotations

import hashlib
import hmac
import secrets
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import psycopg
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError
from fastapi import Request

from datapilot.auth.config import AuthConfig
from datapilot.auth.errors import AppError
from datapilot.auth.models import LoginRequest, Principal, principal_from_user

Connect = Callable[[], AbstractContextManager[psycopg.Connection[dict[str, Any]]]]


def token_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def plausible_token(value: str | None) -> bool:
    return isinstance(value, str) and 40 <= len(value) <= 128 and value.isascii()


class AuthService:
    def __init__(
        self,
        connect: Connect,
        config: AuthConfig,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        hasher: PasswordHasher | None = None,
    ) -> None:
        self.connect, self.config, self.clock = connect, config, clock
        self.hasher = hasher or PasswordHasher()
        self._dummy_hash = self.hasher.hash(secrets.token_urlsafe(32))

    def now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("AUTH_CLOCK_REQUIRES_TIMEZONE")
        return value

    def csrf_for(self, token: str, *, anonymous: bool = False) -> str:
        context = "anonymous:" if anonymous else "session:"
        return hmac.new(
            self.config.session_secret.encode(), (context + token).encode(), hashlib.sha256
        ).hexdigest()

    def check_origin(self, request: Request) -> None:
        origin = request.headers.get("origin")
        if origin is None:
            referer = request.headers.get("referer", "")
            try:
                parsed = urlsplit(referer)
                origin = f"{parsed.scheme}://{parsed.netloc}"
                if parsed.username or parsed.password:
                    origin = None
            except ValueError:
                origin = None
        if origin not in self.config.allowed_origins:
            raise AppError("CSRF_FAILED", 403)

    def _session(self, connection: Any, token: str | None) -> dict[str, Any]:
        if not plausible_token(token):
            raise AppError("AUTH_REQUIRED", 401)
        row = connection.execute(
            "SELECT u.user_id,u.username,u.role,u.authz_version,u.is_active,s.csrf_hash "
            "FROM app.sessions s JOIN app.users u ON u.user_id=s.user_id "
            "WHERE s.session_hash=%s AND s.app_id='datapilot' "
            "AND s.revoked_at IS NULL AND s.expires_at>%s",
            (token_hash(str(token)), self.now()),
        ).fetchone()
        if not isinstance(row, dict) or not row["is_active"]:
            raise AppError("AUTH_REQUIRED", 401)
        return row

    def authenticate(self, token: str | None) -> Principal:
        if not plausible_token(token):
            raise AppError("AUTH_REQUIRED", 401)
        with self.connect() as connection:
            row = self._session(connection, token)
        return principal_from_user(row)

    def require_mutation(self, request: Request) -> Principal:
        token = request.cookies.get(self.config.session_cookie)
        if not plausible_token(token):
            raise AppError("AUTH_REQUIRED", 401)
        with self.connect() as connection:
            row = self._session(connection, token)
        self.check_origin(request)
        csrf = request.headers.get("x-csrf-token")
        if not plausible_token(csrf) or not hmac.compare_digest(
            token_hash(str(csrf)), row["csrf_hash"]
        ):
            raise AppError("CSRF_FAILED", 403)
        principal = principal_from_user(row)
        request.state.audit_actor_id = principal.user_id
        return principal

    @staticmethod
    def require_capability(principal: Principal, capability: str) -> None:
        if capability not in principal.capabilities:
            raise AppError("FORBIDDEN", 403)

    def bootstrap(self, request: Request) -> tuple[str, str | None]:
        if (
            "origin" in request.headers
            and request.headers["origin"] not in self.config.allowed_origins
        ):
            raise AppError("CSRF_FAILED", 403)
        token = request.cookies.get(self.config.session_cookie)
        if plausible_token(token):
            try:
                self.authenticate(token)
            except AppError as exc:
                if exc.status_code != 401:
                    raise
            else:
                return self.csrf_for(str(token)), None
        nonce = secrets.token_urlsafe(32)
        csrf = self.csrf_for(nonce, anonymous=True)
        now = self.now()
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO app.anonymous_csrf(nonce_hash,csrf_hash,created_at,expires_at) "
                "VALUES(%s,%s,%s,%s)",
                (
                    token_hash(nonce),
                    token_hash(csrf),
                    now,
                    now + timedelta(seconds=self.config.anonymous_seconds),
                ),
            )
        return csrf, nonce

    def login(self, request: Request, credentials: LoginRequest) -> tuple[Principal, str, str]:
        self.check_origin(request)
        if (
            request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
            != "application/json"
        ):
            raise AppError("INVALID_REQUEST", 422)
        nonce = request.cookies.get(self.config.anonymous_cookie)
        csrf = request.headers.get("x-csrf-token")
        old = request.cookies.get(self.config.session_cookie)
        if not plausible_token(csrf):
            raise AppError("CSRF_FAILED", 403)
        now = self.now()
        with self.connect() as connection, connection.transaction():
            anonymous = None
            if plausible_token(old):
                previous = self._session(connection, old)
                if not hmac.compare_digest(token_hash(str(csrf)), previous["csrf_hash"]):
                    raise AppError("CSRF_FAILED", 403)
            else:
                if not plausible_token(nonce):
                    raise AppError("CSRF_FAILED", 403)
                anonymous = connection.execute(
                    "SELECT csrf_hash,expires_at,used_at FROM app.anonymous_csrf "
                    "WHERE nonce_hash=%s AND app_id='datapilot' FOR UPDATE",
                    (token_hash(str(nonce)),),
                ).fetchone()
                if (
                    not anonymous
                    or anonymous["used_at"] is not None
                    or anonymous["expires_at"] <= now
                    or not hmac.compare_digest(token_hash(str(csrf)), anonymous["csrf_hash"])
                ):
                    raise AppError("CSRF_FAILED", 403)
            user = connection.execute(
                "SELECT user_id,username,password_hash,role,is_active,authz_version "
                "FROM app.users WHERE username=%s",
                (credentials.username,),
            ).fetchone()
            encoded = user["password_hash"] if user else self._dummy_hash
            valid: bool = False
            try:
                valid = self.hasher.verify(encoded, credentials.password)
            except (VerificationError, InvalidHashError):
                valid = False
            if not user or not user["is_active"] or not valid:
                raise AppError("INVALID_CREDENTIALS", 401)
            # Reading current membership above and committing this token belong
            # to one short transaction. Every later request rereads membership.
            if anonymous is not None:
                connection.execute(
                    "UPDATE app.anonymous_csrf SET used_at=%s WHERE nonce_hash=%s",
                    (now, token_hash(str(nonce))),
                )
            if plausible_token(old):
                connection.execute(
                    "UPDATE app.sessions SET revoked_at=%s "
                    "WHERE session_hash=%s AND revoked_at IS NULL",
                    (now, token_hash(str(old))),
                )
            token = secrets.token_urlsafe(32)
            session_csrf = self.csrf_for(token)
            connection.execute(
                "INSERT INTO app.sessions(session_hash,user_id,csrf_hash,created_at,expires_at) "
                "VALUES(%s,%s,%s,%s,%s)",
                (
                    token_hash(token),
                    user["user_id"],
                    token_hash(session_csrf),
                    now,
                    now + timedelta(seconds=self.config.session_seconds),
                ),
            )
        return principal_from_user(user), token, session_csrf

    def logout(self, request: Request) -> None:
        token, csrf = (
            request.cookies.get(self.config.session_cookie),
            request.headers.get("x-csrf-token"),
        )
        if not plausible_token(token):
            raise AppError("AUTH_REQUIRED", 401)
        with self.connect() as connection, connection.transaction():
            row = self._session(connection, token)
            self.check_origin(request)
            if not plausible_token(csrf) or not hmac.compare_digest(
                token_hash(str(csrf)), row["csrf_hash"]
            ):
                raise AppError("CSRF_FAILED", 403)
            connection.execute(
                "UPDATE app.sessions SET revoked_at=%s "
                "WHERE session_hash=%s AND revoked_at IS NULL",
                (self.now(), token_hash(str(token))),
            )
