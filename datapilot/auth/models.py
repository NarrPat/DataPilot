"""Closed HTTP credentials and current server-owned authorization principal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Self

from pydantic import Field, model_validator

from datapilot.contracts import ClosedModel, Scope


class LoginRequest(ClosedModel):
    username: str = Field(strict=True, min_length=1, max_length=128)
    password: str = Field(strict=True, min_length=1, repr=False)

    @model_validator(mode="after")
    def password_bound(self) -> Self:
        self.username.encode("utf-8")  # Reject lone surrogates before SQL parameter encoding.
        if "\x00" in self.username:
            raise ValueError("USERNAME_NUL_FORBIDDEN")
        if len(self.password.encode("utf-8")) > 1024:
            raise ValueError("PASSWORD_UTF8_LIMIT")
        return self


@dataclass(frozen=True)
class Principal:
    user_id: str
    username: str
    role: str
    scope: Scope | None
    authz_version: int
    capabilities: frozenset[str]

    def public(self) -> dict[str, object]:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "role": self.role,
            "effective_scope": self.scope.value if self.scope else None,
            "authz_version": self.authz_version,
            "capabilities": sorted(self.capabilities),
        }


def principal_from_user(row: dict[str, object]) -> Principal:
    role = str(row["role"])
    if role not in {"analyst_east", "analyst_all", "report_admin"}:
        raise ValueError("UNREGISTERED_ROLE")
    scope = Scope.EAST if role == "analyst_east" else Scope.ALL if role == "analyst_all" else None
    capabilities = (
        frozenset({"reports:read"})
        if role == "report_admin"
        else frozenset(
            {
                "queries:create",
                "queries:read_self",
                "queries:cancel_self",
                "queries:clarify_self",
                "catalog:read",
                "history:read_self",
            }
        )
    )
    return Principal(
        str(row["user_id"]),
        str(row["username"]),
        role,
        scope,
        int(str(row["authz_version"])),
        capabilities,
    )
