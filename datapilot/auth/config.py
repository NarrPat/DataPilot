"""Explicit local HTTP versus deployment HTTPS configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlsplit


@dataclass(frozen=True)
class AuthConfig:
    session_secret: str = field(repr=False)
    profile: str = "dev"
    allowed_origins: tuple[str, ...] = ("http://127.0.0.1:18081", "http://localhost:18081")
    session_seconds: int = 8 * 60 * 60
    anonymous_seconds: int = 15 * 60

    def __post_init__(self) -> None:
        if self.profile not in {"dev", "test", "deploy"} or len(self.session_secret.encode()) < 32:
            raise ValueError("AUTH_CONFIGURATION_INVALID")
        if (
            not self.allowed_origins
            or not 1 <= self.session_seconds <= 28800
            or not 1 <= self.anonymous_seconds <= 900
        ):
            raise ValueError("AUTH_CONFIGURATION_INVALID")
        for origin in self.allowed_origins:
            parsed = urlsplit(origin)
            if (
                not parsed.netloc
                or parsed.path
                or parsed.query
                or parsed.fragment
                or parsed.username
                or parsed.password
                or parsed.scheme not in {"http", "https"}
            ):
                raise ValueError("AUTH_ORIGIN_INVALID")
            if self.profile == "deploy" and parsed.scheme != "https":
                raise ValueError("DEPLOY_REQUIRES_HTTPS")
            if self.profile != "deploy" and parsed.hostname not in {
                "127.0.0.1",
                "localhost",
                "::1",
            }:
                raise ValueError("LOCAL_PROFILE_REQUIRES_LOOPBACK_ORIGIN")

    @property
    def secure(self) -> bool:
        return self.profile == "deploy"

    @property
    def session_cookie(self) -> str:
        return "dp_session" if self.secure else "dp_session_dev"

    @property
    def csrf_cookie(self) -> str:
        return "dp_csrf" if self.secure else "dp_csrf_dev"

    @property
    def anonymous_cookie(self) -> str:
        return "dp_anon" if self.secure else "dp_anon_dev"
