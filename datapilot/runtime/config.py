"""Fixed project targets and bounded controls, without secrets or connections."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from datapilot.dispatch.celery import registered_route


def reject_model_secrets(environment: Mapping[str, str]) -> None:
    for name, value in environment.items():
        upper = name.upper()
        if value and (
            "API_KEY" in upper
            or upper in {"DEEPSEEK_AUTH_TOKEN", "OPENAI_ACCESS_TOKEN", "ANTHROPIC_AUTH_TOKEN"}
            or ("MODEL" in upper and upper.endswith(("_KEY", "_TOKEN")))
        ):
            raise ValueError("TEST_RUNTIME_MODEL_CREDENTIAL_FORBIDDEN")


@dataclass(frozen=True)
class RuntimeConfig:
    profile: str = "dev"
    model_mode: str = "none"
    app_database: str = "dp_app"
    analytics_database: str = "dp_analytics"
    namespace: str | None = None
    interval_seconds: float = 1.0
    batch_limit: int = 10
    live_budget_id: UUID | None = None

    def __post_init__(self) -> None:
        if self.profile not in {"dev", "prod", "test", "eval"}:
            raise ValueError("RUNTIME_PROFILE_INVALID")
        if self.profile in {"test", "eval"}:
            namespace_match = re.fullmatch(
                r"dp_eval_(i009|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019)_[a-z0-9_]{1,24}",
                self.namespace or "",
            )
            if (
                namespace_match is None
                or (
                    self.profile == "test"
                    and (self.model_mode not in {"none", "stub"} or self.live_budget_id is not None)
                )
                or (
                    self.profile == "eval"
                    and (
                        namespace_match.group(1) != "i011"
                        or self.model_mode != "live"
                        or not isinstance(self.live_budget_id, UUID)
                        or self.analytics_database != self.namespace
                        or self.app_database
                        != "dp_app_" + (self.namespace or "").removeprefix("dp_")
                    )
                )
                or re.fullmatch(
                    rf"dp_app_eval_{namespace_match.group(1)}_[a-z0-9_]{{1,30}}", self.app_database
                )
                is None
                or re.fullmatch(
                    rf"dp_eval_{namespace_match.group(1)}_[a-z0-9_]{{1,24}}",
                    self.analytics_database,
                )
                is None
            ):
                raise ValueError("REGISTERED_TEST_RUNTIME_TARGETS_REQUIRED")
        elif (
            self.namespace is not None
            or self.app_database != "dp_app"
            or self.analytics_database != "dp_analytics"
            or self.model_mode not in {"none", "live"}
            or (
                self.live_budget_id is not None
                and (self.model_mode != "live" or not isinstance(self.live_budget_id, UUID))
            )
        ):
            raise ValueError("FIXED_ORDINARY_RUNTIME_TARGETS_REQUIRED")
        registered_route(self.namespace)
        if (
            isinstance(self.interval_seconds, bool)
            or not isinstance(self.interval_seconds, int | float)
            or not math.isfinite(self.interval_seconds)
            or not 1 <= self.interval_seconds <= 30
        ):
            raise ValueError("CONTROL_INTERVAL_OUTSIDE_CONTRACT")
        if type(self.batch_limit) is not int or not 1 <= self.batch_limit <= 100:
            raise ValueError("CONTROL_BATCH_OUTSIDE_CONTRACT")

    @property
    def broker_url(self) -> str:
        return registered_route(self.namespace).url

    @property
    def queue_name(self) -> str:
        return registered_route(self.namespace).queue

    @property
    def broker_key_prefix(self) -> str | None:
        return registered_route(self.namespace).key_prefix

    def validate_environment(self, environment: Mapping[str, str]) -> None:
        expected = {
            "DP_POSTGRES_HOST": "postgres",
            "DP_BROKER_URL": self.broker_url,
            "CELERY_BROKER_URL": self.broker_url,
            "CELERY_BROKER_READ_URL": self.broker_url,
            "CELERY_BROKER_WRITE_URL": self.broker_url,
            "DP_QUEUE_NAME": self.queue_name,
        }
        if any(environment.get(key, value) != value for key, value in expected.items()):
            raise ValueError("RUNTIME_TARGET_OVERRIDE_FORBIDDEN")
        if environment.get("CELERY_RESULT_BACKEND"):
            raise ValueError("RESULT_BACKEND_FORBIDDEN")
        if self.profile == "test":
            reject_model_secrets(environment)

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> RuntimeConfig:
        env = os.environ if environment is None else environment
        try:
            interval = float(env.get("DP_CONTROL_INTERVAL_SECONDS", "1"))
            limit = int(env.get("DP_CONTROL_BATCH_LIMIT", "10"))
            budget = UUID(env["DP_LIVE_AUDIT_BUDGET"]) if env.get("DP_LIVE_AUDIT_BUDGET") else None
        except ValueError:
            raise ValueError("RUNTIME_NUMERIC_CONFIGURATION_INVALID") from None
        config = cls(
            profile=env.get("DP_PROFILE", "dev"),
            model_mode=env.get("DP_MODEL_MODE", "none"),
            app_database=env.get("DP_APP_DATABASE", "dp_app"),
            analytics_database=env.get("DP_ANALYTICS_DATABASE", "dp_analytics"),
            namespace=env.get("DP_RUNTIME_NAMESPACE") or None,
            interval_seconds=interval,
            batch_limit=limit,
            live_budget_id=budget,
        )
        config.validate_environment(env)
        return config
