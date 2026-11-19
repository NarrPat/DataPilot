"""Bounded, fixed DeepSeek JSON transport using the installed LangChain adapter.

Decoded JSON remains untrusted and is not cleaned to pass a schema. The graph
owns closed DTO/scope/SQL validation and the sole permitted repair policy.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from importlib.metadata import version
from typing import Any
from uuid import uuid4

import httpx
import openai
from langchain_core.messages import BaseMessage
from langchain_deepseek import ChatDeepSeek
from langsmith import tracing_context
from pydantic import SecretStr, ValidationError

from datapilot.catalog import LinkedSchema
from datapilot.contracts import ConfirmedSpec, MetricId, QueryPlan, QuerySpec
from datapilot.model.diagnostics import FAILURE_TYPES, TRACE_PHASES
from datapilot.model.prompts import PROMPT_VERSION, parse_messages, plan_messages
from datapilot.pipeline.contracts import ClarificationNeeded, PipelineRequest

OFFICIAL_BASE = "https://api.deepseek.com"
MODEL_NAME = "deepseek-flash"
MAX_RESPONSE_BYTES = 64 * 1024
RecordSink = Callable[[dict[str, Any]], None]
AuditSink = Callable[[str, dict[str, Any]], None]
KEY_PATTERN = re.compile(r"\bsk-[A-Za-z0-9._-]{16,512}\b")
REPAIR_CODES = frozenset(
    {
        "PLAN_OUTPUT_INVALID",
        "PLAN_RESULT_CONTRACT_MISMATCH",
        "PLAN_BASELINE_MISMATCH",
        "SQL_PARSE_ERROR",
        "SQL_COLUMN_UNKNOWN",
        "SQL_COLUMN_AMBIGUOUS",
        "SQL_OUTPUT_ALIAS_REQUIRED",
        "SQL_OUTPUT_CONTRACT_MISMATCH",
        "RESULT_COLUMNS_MISMATCH",
        "RESULT_TYPE_MISMATCH",
        "RESULT_BUCKET_MISMATCH",
        "RESULT_GROUP_MISMATCH",
        "RESULT_TOTAL_REQUIRES_ONE_ROW",
        "SQL_AGGREGATION_ERROR",
    }
)


class GatewayError(Exception):
    """Only the fixed code is safe for logs; never wrap a provider message."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def reject_constant(_: str) -> None:
    raise ValueError("NON_JSON_CONSTANT")


def finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("NON_FINITE_JSON_NUMBER")
    return parsed


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("DUPLICATE_JSON_KEY")
        value[key] = item
    return value


class DeepSeekGateway:
    def __init__(
        self,
        api_key: SecretStr,
        *,
        model: str = MODEL_NAME,
        http_client: httpx.Client | None = None,
        on_record: RecordSink | None = None,
        audit_sink: AuditSink | None = None,
        max_calls: int = 6,
        max_tokens: int = 2048,
        capture_content: bool = False,
    ) -> None:
        key = api_key.get_secret_value()
        if re.fullmatch(r"[A-Za-z0-9._-]{16,512}", key) is None:
            raise GatewayError("MODEL_KEY_CONFIGURATION_INVALID")
        if model != MODEL_NAME:
            raise GatewayError("MODEL_NOT_REGISTERED")
        if type(max_calls) is not int or not 1 <= max_calls <= 6:
            raise GatewayError("MODEL_CALL_LIMIT_INVALID")
        if type(max_tokens) is not int or not 1 <= max_tokens <= 4096:
            raise GatewayError("MODEL_TOKEN_LIMIT_INVALID")
        if type(capture_content) is not bool:
            raise GatewayError("MODEL_CONTENT_CAPTURE_FLAG_INVALID")
        if os.environ.get("OPENAI_LOG", "").lower() in {"debug", "info"}:
            raise GatewayError("UNSAFE_PROVIDER_DEBUG_LOGGING")
        self._secret = key
        self._client = http_client or httpx.Client(follow_redirects=False)
        self._owns_client = http_client is None
        if self._client.follow_redirects:
            raise GatewayError("MODEL_REDIRECTS_FORBIDDEN")
        self._model = ChatDeepSeek(
            model=model,
            api_key=api_key,
            base_url=OFFICIAL_BASE,
            http_client=self._client,
            max_retries=0,
            timeout=30,
            temperature=0,
            max_tokens=max_tokens,
            extra_body={"thinking": {"type": "disabled"}},
            streaming=False,
            cache=False,
        )
        self._client.event_hooks.setdefault("response", []).append(self._capture_response)
        self._client.event_hooks.setdefault("request", []).append(self._capture_request)
        self._lock = threading.Lock()
        self._active_id: str | None = None
        self._raw: dict[str, Any] = {}
        self._active_record: dict[str, Any] | None = None
        self.audit_sink = audit_sink
        self._audit_started = False
        self._closed = False
        self.max_calls, self.max_tokens = max_calls, max_tokens
        self.capture_content = capture_content
        self.calls = 0
        self.records: list[dict[str, Any]] = []
        self.on_record = on_record
        self.dependency_versions = {
            name: version(name)
            for name in ("langchain-deepseek", "langchain-core", "openai", "httpx", "httpcore")
        }

    def bind_audit(self, sink: AuditSink) -> None:
        if self.calls or self.audit_sink is not None:
            raise GatewayError("MODEL_AUDIT_ALREADY_BOUND")
        self.audit_sink = sink

    def _audit(self, event: str) -> None:
        if self.audit_sink is None or self._active_record is None:
            return
        try:
            self.audit_sink(
                event,
                {
                    **self._active_record,
                    **self._raw,
                    "observed_at": datetime.now(UTC).isoformat(),
                },
            )
        except Exception:
            raise GatewayError("MODEL_AUDIT_WRITE_FAILED") from None

    def _capture_request(self, request: httpx.Request) -> None:
        if (
            self._active_id is None
            or request.headers.get("X-DataPilot-Request-ID") != self._active_id
        ):
            return
        if str(request.url) != OFFICIAL_BASE + "/chat/completions" or request.method != "POST":
            raise GatewayError("MODEL_REQUEST_TARGET_INVALID")
        self._audit("dispatch")  # Prepared request; this is not proof of a network write.
        post_body, sent = False, False

        def trace(event: str, info: dict[str, Any]) -> None:
            nonlocal post_body, sent
            operation, _, outcome = event.rpartition(".")
            phase = TRACE_PHASES.get(operation)
            if phase is not None:
                self._raw["transport_phase"] = phase
                if outcome == "failed":
                    name = type(info.get("exception")).__name__
                    self._raw["transport_failure_type"] = (
                        name if name in FAILURE_TYPES else "other_transport_error"
                    )
            if event in {"http11.send_request_body.started", "http2.send_request_body.started"}:
                post_body = getattr(info.get("request"), "method", None) == b"POST"
            if event in {"http11.send_request_body.complete", "http2.send_request_body.complete"}:
                if post_body and not sent:
                    sent = True
                    self._audit("transport_sent")

        request.extensions["trace"] = trace

    def _redact(self, value: str) -> str:
        return KEY_PATTERN.sub("[REDACTED]", value.replace(self._secret, "[REDACTED]"))

    def sanitized(self, value: object) -> object:
        """Explicit export helper; never pass SDK/header objects to it."""
        if isinstance(value, str):
            return self._redact(value)
        if isinstance(value, list | tuple):
            return [self.sanitized(item) for item in value]
        if isinstance(value, dict):
            return {self._redact(str(key)): self.sanitized(item) for key, item in value.items()}
        if value is None or type(value) in {bool, int, float}:
            return value
        raise GatewayError("MODEL_EVIDENCE_VALUE_UNSUPPORTED")

    def _text(self, value: object) -> str | None:
        return self._redact(value)[:256] if isinstance(value, str) else None

    def _captured_text(self, value: str) -> tuple[str, bool]:
        # Redact BEFORE truncating, so a key straddling the capture boundary
        # cannot leave an unrecognizable plaintext prefix in eval evidence.
        encoded = self._redact(value).encode("utf8")
        return encoded[:MAX_RESPONSE_BYTES].decode("utf8", errors="ignore"), len(
            encoded
        ) > MAX_RESPONSE_BYTES

    def _capture_response(self, response: httpx.Response) -> None:
        # A caller-supplied client may be used elsewhere. Only capture our exact request.
        if self._active_id is None or (
            response.request.headers.get("X-DataPilot-Request-ID") != self._active_id
        ):
            return
        try:
            self._read_response(response)
        finally:
            self._audit("response")

    def _read_response(self, response: httpx.Response) -> None:
        self._raw["http_status"] = response.status_code
        self._raw["server_request_id"] = self._text(response.headers.get("x-request-id"))
        try:
            response.read()
            body = response.json()
        except (ValueError, httpx.HTTPError):
            return
        if not isinstance(body, dict):
            return
        self._raw["completion_id"] = self._text(body.get("id"))
        self._raw["response_model"] = self._text(body.get("model"))
        usage = body.get("usage")
        if isinstance(usage, dict):
            self._raw["usage"] = {
                name: (usage[name] if type(usage.get(name)) is int and usage[name] >= 0 else None)
                for name in (
                    "prompt_tokens",
                    "completion_tokens",
                    "total_tokens",
                    "prompt_cache_hit_tokens",
                    "prompt_cache_miss_tokens",
                )
            }
            details = usage.get("completion_tokens_details")
            if isinstance(details, dict) and "reasoning_tokens" in details:
                reasoning = details["reasoning_tokens"]
                self._raw["usage"]["reasoning_tokens"] = (
                    reasoning if type(reasoning) is int and reasoning >= 0 else None
                )
        choices = body.get("choices")
        if isinstance(choices, list) and choices and isinstance(choices[0], dict):
            self._raw["finish_reason"] = self._text(choices[0].get("finish_reason"))
            message = choices[0].get("message")
            content = message.get("content") if isinstance(message, dict) else None
            if isinstance(content, str):
                self._raw["response_content_sha256"] = hashlib.sha256(content.encode()).hexdigest()
                self._raw["response_content_utf8_bytes"] = len(content.encode())
                if self.capture_content:
                    captured, truncated = self._captured_text(content)
                    self._raw["response_content"] = captured
                    self._raw["response_content_capture_truncated"] = truncated

    @staticmethod
    def _schema_valid(stage: str, value: dict[str, Any]) -> bool:
        try:
            if stage in {"parse", "parse_clarification"}:
                if stage == "parse" and value.get("kind") == "clarification":
                    ClarificationNeeded.model_validate(value)
                else:
                    QuerySpec.model_validate(value)
            else:
                QueryPlan.model_validate(value)
            return True
        except ValidationError:
            return False

    def _call(self, stage: str, messages: list[BaseMessage], timeout: float) -> object:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, int | float)
            or not math.isfinite(timeout)
            or not 0 < timeout <= 30
        ):
            raise GatewayError("MODEL_TIMEOUT_ARGUMENT_INVALID")
        with self._lock:
            if self._closed:
                raise GatewayError("MODEL_GATEWAY_CLOSED")
            if self.calls >= self.max_calls:
                raise GatewayError("MODEL_GATEWAY_CALL_BUDGET")
            self.calls += 1
            # A question must not smuggle this transport's credential into a
            # prompt. Keep all fields but redact exact credentials/key tokens.
            messages = [
                message.model_copy(update={"content": self._redact(str(message.content))})
                for message in messages
            ]
            local_id = str(uuid4())
            self._active_id = local_id
            self._raw = {
                "http_status": None,
                "server_request_id": None,
                "completion_id": None,
                "response_model": None,
                "finish_reason": None,
                "usage": None,
                "transport_phase": None,
                "transport_failure_type": None,
            }
            started = datetime.now(UTC).isoformat()
            clock = time.monotonic()
            configuration = {
                "provider": "deepseek",
                "base_url": OFFICIAL_BASE,
                "model": MODEL_NAME,
                "prompt_version": PROMPT_VERSION,
                "temperature": 0,
                "thinking": "disabled",
                "response_format": "json_object",
                "streaming": False,
                "max_retries": 0,
                "max_tokens": self.max_tokens,
                "timeout_seconds": timeout,
                "dependency_versions": self.dependency_versions,
            }
            record: dict[str, Any] = {
                "local_request_id": local_id,
                "stage": stage,
                "attempt": self.calls,
                "provider": "deepseek",
                "configured_model": MODEL_NAME,
                "prompt_version": PROMPT_VERSION,
                "started_at": started,
                "timeout_seconds": timeout,
                "max_tokens": self.max_tokens,
                "request_messages": [
                    {
                        "role": m.type,
                        "sha256": hashlib.sha256(str(m.content).encode()).hexdigest(),
                        "utf8_bytes": len(str(m.content).encode()),
                    }
                    for m in messages
                ],
                "schema_valid": None,
                "validation_code": None,
                "error_code": None,
                "ended_at": None,
                "duration_ms": None,
                "dependency_versions": dict(self.dependency_versions),
                "model_config_sha256": hashlib.sha256(
                    json.dumps(configuration, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            }
            if self.capture_content:
                for entry, message in zip(record["request_messages"], messages, strict=True):
                    captured, truncated = self._captured_text(str(message.content))
                    entry["content"] = captured
                    entry["content_capture_truncated"] = truncated
            self._active_record = record
            self._audit_started = False
            try:
                self._audit("intent")
                self._audit_started = True
                with tracing_context(enabled=False):
                    response = self._model.invoke(
                        messages,
                        config={"callbacks": []},
                        timeout=timeout,
                        response_format={"type": "json_object"},
                        extra_headers={"X-DataPilot-Request-ID": local_id},
                    )
                finish = self._raw.get("finish_reason")
                if finish == "length":
                    raise GatewayError("MODEL_OUTPUT_TRUNCATED")
                if finish != "stop":
                    raise GatewayError("MODEL_FINISH_UNSUPPORTED")
                content = response.content
                if not isinstance(content, str):
                    raise GatewayError("MODEL_CONTENT_TYPE_INVALID")
                if len(content.encode()) > MAX_RESPONSE_BYTES:
                    raise GatewayError("MODEL_OUTPUT_TOO_LARGE")
                record["response_content_sha256"] = hashlib.sha256(content.encode()).hexdigest()
                record["response_content_utf8_bytes"] = len(content.encode())
                if not content.strip():
                    raise GatewayError("MODEL_BLANK_OUTPUT")
                try:
                    value = json.loads(
                        content,
                        parse_constant=reject_constant,
                        parse_float=finite_float,
                        object_pairs_hook=unique_object,
                    )
                except (ValueError, TypeError, RecursionError):
                    raise GatewayError("MODEL_JSON_INVALID") from None
                if not isinstance(value, dict):
                    raise GatewayError("MODEL_JSON_OBJECT_REQUIRED")
                record["schema_valid"] = self._schema_valid(stage, value)
                if not record["schema_valid"]:
                    record["validation_code"] = "MODEL_SCHEMA_INVALID"
                return self.sanitized(value)
            except (openai.APITimeoutError, httpx.TimeoutException):
                record["error_code"] = "MODEL_TIMEOUT"
                raise TimeoutError("MODEL_TIMEOUT") from None
            except openai.APIStatusError as exc:
                code = {
                    401: "MODEL_AUTHENTICATION_FAILED",
                    402: "MODEL_BALANCE_UNAVAILABLE",
                    429: "MODEL_RATE_LIMITED",
                }.get(
                    exc.status_code,
                    "MODEL_PROVIDER_UNAVAILABLE" if exc.status_code >= 500 else "MODEL_HTTP_ERROR",
                )
                record["error_code"] = code
                raise GatewayError(code) from None
            except openai.APIConnectionError:
                record["error_code"] = "MODEL_CONNECTION_FAILED"
                raise GatewayError("MODEL_CONNECTION_FAILED") from None
            except GatewayError as exc:
                record["error_code"] = exc.code
                raise
            except Exception:
                # The locked SDK raises for length/content_filter before it
                # constructs AIMessage. Our response hook still has the raw
                # finish field, so classification does not rely on error text.
                finish = self._raw.get("finish_reason")
                code = (
                    "MODEL_OUTPUT_TRUNCATED"
                    if finish == "length"
                    else "MODEL_FINISH_UNSUPPORTED"
                    if finish not in {None, "stop"}
                    else "MODEL_PROTOCOL_ERROR"
                )
                record["error_code"] = code
                raise GatewayError(code) from None
            finally:
                record.update(self._raw)
                record["ended_at"] = datetime.now(UTC).isoformat()
                record["duration_ms"] = (time.monotonic() - clock) * 1000
                self.records.append(record)
                try:
                    if self._audit_started:
                        self._audit("finish")
                finally:
                    self._active_record = None
                    self._active_id = None
                if self.on_record is not None:
                    try:
                        self.on_record(dict(record))
                    except Exception:
                        raise GatewayError("MODEL_EVIDENCE_WRITE_FAILED") from None

    def parse(
        self, request: PipelineRequest, vocabulary: dict[str, Any], *, timeout: float
    ) -> object:
        return self._call("parse", parse_messages(request, vocabulary), timeout)

    def parse_clarification(
        self,
        request: PipelineRequest,
        vocabulary: dict[str, Any],
        answer: MetricId,
        *,
        timeout: float,
    ) -> object:
        if not isinstance(answer, MetricId):
            raise GatewayError("CLARIFICATION_ANSWER_INVALID")
        return self._call(
            "parse_clarification", parse_messages(request, vocabulary, answer), timeout
        )

    def generate(self, confirmed: ConfirmedSpec, linked: LinkedSchema, *, timeout: float) -> object:
        return self._call("generate", plan_messages(confirmed, linked), timeout)

    def repair(
        self,
        confirmed: ConfirmedSpec,
        linked: LinkedSchema,
        initial_plan: QueryPlan | None,
        error_code: str,
        *,
        timeout: float,
    ) -> object:
        if error_code not in REPAIR_CODES:
            raise GatewayError("MODEL_REPAIR_CODE_FORBIDDEN")
        return self._call(
            "repair", plan_messages(confirmed, linked, initial_plan, error_code), timeout
        )

    def close(self) -> None:
        with self._lock:
            self._closed = True
            hooks = self._client.event_hooks.get("response", [])
            if self._capture_response in hooks:
                hooks.remove(self._capture_response)
            request_hooks = self._client.event_hooks.get("request", [])
            if self._capture_request in request_hooks:
                request_hooks.remove(self._capture_request)
            if self._owns_client:
                self._client.close()
