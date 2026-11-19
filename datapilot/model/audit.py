"""Whitelist-only, committed-before-network model evidence; no prompt/result text.

An intent consumes an optional admin-registered probe budget conservatively.
Client body-write evidence is distinct from provider receipt and billing.
Crashes leave finished_at/usage unknown; readers must keep those rows.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Annotated, Any, Literal, cast
from uuid import UUID

from psycopg.types.json import Jsonb
from pydantic import Field

from datapilot.contracts import ClosedModel
from datapilot.model.diagnostics import TransportFailure, TransportPhase

if TYPE_CHECKING:
    from datapilot.worker.fragment import FragmentStore

Count = Annotated[int, Field(strict=True, ge=0)]
Hash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class MessageFingerprint(ClosedModel):
    role: Literal["system", "human", "ai"]
    sha256: Hash
    utf8_bytes: Count


class Usage(ClosedModel):
    prompt_tokens: Count | None = None
    completion_tokens: Count | None = None
    total_tokens: Count | None = None
    prompt_cache_hit_tokens: Count | None = None
    prompt_cache_miss_tokens: Count | None = None
    reasoning_tokens: Count | None = None


class Metadata(ClosedModel):
    provider: Literal["deepseek"]
    configured_model: str = Field(min_length=1, max_length=128)
    prompt_version: str = Field(min_length=1, max_length=128)
    dependency_versions: dict[
        Literal["langchain-deepseek", "langchain-core", "openai", "httpx", "httpcore"],
        Annotated[str, Field(min_length=1, max_length=64)],
    ]
    model_config_sha256: Hash
    timeout_seconds: float = Field(gt=0, le=30, allow_inf_nan=False)
    max_tokens: Annotated[int, Field(strict=True, ge=1, le=4096)]
    request_messages: tuple[MessageFingerprint, ...] = Field(min_length=1, max_length=4)
    http_status: Annotated[int, Field(strict=True, ge=100, le=599)] | None = None
    server_request_id: str | None = Field(default=None, max_length=256)
    completion_id: str | None = Field(default=None, max_length=256)
    response_model: str | None = Field(default=None, max_length=256)
    finish_reason: str | None = Field(default=None, max_length=256)
    usage: Usage | None = None
    response_content_sha256: Hash | None = None
    response_content_utf8_bytes: Count | None = None
    schema_valid: bool | None = None
    validation_code: str | None = Field(default=None, max_length=128)
    error_code: str | None = Field(default=None, max_length=128)
    duration_ms: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    transport_phase: TransportPhase | None = None
    transport_failure_type: TransportFailure | None = None


def projected_metadata(record: dict[str, Any]) -> dict[str, Any]:
    return Metadata.model_validate(
        {name: record[name] for name in Metadata.model_fields if name in record}
    ).model_dump(mode="json")


class ModelAudit:
    def __init__(self, store: FragmentStore, *, budget_id: UUID | None = None) -> None:
        self.store, self.budget_id = store, budget_id

    def __call__(self, event: str, record: dict[str, Any]) -> None:
        if event not in {"intent", "dispatch", "transport_sent", "response", "finish"}:
            raise ValueError("MODEL_AUDIT_EVENT_INVALID")
        invocation = UUID(record["local_request_id"])
        stage = record["stage"]
        if stage not in {"parse", "parse_clarification", "generate", "repair"}:
            raise ValueError("MODEL_AUDIT_STAGE_INVALID")
        metadata = projected_metadata(record)
        milestone = {
            "intent": "intended_at",
            "dispatch": "dispatch_prepared_at",
            "transport_sent": "client_body_sent_at",
            "response": "response_received_at",
            "finish": "finished_at",
        }[event]
        patch = {**metadata, milestone: record["observed_at"]}
        if len(json.dumps(patch).encode()) > 16384:
            raise ValueError("MODEL_AUDIT_SIZE_LIMIT")
        store = self.store
        with store.serialized_transaction():
            conn = store.lease.connection
            if event == "intent":
                row = store.locked_row()
                store.check_row(row)
                ordinal = row["model_call_count"]
                if not 1 <= ordinal <= 4:
                    raise ValueError("MODEL_AUDIT_RESERVATION_REQUIRED")
                if self.budget_id is not None:
                    budget = conn.execute(
                        "SELECT * FROM app.model_audit_budgets WHERE budget_id=%s "
                        "AND deadline_at>clock_timestamp() FOR UPDATE",
                        (self.budget_id,),
                    ).fetchone()
                    if not budget or budget["reserved_attempts"] >= budget["attempt_limit"]:
                        raise ValueError("MODEL_PROBE_BUDGET_EXHAUSTED")
                    queries = conn.execute(
                        "SELECT DISTINCT query_id FROM app.model_invocations WHERE budget_id=%s",
                        (self.budget_id,),
                    ).fetchall()
                    if (
                        store.lease.query_id not in {item["query_id"] for item in queries}
                        and len(queries) >= budget["query_limit"]
                    ):
                        raise ValueError("MODEL_PROBE_QUERY_LIMIT")
                    if metadata["max_tokens"] > 2048:
                        raise ValueError("MODEL_PROBE_TOKEN_LIMIT")
                    conn.execute(
                        "UPDATE app.model_audit_budgets SET reserved_attempts=reserved_attempts+1 "
                        "WHERE budget_id=%s",
                        (self.budget_id,),
                    )
                patch.update(
                    schema_version=1,
                    query_id=str(store.lease.query_id),
                    execution_generation=store.generation,
                    call_ordinal=ordinal,
                    request_id=str(row["accepted_request_id"])
                    if row["accepted_request_id"]
                    else None,
                    invocation_id=str(invocation),
                    stage=stage,
                    process_id=os.getpid(),
                    graph_version=row["graph_version"],
                    model_config_version=row["model_config_version"],
                    actor_id=str(row["owner_user_id"]),
                    effective_scope=row["effective_scope"],
                    authz_version=row["authz_version"],
                    catalog_version=row["catalog_version"],
                    data_version=row["data_version"],
                    spec_version=row["spec_version"],
                    budget_id=str(self.budget_id) if self.budget_id else None,
                    cost_cny=None,
                    price_version=None,
                    transport_semantics="client_write_only",
                )
                conn.execute(
                    "INSERT INTO app.model_invocations(invocation_id,query_id,execution_generation,"
                    "call_ordinal,request_id,budget_id,stage,started_at,document) "
                    "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        invocation,
                        store.lease.query_id,
                        store.generation,
                        ordinal,
                        row["accepted_request_id"],
                        self.budget_id,
                        stage,
                        record["started_at"],
                        Jsonb(patch),
                    ),
                )
            else:
                updated = conn.execute(
                    "UPDATE app.model_invocations SET document=document||%s,"
                    "finished_at=CASE WHEN %s THEN %s::timestamptz ELSE finished_at END "
                    "WHERE invocation_id=%s AND query_id=%s AND execution_generation=%s "
                    "AND stage=%s AND finished_at IS NULL RETURNING invocation_id",
                    (
                        Jsonb(patch),
                        event == "finish",
                        record.get("ended_at"),
                        invocation,
                        store.lease.query_id,
                        store.generation,
                        stage,
                    ),
                ).fetchone()
                if not updated:
                    raise ValueError("MODEL_AUDIT_INTENT_REQUIRED")


def summarize_usage(documents: list[dict[str, Any]]) -> dict[str, Any]:
    """Keep every unique invocation, including failed and unfinished attempts."""
    identities = [row["invocation_id"] for row in documents]
    if len(set(identities)) != len(identities):
        raise ValueError("DUPLICATE_MODEL_INVOCATION")
    known = []
    for row in documents:
        usage = row.get("usage")
        if isinstance(usage, dict):
            values = [
                usage.get(key) for key in ("prompt_tokens", "completion_tokens", "total_tokens")
            ]
            if (
                all(type(v) is int and v >= 0 for v in values)
                and cast(int, values[0]) + cast(int, values[1]) == values[2]
            ):
                known.append(usage)
    return {
        "known_subset_definition": (
            "consistent prompt/completion/total triads; partial fields retained in documents"
        ),
        "invocation_count": len(documents),
        "dispatch_prepared_count": sum(
            row.get("dispatch_prepared_at") is not None for row in documents
        ),
        "client_body_sent_count": sum(
            row.get("client_body_sent_at") is not None for row in documents
        ),
        "response_count": sum(row.get("response_received_at") is not None for row in documents),
        "unfinished_count": sum(row.get("finished_at") is None for row in documents),
        "usage_known_numerator": len(known),
        "usage_known_denominator": len(documents),
        "known_input_tokens": sum(row["prompt_tokens"] for row in known) if known else None,
        "known_output_tokens": sum(row["completion_tokens"] for row in known) if known else None,
        "total_input_tokens": sum(row["prompt_tokens"] for row in known)
        if len(known) == len(documents) and known
        else None,
        "total_output_tokens": sum(row["completion_tokens"] for row in known)
        if len(known) == len(documents) and known
        else None,
        "cost_cny": None,
        "price_version": None,
    }
