"""Immutable execution provenance, not an online semantic/gold scorer.

The executor supplies its actual frozen inputs and transaction observations.
Recovery must match that record, rather than trusting a checkpoint's new claim.
Hashes detect mismatch/corruption; a DB administrator can rewrite both inputs.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field

from datapilot.catalog import Catalog
from datapilot.contracts import ClosedModel, ConfirmedSpec, QueryPlan, QueryResult
from datapilot.executor import ExecutedAttempt, parameters
from datapilot.pipeline.contracts import GRAPH_VERSION, PipelineRequest
from datapilot.specification import confirm_spec
from datapilot.sql_guard import add_probe_limit, guard_plan

Hash = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Positive = Annotated[int, Field(strict=True, ge=1)]


def canonical(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return canonical(value.model_dump(mode="python"))
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("ARTIFACT_NAIVE_TIME")
        return value.astimezone(UTC).isoformat(timespec="microseconds")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("ARTIFACT_NONFINITE_NUMBER")
        return str(value)
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("ARTIFACT_KEY_INVALID")
        return {key: canonical(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [canonical(item) for item in value]
    if value is None or isinstance(value, str | bool | int | float):
        return value
    raise ValueError("ARTIFACT_VALUE_UNSUPPORTED")


def encoded(value: Any) -> bytes:
    return json.dumps(
        canonical(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf8")


def fingerprint(value: Any) -> str:
    return hashlib.sha256(encoded(value)).hexdigest()


def invocation_hash(request: PipelineRequest) -> str:
    # Reservations can exceed the saved CP after a crash. They are cumulative
    # app facts, not the immutable input identity of an earlier SQL attempt.
    return fingerprint(
        request.model_dump(exclude={"model_call_count", "repair_count", "graph_step_count"})
    )


class BoundStatement(ClosedModel):
    period: Literal["current", "baseline"]
    input_sql_sha256: Hash
    guarded_sql_sha256: Hash
    executed_sql_sha256: Hash
    parameters_sha256: Hash


class BoundTransaction(ClosedModel):
    backend_pid: Positive
    snapshot: str = Field(min_length=1, max_length=256)
    role: Literal["analyst_all", "analyst_east"]
    isolation: Literal["repeatable read"]
    read_only: Literal["on"]
    timezone: Literal["Asia/Shanghai"]


class ArtifactBinding(ClosedModel):
    binding_version: Literal[1]
    attempt_id: UUID
    query_id: UUID
    execution_generation: Positive
    owner_user_id: UUID
    graph_version: str = Field(min_length=1, max_length=128)
    model_config_version: str = Field(min_length=1, max_length=128)
    invocation_sha256: Hash
    confirmed: ConfirmedSpec
    query_plan: QueryPlan
    statements: tuple[BoundStatement, ...] = Field(min_length=1, max_length=2)
    transaction: BoundTransaction
    result_sha256: Hash


def create_binding(
    attempt: ExecutedAttempt, row: dict[str, Any], request: PipelineRequest, catalog: Catalog
) -> ArtifactBinding:
    confirmed, plan = attempt.confirmed, attempt.query_plan
    if confirmed is None or plan is None:
        raise ValueError("ARTIFACT_EXECUTION_INPUTS_REQUIRED")
    if (
        confirm_spec(confirmed.spec, request.context, catalog) != confirmed
        or confirmed.spec.time_range.reference_time != request.reference_time
        or row["graph_version"] != GRAPH_VERSION
        or row["model_config_version"] != request.model_config_version
    ):
        raise ValueError("ARTIFACT_INPUT_CONTEXT_MISMATCH")
    checked = guard_plan(plan, confirmed, catalog)
    if len(checked) != len(attempt.plans):
        raise ValueError("ARTIFACT_STATEMENTS_MISMATCH")
    statements = []
    for index, (guarded, observed) in enumerate(zip(checked, attempt.plans, strict=True)):
        executed = add_probe_limit(guarded, confirmed, catalog)
        period: Literal["current", "baseline"] = "baseline" if index else "current"
        expected_params = parameters(confirmed, baseline=index == 1)
        if (
            observed["attempt_id"] != attempt.attempt_id
            or observed["period"] != period
            or observed["input_hash"] != executed.input_sha256
            or observed["sql_hash"] != executed.sql_sha256
            or fingerprint(observed["bindings"]) != fingerprint(expected_params)
            or observed["snapshot"] != attempt.settings["snapshot"]
            or observed["pid"] != attempt.settings["pid"]
            or observed["role"] != "analyst_" + confirmed.effective_scope.value
            or observed["role"] != attempt.settings["role"]
        ):
            raise ValueError("ARTIFACT_EXECUTION_OBSERVATION_MISMATCH")
        statements.append(
            BoundStatement(
                period=period,
                input_sql_sha256=guarded.input_sha256,
                guarded_sql_sha256=guarded.sql_sha256,
                executed_sql_sha256=executed.sql_sha256,
                parameters_sha256=fingerprint(expected_params),
            )
        )
    result = QueryResult.model_validate(attempt.result)
    definition = catalog.get_metric(confirmed.spec.metric_id, confirmed.effective_scope)
    if result.metric_id != confirmed.spec.metric_id or result.unit != definition.unit:
        raise ValueError("ARTIFACT_RESULT_CONTEXT_MISMATCH")
    binding = ArtifactBinding(
        binding_version=1,
        attempt_id=UUID(attempt.attempt_id),
        query_id=row["query_id"],
        execution_generation=row["execution_generation"],
        owner_user_id=row["owner_user_id"],
        graph_version=row["graph_version"],
        model_config_version=row["model_config_version"],
        invocation_sha256=invocation_hash(request),
        confirmed=confirmed,
        query_plan=plan,
        statements=tuple(statements),
        result_sha256=fingerprint(result),
        transaction=BoundTransaction(
            backend_pid=attempt.settings["pid"],
            snapshot=attempt.settings["snapshot"],
            role=attempt.settings["role"],
            isolation=attempt.settings["isolation"],
            read_only=attempt.settings["read_only"],
            timezone=attempt.settings["timezone"],
        ),
    )
    if len(encoded(binding)) > 32768:
        raise ValueError("ARTIFACT_BINDING_SIZE_LIMIT")
    return binding


def verify_binding(
    saved: dict[str, Any],
    row: dict[str, Any],
    request: PipelineRequest,
    confirmed: ConfirmedSpec,
    plan: QueryPlan,
) -> QueryResult:
    """One gate shared by ordinary completion, metadata projection and recovery."""
    binding = ArtifactBinding.model_validate(saved.get("binding"))
    result = QueryResult.model_validate(saved["result"])
    if (
        fingerprint(binding) != saved.get("binding_sha256")
        or binding.query_id != row["query_id"]
        or binding.query_id != saved["query_id"]
        or binding.attempt_id != saved["attempt_id"]
        or binding.execution_generation != saved["execution_generation"]
        or binding.owner_user_id != row["owner_user_id"]
        or binding.graph_version != row["graph_version"]
        or binding.model_config_version != row["model_config_version"]
        or binding.invocation_sha256 != invocation_hash(request)
        or binding.confirmed != confirmed
        or binding.query_plan != plan
        or binding.result_sha256 != fingerprint(result)
    ):
        raise ValueError("ARTIFACT_BINDING_MISMATCH")
    return result
