"""One bounded query fragment, with server-injected models and read-only execution.

Checkpoint writes are synchronous. Full results are returned and optionally sent
to a durable sink; only the sink reference is checkpointed. Recovery verifies
the original app authority and cumulative budget before continuing a safe saved
boundary. The app adapters own waiting/terminal projection and admission.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, TypedDict, cast
from uuid import NAMESPACE_URL, uuid5

from langchain_core.runnables import RunnableConfig, RunnableLambda
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, StateSnapshot, interrupt
from pydantic import ValidationError

from datapilot.catalog import Catalog, CatalogError, LinkedSchema
from datapilot.config_versions import MODEL_CONFIG_VERSION
from datapilot.contracts import (
    ConfirmedSpec,
    Dimension,
    MetricId,
    QueryPlan,
    QueryResult,
    QuerySpec,
)
from datapilot.executor import Cancellation, Deadline, ExecutedAttempt, ExecutionError
from datapilot.pipeline.contracts import (
    GRAPH_VERSION,
    ClarificationGateway,
    ClarificationNeeded,
    ClarificationSink,
    Control,
    Executor,
    ModelGateway,
    PhaseMeasure,
    PhaseSink,
    PipelineOutcome,
    PipelineRequest,
    ResultSink,
)
from datapilot.pipeline.repair import REPAIRABLE, repair_is_safe
from datapilot.results import ResultError
from datapilot.specification import SpecError, confirm_spec
from datapilot.sql_guard import GuardError, guard_plan
from datapilot.tasks.contracts import TERMINAL, Status


class PipelineState(TypedDict):
    graph_version: str
    query_id: str
    candidate: dict[str, Any] | None
    confirmed: dict[str, Any] | None
    plan: dict[str, Any] | None
    guard_decisions: list[dict[str, Any]]
    attempt_id: str | None
    result_ref: str | None
    clarification: dict[str, Any] | None
    status: str
    reason: str | None
    model_call_count: int
    repair_count: int
    graph_step_count: int
    invocation: dict[str, Any]
    catalog_version: str
    data_version: str
    clarification_count: int
    model_config_version: str


class ModelFailure(Exception):
    pass


NODE_NAMES = (
    "parse_spec",
    "clarify",
    "validate_spec",
    "link_schema",
    "generate_sql",
    "guard_plan",
    "execute_plan",
    "validate_result",
    "repair",
    "present",
)
COUNTERS = ("model_call_count", "repair_count", "graph_step_count")


def read_checkpoint(checkpointer: BaseCheckpointSaver[Any], query_id: str) -> StateSnapshot:
    """Reconstruct saved task metadata without constructing model or SQL dependencies."""
    inspection = StateGraph(PipelineState)
    for name in NODE_NAMES:
        inspection.add_node(name, lambda state: state)
        inspection.add_edge(name, END)
    inspection.add_edge(START, "parse_spec")
    return inspection.compile(checkpointer=checkpointer).get_state(
        {"configurable": {"thread_id": query_id}, "recursion_limit": 24}
    )


def validate_checkpoint_snapshot(
    snapshot: StateSnapshot, request: PipelineRequest, catalog: Catalog
) -> PipelineState | None:
    """Validate checkpoint identity against app authority before any graph writes.

    Application reservation counters may exceed the last checkpoint after an
    interrupted call. The inverse is corrupt/incompatible state and is refused.
    """
    if not snapshot.values:
        return None
    value = snapshot.values
    try:
        if (
            not isinstance(value, dict)
            or value["graph_version"] != GRAPH_VERSION
            or value["query_id"] != request.context.source_query_id
            or value["catalog_version"] != catalog.version
            or value["data_version"] != request.context.data_version
            or value["model_config_version"] != MODEL_CONFIG_VERSION
            or request.model_config_version != MODEL_CONFIG_VERSION
        ):
            raise ValueError("PIPELINE_RECOVERY_CONFIG_MISMATCH")
        saved = PipelineRequest.model_validate(value["invocation"])
        ignored = set(COUNTERS)
        if saved.model_dump(exclude=ignored) != request.model_dump(exclude=ignored):
            raise ValueError("PIPELINE_RECOVERY_CONTEXT_MISMATCH")
        for key in COUNTERS:
            if type(value[key]) is not int or not 0 <= value[key] <= getattr(request, key):
                raise ValueError("PIPELINE_RECOVERY_COUNTER_MISMATCH")
        Status(value["status"])
        confirmed = None
        if value["confirmed"] is not None:
            confirmed = ConfirmedSpec.model_validate(value["confirmed"])
            rebuilt = confirm_spec(confirmed.spec, request.context, catalog)
            if (
                confirmed.spec.time_range.reference_time != request.reference_time
                or rebuilt.model_dump() != confirmed.model_dump()
            ):
                raise ValueError("PIPELINE_RECOVERY_CONTEXT_MISMATCH")
        output = QueryPlan.model_validate(value["plan"]) if value["plan"] is not None else None
        if value["result_ref"] is not None or value["status"] == Status.COMPLETED.value:
            # A reusable durable result claims validation already succeeded.
            # Revalidate its exact plan before copying it into public history;
            # ordinary pending guard/repair nodes retain their normal routing.
            if output is None or confirmed is None:
                raise ValueError("PIPELINE_RECOVERY_CONFIG_MISMATCH")
            guard_plan(output, confirmed, catalog)
    except (KeyError, TypeError, ValidationError, CatalogError, SpecError, GuardError) as exc:
        raise ValueError("PIPELINE_RECOVERY_CONFIG_MISMATCH") from exc
    return cast(PipelineState, value)


def validate_checkpoint_clarification(
    snapshot: StateSnapshot,
    request: PipelineRequest,
    catalog: Catalog,
    clarification: dict[str, Any] | None,
) -> PipelineState | None:
    """Keep an accepted finite input authoritative across checkpoint recovery.

    Missing checkpoint state is safe to restart only before a durable input
    exists. An answered interrupt may resume; an already consumed answer must
    still be represented by its exact input and selected metric in saved state.
    """
    state = validate_checkpoint_snapshot(snapshot, request, catalog)
    if clarification is None:
        return state
    try:
        payload = clarification["payload"]
        if (
            state is None
            or state["clarification"] != payload
            or str(clarification["clarification_id"]) != payload["clarification_id"]
            or clarification["spec_version"] != payload["spec_version"]
            or payload["spec_version"] != request.context.spec_version
        ):
            raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
        answer = clarification["answer"]
        if answer is None:
            if state["clarification_count"] != 0:
                raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
            return state
        if (
            not isinstance(answer, dict)
            or set(answer) != {"metric_id"}
            or answer["metric_id"] not in payload["options"]
        ):
            raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
        if (
            not snapshot.next
            and not snapshot.interrupts
            and Status(state["status"]) in TERMINAL - {Status.COMPLETED}
        ):
            # A failed/needs_refine answered call can itself have a projection
            # gap; retaining its recorded failure never reruns the lost input.
            return state
        if state["clarification_count"] == 0:
            proof = waiting_checkpoint(snapshot, request.context.source_query_id)
            if proof is None or proof["clarification"] != payload:
                raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
            if any(proof[key] > getattr(request, key) for key in COUNTERS):
                raise ValueError("PIPELINE_RECOVERY_COUNTER_MISMATCH")
            return state
        if state["clarification_count"] != 1 or snapshot.interrupts:
            raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
        candidate = QuerySpec.model_validate(state["candidate"])
        if candidate.metric_id.value != answer["metric_id"]:
            raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
        if state["confirmed"] is not None:
            confirmed = ConfirmedSpec.model_validate(state["confirmed"])
            if confirmed.spec.metric_id.value != answer["metric_id"]:
                raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH")
    except (KeyError, TypeError, ValidationError) as exc:
        raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINT_MISMATCH") from exc
    return state


def waiting_checkpoint(snapshot: StateSnapshot, query_id: str) -> dict[str, Any] | None:
    if len(snapshot.interrupts) != 1:
        return None
    value, state = snapshot.interrupts[0].value, snapshot.values
    if not isinstance(value, dict) or not isinstance(state, dict) or not value.get("clarification"):
        return None
    checkpoint_id = snapshot.config.get("configurable", {}).get("checkpoint_id")
    if not isinstance(checkpoint_id, str):
        return None
    return {
        "query_id": query_id,
        "checkpoint_id": checkpoint_id,
        "graph_version": state["graph_version"],
        "catalog_version": state["catalog_version"],
        "data_version": state["data_version"],
        "clarification": value["clarification"],
        **{key: max(state[key], value[key]) for key in COUNTERS},
    }


def terminal_checkpoint_outcome(
    snapshot: StateSnapshot, request: PipelineRequest, catalog: Catalog
) -> PipelineOutcome | None:
    state = validate_checkpoint_snapshot(snapshot, request, catalog)
    if (
        state is None
        or snapshot.next
        or snapshot.interrupts
        or Status(state["status"]) not in TERMINAL
    ):
        return None
    return PipelineOutcome(
        Status(state["status"]),
        state["reason"],
        ConfirmedSpec.model_validate(state["confirmed"]) if state["confirmed"] else None,
        QueryPlan.model_validate(state["plan"]) if state["plan"] else None,
        None,
        state["attempt_id"],
        state["result_ref"],
        state["clarification"],
        request.model_call_count,
        request.repair_count,
        request.graph_step_count,
        (),
        snapshot.config.get("configurable", {}).get("checkpoint_id"),
    )


def execution_status(code: str) -> Status:
    if code == "CANCELED":
        return Status.CANCELED
    if code == "AUTHORIZATION_REVOKED":
        return Status.REJECTED
    if code == "RESULT_TOO_LARGE":
        return Status.NEEDS_REFINE
    if code in {"PROCESSING_TIMEOUT", "PROCESSING_DEADLINE", "STATEMENT_TIMEOUT", "LOCK_TIMEOUT"}:
        return Status.TIMED_OUT
    return Status.FAILED


@dataclass
class Fragment:
    request: PipelineRequest
    deadline: Deadline
    cancellation: Cancellation
    control: Control
    phase: PhaseSink
    sink: ResultSink | None
    model_calls: int
    repairs: int
    steps: int
    trace: list[dict[str, Any]] = field(default_factory=list)
    linked: LinkedSchema | None = None
    attempt: ExecutedAttempt | None = None
    result: dict[str, Any] | None = None

    def check(self, boundary: str) -> None:
        self.cancellation.check()
        self.deadline.remaining()
        self.control(boundary)
        self.cancellation.check()
        self.deadline.remaining()

    def counters(self) -> dict[str, int]:
        return {
            "model_call_count": self.model_calls,
            "repair_count": self.repairs,
            "graph_step_count": self.steps,
        }


class QueryPipeline:
    def __init__(
        self,
        gateway: ModelGateway,
        executor: Executor,
        catalog: Catalog,
        *,
        checkpointer: BaseCheckpointSaver[Any] | None = None,
        measure: PhaseMeasure | None = None,
    ) -> None:
        self.gateway, self.executor, self.catalog = gateway, executor, catalog
        self.checkpointer = checkpointer
        self.measure = measure or (lambda _phase: nullcontext())

    def run(
        self,
        request: PipelineRequest,
        *,
        deadline: Deadline,
        cancellation: Cancellation | None = None,
        control: Control = lambda _boundary: None,
        on_phase: PhaseSink = lambda _status: None,
        on_result: ResultSink | None = None,
        on_clarification: ClarificationSink | None = None,
    ) -> PipelineOutcome:
        return self._invoke(
            request,
            deadline=deadline,
            cancellation=cancellation,
            control=control,
            on_phase=on_phase,
            on_result=on_result,
            on_clarification=on_clarification,
        )

    @staticmethod
    def _request_identity(request: PipelineRequest) -> dict[str, Any]:
        return request.model_dump(exclude={"model_call_count", "repair_count", "graph_step_count"})

    @staticmethod
    def _config(query_id: str) -> RunnableConfig:
        return {"configurable": {"thread_id": query_id}, "recursion_limit": 24}

    def _read_snapshot(self, query_id: str) -> StateSnapshot:
        if self.checkpointer is None:
            raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINTER_REQUIRED")
        return read_checkpoint(self.checkpointer, query_id)

    def waiting_snapshot(self, query_id: str) -> dict[str, Any] | None:
        """Read only actual outstanding interrupt state, never infer it from status."""
        if self.checkpointer is None:
            return None
        return waiting_checkpoint(self._read_snapshot(query_id), query_id)

    def checkpoint_outcome(self, request: PipelineRequest) -> PipelineOutcome | None:
        return terminal_checkpoint_outcome(
            self._read_snapshot(request.context.source_query_id), request, self.catalog
        )

    def recover(
        self,
        request: PipelineRequest,
        *,
        deadline: Deadline,
        cancellation: Cancellation | None = None,
        control: Control = lambda _boundary: None,
        on_phase: PhaseSink = lambda _status: None,
        on_result: ResultSink | None = None,
        on_clarification: ClarificationSink | None = None,
    ) -> PipelineOutcome:
        """Continue registered durable nodes; ephemeral results are never reused."""
        snapshot = self._read_snapshot(request.context.source_query_id)
        state = validate_checkpoint_snapshot(snapshot, request, self.catalog)
        if state is not None and snapshot.interrupts:
            raise ValueError("PIPELINE_RECOVERY_INTERRUPT_REQUIRES_ANSWER")
        completed = terminal_checkpoint_outcome(snapshot, request, self.catalog)
        if completed is not None:
            return completed
        return self._invoke(
            request,
            deadline=deadline,
            cancellation=cancellation,
            control=control,
            on_phase=on_phase,
            on_result=on_result,
            on_clarification=on_clarification,
            recovering=state is not None,
        )

    def get_resume_request(
        self,
        query_id: str,
        *,
        model_call_count: int | None = None,
        repair_count: int | None = None,
        graph_step_count: int | None = None,
    ) -> PipelineRequest:
        proof = self.waiting_snapshot(query_id)
        if proof is None:
            raise ValueError("PIPELINE_NOT_WAITING")
        state = self._read_snapshot(query_id).values
        counts = {
            key: max(proof[key], number or 0)
            for key, number in (
                ("model_call_count", model_call_count),
                ("repair_count", repair_count),
                ("graph_step_count", graph_step_count),
            )
        }
        return PipelineRequest.model_validate({**state["invocation"], **counts})

    def resume(
        self,
        request: PipelineRequest,
        answer: MetricId,
        *,
        clarification_id: str,
        expected_spec_version: int,
        deadline: Deadline,
        cancellation: Cancellation | None = None,
        control: Control = lambda _boundary: None,
        on_phase: PhaseSink = lambda _status: None,
        on_result: ResultSink | None = None,
        on_clarification: ClarificationSink | None = None,
    ) -> PipelineOutcome:
        proof = self.waiting_snapshot(request.context.source_query_id)
        if proof is None:
            raise ValueError("PIPELINE_NOT_WAITING")
        saved = self.get_resume_request(request.context.source_query_id)
        if (
            proof["graph_version"] != GRAPH_VERSION
            or proof["catalog_version"] != self.catalog.version
            or proof["data_version"] != request.context.data_version
            or self._request_identity(saved) != self._request_identity(request)
        ):
            raise ValueError("PIPELINE_RESUME_CONTEXT_MISMATCH")
        clarification = proof["clarification"]
        if (
            clarification_id != clarification["clarification_id"]
            or expected_spec_version != clarification["spec_version"]
            or answer not in clarification["options"]
        ):
            raise ValueError("PIPELINE_CLARIFICATION_INPUT_INVALID")
        if on_clarification is None:
            raise ValueError("PIPELINE_CLARIFICATION_PREPARE_REQUIRED")
        effective = self.get_resume_request(
            request.context.source_query_id,
            model_call_count=request.model_call_count,
            repair_count=request.repair_count,
            graph_step_count=request.graph_step_count,
        )
        return self._invoke(
            effective,
            deadline=deadline,
            cancellation=cancellation,
            control=control,
            on_phase=on_phase,
            on_result=on_result,
            on_clarification=on_clarification,
            resume_value={
                "clarification_id": clarification_id,
                "spec_version": expected_spec_version,
                "answer": answer.value,
            },
        )

    def _invoke(
        self,
        request: PipelineRequest,
        *,
        deadline: Deadline,
        cancellation: Cancellation | None = None,
        control: Control = lambda _boundary: None,
        on_phase: PhaseSink = lambda _status: None,
        on_result: ResultSink | None = None,
        on_clarification: ClarificationSink | None = None,
        resume_value: dict[str, Any] | None = None,
        recovering: bool = False,
    ) -> PipelineOutcome:
        if request.model_config_version != MODEL_CONFIG_VERSION:
            raise ValueError("PIPELINE_MODEL_CONFIG_MISMATCH")
        if on_clarification is not None and self.checkpointer is None:
            raise ValueError("PIPELINE_CLARIFICATION_CHECKPOINTER_REQUIRED")
        fragment = Fragment(
            request,
            deadline,
            cancellation or Cancellation(),
            control,
            on_phase,
            on_result,
            request.model_call_count,
            request.repair_count,
            request.graph_step_count,
        )

        def failure(state: PipelineState, code: str, status: Status) -> PipelineState:
            fragment.trace.append({"event": "stopped", "code": code, "status": status.value})
            return cast(
                PipelineState,
                {**state, **fragment.counters(), "reason": code, "status": status.value},
            )

        def node(
            name: str, operation: Callable[[PipelineState], PipelineState]
        ) -> Callable[[PipelineState], PipelineState]:
            def perform(state: PipelineState) -> PipelineState:
                try:
                    if fragment.steps >= 24:
                        return failure(state, "GRAPH_STEP_BUDGET", Status.FAILED)
                    fragment.check("before_node:" + name)
                    fragment.steps += 1
                    fragment.trace.append({"event": "node", "name": name, "step": fragment.steps})
                    result = operation(state)
                    return cast(PipelineState, {**result, **fragment.counters()})
                except ExecutionError as exc:
                    return failure(state, exc.code, execution_status(exc.code))
                except ModelFailure as exc:
                    code = str(exc)
                    status = Status.TIMED_OUT if code == "MODEL_TIMEOUT" else Status.FAILED
                    return failure(state, code, status)

            def invoke(state: PipelineState) -> PipelineState:
                with self.measure("node:" + name):
                    return perform(state)

            return invoke

        def model_call(method: str, call: Callable[[float], object]) -> object:
            if fragment.model_calls >= 4:
                raise ModelFailure("MODEL_CALL_BUDGET")
            fragment.check("before_" + method)
            fragment.model_calls += 1
            if method == "repair":
                fragment.repairs += 1
            fragment.trace.append(
                {
                    "event": "model_attempt",
                    "method": method,
                    "call": fragment.model_calls,
                    "usage": None,
                }
            )
            try:
                with self.measure("model:" + method):
                    value = call(min(30.0, fragment.deadline.remaining()))
            except TimeoutError:
                raise ModelFailure("MODEL_TIMEOUT") from None
            except Exception:
                # Gateway/provider details, request content and stack traces are not feedback.
                raise ModelFailure("MODEL_CALL_FAILED") from None
            fragment.check("after_" + method)
            return value

        def confirmed(state: PipelineState) -> ConfirmedSpec:
            return ConfirmedSpec.model_validate(state["confirmed"])

        def plan(state: PipelineState) -> QueryPlan | None:
            return QueryPlan.model_validate(state["plan"]) if state["plan"] is not None else None

        def parse(state: PipelineState) -> PipelineState:
            previous = request.previous_confirmed
            if previous and (
                previous.catalog_version != self.catalog.version
                or previous.data_version != request.context.data_version
            ):
                return failure(state, "CONFIG_INCOMPATIBLE", Status.NEEDS_REFINE)
            fragment.phase(Status.PARSING)
            vocabulary = {
                "catalog_version": self.catalog.version,
                "metrics": [
                    m.model_dump(mode="json")
                    for m in self.catalog.list_visible_metrics(request.context.effective_scope)
                ],
                "scope": request.context.effective_scope.value,
                "reference_time": request.reference_time.isoformat(),
            }
            value = model_call(
                "parse", lambda timeout: self.gateway.parse(request, vocabulary, timeout=timeout)
            )
            try:
                if isinstance(value, dict) and value.get("kind") == "clarification":
                    clarification = ClarificationNeeded.model_validate(value).model_dump(
                        mode="json", exclude={"kind"}
                    )
                    clarification["spec_version"] = request.context.spec_version
                    clarification["clarification_id"] = str(
                        uuid5(
                            NAMESPACE_URL,
                            f"datapilot:{request.context.source_query_id}:"
                            f"{request.context.spec_version}:clarification",
                        )
                    )
                    return cast(
                        PipelineState,
                        {
                            **state,
                            "clarification": clarification,
                            "status": Status.WAITING.value,
                            "reason": "SEMANTIC_CLARIFICATION_REQUIRED",
                        },
                    )
                candidate = QuerySpec.model_validate(value)
            except ValidationError:
                return failure(state, "SPEC_OUTPUT_INVALID", Status.NEEDS_REFINE)
            return {**state, "candidate": candidate.model_dump(mode="json")}

        def clarify(state: PipelineState) -> PipelineState:
            fragment.phase(Status.PARSING)
            if on_clarification is None:
                return state
            clarification = state["clarification"]
            if clarification is None or state["clarification_count"] >= 1:
                return failure(state, "CLARIFICATION_UNRESOLVED", Status.NEEDS_REFINE)
            fragment.check("before_clarification")
            with self.measure("persist_clarification"):
                on_clarification(clarification)
            answer_envelope = interrupt({"clarification": clarification, **fragment.counters()})
            if (
                not isinstance(answer_envelope, dict)
                or set(answer_envelope) != {"clarification_id", "spec_version", "answer"}
                or answer_envelope["clarification_id"] != clarification["clarification_id"]
                or answer_envelope["spec_version"] != clarification["spec_version"]
                or answer_envelope["answer"] not in clarification["options"]
            ):
                return failure(state, "CLARIFICATION_UNRESOLVED", Status.NEEDS_REFINE)
            answer = MetricId(answer_envelope["answer"])
            vocabulary = {
                "catalog_version": self.catalog.version,
                "metrics": [
                    m.model_dump(mode="json")
                    for m in self.catalog.list_visible_metrics(request.context.effective_scope)
                ],
                "scope": request.context.effective_scope.value,
                "reference_time": request.reference_time.isoformat(),
                "clarification": clarification,
            }
            gateway = cast(ClarificationGateway, self.gateway)
            value = model_call(
                "parse_clarification",
                lambda timeout: gateway.parse_clarification(
                    request, vocabulary, answer, timeout=timeout
                ),
            )
            try:
                candidate = QuerySpec.model_validate(value)
            except ValidationError:
                return failure(state, "CLARIFICATION_UNRESOLVED", Status.NEEDS_REFINE)
            if candidate.metric_id != answer:
                return failure(state, "CLARIFICATION_UNRESOLVED", Status.NEEDS_REFINE)
            return {
                **state,
                "candidate": candidate.model_dump(mode="json"),
                "clarification_count": 1,
                "status": Status.PARSING.value,
                "reason": None,
            }

        def validate_spec(state: PipelineState) -> PipelineState:
            candidate = QuerySpec.model_validate(state["candidate"])
            if candidate.time_range.reference_time != request.reference_time:
                return failure(state, "REFERENCE_TIME_MISMATCH", Status.NEEDS_REFINE)
            try:
                value = confirm_spec(candidate, request.context, self.catalog)
            except SpecError as exc:
                code = "FORBIDDEN_SCOPE" if str(exc) == "SCOPE_FORBIDDEN" else "SPEC_UNSUPPORTED"
                return failure(state, code, Status.REJECTED)
            except CatalogError:
                return failure(state, "CATALOG_SCHEMA_MISMATCH", Status.NEEDS_REFINE)
            return {**state, "confirmed": value.model_dump(mode="json")}

        def link(state: PipelineState) -> PipelineState:
            value = confirmed(state)
            fragment.linked = self.catalog.link_schema(
                value.spec.metric_id,
                value.effective_scope,
                category=value.spec.group_by == Dimension.CATEGORY
                or bool(value.spec.filters.category_codes),
            )
            return state

        def linked(state: PipelineState) -> LinkedSchema:
            if fragment.linked is None:
                # LinkedSchema is invocation-local metadata, never pickled into
                # checkpoint state or assumed to survive a worker replacement.
                link(state)
            assert fragment.linked is not None
            return fragment.linked

        def generation_output(state: PipelineState, value: object) -> PipelineState:
            try:
                output = QueryPlan.model_validate(value)
            except ValidationError:
                # Every present period must be inspected independently. A missing
                # current SQL must not hide an unsafe baseline. Unknown fields
                # are conservatively rejected, never forwarded as repair text.
                if isinstance(value, dict):
                    if set(value) - {"current_sql", "baseline_sql", "expected_columns"}:
                        return failure(state, "PLAN_FIELDS_FORBIDDEN", Status.REJECTED)
                    expected = self.catalog.get_metric(
                        confirmed(state).spec.metric_id, confirmed(state).effective_scope
                    ).result_contract
                    for period in ("current_sql", "baseline_sql"):
                        sql = value.get(period)
                        if sql is None:
                            continue
                        if not isinstance(sql, str):
                            return failure(state, "SQL_UNSAFE_MODEL_OUTPUT", Status.REJECTED)
                        try:
                            provisional = QueryPlan.model_validate(
                                {
                                    "current_sql": sql,
                                    "baseline_sql": None,
                                    "expected_columns": {"current": expected, "baseline": None},
                                }
                            )
                        except ValidationError:
                            return failure(state, "SQL_UNSAFE_MODEL_OUTPUT", Status.REJECTED)
                        if not repair_is_safe(provisional, confirmed(state), self.catalog):
                            return failure(state, "SQL_UNSAFE_MODEL_OUTPUT", Status.REJECTED)
                return {**state, "plan": None, "reason": "PLAN_OUTPUT_INVALID"}
            return {**state, "plan": output.model_dump(mode="json"), "reason": None}

        def generate(state: PipelineState) -> PipelineState:
            fragment.phase(Status.GENERATING)
            value = model_call(
                "generate",
                lambda timeout: self.gateway.generate(
                    confirmed(state), linked(state), timeout=timeout
                ),
            )
            return generation_output(state, value)

        def guard(state: PipelineState) -> PipelineState:
            fragment.phase(Status.GUARDING)
            output = plan(state)
            if output is None:
                code = "PLAN_OUTPUT_INVALID"
            else:
                try:
                    checked = guard_plan(output, confirmed(state), self.catalog)
                    return {
                        **state,
                        "reason": None,
                        "guard_decisions": [
                            {
                                "allowed": True,
                                "sql_hash": item.sql_sha256,
                                "complexity": item.complexity,
                            }
                            for item in checked
                        ],
                    }
                except (GuardError, SpecError) as exc:
                    code = exc.code if isinstance(exc, GuardError) else str(exc)
            return repair_or_stop(state, code)

        def repair_or_stop(state: PipelineState, code: str) -> PipelineState:
            if code in REPAIRABLE and repair_is_safe(plan(state), confirmed(state), self.catalog):
                if fragment.repairs >= 1:
                    return failure(state, "SQL_REPAIR_EXHAUSTED", Status.FAILED)
                return {**state, "reason": code, "status": Status.REPAIRING.value}
            if code == "RESULT_TOO_LARGE":
                return failure(state, code, Status.NEEDS_REFINE)
            if code in {"STATEMENT_TIMEOUT", "LOCK_TIMEOUT", "PROCESSING_TIMEOUT"}:
                return failure(state, code, Status.TIMED_OUT)
            if code == "CANCELED":
                return failure(state, code, Status.CANCELED)
            if code.startswith("SQL_"):
                return failure(state, code, Status.REJECTED)
            return failure(state, code, Status.FAILED)

        def execute(state: PipelineState) -> PipelineState:
            fragment.phase(Status.PLANNING)
            fragment.phase(Status.EXECUTING)
            fragment.check("before_execute")
            output = plan(state)
            assert output is not None
            try:
                with self.measure("sql_attempt"):
                    fragment.attempt = self.executor.execute(
                        confirmed(state),
                        output,
                        deadline=deadline,
                        cancellation=fragment.cancellation,
                    )
            except ExecutionError as exc:
                # Only explicit structure SQLSTATEs can feed repair; never raw database messages.
                code = (
                    "SQL_AGGREGATION_ERROR"
                    if exc.code == "SQL_EXECUTION_ERROR" and (exc.sqlstate in {"42703", "42803"})
                    else exc.code
                )
                return repair_or_stop(state, code)
            except ResultError as exc:
                return repair_or_stop(state, str(exc))
            except CatalogError:
                return failure(state, "CATALOG_SCHEMA_MISMATCH", Status.NEEDS_REFINE)
            fragment.check("after_execute")
            return {
                **state,
                "attempt_id": fragment.attempt.attempt_id,
                "reason": None,
                "status": Status.VALIDATING.value,
            }

        def validate_result(state: PipelineState) -> PipelineState:
            if recovering and fragment.attempt is None:
                if state["result_ref"] is not None:
                    # Validation and immutable result storage completed in an
                    # earlier process. Only the exact durable reference is used.
                    return state
                fragment.trace.append({"event": "recovery_reexecute_full_plan"})
                state = execute(state)
                if fragment.attempt is None:
                    return state
            fragment.phase(Status.VALIDATING)
            assert fragment.attempt is not None
            try:
                fragment.result = QueryResult.model_validate(fragment.attempt.result).model_dump(
                    mode="json"
                )
            except ValidationError:
                return failure(state, "RESULT_DTO_INVALID", Status.FAILED)
            expected = confirmed(state)
            if (
                fragment.attempt.confirmed is not None and fragment.attempt.confirmed != expected
            ) or (
                fragment.attempt.query_plan is not None
                and fragment.attempt.query_plan != plan(state)
            ):
                return failure(state, "CONFIG_INCOMPATIBLE", Status.NEEDS_REFINE)
            definition = self.catalog.get_metric(expected.spec.metric_id, expected.effective_scope)
            if (
                fragment.result["metric_id"] != expected.spec.metric_id.value
                or fragment.result["unit"] != definition.unit
            ):
                return failure(state, "RESULT_CONTEXT_MISMATCH", Status.FAILED)
            if fragment.sink is not None:
                fragment.check("before_result")
                with self.measure("persist_result"):
                    reference = fragment.sink(fragment.attempt)
                if not isinstance(reference, str) or not 1 <= len(reference) <= 200:
                    raise ValueError("DURABLE_RESULT_REFERENCE_REQUIRED")
                fragment.check("after_result")
                return {**state, "result_ref": reference}
            return state

        def repair(state: PipelineState) -> PipelineState:
            fragment.phase(Status.REPAIRING)
            if fragment.repairs >= 1:
                return failure(state, "SQL_REPAIR_EXHAUSTED", Status.FAILED)
            # model_call reserves only after its call-budget/control gates pass.
            value = model_call(
                "repair",
                lambda timeout: self.gateway.repair(
                    confirmed(state),
                    linked(state),
                    plan(state),
                    cast(str, state["reason"]),
                    timeout=timeout,
                ),
            )
            return generation_output({**state, "status": Status.GUARDING.value}, value)

        def present(state: PipelineState) -> PipelineState:
            if recovering and state["result_ref"] is not None and fragment.attempt is None:
                fragment.phase(Status.VALIDATING)
            fragment.check("before_present")
            return {**state, "status": Status.COMPLETED.value, "reason": None}

        builder = StateGraph(PipelineState)
        functions = {
            "parse_spec": parse,
            "clarify": clarify,
            "validate_spec": validate_spec,
            "link_schema": link,
            "generate_sql": generate,
            "guard_plan": guard,
            "execute_plan": execute,
            "validate_result": validate_result,
            "repair": repair,
            "present": present,
        }
        for name, function in functions.items():
            builder.add_node(name, RunnableLambda(node(name, function)))
        builder.add_edge(START, "parse_spec")
        next_nodes = {
            "parse_spec": "validate_spec",
            "clarify": "validate_spec",
            "validate_spec": "link_schema",
            "link_schema": "generate_sql",
            "generate_sql": "guard_plan",
            "guard_plan": "execute_plan",
            "execute_plan": "validate_result",
            "validate_result": "present",
            "repair": "guard_plan",
        }

        def route(name: str) -> Callable[[PipelineState], str]:
            def choose(state: PipelineState) -> str:
                status = Status(state["status"])
                if (
                    status == Status.WAITING
                    and name == "parse_spec"
                    and on_clarification is not None
                ):
                    return "clarify"
                if status in {
                    Status.FAILED,
                    Status.REJECTED,
                    Status.NEEDS_REFINE,
                    Status.TIMED_OUT,
                    Status.CANCELED,
                    Status.WAITING,
                }:
                    return END
                if status == Status.REPAIRING:
                    return "repair"
                return next_nodes[name]

            return choose

        for name in next_nodes:
            builder.add_conditional_edges(name, route(name))
        builder.add_edge("present", END)
        graph = builder.compile(checkpointer=self.checkpointer, name=GRAPH_VERSION)
        config = self._config(request.context.source_query_id)
        if (
            resume_value is None
            and not recovering
            and self.checkpointer is not None
            and graph.get_state(config).values
        ):
            raise ValueError("PIPELINE_CHECKPOINT_ALREADY_EXISTS_RESUME_ADAPTER_REQUIRED")
        initial: PipelineState = {
            "graph_version": GRAPH_VERSION,
            "query_id": request.context.source_query_id,
            "candidate": None,
            "confirmed": None,
            "plan": None,
            "guard_decisions": [],
            "attempt_id": None,
            "result_ref": None,
            "clarification": None,
            "status": Status.PARSING.value,
            "reason": None,
            "model_call_count": request.model_call_count,
            "repair_count": request.repair_count,
            "graph_step_count": request.graph_step_count,
            "invocation": request.model_dump(
                mode="json", exclude={"model_call_count", "repair_count", "graph_step_count"}
            ),
            "catalog_version": self.catalog.version,
            "data_version": request.context.data_version,
            "clarification_count": 0,
            "model_config_version": request.model_config_version,
        }
        final = graph.invoke(
            Command(resume=resume_value)
            if resume_value is not None
            else None
            if recovering
            else initial,
            config,
            durability="sync" if self.checkpointer else None,
        )
        checkpoint_id = None
        if self.checkpointer is not None:
            checkpoint_id = (
                graph.get_state(config).config.get("configurable", {}).get("checkpoint_id")
            )
        waiting = (
            self.waiting_snapshot(request.context.source_query_id) if self.checkpointer else None
        )
        return PipelineOutcome(
            Status.WAITING if waiting is not None else Status(final["status"]),
            final["reason"],
            ConfirmedSpec.model_validate(final["confirmed"]) if final["confirmed"] else None,
            QueryPlan.model_validate(final["plan"]) if final["plan"] else None,
            fragment.result if final["status"] == Status.COMPLETED else None,
            final["attempt_id"],
            final["result_ref"],
            waiting["clarification"] if waiting is not None else final["clarification"],
            fragment.model_calls,
            fragment.repairs,
            fragment.steps,
            tuple(fragment.trace),
            checkpoint_id,
        )
