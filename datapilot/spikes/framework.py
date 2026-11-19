"""LangChain/LangGraph protocol spike with an injected model and read-only tool.

This does not implement the product worker, admission, HTTP resume or event store.
The Postgres helper deliberately accepts only isolated I006 application eval DBs.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, Literal, TypedDict, cast
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from datapilot.contracts import MetricId

GRAPH_VERSION = "framework-spike-v1"
EVAL_DATABASE = re.compile(
    r"dp_app_eval_(?:i00[6-9]|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019)_[a-z0-9_]{1,40}\Z"
)
CHECKPOINT_SCHEMA = "framework_spike"
type EventSink = Callable[[dict[str, Any]], None]


class SpikeError(Exception):
    """A stable code; model/connection internals are not an error response."""


class ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SpikeRequest(ClosedModel):
    query_id: UUID
    question: str = Field(min_length=1, max_length=2000)


class SpikeCandidate(ClosedModel):
    """Minimal structured candidate, deliberately not the complete QuerySpec."""

    metric_id: MetricId
    needs_clarification: bool


class ResumeInput(ClosedModel):
    clarification_id: UUID
    spec_version: int = Field(ge=1)
    status_version: int = Field(ge=1)
    answer: MetricId


class LookupArguments(ClosedModel):
    metric_id: MetricId


class MetricObservation(ClosedModel):
    metric_id: MetricId
    catalog_version: str
    unit: str


class Versions(ClosedModel):
    graph_version: str = GRAPH_VERSION
    catalog_version: str = "spike-catalog-v1"
    data_version: str = "spike-data-v1"


class SpikeState(TypedDict):
    query_id: str
    question: str
    clarification_id: str
    spec_version: int
    status_version: int
    versions: dict[str, Any]
    candidate: dict[str, Any] | None
    status: str
    model_call_count: int
    tool_call_count: int
    observation: dict[str, Any] | None


class FrameworkSpike:
    """Compile anew for each fragment; persisted state lives in the checkpointer."""

    def __init__(
        self,
        *,
        model: BaseChatModel,
        lookup_tool: BaseTool,
        checkpointer: BaseCheckpointSaver[Any],
        on_event: EventSink,
        versions: Versions | None = None,
        model_mode: Literal["stub"] = "stub",
    ) -> None:
        if model_mode != "stub":
            raise SpikeError("LIVE_SPIKE_NOT_IMPLEMENTED")
        if lookup_tool.name != "lookup_metric":
            raise SpikeError("SPIKE_TOOL_NOT_ALLOWED")
        self.model = model
        self.lookup_tool = lookup_tool
        self.on_event = on_event
        self.versions = versions if versions is not None else Versions()
        self.structured = model.with_structured_output(SpikeCandidate)
        builder = StateGraph(SpikeState)
        builder.add_node("parse_candidate", self._parse)
        builder.add_node("clarify", self._clarify)
        builder.add_node("lookup", self._lookup)
        builder.add_edge(START, "parse_candidate")
        builder.add_conditional_edges(
            "parse_candidate",
            lambda state: "clarify" if self._candidate(state).needs_clarification else "lookup",
        )
        builder.add_edge("clarify", "lookup")
        builder.add_edge("lookup", END)
        self.graph = builder.compile(checkpointer=checkpointer, name=GRAPH_VERSION)

    @staticmethod
    def config(query_id: UUID) -> RunnableConfig:
        return {"configurable": {"thread_id": str(query_id)}, "recursion_limit": 24}

    def start(self, source: object) -> dict[str, Any]:
        try:
            request = SpikeRequest.model_validate(source)
        except ValidationError:
            raise SpikeError("SPIKE_REQUEST_INVALID") from None
        if self.graph.get_state(self.config(request.query_id)).values:
            raise SpikeError("SPIKE_QUERY_ALREADY_EXISTS")
        state: SpikeState = {
            "query_id": str(request.query_id),
            "question": request.question,
            "clarification_id": str(uuid5(NAMESPACE_URL, f"datapilot/spike/{request.query_id}/1")),
            "spec_version": 1,
            "status_version": 1,
            "versions": self.versions.model_dump(),
            "candidate": None,
            "status": "parsing",
            "model_call_count": 0,
            "tool_call_count": 0,
            "observation": None,
        }
        return dict(self.graph.invoke(state, self.config(request.query_id)))

    def resume(self, query_id: UUID, source: object) -> dict[str, Any]:
        try:
            answer = ResumeInput.model_validate(source)
        except ValidationError:
            raise SpikeError("SPIKE_RESUME_INVALID") from None
        snapshot = self.graph.get_state(self.config(query_id))
        if not snapshot.values or snapshot.next != ("clarify",):
            raise SpikeError("SPIKE_NOT_WAITING")
        state = cast(SpikeState, snapshot.values)
        if state["versions"] != self.versions.model_dump():
            raise SpikeError("CONFIG_INCOMPATIBLE")
        self._check_answer(state, answer)
        return dict(
            self.graph.invoke(Command(resume=answer.model_dump(mode="json")), self.config(query_id))
        )

    def _emit(self, stage: str, state: SpikeState) -> None:
        self.on_event(
            {
                "stage": stage,
                "query_id": state["query_id"],
                "clarification_id": state["clarification_id"],
                "spec_version": state["spec_version"],
                "status_version": state["status_version"],
                "model_mode": "stub",
                "real_model_calls": 0,
                "model_call_count": state["model_call_count"],
            }
        )

    @staticmethod
    def _candidate(state: SpikeState) -> SpikeCandidate:
        return SpikeCandidate.model_validate(state["candidate"])

    def _parse(self, state: SpikeState) -> dict[str, Any]:
        self._emit("parse.enter", state)
        try:
            candidate = self.structured.invoke(
                [
                    SystemMessage(content="Select a metric candidate; do not invent a result."),
                    HumanMessage(content=state["question"]),
                ]
            )
            if not isinstance(candidate, SpikeCandidate):
                raise SpikeError("STRUCTURED_CANDIDATE_INVALID")
        except (ValidationError, ValueError):
            raise SpikeError("STRUCTURED_CANDIDATE_INVALID") from None
        return {
            "candidate": candidate.model_dump(mode="json"),
            "model_call_count": state["model_call_count"] + 1,
            "status_version": state["status_version"] + 1,
            "status": "needs_clarification" if candidate.needs_clarification else "generating",
        }

    @staticmethod
    def _check_answer(state: SpikeState, answer: ResumeInput) -> None:
        if (
            str(answer.clarification_id) != state["clarification_id"]
            or answer.spec_version != state["spec_version"]
            or answer.status_version != state["status_version"]
        ):
            raise SpikeError("SPIKE_INPUT_VERSION_CONFLICT")

    def _clarify(self, state: SpikeState) -> dict[str, Any]:
        # This entry audit deliberately repeats on resume. No task/action/input is
        # created here: the stable ID already belongs to the prior checkpoint.
        self._emit("clarify.enter", state)
        raw = interrupt(
            {
                "clarification_id": state["clarification_id"],
                "spec_version": state["spec_version"],
                "status_version": state["status_version"],
                "choices": [metric.value for metric in MetricId],
            }
        )
        answer = ResumeInput.model_validate(raw)
        self._check_answer(state, answer)
        candidate = self._candidate(state).model_copy(update={"metric_id": answer.answer})
        return {
            "candidate": candidate.model_dump(mode="json"),
            "spec_version": state["spec_version"] + 1,
            "status_version": state["status_version"] + 1,
            "status": "generating",
        }

    def _lookup(self, state: SpikeState) -> dict[str, Any]:
        self._emit("lookup.enter", state)
        candidate = self._candidate(state)
        message = self.model.bind_tools([self.lookup_tool], tool_choice="lookup_metric").invoke(
            [HumanMessage(content=f"Read the visible definition of {candidate.metric_id.value}.")]
        )
        if not isinstance(message, AIMessage) or len(message.tool_calls) != 1:
            raise SpikeError("SPIKE_TOOL_CALL_INVALID")
        call = message.tool_calls[0]
        if call["name"] != self.lookup_tool.name:
            raise SpikeError("SPIKE_TOOL_NOT_ALLOWED")
        try:
            arguments = LookupArguments.model_validate(call["args"])
        except ValidationError:
            raise SpikeError("SPIKE_TOOL_ARGUMENTS_INVALID") from None
        if arguments.metric_id != candidate.metric_id:
            raise SpikeError("SPIKE_TOOL_METRIC_MISMATCH")
        try:
            observation = MetricObservation.model_validate(
                self.lookup_tool.invoke(arguments.model_dump(mode="json"))
            )
        except ValidationError:
            raise SpikeError("SPIKE_TOOL_OBSERVATION_INVALID") from None
        if (
            observation.metric_id != candidate.metric_id
            or observation.catalog_version != self.versions.catalog_version
        ):
            raise SpikeError("SPIKE_TOOL_OBSERVATION_MISMATCH")
        return {
            "observation": observation.model_dump(mode="json"),
            "tool_call_count": state["tool_call_count"] + 1,
            "model_call_count": state["model_call_count"] + 1,
            "status_version": state["status_version"] + 1,
            "status": "completed",
        }


@contextmanager
def postgres_saver(
    connection: psycopg.Connection[dict[str, Any]], query_id: UUID, *, setup: bool = False
) -> Iterator[PostgresSaver]:
    """Same fixed connection owns the session advisory lock and PostgresSaver.

    A closed/broken connection is never replaced here. Schema DDL is supplied by
    the test DB factory; runtime setup is confined to this experiment's schema.
    """
    if not connection.autocommit:
        raise SpikeError("SPIKE_AUTOCOMMIT_CONNECTION_REQUIRED")
    if not EVAL_DATABASE.fullmatch(connection.info.dbname) or connection.info.user != "app_runtime":
        raise SpikeError("SPIKE_APP_EVAL_CONNECTION_REQUIRED")
    previous = connection.execute("SHOW search_path").fetchone()
    assert previous is not None
    connection.execute("SET search_path TO framework_spike, pg_catalog")
    lock_key = int.from_bytes(
        hashlib.sha256(f"datapilot/framework-spike/{query_id}".encode()).digest()[:8],
        "big",
        signed=True,
    )
    acquired = False
    previous_prepare_threshold = connection.prepare_threshold
    try:
        schema = connection.execute("SELECT current_schema() AS name").fetchone()
        if not schema or schema["name"] != CHECKPOINT_SCHEMA:
            raise SpikeError("SPIKE_CHECKPOINT_SCHEMA_REQUIRED")
        connection.prepare_threshold = 0
        locked = connection.execute(
            "SELECT pg_try_advisory_lock(%s) AS acquired", (lock_key,)
        ).fetchone()
        if not locked or not locked["acquired"]:
            raise SpikeError("SPIKE_FRAGMENT_LOCKED")
        acquired = True
        saver = PostgresSaver(connection)
        if setup:
            saver.setup()
        yield saver
    finally:
        if not connection.closed and not connection.broken:
            if acquired:
                connection.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
            connection.execute(
                "SELECT set_config('search_path', %s, false)", (previous["search_path"],)
            )
            connection.prepare_threshold = previous_prepare_threshold
