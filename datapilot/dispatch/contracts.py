"""Small broker payload: no question, SQL, credential, graph state or gold."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field

from datapilot.contracts import ClosedModel


class DispatchMessage(ClosedModel):
    query_id: UUID
    execution_generation: Annotated[int, Field(strict=True, ge=1)]
    kind: Literal["create", "resume", "recover_fragment"]

    @property
    def delivery_id(self) -> str:
        """Correlation only: Celery task IDs do not provide execution deduplication."""
        key = f"datapilot:{self.query_id}:{self.execution_generation}:{self.kind}"
        return str(uuid5(NAMESPACE_URL, key))
