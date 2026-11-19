"""Owner-filtered immutable reads and bounded administrator report import."""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

from datapilot.api.read_models import ReportDocument, ReportSummary
from datapilot.auth.errors import AppError
from datapilot.auth.models import Principal
from datapilot.catalog import Catalog, CatalogError
from datapilot.contracts import Scope, aware_datetime
from datapilot.tasks.contracts import Status
from datapilot.tasks.service import TaskService

CatalogState = Callable[[str, Scope], dict[str, Any]]


def encode_cursor(kind: str, actor: str, at: datetime, identity: UUID, status: str | None) -> str:
    payload = {
        "v": 1,
        "kind": kind,
        "actor": actor,
        "at": at.isoformat(),
        "id": str(identity),
        "status": status,
    }
    return (
        base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode())
        .rstrip(b"=")
        .decode()
    )


def decode_cursor(value: str, kind: str, actor: str, status: str | None) -> tuple[datetime, UUID]:
    try:
        if not value or len(value) > 2048 or not value.isascii():
            raise ValueError("CURSOR_INVALID")
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        data = json.loads(raw)
        if not isinstance(data, dict) or set(data) != {"v", "kind", "actor", "at", "id", "status"}:
            raise ValueError("CURSOR_INVALID")
        if (
            type(data["v"]) is not int
            or data["v"] != 1
            or (data["kind"], data["actor"], data["status"]) != (kind, actor, status)
        ):
            raise ValueError("CURSOR_INVALID")
        if not isinstance(data["at"], str) or not isinstance(data["id"], str):
            raise ValueError("CURSOR_INVALID")
        at = aware_datetime(data["at"])
        return at, UUID(data["id"])
    except (ValueError, TypeError, KeyError, UnicodeDecodeError):
        raise AppError("CURSOR_INVALID", 422) from None


def cursor_actor(actor: Principal) -> str:
    return (
        f"{actor.user_id}:{actor.authz_version}:{actor.scope.value if actor.scope else 'reports'}"
    )


def report_bytes(document: ReportDocument) -> bytes:
    return json.dumps(
        document.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()


def checked_report(row: dict[str, Any]) -> ReportDocument:
    try:
        document = ReportDocument.model_validate(row["document"])
        if (
            document.report_id != row["report_id"]
            or document.created_at != row["created_at"]
            or hashlib.sha256(report_bytes(document)).hexdigest() != row["document_sha256"]
        ):
            raise ValueError("REPORT_INTEGRITY_MISMATCH")
        return document
    except (ValueError, KeyError, TypeError):
        raise AppError("REPORT_UNAVAILABLE", 503) from None


class ReadService:
    def __init__(self, tasks: TaskService, catalog_state: CatalogState | None = None) -> None:
        self.tasks, self.catalog_state = tasks, catalog_state

    def catalog(self, actor: Principal, version: str | None, *, schema: bool) -> dict[str, Any]:
        with self.tasks.connect() as conn:
            current = self.tasks._actor(conn, actor, "catalog:read")
        if current.scope is None:
            raise AppError("FORBIDDEN", 403)
        scope = current.scope
        try:
            catalog = Catalog(version or self.tasks.catalog_version)
        except CatalogError:
            raise AppError("CATALOG_NOT_FOUND", 404) from None
        state: dict[str, Any] | None = None
        if self.catalog_state is not None:
            try:
                state = self.catalog_state(catalog.version, current.scope)
                if (
                    state["catalog_version"] != catalog.version
                    or state["definition_hash"] != catalog.definition_hash
                    or state["schema_fingerprint"] != catalog.schema_fingerprint
                    or state["role"] != "analyst_" + current.scope.value
                ):
                    raise ValueError("CATALOG_OBSERVATION_MISMATCH")
            except (KeyError, TypeError, ValueError, CatalogError):
                raise AppError("CATALOG_OBSERVATION_UNAVAILABLE", 503) from None
        metric_states = []
        for metric in catalog.list_visible_metrics(current.scope):
            entry = state.get("metrics", {}).get(metric.metric_id.value) if state else None
            ready = entry.get("ready") if isinstance(entry, dict) else None
            if ready is not None and type(ready) is not bool:
                raise AppError("CATALOG_OBSERVATION_UNAVAILABLE", 503)
            metric_states.append(
                {
                    "metric_id": metric.metric_id.value,
                    "ready": ready,
                    "code": "CATALOG_READINESS_UNKNOWN"
                    if ready is None
                    else "ready"
                    if ready
                    else "CATALOG_SCHEMA_MISMATCH",
                }
            )
        readiness = (
            "unknown"
            if any(m["ready"] is None for m in metric_states)
            else "ready"
            if all(m["ready"] for m in metric_states)
            else "not_ready"
        )
        result: dict[str, Any] = {
            "catalog_version": catalog.version,
            "definition_hash": catalog.definition_hash,
            "schema_fingerprint": catalog.schema_fingerprint,
            "effective_scope": current.scope.value,
            "readiness": {
                "state": readiness,
                "code": "CATALOG_READINESS_UNKNOWN"
                if readiness == "unknown"
                else "ready"
                if readiness == "ready"
                else "CATALOG_SCHEMA_MISMATCH",
            },
            "metric_readiness": metric_states,
        }
        if schema:
            allowed = catalog.get_allowed_schema(current.scope)

            def endpoint(value: str) -> str:
                table, column = value.split(".")
                return f"analytics_views.{table}_{scope.value}.{column}"

            result.update(
                views={v: dict(c) for v, c in allowed.views.items()},
                join_paths=[[endpoint(a), endpoint(b)] for a, b in allowed.join_paths],
                allowed_features=[
                    "single_metric",
                    "single_business_group",
                    "total",
                    "day",
                    "week",
                    "month",
                    "period_comparison",
                    "top_k_1_10",
                ],
            )
        else:
            result["metrics"] = [
                m.model_dump(mode="json") for m in catalog.list_visible_metrics(current.scope)
            ]
        return result

    def history(
        self,
        actor: Principal,
        *,
        limit: int = 20,
        cursor: str | None = None,
        status: Status | None = None,
    ) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= 50:
            raise AppError("LIMIT_INVALID", 422)
        with self.tasks.connect() as conn:
            current = self.tasks._actor(conn, actor, "history:read_self")
            if current.scope is None:
                raise AppError("FORBIDDEN", 403)
            state = status.value if status is not None else None
            after = (
                decode_cursor(cursor, "history", cursor_actor(current), state)
                if cursor is not None
                else None
            )
            clauses = ["owner_user_id=%s", "(effective_scope=%s OR %s='all')"]
            params: list[Any] = [current.user_id, current.scope.value, current.scope.value]
            if state is not None:
                clauses.append("status=%s")
                params.append(state)
            if after is not None:
                clauses.append("(api_received_at,query_id)<(%s,%s)")
                params.extend(after)
            rows = conn.execute(
                "SELECT query_id,question,status,status_version,spec_version,effective_scope,"
                "catalog_version,data_version,api_received_at,finished_at "
                "FROM app.query_runs WHERE "
                + " AND ".join(clauses)
                + " ORDER BY api_received_at DESC,query_id DESC LIMIT %s",
                (*params, limit + 1),
            ).fetchall()
            has_more, rows = len(rows) > limit, rows[:limit]
            return {
                "items": [
                    {
                        **{
                            k: v.isoformat()
                            if isinstance(v, datetime)
                            else str(v)
                            if isinstance(v, UUID)
                            else v
                            for k, v in dict(row).items()
                        },
                        "current_version_matches": row["catalog_version"]
                        == self.tasks.catalog_version
                        and row["data_version"] == self.tasks.current_data_version(conn),
                    }
                    for row in rows
                ],
                "next_cursor": encode_cursor(
                    "history",
                    cursor_actor(current),
                    rows[-1]["api_received_at"],
                    rows[-1]["query_id"],
                    state,
                )
                if has_more
                else None,
            }

    def reports(
        self, actor: Principal, *, limit: int = 20, cursor: str | None = None
    ) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= 50:
            raise AppError("LIMIT_INVALID", 422)
        with self.tasks.connect() as conn:
            current = self.tasks._actor(conn, actor, "reports:read")
            after = (
                decode_cursor(cursor, "reports", cursor_actor(current), None)
                if cursor is not None
                else None
            )
            rows = conn.execute(
                "SELECT * FROM app.eval_reports "
                + ("WHERE (created_at,report_id)<(%s,%s) " if after else "")
                + "ORDER BY created_at DESC,report_id DESC LIMIT %s",
                (*after, limit + 1) if after else (limit + 1,),
            ).fetchall()
            has_more, rows = len(rows) > limit, rows[:limit]
            items = []
            for row in rows:
                document = checked_report(dict(row))
                items.append(
                    ReportSummary.model_validate(
                        document.model_dump(include=set(ReportSummary.model_fields))
                    ).model_dump(mode="json")
                )
            return {
                "items": items,
                "next_cursor": encode_cursor(
                    "reports",
                    cursor_actor(current),
                    rows[-1]["created_at"],
                    rows[-1]["report_id"],
                    None,
                )
                if has_more
                else None,
            }

    def report(self, actor: Principal, report_id: UUID) -> dict[str, Any]:
        with self.tasks.connect() as conn:
            self.tasks._actor(conn, actor, "reports:read")
            row = conn.execute(
                "SELECT * FROM app.eval_reports WHERE report_id=%s", (report_id,)
            ).fetchone()
            if row is None:
                raise AppError("NOT_FOUND", 404)
            return {"report": checked_report(dict(row)).model_dump(mode="json")}
