"""Small, immutable metric directory; JSON syntax is a strict subset of YAML.

Only registered calculation keys are descriptive data. This module never loads
formulas or executes SQL from the directory. Schema inspection uses the SAME
analyst role as execution and checks affected metrics independently.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

from pydantic import Field

from datapilot.contracts import Category, ClosedModel, Dimension, Grain, MetricId, Region, Scope

CATALOG_ROOT = Path(__file__).resolve().parents[1] / "apps/datapilot/catalog"
DEFAULT_CATALOG = "dp-catalog-v2-five"
SNAPSHOTS = {"dp-catalog-v1-four": "metrics.v1.yaml", DEFAULT_CATALOG: "metrics.v2.yaml"}
CalcKey = Literal[
    "paid_item_cents",
    "distinct_paid_orders",
    "succeeded_refund_cents",
    "paid_minus_succeeded_refund_cents",
]


class CatalogError(Exception):
    pass


class MetricDefinition(ClosedModel):
    metric_id: MetricId
    metric_version: str
    name: str
    aliases: tuple[str, ...]
    fact_grain: str
    time_fields: tuple[str, ...]
    unit: Literal["CNY", "orders"]
    numerator_key: CalcKey
    denominator_key: CalcKey | None
    required_columns: dict[str, tuple[str, ...]]
    allowed_join_paths: tuple[tuple[str, str], ...]
    allowed_dimensions: tuple[Dimension, ...]
    allowed_grains: tuple[Grain, ...]
    filter_contract: dict[str, str]
    result_contract: tuple[str, ...]
    zero_policy: Literal["zero_total_empty_groups", "null_no_paid_orders"]
    rollup_policy: Literal[
        "sum_cents",
        "distinct_orders_no_cross_category_sum",
        "sum_independent_axes_allow_negative",
        "ratio_of_summed_components_no_cross_category_sum",
    ]


class Directory(ClosedModel):
    catalog_version: str
    schema_version: str
    stage: str
    regions: dict[Region, tuple[str, ...]]
    categories: dict[Category, tuple[str, ...]]
    metrics: tuple[MetricDefinition, ...] = Field(min_length=1, max_length=5)


@dataclass(frozen=True)
class LinkedSchema:
    views: Mapping[str, Mapping[str, str]]
    join_paths: tuple[tuple[str, str], ...]
    definition_hash: str
    schema_fingerprint: str


class Catalog:
    def __init__(self, version: str = DEFAULT_CATALOG) -> None:
        if version not in SNAPSHOTS:
            raise CatalogError("CATALOG_VERSION_UNKNOWN")
        content = (CATALOG_ROOT / SNAPSHOTS[version]).read_bytes()
        directory = Directory.model_validate_json(content)
        schema = json.loads((CATALOG_ROOT / "schema.v1.yaml").read_text(encoding="utf-8"))
        ids = [m.metric_id for m in directory.metrics]
        if directory.catalog_version != version or len(set(ids)) != len(ids):
            raise CatalogError("CATALOG_REGISTRATION_MISMATCH")
        if directory.schema_version != schema["schema_version"]:
            raise CatalogError("CATALOG_SCHEMA_VERSION_MISMATCH")
        if set(directory.regions) != set(Region) or set(directory.categories) != set(Category):
            raise CatalogError("CATALOG_ENUM_MISMATCH")
        if version == DEFAULT_CATALOG and set(ids) != set(MetricId):
            raise CatalogError("CATALOG_METRIC_ENUM_MISMATCH")
        self.version = version
        # JSON is private; callers receive validated copies or recursively read-only schema maps.
        self._directory_json = directory.model_dump_json()
        self._schema_json = json.dumps(schema, sort_keys=True)
        self.definition_hash = hashlib.sha256(content).hexdigest()
        self.schema_fingerprint = hashlib.sha256(self._schema_json.encode()).hexdigest()

    def list_visible_metrics(self, scope: Scope) -> tuple[MetricDefinition, ...]:
        Scope(scope)
        return Directory.model_validate_json(self._directory_json).metrics

    def get_metric(self, metric_id: str, scope: Scope) -> MetricDefinition:
        for metric in self.list_visible_metrics(scope):
            if metric.metric_id == metric_id:
                return metric
        raise CatalogError("METRIC_NOT_READY")

    def get_allowed_schema(self, scope: Scope) -> LinkedSchema:
        suffix = Scope(scope).value
        schema = json.loads(self._schema_json)
        return LinkedSchema(
            MappingProxyType(
                {
                    f"analytics_views.{table}_{suffix}": MappingProxyType(columns)
                    for table, columns in schema["views"].items()
                }
            ),
            tuple(tuple(pair) for pair in schema["joins"]),
            self.definition_hash,
            self.schema_fingerprint,
        )

    def link_schema(self, metric_id: str, scope: Scope, *, category: bool = False) -> LinkedSchema:
        metric = self.get_metric(metric_id, scope)
        columns = {t: set(c) for t, c in metric.required_columns.items()}
        if category:
            columns.setdefault("orders", set()).add("order_id")
            columns.setdefault("order_items", set()).update(("order_id", "product_id"))
            columns["products"] = {"product_id", "category_code"}
        allowed = self.get_allowed_schema(scope)
        prefix = "analytics_views."
        views = {
            f"{prefix}{table}_{scope.value}": MappingProxyType(
                {c: allowed.views[f"{prefix}{table}_{scope.value}"][c] for c in sorted(names)}
            )
            for table, names in columns.items()
        }
        joins = tuple(
            pair
            for pair in metric.allowed_join_paths
            if all(endpoint.split(".")[0] in columns for endpoint in pair)
        )
        return LinkedSchema(
            MappingProxyType(views), joins, self.definition_hash, self.schema_fingerprint
        )

    def inspect_schema(self, connection: Any, scope: Scope) -> dict[str, Any]:
        """Metadata queries are trusted startup operations, never model SQL."""
        expected_role = "analyst_" + Scope(scope).value
        identity = connection.execute("SELECT current_user AS role").fetchone()
        if identity["role"] != expected_role:
            raise CatalogError("CATALOG_REQUIRES_EXECUTION_ROLE")
        rows = connection.execute(
            "SELECT table_name, column_name, udt_name FROM information_schema.columns "
            "WHERE table_schema = 'analytics_views' ORDER BY table_name, ordinal_position"
        ).fetchall()
        actual = {
            f"analytics_views.{row['table_name']}.{row['column_name']}": row["udt_name"]
            for row in rows
        }
        states = {}
        for metric in self.list_visible_metrics(scope):
            # Both business dimensions are advertised, so their dependencies must exist too.
            linked = self.link_schema(metric.metric_id, scope, category=True)
            drift = [
                f"{view}.{column}"
                for view, columns in linked.views.items()
                for column, dtype in columns.items()
                if actual.get(f"{view}.{column}") != dtype
            ]
            states[metric.metric_id.value] = {
                "ready": not drift,
                "code": "ready" if not drift else "CATALOG_SCHEMA_MISMATCH",
                "mismatched_columns": sorted(drift),
            }
        return {
            "role": identity["role"],
            "catalog_version": self.version,
            "definition_hash": self.definition_hash,
            "schema_fingerprint": self.schema_fingerprint,
            "observed_schema_hash": hashlib.sha256(
                json.dumps(actual, sort_keys=True).encode()
            ).hexdigest(),
            "metrics": states,
        }

    def require_ready(self, metric_id: str, scope: Scope, state: dict[str, Any]) -> None:
        if (
            state.get("catalog_version") != self.version
            or state.get("definition_hash") != self.definition_hash
            or state.get("role") != "analyst_" + scope.value
        ):
            raise CatalogError("CONFIG_INCOMPATIBLE")
        entry = state.get("metrics", {}).get(metric_id)
        if not entry or not entry["ready"]:
            raise CatalogError("CATALOG_SCHEMA_MISMATCH")


def get_metric(metric_id: str, catalog_version: str, effective_scope: Scope) -> MetricDefinition:
    return Catalog(catalog_version).get_metric(metric_id, effective_scope)


def list_visible_metrics(
    catalog_version: str, effective_scope: Scope
) -> tuple[MetricDefinition, ...]:
    return Catalog(catalog_version).list_visible_metrics(effective_scope)


def get_allowed_schema(catalog_version: str, effective_scope: Scope) -> LinkedSchema:
    return Catalog(catalog_version).get_allowed_schema(effective_scope)
