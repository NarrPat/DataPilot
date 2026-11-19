"""Deterministic confirmation and plan shape checks; no model or gold access."""

from __future__ import annotations

import hashlib
import json
from zoneinfo import ZoneInfo

from datapilot.catalog import Catalog
from datapilot.contracts import (
    ConfirmedSpec,
    Filters,
    QueryPlan,
    QuerySpec,
    Region,
    Scope,
    TrustedContext,
)


class SpecError(Exception):
    pass


def confirm_spec(candidate: QuerySpec, context: TrustedContext, catalog: Catalog) -> ConfirmedSpec:
    metric = catalog.get_metric(candidate.metric_id, context.effective_scope)
    regions = candidate.filters.sales_regions
    if context.effective_scope == Scope.EAST:
        if any(region != Region.EAST for region in regions):
            raise SpecError("SCOPE_FORBIDDEN")
        regions = (Region.EAST,)
    zone = ZoneInfo("Asia/Shanghai")
    data = candidate.model_dump(mode="json")
    for key in ("start_at", "end_at", "reference_time"):
        data["time_range"][key] = getattr(candidate.time_range, key).astimezone(zone).isoformat()
    if candidate.comparison:
        for key in ("baseline_start_at", "baseline_end_at"):
            data["comparison"][key] = (
                getattr(candidate.comparison, key).astimezone(zone).isoformat()
            )
    data["filters"] = Filters(
        sales_regions=tuple(sorted(regions)),
        category_codes=tuple(sorted(candidate.filters.category_codes)),
    ).model_dump(mode="json")
    spec = QuerySpec.model_validate(data)
    envelope = {
        "spec": spec.model_dump(mode="json"),
        "spec_version": context.spec_version,
        "metric_version": metric.metric_version,
        "catalog_version": catalog.version,
        "data_version": context.data_version,
        "effective_scope": context.effective_scope.value,
        "authz_version": context.authz_version,
        "source_query_id": context.source_query_id,
    }
    digest = hashlib.sha256(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ConfirmedSpec.model_validate({**envelope, "canonical_spec_hash": digest})


def validate_plan(plan: QueryPlan, confirmed: ConfirmedSpec, catalog: Catalog) -> tuple[str, ...]:
    if catalog.version != confirmed.catalog_version:
        raise SpecError("CONFIG_INCOMPATIBLE")
    expected = catalog.get_metric(
        confirmed.spec.metric_id, confirmed.effective_scope
    ).result_contract
    comparison = confirmed.spec.comparison is not None
    if (plan.baseline_sql is not None) != comparison:
        raise SpecError("PLAN_BASELINE_MISMATCH")
    if plan.expected_columns.current != expected or plan.expected_columns.baseline != (
        expected if comparison else None
    ):
        raise SpecError("PLAN_RESULT_CONTRACT_MISMATCH")
    return expected
