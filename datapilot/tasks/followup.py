"""Validate one persisted base before copying semantic input into a new run."""

from __future__ import annotations

from typing import Any

from datapilot.auth.errors import AppError
from datapilot.catalog import Catalog, CatalogError
from datapilot.contracts import ConfirmedSpec, Scope, TrustedContext
from datapilot.specification import SpecError, confirm_spec


def validated_base(row: dict[str, Any], catalog_version: str, data_version: str) -> ConfirmedSpec:
    """The caller checks owner/current scope first; this restores no old authority."""
    try:
        confirmed = ConfirmedSpec.model_validate(row["confirmed_spec"])
        if confirmed.spec_version != row["spec_version"] or confirmed.spec_version < 1:
            raise ValueError("BASE_SPEC_VERSION_INVALID")
        original_context = TrustedContext(
            effective_scope=Scope(row["effective_scope"]),
            authz_version=row["authz_version"],
            data_version=row["data_version"],
            source_query_id=str(row["query_id"]),
            spec_version=row["spec_version"],
        )
        rebuilt = confirm_spec(confirmed.spec, original_context, Catalog(row["catalog_version"]))
        if rebuilt.model_dump() != confirmed.model_dump():
            raise ValueError("BASE_SPEC_IDENTITY_INVALID")
        if confirmed.catalog_version != catalog_version or confirmed.data_version != data_version:
            raise ValueError("BASE_CONFIG_INCOMPATIBLE")
        return confirmed
    except (KeyError, TypeError, ValueError, CatalogError, SpecError):
        raise AppError("BASE_SPEC_INCOMPATIBLE", 409) from None
