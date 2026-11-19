"""Explicit ordinary installation/upgrade, without a test image or destructive reset."""

from __future__ import annotations

import argparse
import json
import os

from datapilot.auth.seed import seed_users
from datapilot.data.fixtures import load_fixture
from datapilot.db.application import migrate_app
from datapilot.db.bootstrap import ROOT, connect, initialize, migrate, password
from datapilot.db.seed import load_seed


def install(mode: str, dataset: str = "tiny_v1") -> dict[str, object]:
    if mode not in {"initialize", "upgrade"}:
        raise ValueError("INSTALL_MODE_INVALID")
    if dataset not in {"tiny_v1", "demo_v1"} or (mode == "upgrade" and dataset != "tiny_v1"):
        raise ValueError("INSTALL_DATASET_INVALID")
    # Check inputs before the first connection or schema change. Neither mode
    # accepts a database, DSN, role, arbitrary SQL or an arbitrary fixture path.
    for role in ("dp_bootstrap", "app_runtime", "analyst_all", "analyst_east"):
        password(role)
    source = None
    seed = ""
    if mode == "initialize":
        source = load_fixture(ROOT / f"fixtures/datapilot/{dataset}.source.json")
        seed = os.environ.get("DP_SEED_PASSWORD", "")
        if not seed or len(seed.encode()) > 1024:
            raise ValueError("LOCAL_DEMO_PASSWORD_REQUIRED")
    initialize()
    migrate("dp_analytics")
    migrate_app()
    data: dict[str, object] | None = None
    accounts: list[dict[str, object]] = []
    if source is not None:
        # Same-version/hash re-use only; a different loaded dataset is refused.
        # No reset, drop, truncate, password rotation or old-history deletion.
        with connect("dp_analytics") as admin:
            data = load_seed(admin, source)
        with connect("dp_app") as admin:
            accounts = seed_users(admin, seed)
    return {
        "operation": mode,
        "schema_migrations": "completed",
        "data": data,
        "accounts": accounts,
        "query_processing_configuration": "not_modified",
        "model_calls": 0,
        "product_validation": "not_run",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("initialize", "upgrade"))
    parser.add_argument(
        "--dataset",
        choices=("tiny_v1", "demo_v1"),
        default="tiny_v1",
        help="Initial seed for an empty analytics database; never replaces existing data",
    )
    args = parser.parse_args()
    try:
        print(json.dumps(install(args.mode, args.dataset), ensure_ascii=False, default=str))
        return 0
    except Exception:
        # A failed multi-database installation can be partial. Do not erase it
        # or describe it as rolled back across databases; rerun the same mode.
        print(
            json.dumps(
                {
                    "code": "INSTALL_INCOMPLETE",
                    "operation": args.mode,
                    "product_validation": "not_run",
                    "may_be_partial": True,
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
