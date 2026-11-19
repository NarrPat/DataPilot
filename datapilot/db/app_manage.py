"""Fixed local app migration/seed CLI. Secrets are read only from local env."""

import argparse
import json
import os

from datapilot.auth.seed import seed_users
from datapilot.db.application import migrate_app
from datapilot.db.bootstrap import connect, initialize


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", action="store_true")
    args = parser.parse_args()
    try:
        initialize()
        migrate_app()
        seeded = []
        if args.seed:
            value = os.environ.get("DP_SEED_PASSWORD", "")
            if not value:
                print(json.dumps({"code": "LOCAL_DEMO_PASSWORD_REQUIRED", "migrated": True}))
                return 1
            with connect("dp_app") as admin:
                seeded = seed_users(admin, value)
        with connect("dp_app", "app_runtime") as runtime:
            counts = runtime.execute("SELECT count(*) AS users FROM app.users").fetchone()
        print(
            json.dumps(
                {
                    "database": "dp_app",
                    "migrated": True,
                    "seeded": seeded,
                    "runtime": "app_runtime",
                    "counts": counts,
                }
            )
        )
        return 0
    except Exception as exc:
        print(json.dumps({"code": "APPLICATION_SETUP_FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
