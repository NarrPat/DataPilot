"""Operator-only timing export using fixed runtime credentials, never a model.

Run inside this project's control/runtime environment. The CLI accepts a query
UUID, not a DSN, role, SQL statement or caller-supplied timing/model state.
"""

from __future__ import annotations

import argparse
import json
from uuid import UUID

from datapilot.runtime.config import RuntimeConfig
from datapilot.runtime.worker import app_connection
from datapilot.tasks.timing import read_server_timing


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query-id", type=UUID, required=True)
    args = parser.parse_args()
    try:
        config = RuntimeConfig.from_environment()
        with app_connection(config, autocommit=True) as conn:
            with conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                report = read_server_timing(conn, args.query_id)
        print(json.dumps(report, ensure_ascii=False, default=str))
        return 0
    except Exception:
        print(
            json.dumps({"code": "QUERY_TIMING_EXPORT_UNAVAILABLE", "query_id": str(args.query_id)})
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
