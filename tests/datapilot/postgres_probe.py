"""Run test plans in the Compose network using the application's read-only executor."""

import json
import sys

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec, QueryPlan
from datapilot.db.analytics import create_analyst_pools
from datapilot.executor import Deadline, ReadonlyExecutor


def main() -> None:
    requests = json.load(sys.stdin)
    pools = create_analyst_pools()
    try:
        for pool in pools.values():
            pool.open()
        executor = ReadonlyExecutor(pools, Catalog())
        executor.startup()
        results = {}
        for name, request in requests.items():
            response = executor.execute(
                ConfirmedSpec.model_validate(request["confirmed"]),
                QueryPlan.model_validate(request["plan"]),
                deadline=Deadline.after(),
            )
            results[name] = {
                "role": response.settings["role"],
                "read_only": response.settings["read_only"],
                "isolation": response.settings["isolation"],
                "periods": response.result["periods"],
                "series": response.result["series"],
            }
        json.dump(results, sys.stdout)
    finally:
        for pool in pools.values():
            pool.close()


if __name__ == "__main__":
    main()
