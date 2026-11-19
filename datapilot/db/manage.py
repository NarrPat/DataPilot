"""Initialize this project's analytics foundation; no replacement/reset operation."""

import argparse
import json
from pathlib import Path

import psycopg
from sqlalchemy.exc import SQLAlchemyError

from datapilot.data.fixtures import FixtureInvalid, load_fixture
from datapilot.db.bootstrap import DatabaseSetupError, connect, initialize, migrate
from datapilot.db.seed import SeedConflict, load_seed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    args = parser.parse_args()
    try:
        source = load_fixture(args.fixture)
        initialize()
        migrate("dp_analytics")
        with connect("dp_analytics") as connection:
            report = load_seed(connection, source)
        print(json.dumps({"database": "dp_analytics", "data_status": "ready", **report}))
        return 0
    except FixtureInvalid as exc:
        print(json.dumps({"data_status": "invalid_fixture", "errors": exc.errors}))
    except (DatabaseSetupError, SeedConflict) as exc:
        print(json.dumps({"data_status": "not_ready", "reason": str(exc)}))
    except psycopg.Error as exc:
        print(json.dumps({"data_status": "not_ready", "sqlstate": exc.sqlstate}))
    except SQLAlchemyError as exc:
        original = getattr(exc, "orig", None)
        code = original.sqlstate if isinstance(original, psycopg.Error) else None
        print(
            json.dumps(
                {
                    "data_status": "not_ready",
                    "reason": "DATABASE_OPERATION_FAILED",
                    "sqlstate": code,
                }
            )
        )
    except OSError:
        print(json.dumps({"data_status": "not_ready", "reason": "SOURCE_IO_FAILED"}))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
