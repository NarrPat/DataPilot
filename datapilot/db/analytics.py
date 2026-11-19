"""Fixed server-owned analyst pools. No request-supplied DSN or role."""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from datapilot.contracts import Scope
from datapilot.db.bootstrap import password

AnalystPool = ConnectionPool[psycopg.Connection[dict[str, Any]]]


def create_analyst_pools() -> dict[Scope, AnalystPool]:
    pools: dict[Scope, AnalystPool] = {}
    for scope in Scope:
        role = "analyst_" + scope.value
        pools[scope] = ConnectionPool[psycopg.Connection[dict[str, Any]]](
            conninfo="",
            kwargs={
                "host": "postgres",
                "port": 5432,
                "dbname": "dp_analytics",
                "user": role,
                "password": password(role),
                "row_factory": dict_row,
                "autocommit": True,
                "connect_timeout": 5,
                "application_name": "datapilot-analytics",
            },
            min_size=0,
            max_size=2,
            max_waiting=10,
            timeout=5,
            max_idle=30,
            open=False,
        )
    return pools
