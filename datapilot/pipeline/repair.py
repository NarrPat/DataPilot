"""Conservative authorization for feedback, separate from execution permission.

Guard order can expose a result-shape error before resolving source objects.
Therefore a repairable error code alone never authorizes a repair. Both periods
must also have a safe SELECT skeleton using current-scope registered objects.
The repaired plan still traverses the complete unchanged execution guard.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, SqlglotError

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec, QueryPlan
from datapilot.sql_guard import GuardError, guard_sql

REPAIRABLE = frozenset(
    {
        "PLAN_OUTPUT_INVALID",
        "PLAN_RESULT_CONTRACT_MISMATCH",
        "PLAN_BASELINE_MISMATCH",
        "SQL_PARSE_ERROR",
        "SQL_COLUMN_UNKNOWN",
        "SQL_COLUMN_AMBIGUOUS",
        "SQL_OUTPUT_ALIAS_REQUIRED",
        "SQL_OUTPUT_CONTRACT_MISMATCH",
        "RESULT_COLUMNS_MISMATCH",
        "RESULT_TYPE_MISMATCH",
        "RESULT_BUCKET_MISMATCH",
        "RESULT_GROUP_MISMATCH",
        "RESULT_TOTAL_REQUIRES_ONE_ROW",
        "SQL_AGGREGATION_ERROR",
    }
)
FORBIDDEN_TOKEN_NAMES = frozenset(
    {
        "ALTER",
        "ANALYZE",
        "BEGIN",
        "CALL",
        "COMMAND",
        "COMMIT",
        "COPY",
        "CREATE",
        "DELETE",
        "DROP",
        "EXECUTE",
        "GRANT",
        "INSERT",
        "INTO",
        "LOCK",
        "MERGE",
        "REVOKE",
        "ROLLBACK",
        "SET",
        "TRUNCATE",
        "UPDATE",
        "USE",
    }
)


def repair_is_safe(plan: QueryPlan | None, confirmed: ConfirmedSpec, catalog: Catalog) -> bool:
    if plan is None:
        # Invalid DTOs are discarded. Repair sees no unvalidated SQL, DSN or extra fields.
        return True
    allowed = set(catalog.get_allowed_schema(confirmed.effective_scope).views)
    for sql in (plan.current_sql, plan.baseline_sql):
        if sql is None:
            continue
        try:
            if any(
                t.token_type.name in FORBIDDEN_TOKEN_NAMES
                for t in sqlglot.tokenize(sql, read="postgres")
            ):
                return False
            trees = [
                node
                for node in sqlglot.parse(sql, read="postgres", error_level=ErrorLevel.IGNORE)
                if node is not None and not (isinstance(node, exp.Semicolon) and not node.args)
            ]
            if len(trees) != 1 or not isinstance(trees[0], exp.Select):
                return False
            tree = trees[0]
            ctes = {cte.alias_or_name for cte in tree.find_all(exp.CTE)}
            for table in tree.find_all(exp.Table):
                if table.catalog:
                    return False
                if table.db:
                    if f"{table.db}.{table.name}" not in allowed:
                        return False
                elif table.name not in ctes:
                    return False
            try:
                guard_sql(tree.sql(dialect="postgres"), confirmed, catalog)
            except GuardError as exc:
                if exc.code not in REPAIRABLE:
                    return False
        except (SqlglotError, ValueError, TypeError, RecursionError):
            return False
    return True
