"""Fail-closed PostgreSQL AST, scope/lineage, join and binding boundary.

guard_sql checks structural authorization. guard_plan additionally checks the
finite five-metric language against QuerySpec. Neither can validate that the
user's natural-language intent was parsed correctly; the independent oracle and
specification evaluation remain necessary. Only the checked AST is executed.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import OptimizeError, ParseError, SqlglotError
from sqlglot.optimizer.scope import Scope, traverse_scope

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec, QueryPlan
from datapilot.metric_guard import MetricSQLRejected, validate_metric_sql
from datapilot.specification import validate_plan

MAX_BYTES = 8192
MAX_NODES = 512
MAX_CTES = 6
MAX_JOINS = 6
MAX_SUBQUERY_DEPTH = 3
ALLOWED_NODES = frozenset(
    "Select From Table Identifier TableAlias Alias Column With CTE Subquery Join Where "
    "Group Having Order Ordered Paren And Or Not EQ NEQ GT GTE LT LTE Is NullSafeEQ "
    "Literal Null Boolean Placeholder Distinct Star Sum Count Avg Min Max Coalesce Nullif "
    "Round DateTrunc TimestampTrunc AtTimeZone Anonymous Case If Cast DataType DataTypeParam "
    "Add Sub Mul Div Neg Any In Between Var Limit".split()
)
ALLOWED_TYPES = frozenset("BIGINT INT DECIMAL TEXT BOOLEAN DATE TIMESTAMP TIMESTAMPTZ".split())


class GuardError(Exception):
    def __init__(self, code: str, **details: Any) -> None:
        self.code, self.details = code, details
        super().__init__(code)


@dataclass(frozen=True)
class CheckedSQL:
    sql: str
    input_sha256: str
    sql_sha256: str
    columns: tuple[str, ...]
    parameters: tuple[str, ...]
    complexity: dict[str, int]
    lineage: tuple[dict[str, Any], ...]


@dataclass
class Source:
    columns: dict[str, frozenset[str]]
    direct: set[str]
    grouped: bool = False
    grouping_keys: frozenset[str] = frozenset()
    period_keys: frozenset[str] = frozenset()


def parse_single(sql: str) -> exp.Select:
    try:
        statements = [
            node
            for node in sqlglot.parse(sql, read="postgres")
            if node is not None and not (isinstance(node, exp.Semicolon) and not node.args)
        ]
    except (ParseError, RecursionError) as exc:
        raise GuardError("SQL_PARSE_ERROR") from exc
    if len(statements) != 1:
        raise GuardError("SQL_STATEMENT_COUNT", count=len(statements))
    if not isinstance(statements[0], exp.Select):
        raise GuardError("SQL_STATEMENT_FORBIDDEN", node=type(statements[0]).__name__)
    return statements[0]


def complexity(tree: exp.Select, sql: str) -> dict[str, int]:
    nodes = list(tree.walk())
    depths = []
    for node in tree.find_all(exp.Subquery):
        depth, ancestor = 1, node.parent
        while ancestor is not None:
            depth += isinstance(ancestor, exp.Subquery)
            ancestor = ancestor.parent
        depths.append(depth)
    return {
        "bytes": len(sql.encode()),
        "nodes": len(nodes),
        "ctes": len(list(tree.find_all(exp.CTE))),
        "joins": len(list(tree.find_all(exp.Join))),
        "subquery_depth": max(depths, default=0),
    }


def _check_nodes(tree: exp.Select, *, probe_limit: int | None) -> None:
    for index, node in enumerate(tree.walk()):
        name = type(node).__name__
        if name not in ALLOWED_NODES:
            raise GuardError("SQL_NODE_FORBIDDEN", node=name, node_index=index)
        if isinstance(node, exp.With) and node.args.get("recursive"):
            raise GuardError("SQL_RECURSIVE_FORBIDDEN")
        if isinstance(node, exp.Table) and (
            not isinstance(node.this, exp.Identifier) or node.catalog
        ):
            raise GuardError("SQL_OBJECT_FORBIDDEN")
        if isinstance(node, exp.Column) and (node.db or node.catalog):
            raise GuardError("SQL_COLUMN_QUALIFIER_FORBIDDEN")
        if isinstance(node, exp.TableAlias) and node.args.get("columns"):
            raise GuardError("SQL_ALIAS_COLUMN_LIST_FORBIDDEN")
        if isinstance(node, exp.Star) and not isinstance(node.parent, exp.Count):
            raise GuardError("SQL_STAR_FORBIDDEN")
        if isinstance(node, exp.Limit):
            if (
                node.parent is not tree
                or probe_limit is None
                or not isinstance(node.expression, exp.Literal)
                or node.expression.this != str(probe_limit)
            ):
                raise GuardError("SQL_MODEL_LIMIT_FORBIDDEN")
        if isinstance(node, exp.DataType):
            if node.this.value not in ALLOWED_TYPES or node.args.get("nested"):
                raise GuardError("SQL_CAST_FORBIDDEN")
        if isinstance(node, exp.Anonymous):
            if node.name.upper() != "TIMEZONE" or len(node.expressions) != 2:
                raise GuardError("SQL_FUNCTION_FORBIDDEN", function=node.name)
            if node.expressions[0] != exp.Literal.string("Asia/Shanghai"):
                raise GuardError("SQL_TIMEZONE_FORBIDDEN")
        if isinstance(node, exp.AtTimeZone) and node.args.get("zone") != exp.Literal.string(
            "Asia/Shanghai"
        ):
            raise GuardError("SQL_TIMEZONE_FORBIDDEN")
        if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
            unit = node.args.get("unit")
            if (
                not isinstance(unit, exp.Expr)
                or unit.name.upper() not in {"DAY", "WEEK", "MONTH"}
                or node.args.get("zone")
            ):
                raise GuardError("SQL_GRAIN_FORBIDDEN")
        if isinstance(node, exp.Var) and not isinstance(
            node.parent, (exp.TimestampTrunc, exp.DateTrunc)
        ):
            raise GuardError("SQL_VARIABLE_FORBIDDEN")
        if isinstance(node, exp.Literal):
            if node.is_string:
                parent = node.parent
                zone = isinstance(parent, exp.AtTimeZone) and node.arg_key == "zone"
                zone |= (
                    isinstance(parent, exp.Anonymous)
                    and parent.name.upper() == "TIMEZONE"
                    and node is parent.expressions[0]
                )
                status = (
                    isinstance(parent, exp.EQ)
                    and isinstance(parent.this, exp.Column)
                    and (
                        (parent.this.name == "payment_status" and node.this == "paid")
                        or (parent.this.name == "status" and node.this == "succeeded")
                    )
                )
                if not (zone and node.this == "Asia/Shanghai" or status):
                    raise GuardError("SQL_LITERAL_VALUE_FORBIDDEN")
            elif not re.fullmatch(r"[0-9]{1,19}(?:\.[0-9]{1,6})?", node.this):
                raise GuardError("SQL_LITERAL_SIZE_LIMIT")


def _grouping_names(query: exp.Select) -> set[str]:
    group = query.args.get("group")
    if group is None:
        return set()
    names = set()
    for expression in group.expressions:
        if isinstance(expression, exp.Literal) and expression.is_int:
            index = int(expression.this) - 1
            if not 0 <= index < len(query.expressions):
                raise GuardError("SQL_GROUP_POSITION_INVALID")
            names.add(query.expressions[index].alias_or_name)
        elif (
            isinstance(expression, exp.Column)
            and not expression.table
            and expression.name in query.named_selects
        ):
            names.add(expression.name)
        else:
            for projection in query.expressions:
                value = projection.this if isinstance(projection, exp.Alias) else projection
                if value == expression:
                    names.add(projection.alias_or_name)
    if len(names) != len(group.expressions):
        # Unknown grouping is not used to authorize aggregate joins.
        return {"__unregistered_grain__"}
    return names


def _resolve(
    tree: exp.Select, catalog: Catalog, confirmed: ConfirmedSpec
) -> tuple[dict[str, Any], ...]:
    allowed = catalog.get_allowed_schema(confirmed.effective_scope)
    paths = {frozenset(pair) for pair in allowed.join_paths}
    scope_sources: dict[Scope, dict[str, Source]] = {}
    exported: dict[Scope, Source] = {}
    scope_by_select: dict[int, Scope] = {}
    records: list[dict[str, Any]] = []
    scopes = traverse_scope(tree)
    for scope in scopes:
        scope_by_select[id(scope.expression)] = scope

    def resolve_column(column: exp.Column, scope: Scope) -> tuple[Source, str]:
        sources = scope_sources[scope]
        if column.table:
            source = sources.get(column.table)
            if source is None or column.name not in source.columns:
                raise GuardError("SQL_COLUMN_UNKNOWN")
            return source, column.name
        matches = [
            (source, column.name) for source in sources.values() if column.name in source.columns
        ]
        if len(matches) != 1:
            raise GuardError("SQL_COLUMN_AMBIGUOUS" if matches else "SQL_COLUMN_UNKNOWN")
        return matches[0]

    def lineage(node: exp.Expr, scope: Scope) -> frozenset[str]:
        if isinstance(node, exp.Column):
            source, name = resolve_column(node, scope)
            return source.columns[name]
        if isinstance(node, (exp.Subquery, exp.Select)):
            select = node.this if isinstance(node, exp.Subquery) else node
            child = exported.get(scope_by_select[id(select)])
            if child is None:
                raise GuardError("SQL_SUBQUERY_UNRESOLVED")
            return frozenset().union(*child.columns.values())
        return frozenset().union(*(lineage(child, scope) for child in node.iter_expressions()))

    for scope in scopes:
        query = scope.expression
        if not isinstance(query, exp.Select):
            raise GuardError("SQL_SCOPE_UNSUPPORTED")
        sources: dict[str, Source] = {}
        for alias, (_, source) in scope.selected_sources.items():
            if isinstance(source, exp.Table):
                name = f"{source.db}.{source.name}"
                if name not in allowed.views or source.db != "analytics_views":
                    raise GuardError("SQL_OBJECT_FORBIDDEN")
                base = source.name.removesuffix("_" + confirmed.effective_scope.value)
                sources[alias] = Source(
                    {column: frozenset({base + "." + column}) for column in allowed.views[name]},
                    set(allowed.views[name]),
                )
                records.append({"scope_index": len(records), "alias": alias, "physical_view": name})
            elif isinstance(source, Scope) and source in exported:
                sources[alias] = exported[source]
            else:
                raise GuardError("SQL_SOURCE_UNRESOLVED")
        scope_sources[scope] = sources
        # SQLGlot marks unresolved unqualified CTE columns as external before
        # qualification. Resolve our exact known columns instead of trusting
        # that heuristic or inheriting correlated columns into the parent.
        for column in (node for node in scope.walk() if isinstance(node, exp.Column)):
            ancestor = column.find_ancestor(exp.Group, exp.Order)
            is_alias = (
                ancestor is not None and column.table == "" and column.name in query.named_selects
            )
            if not is_alias:
                resolve_column(column, scope)

        source_from = query.args.get("from_")
        seen = {source_from.this.alias_or_name} if source_from is not None else set()
        for join in query.args.get("joins", []):
            alias = join.this.alias_or_name
            if (
                join.args.get("method")
                or join.args.get("using")
                or join.args.get("kind", "").upper() in {"CROSS", "SEMI", "ANTI"}
                or not join.args.get("on")
            ):
                raise GuardError("SQL_JOIN_FORBIDDEN")
            conditions = (
                list(join.args["on"].flatten())
                if isinstance(join.args["on"], exp.And)
                else [join.args["on"]]
            )
            aggregate_pairs: set[str] = set()
            aggregate_sources: tuple[Source, Source] | None = None
            for condition in conditions:
                if (
                    not isinstance(condition, exp.EQ)
                    or not isinstance(condition.this, exp.Column)
                    or not isinstance(condition.expression, exp.Column)
                ):
                    raise GuardError("SQL_JOIN_PATH_FORBIDDEN")
                left, right = condition.this, condition.expression
                if not (
                    (left.table == alias and right.table in seen)
                    or (right.table == alias and left.table in seen)
                ):
                    raise GuardError("SQL_JOIN_PATH_FORBIDDEN")
                ls, lc = resolve_column(left, scope)
                rs, rc = resolve_column(right, scope)
                endpoints = ls.columns[lc] | rs.columns[rc]
                raw_fk = (
                    lc in ls.direct
                    and rc in rs.direct
                    and len(endpoints) == 2
                    and frozenset(endpoints) in paths
                    and (not ls.grouped or lc in ls.grouping_keys)
                    and (not rs.grouped or rc in rs.grouping_keys)
                )
                period_join = (
                    ls.grouped
                    and rs.grouped
                    and ls.period_keys == rs.period_keys
                    and lc == rc
                    and lc in ls.period_keys
                )
                if not raw_fk and not period_join:
                    raise GuardError("SQL_JOIN_PATH_FORBIDDEN")
                if period_join:
                    aggregate_pairs.add(lc)
                    aggregate_sources = ls, rs
            if aggregate_sources and aggregate_pairs != aggregate_sources[0].period_keys:
                raise GuardError("SQL_AGGREGATE_JOIN_GRAIN_MISMATCH")
            seen.add(alias)

        output: dict[str, frozenset[str]] = {}
        direct = set()
        for projection in query.expressions:
            name = projection.alias_or_name
            if not name or name in output:
                raise GuardError("SQL_OUTPUT_ALIAS_REQUIRED")
            value = projection.this if isinstance(projection, exp.Alias) else projection
            output[name] = lineage(value, scope)
            if isinstance(value, exp.Column):
                original, column_name = resolve_column(value, scope)
                if column_name in original.direct:
                    direct.add(name)
        grouping = _grouping_names(query)
        aggregated = any(isinstance(node, exp.AggFunc) for node in scope.walk())
        period_keys = (
            frozenset(name for name in grouping if output[name])
            if grouping and grouping <= {"bucket_start", "group_key"} and aggregated
            else frozenset()
        )
        exported[scope] = Source(
            output, direct, aggregated or bool(grouping), frozenset(grouping), period_keys
        )
        records.append(
            {
                "scope_index": len(records),
                "exports": {name: sorted(values) for name, values in output.items()},
                "grouping_keys": sorted(grouping),
            }
        )
    return tuple(records)


def _inspect(
    sql: str, confirmed: ConfirmedSpec, catalog: Catalog, *, probe_limit: int | None = None
) -> CheckedSQL:
    if len(sql.encode()) > MAX_BYTES:
        raise GuardError(
            "SQL_COMPLEXITY_LIMIT", dimension="bytes", actual=len(sql.encode()), maximum=MAX_BYTES
        )
    tree = parse_single(sql)
    counts = complexity(tree, sql)
    for key, maximum in (
        ("nodes", MAX_NODES),
        ("ctes", MAX_CTES),
        ("joins", MAX_JOINS),
        ("subquery_depth", MAX_SUBQUERY_DEPTH),
    ):
        if counts[key] > maximum:
            raise GuardError(
                "SQL_COMPLEXITY_LIMIT", dimension=key, actual=counts[key], maximum=maximum
            )
    _check_nodes(tree, probe_limit=probe_limit)
    expected = catalog.get_metric(
        confirmed.spec.metric_id, confirmed.effective_scope
    ).result_contract
    if tuple(tree.named_selects) != expected or len(tree.expressions) != len(expected):
        raise GuardError("SQL_OUTPUT_CONTRACT_MISMATCH")
    used = {node.name for node in tree.find_all(exp.Placeholder)}
    registered = {"start_at", "end_at"}
    if confirmed.spec.filters.sales_regions:
        registered.add("sales_regions")
    if confirmed.spec.filters.category_codes:
        registered.add("category_codes")
    if used != registered:
        raise GuardError("SQL_PARAMETER_MISMATCH", registered=sorted(registered), used=sorted(used))
    try:
        resolved = _resolve(tree, catalog, confirmed)
    except (OptimizeError, KeyError) as exc:
        raise GuardError("SQL_SCOPE_UNRESOLVED") from exc
    canonical = tree.sql(dialect="postgres", comments=False)
    return CheckedSQL(
        canonical,
        hashlib.sha256(sql.encode()).hexdigest(),
        hashlib.sha256(canonical.encode()).hexdigest(),
        expected,
        tuple(sorted(used)),
        counts,
        resolved,
    )


def guard_sql(sql: str, confirmed: ConfirmedSpec, catalog: Catalog) -> CheckedSQL:
    try:
        return _inspect(sql, confirmed, catalog)
    except GuardError:
        raise
    except (SqlglotError, ValueError, TypeError, AttributeError, RecursionError) as exc:
        raise GuardError("SQL_UNSUPPORTED_AST") from exc


def guard_plan(
    plan: QueryPlan, confirmed: ConfirmedSpec, catalog: Catalog
) -> tuple[CheckedSQL, ...]:
    validate_plan(plan, confirmed, catalog)
    # Both statements must pass before the caller may acquire a SQL connection.
    checked = tuple(
        guard_sql(sql, confirmed, catalog)
        for sql in (plan.current_sql, plan.baseline_sql)
        if sql is not None
    )
    for index, statement in enumerate(checked):
        try:
            validate_metric_sql(
                parse_single(statement.sql), confirmed, catalog, baseline=index == 1
            )
        except MetricSQLRejected as exc:
            raise GuardError(str(exc)) from exc
        except (KeyError, ValueError, TypeError, AttributeError, IndexError, RecursionError) as exc:
            raise GuardError("SQL_METRIC_EXPRESSION_MISMATCH") from exc
    return checked


def add_probe_limit(checked: CheckedSQL, confirmed: ConfirmedSpec, catalog: Catalog) -> CheckedSQL:
    tree = parse_single(checked.sql)
    limit = min(200, confirmed.spec.result_limit) + 1
    tree.set("limit", exp.Limit(expression=exp.Literal.number(limit)))
    # Re-run the complete boundary after the trusted AST change.
    return _inspect(tree.sql(dialect="postgres"), confirmed, catalog, probe_limit=limit)
