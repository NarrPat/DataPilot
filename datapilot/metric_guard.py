"""Check the finite metric SQL language against the confirmed specification.

Runs AFTER the structural/authorization guard. No SQL is generated, no data is
read and no reference answer is available here. Expressions are resolved through
aggregate CTEs and scalar reads; unknown semantic forms fail closed. This is a
contract check for five metrics, not a general SQL equivalence prover.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, NoReturn

from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from datapilot.catalog import Catalog
from datapilot.contracts import ConfirmedSpec

type Symbol = tuple[Any, ...]
NULL: Symbol = ("null",)
ZERO: Symbol = ("number", "0")


class MetricSQLRejected(ValueError):
    pass


def reject(part: str) -> NoReturn:
    raise MetricSQLRejected("SQL_METRIC_" + part + "_MISMATCH")


@dataclass
class Relation:
    columns: dict[str, Symbol]
    raw: str | None = None


def column(table: str, name: str) -> Symbol:
    return ("column", table, name)


def _zero_filled(value: Symbol) -> Symbol:
    if value[0] == "coalesce" and len(value) == 3 and value[2] == ZERO:
        return _zero_filled(value[1])
    return value


def _key(value: Symbol) -> frozenset[Symbol]:
    if value[0] == "coalesce":
        return frozenset().union(*(_key(child) for child in value[1:]))
    return frozenset({value})


def validate_metric_sql(
    tree: exp.Select, confirmed: ConfirmedSpec, catalog: Catalog, *, baseline: bool
) -> None:
    spec = confirmed.spec
    grain = "total" if baseline else spec.grain.value
    metric = spec.metric_id.value
    net = metric == "period_net_goods_amount"
    category = spec.group_by == "category" or bool(spec.filters.category_codes)
    allowed = catalog.get_allowed_schema(confirmed.effective_scope)
    scopes = traverse_scope(tree)
    by_select = {id(scope.expression): scope for scope in scopes}
    exports: dict[Scope, Relation] = {}
    sources: dict[Scope, dict[str, Relation]] = {}

    def symbol(node: exp.Expr, scope: Scope) -> Symbol:
        if isinstance(node, (exp.Alias, exp.Paren)):
            return symbol(node.this, scope)
        if isinstance(node, exp.Cast):
            value = symbol(node.this, scope)
            dtype = node.args["to"]
            # Integer component casts don't change values; typeless NULL slots
            # are accepted structurally and checked again by the row decoder.
            if value == NULL or (
                dtype.this.value in {"BIGINT", "INT", "DECIMAL"} and not dtype.expressions
            ):
                return value
            return ("unsupported_cast",)
        if isinstance(node, exp.Column):
            candidates = [
                relation.columns[node.name]
                for alias, relation in sources[scope].items()
                if (not node.table or alias == node.table) and node.name in relation.columns
            ]
            if len(candidates) != 1:
                reject("EXPRESSION")
            return candidates[0]
        if isinstance(node, exp.Null):
            return NULL
        if isinstance(node, exp.Literal):
            return ("string" if node.is_string else "number", node.this)
        if isinstance(node, exp.Placeholder):
            return ("parameter", node.name)
        if isinstance(node, exp.Subquery):
            child = exports[by_select[id(node.this)]]
            if len(child.columns) != 1:
                reject("COMPONENT")
            return next(iter(child.columns.values()))
        if isinstance(node, exp.Coalesce):
            return (
                "coalesce",
                symbol(node.this, scope),
                *(symbol(x, scope) for x in node.expressions),
            )
        if isinstance(node, exp.Sum):
            return ("sum", symbol(node.this, scope))
        if isinstance(node, exp.Count):
            argument = symbol(node.this, scope)
            if argument == ("star",) and [r.raw for r in sources[scope].values()] == ["orders"]:
                return ("count", ("distinct", column("orders", "order_id")))
            return ("count", argument)
        if isinstance(node, exp.Distinct):
            return ("distinct", *(symbol(x, scope) for x in node.expressions))
        if isinstance(node, exp.Star):
            return ("star",)
        if isinstance(node, exp.Any):
            return ("any", symbol(node.this, scope))
        if isinstance(node, exp.AtTimeZone):
            return ("zone", symbol(node.this, scope), symbol(node.args["zone"], scope))
        if isinstance(node, exp.Anonymous) and node.name.upper() == "TIMEZONE":
            return ("zone", symbol(node.expressions[1], scope), symbol(node.expressions[0], scope))
        if isinstance(node, (exp.TimestampTrunc, exp.DateTrunc)):
            return ("bucket", node.args["unit"].name.lower(), symbol(node.this, scope))
        if isinstance(node, (exp.EQ, exp.GTE, exp.LT, exp.LTE, exp.GT)):
            left, right = symbol(node.this, scope), symbol(node.expression, scope)
            op = type(node).__name__
            if left[0] != "column" and right[0] == "column":
                left, right = right, left
                op = {"EQ": "EQ", "GTE": "LTE", "GT": "LT", "LT": "GT", "LTE": "GTE"}[op]
            return (op, left, right)
        # Retain a distinct marker for unsupported arithmetic/predicates. The
        # caller returns the relevant contract failure, never repairs a value.
        return ("unsupported", type(node).__name__)

    def bucket(refund: bool) -> Symbol:
        if grain == "total":
            return NULL
        event = column("refunds", "succeeded_at") if refund else column("orders", "paid_at")
        zone = ("string", "Asia/Shanghai")
        return ("zone", ("bucket", grain, ("zone", event, zone)), zone)

    def group_key() -> Symbol:
        return {
            None: NULL,
            "category": column("products", "category_code"),
            "sales_region": column("orders", "sales_region"),
        }[spec.group_by]

    def predicates(query: exp.Select, scope: Scope) -> set[Symbol]:
        where = query.args.get("where")
        if where is None:
            return set()

        def flatten(node: exp.Expr) -> list[Symbol]:
            if isinstance(node, exp.Paren):
                return flatten(node.this)
            if isinstance(node, exp.And):
                return flatten(node.this) + flatten(node.expression)
            return [symbol(node, scope)]

        return set(flatten(where.this))

    def expected_predicates(refund: bool) -> set[Symbol]:
        event = column("refunds", "succeeded_at") if refund else column("orders", "paid_at")
        status = column("refunds", "status") if refund else column("orders", "payment_status")
        expected: set[Symbol] = {
            ("EQ", status, ("string", "succeeded" if refund else "paid")),
            ("GTE", event, ("parameter", "start_at")),
            ("LT", event, ("parameter", "end_at")),
        }
        if spec.filters.sales_regions:
            expected.add(
                ("EQ", column("orders", "sales_region"), ("any", ("parameter", "sales_regions")))
            )
        if spec.filters.category_codes:
            expected.add(
                (
                    "EQ",
                    column("products", "category_code"),
                    ("any", ("parameter", "category_codes")),
                )
            )
        return expected

    paid = ("sum", column("order_items", "paid_amount_cents"))
    refunded = ("sum", column("refunds", "amount_cents"))
    count = ("count", ("distinct", column("orders", "order_id")))
    expected_components: dict[str, Symbol] = {
        "paid_goods_amount": {"amount_cents": paid},
        "paid_order_count": {"order_count": count},
        "refund_goods_amount": {"amount_cents": refunded},
        "period_net_goods_amount": {"paid_amount_cents": paid, "refund_amount_cents": refunded},
        "paid_aov": {"amount_cents": paid, "order_count": count},
    }[metric]

    for scope in scopes:
        query = scope.expression
        if not isinstance(query, exp.Select):
            reject("EXPRESSION")
        local: dict[str, Relation] = {}
        for alias, (_, source) in scope.selected_sources.items():
            if isinstance(source, exp.Table):
                table = source.name.removesuffix("_" + confirmed.effective_scope.value)
                # Columns have already been checked against the scoped catalog.
                names = allowed.views[f"{source.db}.{source.name}"]
                local[alias] = Relation({name: column(table, name) for name in names}, raw=table)
            else:
                local[alias] = exports[source]
        sources[scope] = local
        output = {
            projection.alias_or_name: symbol(projection, scope) for projection in query.expressions
        }
        aggregates = [node for node in scope.walk() if isinstance(node, exp.AggFunc)]
        if query.args.get("having") or query.args.get("distinct"):
            reject("PREDICATE")
        if aggregates:
            tables = [relation.raw for relation in local.values()]
            refund_axis = "refunds" in tables and metric in {
                "refund_goods_amount",
                "period_net_goods_amount",
            }
            expected_tables = {"orders"}
            if refund_axis:
                expected_tables |= {"refunds", "order_items"}
            elif metric != "paid_order_count" or category:
                expected_tables.add("order_items")
            if category:
                expected_tables.add("products")
            if set(tables) != expected_tables or len(tables) != len(expected_tables):
                reject("RELATION")
            if any(
                join.args.get("side") or join.args.get("kind", "").upper() not in {"", "INNER"}
                for join in query.args.get("joins", [])
            ):
                reject("RELATION")
            if predicates(query, scope) != expected_predicates(refund_axis):
                reject("PREDICATE")
            valid_aggregates = {refunded} if refund_axis else {paid, count}
            for aggregate in aggregates:
                value = symbol(aggregate, scope)
                if value == ("count", ("star",)) and tables == ["orders"]:
                    # COUNT(*) is valid only when one input row is one order.
                    for name, projected in output.items():
                        if projected == value:
                            output[name] = count
                    continue
                if value not in valid_aggregates:
                    reject("COMPONENT")
            grouping: set[Symbol] = set()
            group = query.args.get("group")
            for expression in group.expressions if group is not None else []:
                if isinstance(expression, exp.Literal) and expression.is_int:
                    value = output[query.expressions[int(expression.this) - 1].alias_or_name]
                elif (
                    isinstance(expression, exp.Column)
                    and not expression.table
                    and expression.name in output
                ):
                    value = output[expression.name]
                else:
                    value = symbol(expression, scope)
                if value != NULL:
                    grouping.add(value)
            if grouping != {value for value in (bucket(refund_axis), group_key()) if value != NULL}:
                reject("GRAIN")
            # Every amount/count projection must itself be the aggregate, not
            # an expression that scales it or substitutes another component.
            for name, value in output.items():
                if name in {"bucket_start", "group_key"}:
                    if (
                        value
                        != {"bucket_start": bucket(refund_axis), "group_key": group_key()}[name]
                    ):
                        reject("GRAIN")
                elif _zero_filled(value) not in valid_aggregates:
                    reject("COMPONENT")
        else:
            if any(relation.raw for relation in local.values()):
                reject("RELATION")
            if query.args.get("where") or query.args.get("group"):
                reject("PREDICATE")
            joins = query.args.get("joins", [])
            if joins and (
                not net
                or len(joins) != 1
                or len(local) != 2
                or joins[0].args.get("side", "").upper() != "FULL"
            ):
                reject("COMBINATION")
        exports[scope] = Relation(output)

    output = exports[scopes[-1]].columns
    for name, expected in expected_components.items():
        if _zero_filled(output[name]) != expected:
            reject("COMPONENT")
    expected_buckets = (
        frozenset({bucket(False), bucket(True)})
        if net
        else frozenset({bucket(metric == "refund_goods_amount")})
    )
    if _key(output["bucket_start"]) != expected_buckets or _key(output["group_key"]) != frozenset(
        {group_key()}
    ):
        reject("GRAIN")
    # Same dimension lineage on both axes is not enough: a one-sided key loses
    # refund-only groups. Check the actual coalescing references before expansion.
    if net and (grain != "total" or spec.group_by is not None):
        for name in ("bucket_start", "group_key"):
            if (
                name == "bucket_start"
                and grain == "total"
                or name == "group_key"
                and spec.group_by is None
            ):
                continue
            expression = next(p for p in tree.expressions if p.alias_or_name == name)
            if (
                not isinstance(expression.this, exp.Coalesce)
                or len({c.table for c in expression.find_all(exp.Column)}) != 2
            ):
                reject("GRAIN")
