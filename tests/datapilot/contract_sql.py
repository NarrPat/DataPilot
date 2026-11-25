"""Test-only correct SQL templates, never a product/model fallback or oracle input."""

from __future__ import annotations

from datapilot.contracts import ConfirmedSpec


def contract_sql(envelope: ConfirmedSpec, *, baseline: bool = False) -> str:
    spec, scope = envelope.spec, envelope.effective_scope.value
    metric = spec.metric_id.value
    grain = "total" if baseline else spec.grain.value
    category = spec.group_by == "category" or bool(spec.filters.category_codes)

    def axis(refund: bool) -> str:
        timestamp = "r.succeeded_at" if refund else "o.paid_at"
        bucket = (
            "NULL::timestamptz"
            if grain == "total"
            else (
                f"DATE_TRUNC('{grain}', {timestamp} AT TIME ZONE 'Asia/Shanghai') "
                "AT TIME ZONE 'Asia/Shanghai'"
            )
        )
        group = {
            None: "NULL::text",
            "category": "p.category_code",
            "sales_region": "o.sales_region",
        }[spec.group_by]
        if refund:
            relation = (
                f"analytics_views.refunds_{scope} AS r "
                f"JOIN analytics_views.order_items_{scope} AS i "
                "ON i.order_item_id=r.order_item_id "
                f"JOIN analytics_views.orders_{scope} AS o ON o.order_id=i.order_id"
            )
            measures = "COALESCE(SUM(r.amount_cents),0) AS " + (
                "refund_amount_cents" if metric == "period_net_goods_amount" else "amount_cents"
            )
            predicate = "r.status='succeeded'"
        else:
            relation = f"analytics_views.orders_{scope} AS o"
            if metric != "paid_order_count" or category:
                relation += (
                    f" JOIN analytics_views.order_items_{scope} AS i ON i.order_id=o.order_id"
                )
            if metric == "paid_order_count":
                measures = "COUNT(DISTINCT o.order_id) AS order_count"
            else:
                measures = "COALESCE(SUM(i.paid_amount_cents),0) AS " + (
                    "paid_amount_cents" if metric == "period_net_goods_amount" else "amount_cents"
                )
                if metric == "paid_aov":
                    measures += ", COUNT(DISTINCT o.order_id) AS order_count"
            predicate = "o.payment_status='paid'"
        if category:
            relation += f" JOIN analytics_views.products_{scope} AS p ON p.product_id=i.product_id"
        predicate += f" AND {timestamp}>=%(start_at)s AND {timestamp}<%(end_at)s"
        if spec.filters.sales_regions:
            predicate += " AND o.sales_region=ANY(%(sales_regions)s)"
        if spec.filters.category_codes:
            predicate += " AND p.category_code=ANY(%(category_codes)s)"
        grouping = " GROUP BY 1,2" if grain != "total" or spec.group_by else ""
        return (
            f"SELECT {bucket} AS bucket_start, {group} AS group_key, {measures} "
            f"FROM {relation} WHERE {predicate}{grouping}"
        )

    order = " ORDER BY bucket_start NULLS FIRST, group_key NULLS FIRST"
    if metric != "period_net_goods_amount":
        return axis(metric == "refund_goods_amount") + order
    ctes = f"WITH paid AS ({axis(False)}), refunded AS ({axis(True)}) "
    if grain == "total" and spec.group_by is None:
        # Each aggregate returns one row; scalar reads avoid a Cartesian join.
        return (
            ctes
            + (
                "SELECT NULL::timestamptz AS bucket_start, NULL::text AS group_key, "
                "(SELECT paid_amount_cents FROM paid) AS paid_amount_cents, "
                "(SELECT refund_amount_cents FROM refunded) AS refund_amount_cents"
            )
            + order
        )
    keys = []
    if grain != "total":
        keys.append("p.bucket_start=r.bucket_start")
    if spec.group_by is not None:
        keys.append("p.group_key=r.group_key")
    return (
        ctes + "SELECT COALESCE(p.bucket_start,r.bucket_start) AS bucket_start, "
        "COALESCE(p.group_key,r.group_key) AS group_key, "
        "COALESCE(p.paid_amount_cents,0) AS paid_amount_cents, "
        "COALESCE(r.refund_amount_cents,0) AS refund_amount_cents "
        "FROM paid AS p FULL OUTER JOIN refunded AS r "
        "ON " + " AND ".join(keys) + order
    )
