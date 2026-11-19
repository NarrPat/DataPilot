"""Five analytics business tables, fixed scope views and seed-version metadata.

Requirement: DP-F01; supports DP-T001/002/009/068. No model or application tables.
Global roles and CONNECT grants are bootstrap responsibilities, not this revision.
Cross-row sums and event-time relationships belong to the fixture validator.
"""

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | tuple[str, ...] | None = None
depends_on: str | tuple[str, ...] | None = None


TABLE_DDL = (
    """
    CREATE TABLE raw.customers (
        customer_id bigint PRIMARY KEY,
        registered_at timestamptz NOT NULL,
        home_region text NOT NULL,
        is_active boolean NOT NULL,
        CONSTRAINT ck_customers_home_region
            CHECK (home_region IN ('east', 'north', 'south', 'west'))
    )
    """,
    """
    CREATE TABLE raw.products (
        product_id bigint PRIMARY KEY,
        category_code text NOT NULL,
        product_name text NOT NULL,
        status text NOT NULL,
        CONSTRAINT ck_products_category_code CHECK (
            category_code IN (
                'books', 'electronics', 'apparel', 'food',
                'home', 'sports', 'beauty', 'toys'
            )
        )
    )
    """,
    """
    CREATE TABLE raw.orders (
        order_id bigint PRIMARY KEY,
        customer_id bigint NOT NULL REFERENCES raw.customers (customer_id),
        created_at timestamptz NOT NULL,
        paid_at timestamptz,
        payment_status text NOT NULL,
        sales_region text NOT NULL,
        paid_goods_amount_cents bigint NOT NULL,
        currency char(3) NOT NULL,
        CONSTRAINT ck_orders_payment_status
            CHECK (payment_status IN ('unpaid', 'paid', 'canceled_unpaid')),
        CONSTRAINT ck_orders_paid_at
            CHECK (payment_status <> 'paid' OR paid_at IS NOT NULL),
        CONSTRAINT ck_orders_sales_region
            CHECK (sales_region IN ('east', 'north', 'south', 'west')),
        CONSTRAINT ck_orders_paid_goods_amount CHECK (paid_goods_amount_cents >= 0),
        CONSTRAINT ck_orders_currency CHECK (currency = 'CNY')
    )
    """,
    """
    CREATE TABLE raw.order_items (
        order_item_id bigint PRIMARY KEY,
        order_id bigint NOT NULL REFERENCES raw.orders (order_id),
        product_id bigint NOT NULL REFERENCES raw.products (product_id),
        quantity integer NOT NULL,
        paid_amount_cents bigint NOT NULL,
        CONSTRAINT ck_order_items_quantity CHECK (quantity > 0),
        CONSTRAINT ck_order_items_paid_amount CHECK (paid_amount_cents >= 0)
    )
    """,
    """
    CREATE TABLE raw.refunds (
        refund_id bigint PRIMARY KEY,
        order_item_id bigint NOT NULL REFERENCES raw.order_items (order_item_id),
        requested_at timestamptz NOT NULL,
        succeeded_at timestamptz,
        status text NOT NULL,
        amount_cents bigint NOT NULL,
        CONSTRAINT ck_refunds_status CHECK (status IN ('pending', 'succeeded', 'failed')),
        CONSTRAINT ck_refunds_succeeded_at
            CHECK (status <> 'succeeded' OR succeeded_at IS NOT NULL),
        CONSTRAINT ck_refunds_amount CHECK (amount_cents > 0)
    )
    """,
)

INDEX_DDL = (
    "CREATE INDEX ix_orders_customer_id ON raw.orders (customer_id)",
    """CREATE INDEX ix_orders_scope_paid_at ON raw.orders (sales_region, paid_at)
       WHERE payment_status = 'paid'""",
    "CREATE INDEX ix_order_items_order_id ON raw.order_items (order_id)",
    "CREATE INDEX ix_order_items_product_id ON raw.order_items (product_id)",
    "CREATE INDEX ix_refunds_order_item_id ON raw.refunds (order_item_id)",
    """CREATE INDEX ix_refunds_succeeded_at ON raw.refunds (succeeded_at)
       WHERE status = 'succeeded'""",
)

# Explicit projected columns prevent a later raw-column addition from broadening
# the model's allowed schema. EXISTS preserves dimension grain without DISTINCT.
VIEW_QUERIES = {
    "customers": """
        SELECT c.customer_id, c.registered_at, c.home_region, c.is_active
        FROM raw.customers AS c
        WHERE EXISTS (
            SELECT 1 FROM raw.orders AS o
            WHERE o.customer_id = c.customer_id {scope_filter}
        )
    """,
    "products": """
        SELECT p.product_id, p.category_code, p.product_name, p.status
        FROM raw.products AS p
        WHERE EXISTS (
            SELECT 1 FROM raw.order_items AS i
            JOIN raw.orders AS o ON o.order_id = i.order_id
            WHERE i.product_id = p.product_id {scope_filter}
        )
    """,
    "orders": """
        SELECT o.order_id, o.customer_id, o.created_at, o.paid_at,
               o.payment_status, o.sales_region, o.paid_goods_amount_cents, o.currency
        FROM raw.orders AS o
        WHERE true {scope_filter}
    """,
    "order_items": """
        SELECT i.order_item_id, i.order_id, i.product_id, i.quantity, i.paid_amount_cents
        FROM raw.order_items AS i
        JOIN raw.orders AS o ON o.order_id = i.order_id
        WHERE true {scope_filter}
    """,
    "refunds": """
        SELECT r.refund_id, r.order_item_id, r.requested_at, r.succeeded_at,
               r.status, r.amount_cents
        FROM raw.refunds AS r
        JOIN raw.order_items AS i ON i.order_item_id = r.order_item_id
        JOIN raw.orders AS o ON o.order_id = i.order_id
        WHERE true {scope_filter}
    """,
}


def upgrade() -> None:
    # format(%I, current_database()) safely quotes even a nonstandard database
    # identifier. This also works in offline SQL when applied to its target DB.
    op.execute(
        """
        DO $datapilot$
        BEGIN
            EXECUTE format(
                'REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database()
            );
            EXECUTE format(
                'REVOKE CREATE, TEMPORARY ON DATABASE %I '
                'FROM analyst_all, analyst_east, app_runtime', current_database()
            );
        END
        $datapilot$
        """
    )
    op.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC, analyst_all, analyst_east, app_runtime")
    for schema in ("raw", "analytics_views", "analytics_meta"):
        op.execute(f"CREATE SCHEMA {schema} AUTHORIZATION analytics_owner")
        op.execute(
            f"REVOKE ALL ON SCHEMA {schema} FROM PUBLIC, analyst_all, analyst_east, app_runtime"
        )

    for statement in TABLE_DDL:
        op.execute(statement)
    for table in VIEW_QUERIES:
        op.execute(f"ALTER TABLE raw.{table} OWNER TO analytics_owner")
        op.execute(
            f"REVOKE ALL ON TABLE raw.{table} FROM PUBLIC, analyst_all, analyst_east, app_runtime"
        )
    for statement in INDEX_DDL:
        op.execute(statement)

    op.execute(
        """
        CREATE TABLE analytics_meta.dataset_state (
            id boolean PRIMARY KEY,
            fixture_id text NOT NULL,
            data_version text NOT NULL,
            projection_hash char(64) NOT NULL,
            row_counts jsonb NOT NULL,
            loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CONSTRAINT ck_dataset_state_single_row CHECK (id = true),
            CONSTRAINT ck_dataset_state_projection_hash
                CHECK (projection_hash ~ '^[0-9a-f]{64}$')
        )
        """
    )
    op.execute("ALTER TABLE analytics_meta.dataset_state OWNER TO analytics_owner")
    op.execute(
        "REVOKE ALL ON TABLE analytics_meta.dataset_state "
        "FROM PUBLIC, analyst_all, analyst_east, app_runtime"
    )

    for scope, scope_filter in (("all", ""), ("east", "AND o.sales_region = 'east'")):
        for table, query in VIEW_QUERIES.items():
            view = f"analytics_views.{table}_{scope}"
            op.execute(
                f"CREATE VIEW {view} WITH (security_barrier = true) AS "
                + query.format(scope_filter=scope_filter)
            )
            op.execute(f"ALTER VIEW {view} OWNER TO analytics_owner")
            op.execute(
                f"REVOKE ALL ON TABLE {view} FROM PUBLIC, analyst_all, analyst_east, app_runtime"
            )
            op.execute(f"GRANT SELECT ON TABLE {view} TO analyst_{scope}")
    op.execute("GRANT USAGE ON SCHEMA analytics_views TO analyst_all, analyst_east")


def downgrade() -> None:
    # No CASCADE: an unrelated object/dependency must block downgrade rather than
    # being silently removed. Global roles, database and hardened PUBLIC ACL stay.
    for scope in ("all", "east"):
        for table in reversed(VIEW_QUERIES):
            op.execute(f"DROP VIEW analytics_views.{table}_{scope}")
    op.execute("DROP TABLE analytics_meta.dataset_state")
    for table in ("refunds", "order_items", "orders", "products", "customers"):
        op.execute(f"DROP TABLE raw.{table}")
    for schema in ("analytics_meta", "analytics_views", "raw"):
        op.execute(f"DROP SCHEMA {schema}")
