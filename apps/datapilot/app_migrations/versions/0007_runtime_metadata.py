"""Expose one admin-owned migration version column without public-schema access."""

from alembic import op

revision = "app_0007_runtime_metadata"
down_revision = "app_0006_read_api"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "CREATE VIEW app.schema_version WITH (security_barrier=true,security_invoker=false) "
        "AS SELECT version_num FROM public.alembic_version"
    )
    op.execute("ALTER VIEW app.schema_version OWNER TO dp_bootstrap")
    op.execute("REVOKE ALL ON app.schema_version FROM PUBLIC,app_runtime,analyst_all,analyst_east")
    op.execute("GRANT SELECT ON app.schema_version TO app_runtime")
    op.execute("REVOKE SELECT ON public.alembic_version FROM app_runtime")
    op.execute("REVOKE USAGE ON SCHEMA public FROM app_runtime")


def downgrade() -> None:
    op.execute("DROP VIEW app.schema_version")
    # Restore the immediately preceding revision's explicit privilege contract.
    op.execute("GRANT USAGE ON SCHEMA public TO app_runtime")
    op.execute("GRANT SELECT ON public.alembic_version TO app_runtime")
