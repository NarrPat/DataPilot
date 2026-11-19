"""Immutable follow-up semantic input and administrator-imported reports."""

from alembic import op

revision = "app_0006_read_api"
down_revision = "app_0005_recovery"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE app.query_runs ADD COLUMN base_confirmed_snapshot jsonb")
    op.execute("GRANT USAGE ON SCHEMA public TO app_runtime")
    op.execute("GRANT SELECT ON public.alembic_version TO app_runtime")
    # Legacy base refs never had this validated snapshot: keep them unknown.
    op.execute("""
        CREATE TABLE app.eval_reports (
            report_id uuid PRIMARY KEY,
            created_at timestamptz NOT NULL,
            document jsonb NOT NULL,
            document_sha256 char(64) NOT NULL
        )
    """)
    op.execute("REVOKE ALL ON app.eval_reports FROM PUBLIC,app_runtime,analyst_all,analyst_east")
    op.execute("GRANT SELECT ON app.eval_reports TO app_runtime")
    op.execute(
        "CREATE INDEX ix_reports_created ON app.eval_reports(created_at DESC,report_id DESC)"
    )


def downgrade() -> None:
    op.execute("REVOKE SELECT ON public.alembic_version FROM app_runtime")
    op.execute("REVOKE USAGE ON SCHEMA public FROM app_runtime")
    op.execute("DROP TABLE app.eval_reports")
    op.execute("ALTER TABLE app.query_runs DROP COLUMN base_confirmed_snapshot")
