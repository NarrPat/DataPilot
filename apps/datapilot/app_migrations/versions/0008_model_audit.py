"""Durable model intentions, observed milestones and explicitly bounded live probes."""

from alembic import op

revision = "app_0008_model_audit"
down_revision = "app_0007_runtime_metadata"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE app.query_runs ADD COLUMN accepted_request_id uuid")
    op.execute("""
        CREATE TABLE app.model_audit_budgets (
            budget_id uuid PRIMARY KEY,
            deadline_at timestamptz NOT NULL,
            attempt_limit integer NOT NULL CHECK(attempt_limit BETWEEN 1 AND 4),
            query_limit integer NOT NULL CHECK(query_limit BETWEEN 1 AND 2),
            reserved_attempts integer NOT NULL DEFAULT 0
                CHECK(reserved_attempts BETWEEN 0 AND 4)
        )
    """)
    op.execute("""
        CREATE TABLE app.model_invocations (
            invocation_id uuid PRIMARY KEY,
            query_id uuid NOT NULL REFERENCES app.query_runs(query_id),
            execution_generation integer NOT NULL CHECK(execution_generation > 0),
            call_ordinal integer NOT NULL CHECK(call_ordinal BETWEEN 1 AND 4),
            request_id uuid,
            budget_id uuid REFERENCES app.model_audit_budgets(budget_id),
            stage text NOT NULL CHECK(stage IN ('parse','parse_clarification','generate','repair')),
            started_at timestamptz NOT NULL,
            finished_at timestamptz,
            document jsonb NOT NULL CHECK(jsonb_typeof(document)='object'),
            UNIQUE(query_id,call_ordinal)
        )
    """)
    for table in ("model_audit_budgets", "model_invocations"):
        op.execute(f"REVOKE ALL ON app.{table} FROM PUBLIC,app_runtime,analyst_all,analyst_east")
    # Only the administrator can register a budget. Runtime can consume it.
    op.execute("GRANT SELECT,UPDATE ON app.model_audit_budgets TO app_runtime")
    op.execute("GRANT SELECT,INSERT,UPDATE ON app.model_invocations TO app_runtime")


def downgrade() -> None:
    op.execute("DROP TABLE app.model_invocations")
    op.execute("DROP TABLE app.model_audit_budgets")
    op.execute("ALTER TABLE app.query_runs DROP COLUMN accepted_request_id")
