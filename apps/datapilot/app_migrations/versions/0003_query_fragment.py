"""Bounded query fragment artifacts and outbox retry facts; no recovery scanner."""

from alembic import op

revision = "app_0003_query_fragment"
down_revision = "app_0002_tasks"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        DO $checkpoint_owner$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_namespace WHERE nspname='query_checkpoints'
                AND pg_get_userbyid(nspowner)<>'dp_bootstrap'
            ) THEN
                RAISE EXCEPTION 'CHECKPOINT_SCHEMA_OWNER_MISMATCH';
            END IF;
        END
        $checkpoint_owner$
    """)
    op.execute("CREATE SCHEMA IF NOT EXISTS query_checkpoints AUTHORIZATION dp_bootstrap")
    op.execute(
        "REVOKE ALL ON SCHEMA query_checkpoints FROM PUBLIC,app_runtime,analyst_all,analyst_east"
    )
    op.execute("GRANT USAGE ON SCHEMA query_checkpoints TO app_runtime")
    op.execute("""
        CREATE TABLE app.query_attempt_results (
            attempt_id uuid PRIMARY KEY,
            query_id uuid NOT NULL REFERENCES app.query_runs(query_id),
            execution_generation integer NOT NULL CHECK(execution_generation>=1),
            result jsonb NOT NULL,
            created_at timestamptz NOT NULL
        )
    """)
    op.execute("REVOKE ALL ON app.query_attempt_results FROM PUBLIC,analyst_all,analyst_east")
    op.execute("GRANT SELECT,INSERT ON app.query_attempt_results TO app_runtime")
    op.execute("""
        ALTER TABLE app.query_runs
        ADD COLUMN confirmed_spec jsonb,
        ADD COLUMN query_plan jsonb,
        ADD COLUMN guard_summary jsonb,
        ADD COLUMN result_snapshot jsonb,
        ADD COLUMN result_ref uuid REFERENCES app.query_attempt_results(attempt_id)
    """)
    op.execute("""
        ALTER TABLE app.query_outbox
        ADD COLUMN dispatch_attempts integer NOT NULL DEFAULT 0 CHECK(dispatch_attempts>=0),
        ADD COLUMN next_attempt_at timestamptz,
        ADD COLUMN last_dispatch_code text
    """)


def downgrade() -> None:
    op.execute(
        "ALTER TABLE app.query_outbox DROP COLUMN last_dispatch_code, "
        "DROP COLUMN next_attempt_at,DROP COLUMN dispatch_attempts"
    )
    op.execute(
        "ALTER TABLE app.query_runs DROP COLUMN result_ref,DROP COLUMN result_snapshot,"
        "DROP COLUMN guard_summary,DROP COLUMN query_plan,DROP COLUMN confirmed_spec"
    )
    op.execute("DROP TABLE app.query_attempt_results")
    # Checkpoint history is retained. Destructive rollback of a saver schema is
    # deliberately outside this application migration.
