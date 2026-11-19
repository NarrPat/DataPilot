"""Stable clarification input, verified wait projection and atomic resume facts."""

from alembic import op

revision = "app_0004_clarifications"
down_revision = "app_0003_query_fragment"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE app.query_clarifications (
            clarification_id uuid PRIMARY KEY,
            query_id uuid UNIQUE NOT NULL REFERENCES app.query_runs(query_id),
            spec_version integer NOT NULL CHECK(spec_version=1),
            payload jsonb NOT NULL,
            graph_version text NOT NULL,
            catalog_version text NOT NULL,
            data_version text NOT NULL,
            created_at timestamptz NOT NULL,
            checkpoint_id text,
            wait_started_at timestamptz,
            expires_at timestamptz,
            answer jsonb,
            answered_at timestamptz,
            resume_generation integer,
            status text NOT NULL DEFAULT 'prepared'
                CHECK(status IN (
                    'prepared','waiting','answered','expired','incompatible','canceled')),
            CHECK((answer IS NULL)=(answered_at IS NULL)),
            CHECK((answer IS NULL)=(resume_generation IS NULL)),
            CHECK(expires_at IS NULL OR expires_at>wait_started_at),
            CHECK(resume_generation IS NULL OR resume_generation>=2),
            CHECK(status<>'waiting' OR (
                checkpoint_id IS NOT NULL AND wait_started_at IS NOT NULL
                AND expires_at IS NOT NULL)),
            CHECK(status<>'answered' OR answer IS NOT NULL)
        )
    """)
    op.execute("REVOKE ALL ON app.query_clarifications FROM PUBLIC,analyst_all,analyst_east")
    op.execute("GRANT SELECT,INSERT,UPDATE ON app.query_clarifications TO app_runtime")
    op.execute("""
        ALTER TABLE app.query_runs
        ADD COLUMN clarification_ref uuid REFERENCES app.query_clarifications(clarification_id),
        ADD COLUMN clarification_snapshot jsonb,
        ADD COLUMN waiting_since timestamptz,
        ADD COLUMN wait_expires_at timestamptz,
        ADD COLUMN waiting_checkpoint_id text,
        ADD COLUMN processing_reference_time timestamptz,
        ADD COLUMN graph_step_count integer NOT NULL DEFAULT 0
            CHECK(graph_step_count BETWEEN 0 AND 24)
    """)
    op.execute(
        "CREATE INDEX ix_clarification_expiry ON app.query_runs(wait_expires_at) "
        "WHERE status='needs_clarification'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX app.ix_clarification_expiry")
    op.execute("""
        ALTER TABLE app.query_runs
        DROP COLUMN graph_step_count,
        DROP COLUMN processing_reference_time,
        DROP COLUMN waiting_checkpoint_id,
        DROP COLUMN wait_expires_at,
        DROP COLUMN waiting_since,
        DROP COLUMN clarification_snapshot,
        DROP COLUMN clarification_ref
    """)
    op.execute("DROP TABLE app.query_clarifications")
