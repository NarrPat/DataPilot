"""Claim facts and bounded recovery generations; preserve original admission."""

from alembic import op

revision = "app_0005_recovery"
down_revision = "app_0004_clarifications"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Historical rows have no recorded model snapshot. Do not invent one now.
    op.execute("""
        ALTER TABLE app.query_runs
        ADD COLUMN recover_count integer NOT NULL DEFAULT 0 CHECK(recover_count BETWEEN 0 AND 2),
        ADD COLUMN claimed_generation integer CHECK(claimed_generation>=1),
        ADD COLUMN claimed_at timestamptz,
        ADD COLUMN recovery_pending boolean NOT NULL DEFAULT false,
        ADD COLUMN recovery_scheduled_at timestamptz,
        ADD COLUMN model_config_version text NOT NULL DEFAULT 'legacy:i008-unrecorded',
        ADD CONSTRAINT ck_claim_pair CHECK((claimed_generation IS NULL)=(claimed_at IS NULL)),
        ADD CONSTRAINT ck_claim_generation CHECK(claimed_generation<=execution_generation)
    """)
    op.execute(
        "CREATE INDEX ix_recovery_active ON app.query_runs(claimed_at,query_id) "
        "WHERE admission_state='active'"
    )


def downgrade() -> None:
    op.execute("DROP INDEX app.ix_recovery_active")
    op.execute("""
        ALTER TABLE app.query_runs DROP CONSTRAINT ck_claim_generation,
        DROP CONSTRAINT ck_claim_pair,DROP COLUMN model_config_version,
        DROP COLUMN recovery_scheduled_at,DROP COLUMN recovery_pending,
        DROP COLUMN claimed_at,DROP COLUMN claimed_generation,DROP COLUMN recover_count
    """)
