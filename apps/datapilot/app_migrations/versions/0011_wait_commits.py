"""Original WAIT transaction COMMIT-return facts; no invented historical clocks."""

from alembic import op

revision = "app_0011_wait_commits"
down_revision = "app_0010_server_timing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE app.query_wait_commits (
            query_id uuid NOT NULL,
            waiting_event_seq bigint NOT NULL CHECK(waiting_event_seq>=1),
            status_version bigint NOT NULL CHECK(status_version>=1),
            spec_version integer NOT NULL CHECK(spec_version>=0),
            execution_generation integer NOT NULL CHECK(execution_generation>=1),
            point jsonb NOT NULL CHECK(jsonb_typeof(point)='object'
                AND octet_length(point::text)<=16384),
            db_recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(query_id,waiting_event_seq),
            FOREIGN KEY(query_id,waiting_event_seq)
                REFERENCES app.query_timing_events(query_id,seq)
        )
    """)
    op.execute(
        "REVOKE ALL ON app.query_wait_commits FROM PUBLIC,app_runtime,analyst_all,analyst_east"
    )
    op.execute("GRANT SELECT,INSERT ON app.query_wait_commits TO app_runtime")
    op.execute("""
        ALTER TABLE app.query_timing_checks
        ADD COLUMN timing_version integer NOT NULL DEFAULT 1,
        ADD COLUMN waits_sha256 text,
        ADD COLUMN timing_reason_code text,
        ADD CONSTRAINT timing_version_shape CHECK(
            (timing_version=1 AND waits_sha256 IS NULL AND timing_reason_code IS NULL) OR
            (timing_version=2 AND waits_sha256 ~ '^[0-9a-f]{64}$' AND waits_sha256 IS NOT NULL
             AND (timing_reason_code IS NULL OR timing_reason_code IN (
                'WAIT_COMMIT_POINT_MISSING','WAIT_COMMIT_AFTER_END','WAIT_END_POINT_MISSING')))
        )
    """)


def downgrade() -> None:
    op.execute(
        "ALTER TABLE app.query_timing_checks DROP CONSTRAINT timing_version_shape, "
        "DROP COLUMN timing_reason_code,DROP COLUMN waits_sha256,DROP COLUMN timing_version"
    )
    op.execute("DROP TABLE app.query_wait_commits")
