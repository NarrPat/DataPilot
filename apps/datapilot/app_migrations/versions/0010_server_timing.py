"""Append-only real clock facts; legacy rows receive no invented telemetry."""

from alembic import op

revision = "app_0010_server_timing"
down_revision = "app_0009_artifact_binding"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE app.query_timing_origins (
            query_id uuid PRIMARY KEY REFERENCES app.query_runs(query_id),
            accepted_request_id uuid,
            point jsonb NOT NULL CHECK(jsonb_typeof(point)='object'
                AND octet_length(point::text)<=16384),
            db_recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
        )
    """)
    op.execute("""
        CREATE TABLE app.query_timing_events (
            query_id uuid NOT NULL REFERENCES app.query_runs(query_id),
            seq bigint NOT NULL CHECK(seq BETWEEN 1 AND 4096),
            status_version bigint NOT NULL CHECK(status_version>=1),
            spec_version integer NOT NULL CHECK(spec_version>=0),
            execution_generation integer NOT NULL CHECK(execution_generation>=1),
            status text NOT NULL,
            point jsonb NOT NULL CHECK(jsonb_typeof(point)='object'
                AND octet_length(point::text)<=16384),
            db_recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(query_id,seq)
        )
    """)
    # No FK to public event retention rows: removing expired SSE events must not
    # silently destroy the private timing evidence.
    op.execute("""
        CREATE TABLE app.query_timing_checks (
            query_id uuid PRIMARY KEY REFERENCES app.query_runs(query_id),
            last_event_seq bigint NOT NULL CHECK(last_event_seq>=1),
            final_status text NOT NULL,
            result_ref uuid,
            outcome text NOT NULL CHECK(outcome IN ('verified','invalid')),
            reason_code text,
            inputs_sha256 text NOT NULL CHECK(inputs_sha256 ~ '^[0-9a-f]{64}$'),
            projection_sha256 text NOT NULL CHECK(projection_sha256 ~ '^[0-9a-f]{64}$'),
            point jsonb NOT NULL CHECK(jsonb_typeof(point)='object'
                AND octet_length(point::text)<=16384),
            db_recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CHECK((outcome='verified' AND reason_code IS NULL) OR
                  (outcome='invalid' AND reason_code IS NOT NULL))
        )
    """)
    for table in ("query_timing_origins", "query_timing_events", "query_timing_checks"):
        op.execute(f"REVOKE ALL ON app.{table} FROM PUBLIC,app_runtime,analyst_all,analyst_east")
        op.execute(f"GRANT SELECT,INSERT ON app.{table} TO app_runtime")


def downgrade() -> None:
    for table in ("query_timing_checks", "query_timing_events", "query_timing_origins"):
        op.execute(f"DROP TABLE app.{table}")
