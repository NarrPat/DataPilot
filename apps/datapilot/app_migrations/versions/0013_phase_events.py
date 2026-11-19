"""Private append-only phase facts; never backfill historical durations."""

from alembic import op

revision = "app_0013_phase_events"
down_revision = "app_0012_dataset_reset"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE app.query_phase_events (
            event_id uuid PRIMARY KEY,
            query_id uuid NOT NULL REFERENCES app.query_runs(query_id),
            accepted_request_id uuid,
            execution_generation integer NOT NULL CHECK(execution_generation>=1),
            span_id uuid NOT NULL,
            parent_span_id uuid,
            phase text NOT NULL CHECK(length(phase) BETWEEN 1 AND 64),
            boundary text NOT NULL CHECK(boundary IN ('start','finish','summary')),
            outcome text CHECK(outcome IN ('returned','raised')),
            point jsonb NOT NULL CHECK(jsonb_typeof(point)='object'
                AND octet_length(point::text)<=16384),
            details jsonb NOT NULL CHECK(jsonb_typeof(details)='object'
                AND octet_length(details::text)<=16384),
            db_recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            UNIQUE(span_id,boundary),
            CHECK((boundary='finish' AND outcome IS NOT NULL) OR
                  (boundary IN ('start','summary') AND outcome IS NULL))
        )
    """)
    op.execute("""
        CREATE INDEX query_phase_events_query_generation
        ON app.query_phase_events(query_id,execution_generation,db_recorded_at,event_id)
    """)
    op.execute(
        "REVOKE ALL ON app.query_phase_events FROM PUBLIC,app_runtime,analyst_all,analyst_east"
    )
    op.execute("GRANT SELECT,INSERT ON app.query_phase_events TO app_runtime")


def downgrade() -> None:
    op.execute("DROP TABLE app.query_phase_events")
