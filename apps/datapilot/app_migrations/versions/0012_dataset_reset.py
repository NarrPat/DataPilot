"""Shared maintenance gate, published data version and retained reset audit."""

from alembic import op

revision = "app_0012_dataset_reset"
down_revision = "app_0011_wait_commits"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE app.runtime_limits ADD COLUMN maintenance boolean NOT NULL DEFAULT false, "
        "ADD COLUMN published_data_version text, ADD COLUMN published_data_hash char(64), "
        "ADD COLUMN reset_operation_id uuid"
    )
    op.execute("REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON app.runtime_limits FROM app_runtime")
    op.execute("GRANT UPDATE(active_reserved,logical_queued) ON app.runtime_limits TO app_runtime")
    op.execute(
        "CREATE TABLE app.dataset_resets (operation_id uuid PRIMARY KEY, "
        "manifest_hash char(64) NOT NULL, target jsonb NOT NULL, "
        "expected_data_version text NOT NULL, target_data_version text NOT NULL, "
        "source_projection_hash char(64) NOT NULL, started_at timestamptz NOT NULL, "
        "drain_deadline timestamptz NOT NULL, finished_at timestamptz, reason_code text, "
        "status text NOT NULL CHECK(status IN "
        "('draining','loading','ready','failed','drain_timed_out')))"
    )
    op.execute(
        "CREATE TABLE app.dataset_reset_events ("
        "seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
        "operation_id uuid NOT NULL REFERENCES app.dataset_resets(operation_id), "
        "occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(), "
        "stage text NOT NULL, document jsonb NOT NULL)"
    )
    op.execute(
        "REVOKE ALL ON app.dataset_resets,app.dataset_reset_events "
        "FROM PUBLIC,app_runtime,analyst_all,analyst_east"
    )


def downgrade() -> None:
    op.execute("DROP TABLE app.dataset_reset_events")
    op.execute("DROP TABLE app.dataset_resets")
    op.execute(
        "ALTER TABLE app.runtime_limits DROP COLUMN maintenance, "
        "DROP COLUMN published_data_version, "
        "DROP COLUMN published_data_hash, DROP COLUMN reset_operation_id"
    )
    op.execute("GRANT INSERT,UPDATE ON app.runtime_limits TO app_runtime")
