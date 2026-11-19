"""Bind new immutable results to execution inputs; legacy provenance stays NULL."""

from alembic import op

revision = "app_0009_artifact_binding"
down_revision = "app_0008_model_audit"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE app.query_attempt_results
        ADD COLUMN binding jsonb,
        ADD COLUMN binding_sha256 text,
        ADD CONSTRAINT result_binding_shape CHECK (
            (binding IS NULL AND binding_sha256 IS NULL) OR
            (binding IS NOT NULL AND jsonb_typeof(binding)='object'
             AND binding_sha256 IS NOT NULL AND binding_sha256 ~ '^[0-9a-f]{64}$'
             AND octet_length(binding::text)<=65536)
        )
    """)
    # Existing SELECT/INSERT grants remain; runtime cannot rewrite/delete evidence.


def downgrade() -> None:
    op.execute(
        "ALTER TABLE app.query_attempt_results DROP CONSTRAINT result_binding_shape, "
        "DROP COLUMN binding_sha256,DROP COLUMN binding"
    )
