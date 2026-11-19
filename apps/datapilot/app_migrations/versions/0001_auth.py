"""App-local users, opaque hashed sessions and one-use anonymous CSRF bootstrap.

COM-F01/02; DP-F03. Runtime gets DML/SELECT only, never migration privileges.
The checkpoint schema is separate and is not created by this revision.
"""

from alembic import op

revision = "app_0001_auth"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("REVOKE ALL ON SCHEMA public FROM PUBLIC, app_runtime, analyst_all, analyst_east")
    op.execute("""
        DO $app_owner$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_namespace n WHERE n.nspname='app'
                AND pg_get_userbyid(n.nspowner)<>'dp_bootstrap'
            ) THEN
                RAISE EXCEPTION 'APP_SCHEMA_OWNER_MISMATCH';
            END IF;
        END
        $app_owner$
    """)
    op.execute("CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION dp_bootstrap")
    op.execute("REVOKE ALL ON SCHEMA app FROM PUBLIC, analyst_all, analyst_east, app_runtime")
    op.execute("GRANT USAGE ON SCHEMA app TO app_runtime")
    op.execute("""
        CREATE TABLE app.users (
            user_id uuid PRIMARY KEY,
            username varchar(128) UNIQUE NOT NULL,
            password_hash text NOT NULL,
            role text NOT NULL CHECK (role IN ('analyst_east','analyst_all','report_admin')),
            is_active boolean NOT NULL DEFAULT true,
            authz_version bigint NOT NULL DEFAULT 1 CHECK (authz_version >= 1),
            created_at timestamptz NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TABLE app.sessions (
            session_hash char(64) PRIMARY KEY CHECK (session_hash ~ '^[0-9a-f]{64}$'),
            user_id uuid NOT NULL REFERENCES app.users(user_id),
            app_id text NOT NULL DEFAULT 'datapilot' CHECK (app_id='datapilot'),
            csrf_hash char(64) NOT NULL CHECK (csrf_hash ~ '^[0-9a-f]{64}$'),
            created_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            revoked_at timestamptz,
            CHECK (expires_at > created_at)
        )
    """)
    op.execute("CREATE INDEX ix_sessions_actor ON app.sessions(user_id)")
    op.execute("CREATE INDEX ix_sessions_expiry ON app.sessions(expires_at)")
    op.execute("""
        CREATE TABLE app.anonymous_csrf (
            nonce_hash char(64) PRIMARY KEY CHECK (nonce_hash ~ '^[0-9a-f]{64}$'),
            csrf_hash char(64) NOT NULL CHECK (csrf_hash ~ '^[0-9a-f]{64}$'),
            app_id text NOT NULL DEFAULT 'datapilot' CHECK (app_id='datapilot'),
            created_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            used_at timestamptz,
            CHECK (expires_at > created_at)
        )
    """)
    op.execute("CREATE INDEX ix_anonymous_csrf_expiry ON app.anonymous_csrf(expires_at)")
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA app FROM PUBLIC, analyst_all, analyst_east")
    op.execute("GRANT SELECT ON app.users TO app_runtime")
    op.execute(
        "GRANT SELECT, INSERT, UPDATE, DELETE ON app.sessions, app.anonymous_csrf TO app_runtime"
    )


def downgrade() -> None:
    op.execute("DROP TABLE app.anonymous_csrf")
    op.execute("DROP TABLE app.sessions")
    op.execute("DROP TABLE app.users")
    op.execute("DROP SCHEMA app")
