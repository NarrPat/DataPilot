"""Local admin-only database setup. Passwords and DSNs never enter reports."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import psycopg
from alembic import command
from alembic.config import Config
from psycopg import sql
from psycopg.rows import dict_row
from sqlalchemy import URL, create_engine
from sqlalchemy.pool import NullPool

ROOT = Path(__file__).resolve().parents[2]
PASSWORD_VARIABLES = {
    "dp_bootstrap": "DP_POSTGRES_ADMIN_PASSWORD",
    "app_runtime": "DP_APP_DB_PASSWORD",
    "analyst_all": "DP_ANALYST_ALL_PASSWORD",
    "analyst_east": "DP_ANALYST_EAST_PASSWORD",
}
EVAL_DATABASE = re.compile(
    r"dp_eval_(?:i00[2-57-9]|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019|i023)_[a-z0-9_]{1,24}\Z"
)


class DatabaseSetupError(Exception):
    """A fixed code is safe to report; underlying connection errors stay private."""


def validate_database(name: str) -> None:
    if (
        name not in {"postgres", "dp_app", "dp_analytics"}
        and not EVAL_DATABASE.fullmatch(name)
        and not re.fullmatch(
            r"dp_app_eval_(?:i00[6-9]|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019)_[a-z0-9_]{1,30}",
            name,
        )
    ):
        raise DatabaseSetupError("DATABASE_OUTSIDE_REGISTERED_PROJECT_PREFIX")


def password(role: str) -> str:
    value = os.environ.get(PASSWORD_VARIABLES[role], "")
    if not value:
        raise DatabaseSetupError("REQUIRED_DATABASE_SECRET_MISSING")
    return value


def connect(
    database: str, role: str = "dp_bootstrap", *, autocommit: bool = False
) -> psycopg.Connection[dict[str, Any]]:
    validate_database(database)
    if role not in PASSWORD_VARIABLES:
        raise DatabaseSetupError("UNREGISTERED_DATABASE_ROLE")
    return psycopg.connect(
        host="postgres",
        port=5432,
        dbname=database,
        user=role,
        password=password(role),
        connect_timeout=5,
        autocommit=autocommit,
        application_name="datapilot-data-foundation",
        row_factory=dict_row,
    )


def _database_acl(admin: psycopg.Connection[dict[str, Any]], name: str, app: bool) -> None:
    validate_database(name)
    admin.execute(
        sql.SQL(
            "REVOKE ALL ON DATABASE {} FROM PUBLIC, app_runtime, analyst_all, analyst_east"
        ).format(sql.Identifier(name))
    )
    roles = sql.SQL("app_runtime") if app else sql.SQL("analyst_all, analyst_east")
    admin.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(name), roles))


def initialize() -> dict[str, object]:
    """Create the fixed project roles/databases; refuse incompatible existing roles."""
    with connect("postgres", autocommit=True) as admin:
        admin.execute("SELECT pg_advisory_lock(172903, 1)")
        try:
            for role in ("analytics_owner", "app_runtime", "analyst_all", "analyst_east"):
                current = admin.execute(
                    "SELECT oid, rolsuper, rolcreatedb, rolcreaterole, rolreplication, "
                    "rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname=%s",
                    (role,),
                ).fetchone()
                login = role != "analytics_owner"
                if current is not None:
                    privileged = any(
                        current[k]
                        for k in (
                            "rolsuper",
                            "rolcreatedb",
                            "rolcreaterole",
                            "rolreplication",
                            "rolbypassrls",
                        )
                    )
                    membership = admin.execute(
                        "SELECT count(*) AS n FROM pg_auth_members WHERE member=%s OR roleid=%s",
                        (current["oid"], current["oid"]),
                    ).fetchone()
                    if (
                        privileged
                        or current["rolcanlogin"] != login
                        or not membership
                        or membership["n"]
                    ):
                        raise DatabaseSetupError("EXISTING_ROLE_PRIVILEGES_INCOMPATIBLE")
                else:
                    admin.execute(
                        sql.SQL(
                            "CREATE ROLE {} NOSUPERUSER NOCREATEDB NOCREATEROLE "
                            "NOREPLICATION NOBYPASSRLS NOINHERIT {}"
                        ).format(sql.Identifier(role), sql.SQL("LOGIN" if login else "NOLOGIN"))
                    )
                if login:
                    # libpq creates a SCRAM verifier locally; plaintext never appears in DDL.
                    verifier = admin.pgconn.encrypt_password(
                        password(role).encode(), role.encode(), b"scram-sha-256"
                    ).decode()
                    admin.execute(
                        sql.SQL("ALTER ROLE {} PASSWORD {}").format(
                            sql.Identifier(role), sql.Literal(verifier)
                        )
                    )
            for name in ("dp_app", "dp_analytics"):
                existing = admin.execute(
                    "SELECT pg_get_userbyid(datdba) AS owner FROM pg_database WHERE datname=%s",
                    (name,),
                ).fetchone()
                if existing and existing["owner"] != "dp_bootstrap":
                    raise DatabaseSetupError("EXISTING_DATABASE_OWNER_INCOMPATIBLE")
                if not existing:
                    admin.execute(
                        sql.SQL("CREATE DATABASE {} OWNER dp_bootstrap").format(
                            sql.Identifier(name)
                        )
                    )
                _database_acl(admin, name, app=name == "dp_app")
            admin.execute("REVOKE CONNECT, TEMPORARY ON DATABASE postgres FROM PUBLIC")
        finally:
            admin.execute("SELECT pg_advisory_unlock(172903, 1)")
    with connect("dp_app") as app:
        app.execute(
            "REVOKE ALL ON SCHEMA public FROM PUBLIC, app_runtime, analyst_all, analyst_east"
        )
        app.execute("CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION dp_bootstrap")
        app.execute("REVOKE ALL ON SCHEMA app FROM PUBLIC, analyst_all, analyst_east, app_runtime")
        app.execute("GRANT USAGE ON SCHEMA app TO app_runtime")
    return {"databases": ["dp_app", "dp_analytics"], "roles_initialized": True}


def create_eval_database(name: str, *, on_created: Callable[[], None] | None = None) -> None:
    """Only a fresh, explicitly registered DP-I002 test database is accepted."""
    if not EVAL_DATABASE.fullmatch(name):
        raise DatabaseSetupError("EVAL_DATABASE_PREFIX_REQUIRED")
    with connect("postgres", autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            raise DatabaseSetupError("EVAL_DATABASE_ALREADY_EXISTS")
        admin.execute(sql.SQL("CREATE DATABASE {} OWNER dp_bootstrap").format(sql.Identifier(name)))
        if on_created is not None:
            on_created()
        _database_acl(admin, name, app=False)


def migrate(database: str) -> None:
    validate_database(database)
    if database in {"postgres", "dp_app"} or database.startswith("dp_app_eval_"):
        raise DatabaseSetupError("ANALYTICS_MIGRATION_TARGET_REQUIRED")
    engine = create_engine(
        URL.create(
            "postgresql+psycopg",
            username="dp_bootstrap",
            password=password("dp_bootstrap"),
            host="postgres",
            port=5432,
            database=database,
        ),
        echo=False,
        hide_parameters=True,
        poolclass=NullPool,
    )
    try:
        with engine.begin() as connection:
            config = Config(str(ROOT / "apps/datapilot/alembic.ini"))
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()
