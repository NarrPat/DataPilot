"""Admin-only app migration and strictly registered ephemeral test databases."""

from __future__ import annotations

import os
import re
from collections.abc import Callable

from alembic import command
from alembic.config import Config
from psycopg import sql
from sqlalchemy import URL, create_engine
from sqlalchemy.pool import NullPool

from datapilot.db.bootstrap import ROOT, DatabaseSetupError, _database_acl, connect, password

APP_EVAL_DATABASE = re.compile(
    r"dp_app_eval_(?:i00[6-9]|i010|i011|i012|i013|i014|i015|i016|i017|i018|i019)_[a-z0-9_]{1,30}\Z"
)


def migrate_app(database: str = "dp_app") -> None:
    if database != "dp_app" and not APP_EVAL_DATABASE.fullmatch(database):
        raise DatabaseSetupError("APPLICATION_MIGRATION_TARGET_REQUIRED")
    engine = create_engine(
        URL.create(
            "postgresql+psycopg",
            username="dp_bootstrap",
            password=password("dp_bootstrap"),
            host="postgres",
            port=5432,
            database=database,
        ),
        hide_parameters=True,
        poolclass=NullPool,
    )
    try:
        with engine.begin() as connection:
            config = Config(str(ROOT / "apps/datapilot/app-alembic.ini"))
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()
    setup_query_checkpoints(database)


def setup_query_checkpoints(database: str) -> None:
    """Admin-only, autocommit setup for the locked PostgresSaver migrations.

    Its concurrent indexes cannot run inside the app Alembic transaction. A
    failed setup is reported as incomplete setup; rerunning is idempotent.
    Runtime never calls setup or receives schema CREATE privileges.
    """
    from langgraph.checkpoint.postgres import PostgresSaver

    if database != "dp_app" and not APP_EVAL_DATABASE.fullmatch(database):
        raise DatabaseSetupError("APPLICATION_CHECKPOINT_TARGET_REQUIRED")
    with connect(database, autocommit=True) as admin:
        owner = admin.execute(
            "SELECT pg_get_userbyid(nspowner) AS owner FROM pg_namespace "
            "WHERE nspname='query_checkpoints'"
        ).fetchone()
        if not owner or owner["owner"] != "dp_bootstrap":
            raise DatabaseSetupError("APPLICATION_CHECKPOINT_SCHEMA_REQUIRED")
        admin.execute("SELECT pg_advisory_lock(172903, 7)")
        try:
            admin.execute("SET search_path TO query_checkpoints, pg_catalog")
            admin.prepare_threshold = 0
            PostgresSaver(admin).setup()
            admin.execute(
                "REVOKE ALL ON ALL TABLES IN SCHEMA query_checkpoints "
                "FROM PUBLIC, app_runtime, analyst_all, analyst_east"
            )
            admin.execute(
                "GRANT SELECT,INSERT,UPDATE,DELETE ON query_checkpoints.checkpoints,"
                "query_checkpoints.checkpoint_blobs,query_checkpoints.checkpoint_writes "
                "TO app_runtime"
            )
        finally:
            admin.execute("SELECT pg_advisory_unlock(172903, 7)")


def create_app_eval_database(
    database: str,
    *,
    spike_schema: bool = False,
    precreate_app_schema: bool = False,
    on_created: Callable[[], None] | None = None,
) -> None:
    if os.environ.get("DP_PROFILE") != "test" or not APP_EVAL_DATABASE.fullmatch(database):
        raise DatabaseSetupError("REGISTERED_APP_EVAL_REQUIRED")
    with connect("postgres", autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,)).fetchone():
            raise DatabaseSetupError("EVAL_DATABASE_ALREADY_EXISTS")
        admin.execute(
            sql.SQL("CREATE DATABASE {} OWNER dp_bootstrap").format(sql.Identifier(database))
        )
        if on_created is not None:
            on_created()
        _database_acl(admin, database, app=True)
    if precreate_app_schema:
        with connect(database) as admin:
            admin.execute("CREATE SCHEMA app AUTHORIZATION dp_bootstrap")
    migrate_app(database)
    if spike_schema:
        with connect(database) as admin:
            admin.execute("CREATE SCHEMA framework_spike AUTHORIZATION app_runtime")
            admin.execute(
                "REVOKE ALL ON SCHEMA framework_spike FROM PUBLIC, analyst_all, analyst_east"
            )
