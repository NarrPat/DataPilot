"""Analytics migrations: PostgreSQL offline SQL or an injected online connection."""

from alembic import context
from sqlalchemy.engine import Connection


def run_migrations_offline() -> None:
    """Render PostgreSQL DDL without reading credentials or opening a connection."""
    context.configure(
        dialect_name="postgresql",
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """The caller owns connection lifetime and may wrap migration in a transaction."""
    connection = context.config.attributes.get("connection")
    if not isinstance(connection, Connection):
        raise RuntimeError(
            "Online migration requires Config.attributes['connection']: "
            "an initialized SQLAlchemy Connection. Do not put a DSN in alembic.ini."
        )
    if connection.dialect.name != "postgresql":
        raise RuntimeError("DataPilot analytics migrations require PostgreSQL.")
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
