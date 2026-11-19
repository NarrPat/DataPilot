"""Independent application DB migrations, using an injected admin connection."""

from alembic import context
from sqlalchemy.engine import Connection


def run_offline() -> None:
    context.configure(
        dialect_name="postgresql", literal_binds=True, dialect_opts={"paramstyle": "named"}
    )
    with context.begin_transaction():
        context.run_migrations()


def run_online() -> None:
    connection = context.config.attributes.get("connection")
    if not isinstance(connection, Connection) or connection.dialect.name != "postgresql":
        raise RuntimeError("App migrations require an injected PostgreSQL admin Connection")
    context.configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_offline()
else:
    run_online()
