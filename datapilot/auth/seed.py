"""Admin-only fixed local account seed; password comes from operator/test input."""

from __future__ import annotations

import uuid
from typing import Any

from argon2 import PasswordHasher

ACCOUNTS = (
    ("east_a", "analyst_east"),
    ("east_b", "analyst_east"),
    ("all_a", "analyst_all"),
    ("report_admin", "report_admin"),
)


def seed_users(
    connection: Any, password: str, *, hasher: PasswordHasher | None = None
) -> list[dict[str, object]]:
    if not password or len(password.encode()) > 1024:
        raise ValueError("SEED_PASSWORD_INVALID")
    codec = hasher or PasswordHasher()
    encoded = codec.hash(password)
    seeded = []
    for username, role in ACCOUNTS:
        row = connection.execute(
            "INSERT INTO app.users(user_id,username,password_hash,role) VALUES(%s,%s,%s,%s) "
            "ON CONFLICT(username) DO NOTHING RETURNING user_id",
            (uuid.uuid4(), username, encoded, role),
        ).fetchone()
        seeded.append({"username": username, "created": row is not None})
    return seeded
