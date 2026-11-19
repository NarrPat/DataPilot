"""Create local-only secrets without overwriting an existing environment file."""

from __future__ import annotations

import json
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SECRET_NAMES = (
    "DP_POSTGRES_ADMIN_PASSWORD",
    "DP_APP_DB_PASSWORD",
    "DP_ANALYST_ALL_PASSWORD",
    "DP_ANALYST_EAST_PASSWORD",
    "DP_SESSION_SECRET",
    "DP_SEED_PASSWORD",
)


def main() -> None:
    target = ROOT / ".env.local"
    template = (ROOT / ".env.example").read_text(encoding="utf-8")
    if target.is_symlink():
        raise SystemExit("Refusing a symbolic-link environment file")
    for name in SECRET_NAMES:
        template = template.replace(f"{name}=\n", f"{name}={secrets.token_urlsafe(48)}\n")
    try:
        with target.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(template)
    except FileExistsError:
        print(json.dumps({"path": str(target), "created": False, "reason": "preserved_existing"}))
        return
    print(json.dumps({"path": str(target), "created": True, "secret_values_printed": False}))


if __name__ == "__main__":
    main()
