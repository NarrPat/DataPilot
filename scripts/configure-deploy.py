"""Create a new ignored HTTPS environment without printing secrets or running services."""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import secrets
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--tls-directory", type=Path, required=True)
    parser.add_argument("--bind-address", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18443)
    args = parser.parse_args()
    try:
        origin = urlsplit(args.origin)
        if (
            origin.scheme != "https"
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.path
            or origin.query
            or origin.fragment
            or (origin.port or 443) != args.port
            or not 1024 <= args.port <= 65535
        ):
            raise ValueError("ONE_EXACT_HTTPS_ORIGIN_REQUIRED")
        # IPv4 keeps the Compose short port syntax unambiguous.
        ipaddress.IPv4Address(args.bind_address)
        certs = args.tls_directory.resolve(strict=True)
        if (
            not args.tls_directory.is_absolute()
            or not certs.is_dir()
            or any(c in str(certs) + args.origin for c in "\r\n\"'$#")
        ):
            raise ValueError("ABSOLUTE_CERTIFICATE_DIRECTORY_REQUIRED")
        for name in ("fullchain.pem", "privkey.pem"):
            candidate = certs / name
            if not candidate.is_file() or candidate.is_symlink():
                raise ValueError("CERTIFICATE_FILES_REQUIRED")
        target = ROOT / ".env.deploy"
        if target.is_symlink() or target.is_junction():
            raise ValueError("ENV_LINK_FORBIDDEN")
        ignored = subprocess.run(
            ["git", "-C", str(ROOT), "check-ignore", "-q", "--", target.name],
            capture_output=True,
            check=False,
        )
        tracked = subprocess.run(
            ["git", "-C", str(ROOT), "ls-files", "--error-unmatch", "--", target.name],
            capture_output=True,
            check=False,
        )
        if ignored.returncode != 0 or tracked.returncode != 1:
            raise ValueError("IGNORED_UNTRACKED_ENV_REQUIRED")
        values = {
            "COMPOSE_PROJECT_NAME": "dp-deploy",
            "DP_PROFILE": "deploy",
            "DP_MODEL_MODE": "none",
            "DP_QUERY_PROCESSING_ENABLED": "0",
            "DP_MODEL_PROVIDER": "deepseek",
            "DP_MODEL_NAME": "deepseek-flash",
            "DEEPSEEK_API_BASE": "https://api.deepseek.com",
            "DEEPSEEK_API_KEY": "",
            "DP_PUBLIC_ORIGIN": args.origin,
            "DP_TLS_DIRECTORY": certs.as_posix(),
            "DP_HTTP_PORT": str(args.port),
            "DP_WEB_BIND_ADDRESS": args.bind_address,
            "DP_WEB_CONTAINER_PORT": "8443",
        }
        for key in (
            "DP_POSTGRES_ADMIN_PASSWORD",
            "DP_APP_DB_PASSWORD",
            "DP_ANALYST_ALL_PASSWORD",
            "DP_ANALYST_EAST_PASSWORD",
            "DP_SESSION_SECRET",
            "DP_SEED_PASSWORD",
        ):
            values[key] = secrets.token_urlsafe(48)
        # O_EXCL never replaces an existing installation's passwords/history.
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(
                "# Local secrets; generated offline. No model requests or services started.\n"
            )
            stream.writelines(f'{key}="{value}"\n' for key, value in values.items())
        print(
            json.dumps(
                {
                    "created": True,
                    "path": ".env.deploy",
                    "services_started": False,
                    "certificate_validation": "not_run",
                    "model_calls": 0,
                }
            )
        )
        return 0
    except FileExistsError:
        code = "EXISTING_DEPLOY_ENV_PRESERVED"
    except Exception:
        code = "DEPLOY_ENV_CONFIGURATION_FAILED"
    print(json.dumps({"created": False, "code": code}))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
