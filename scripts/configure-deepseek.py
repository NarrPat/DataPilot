"""Configure DeepSeek locally; --set-key prompts without echo, never via a key argument."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path

PROJECT_ROOT = Path(__file__).absolute().parents[1]
ENV_NAME = ".env.local"
DEFAULTS = {
    "DP_MODEL_PROVIDER": b"deepseek",
    "DP_MODEL_NAME": b"deepseek-flash",
    "DEEPSEEK_API_BASE": b"https://api.deepseek.com",
    "DEEPSEEK_API_KEY": b"",
    "DP_MODEL_MODE": b"none",
}
ASSIGNMENT = re.compile(
    rb"^(?:\xef\xbb\xbf)?[ \t]*(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*=[ \t]*(.*)$"
)


class ConfigError(Exception):
    """Fixed codes avoid echoing secret-bearing input or operating-system errors."""


def git(*arguments: str) -> subprocess.CompletedProcess[str]:
    command = ["git", "-C", str(PROJECT_ROOT), *arguments]
    return subprocess.run(command, capture_output=True, text=True, encoding="utf-8", check=False)


def safe(path: Path) -> None:
    if path.parent != PROJECT_ROOT or PROJECT_ROOT.resolve() != PROJECT_ROOT:
        raise ConfigError("PATH_OUTSIDE_PHYSICAL_PROJECT")
    if any(part.is_symlink() or part.is_junction() for part in (path, PROJECT_ROOT)):
        raise ConfigError("LINK_OR_JUNCTION_NOT_ALLOWED")
    if path.exists() and not path.is_file():
        raise ConfigError("ENV_IS_NOT_A_REGULAR_FILE")


def guard(env_name: str = ENV_NAME) -> Path:
    if env_name not in {ENV_NAME, ".env.deploy"}:
        raise ConfigError("REGISTERED_ENV_REQUIRED")
    target = PROJECT_ROOT / env_name
    safe(target)
    repository = git("rev-parse", "--show-toplevel")
    if repository.returncode or Path(repository.stdout.strip()).resolve() != PROJECT_ROOT:
        raise ConfigError("INDEPENDENT_GIT_ROOT_REQUIRED")
    tracked = git("ls-files", "--error-unmatch", "--", env_name).returncode
    if tracked == 0:
        raise ConfigError("TRACKED_ENV_REFUSED")
    if tracked != 1 or git("check-ignore", "-q", "--", env_name).returncode != 0:
        raise ConfigError("GIT_IGNORED_ENV_REQUIRED")
    return target


def clean(value: bytes) -> bytes:
    value = value.strip()
    return b"" if value.startswith(b"#") else value.strip(b"\"'")


def fields(data: bytes) -> dict[str, tuple[int, bytes]]:
    result: dict[str, tuple[int, bytes]] = {}
    for index, line in enumerate(data.splitlines(keepends=True)):
        match = ASSIGNMENT.fullmatch(line.rstrip(b"\r\n"))
        if match and (name := match[1].decode("ascii")) in DEFAULTS:
            if name in result:
                raise ConfigError("DUPLICATE_MODEL_SETTING")
            result[name] = (index, match[2])
    return result


def prepare(data: bytes, key: str | None = None) -> bytes:
    existing = fields(data)
    lines = data.splitlines(keepends=True)
    newline = b"\r\n" if b"\r\n" in data else b"\n"
    for name, default in DEFAULTS.items():
        value = key.encode("ascii") if name == "DEEPSEEK_API_KEY" and key else default
        previous = existing.get(name)
        force = name == "DP_MODEL_MODE" or (name == "DEEPSEEK_API_KEY" and key is not None)
        if previous and ((not force and clean(previous[1])) or clean(previous[1]) == value):
            continue
        line = name.encode("ascii") + b"=" + value + newline
        if previous:
            index = previous[0]
            bom = b"\xef\xbb\xbf" if lines[index].startswith(b"\xef\xbb\xbf") else b""
            lines[index] = bom + line
        else:
            if lines and not lines[-1].endswith((b"\r", b"\n")):
                lines[-1] += newline
            lines.append(line)
    return b"".join(lines)


def status(data: bytes) -> dict[str, str | bool]:
    values = {name: clean(value) for name, (_, value) in fields(data).items()}
    provider = values.get("DP_MODEL_PROVIDER", b"")
    mode = values.get("DP_MODEL_MODE", b"")
    allowed_modes = {b"none", b"stub", b"replay", b"live"}
    return {
        "provider": "deepseek" if provider == b"deepseek" else "other" if provider else "unset",
        "key_configured": bool(values.get("DEEPSEEK_API_KEY")),
        "mode": mode.decode("ascii") if mode in allowed_modes else "other" if mode else "unset",
    }


def atomic_update(before: bytes, after: bytes, env_name: str = ENV_NAME) -> None:
    target = guard(env_name)
    temporary: Path | None = None
    try:
        prefix = env_name + ".deepseek-"
        with tempfile.NamedTemporaryFile(prefix=prefix, dir=PROJECT_ROOT, delete=False) as handle:
            temporary = Path(handle.name)
            safe(temporary)
            if git("check-ignore", "-q", "--", temporary.name).returncode != 0:
                raise ConfigError("GIT_IGNORED_TEMP_REQUIRED")
            handle.write(after)
            handle.flush()
            os.fsync(handle.fileno())
        guard(env_name)
        if (target.read_bytes() if target.exists() else b"") != before:
            raise ConfigError("ENV_CHANGED_DURING_UPDATE")
        safe(temporary)
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def interactive_key() -> str:
    if not all(stream.isatty() for stream in (sys.stdin, sys.stdout, sys.stderr)):
        raise ConfigError("INTERACTIVE_LOCAL_TERMINAL_REQUIRED")
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        key = getpass.getpass("New DeepSeek API key (hidden): ")
    if re.fullmatch(r"[A-Za-z0-9._-]{16,512}", key) is None:
        raise ConfigError("KEY_MUST_BE_ONE_ASCII_TOKEN_16_TO_512_CHARACTERS")
    return key


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", choices=(ENV_NAME, ".env.deploy"), default=ENV_NAME)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--prepare", action="store_true", help="Fill settings; retain values.")
    action.add_argument("--status", action="store_true", help="Read safe metadata only.")
    action.add_argument("--set-key", action="store_true", help="Local hidden prompt; mode none.")
    args = parser.parse_args()
    try:
        target = guard(args.env_file)
        before = target.read_bytes() if target.exists() else b""
        if args.status:
            report = status(before)
        else:
            after = prepare(before, interactive_key() if args.set_key else None)
            if after != before:
                atomic_update(before, after, args.env_file)
            report = status(after)
        print(json.dumps(report))
        return 0
    except ConfigError as exc:
        code = str(exc)
    except (EOFError, KeyboardInterrupt, getpass.GetPassWarning):
        code = "SECURE_KEY_INPUT_UNAVAILABLE_OR_CANCELLED"
    except (OSError, UnicodeError):
        code = "LOCAL_CONFIG_IO_FAILED"
    print(json.dumps({"error_code": code}))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
