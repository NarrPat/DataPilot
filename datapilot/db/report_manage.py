"""Administrator-only import of an explicit immutable report; no HTTP upload."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from psycopg.types.json import Jsonb

from datapilot.api.read_models import ReportDocument
from datapilot.api.read_service import report_bytes
from datapilot.db.application import APP_EVAL_DATABASE
from datapilot.db.bootstrap import connect


class ReportImportError(Exception):
    pass


def import_report(connection: Any, document: ReportDocument) -> dict[str, Any]:
    """Only the migration identity can add reports; duplicate IDs are immutable."""
    if connection.info.user != "dp_bootstrap" or (
        connection.info.dbname != "dp_app"
        and not APP_EVAL_DATABASE.fullmatch(connection.info.dbname)
    ):
        raise ReportImportError("REPORT_IMPORT_ADMIN_TARGET_REQUIRED")
    digest = hashlib.sha256(report_bytes(document)).hexdigest()
    with connection.transaction():
        inserted = connection.execute(
            "INSERT INTO app.eval_reports(report_id,created_at,document,document_sha256) "
            "VALUES(%s,%s,%s,%s) ON CONFLICT(report_id) DO NOTHING RETURNING report_id",
            (
                document.report_id,
                document.created_at,
                Jsonb(document.model_dump(mode="json")),
                digest,
            ),
        ).fetchone()
        previous = connection.execute(
            "SELECT document_sha256 FROM app.eval_reports WHERE report_id=%s", (document.report_id,)
        ).fetchone()
        if previous is None or previous["document_sha256"] != digest:
            raise ReportImportError("IMMUTABLE_REPORT_CONFLICT")
    return {
        "report_id": str(document.report_id),
        "created": inserted is not None,
        "document_sha256": digest,
        "report_type": document.report_type,
        "status": document.status,
    }


def load_report(path: Path) -> ReportDocument:
    if path.stat().st_size > 512 * 1024:
        raise ReportImportError("REPORT_FILE_TOO_LARGE")
    content = path.read_bytes()
    if len(content) > 512 * 1024:
        raise ReportImportError("REPORT_FILE_TOO_LARGE")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result = {}
        for key, value in pairs:
            if key in result:
                raise ReportImportError("REPORT_DUPLICATE_JSON_KEY")
            result[key] = value
        return result

    return ReportDocument.model_validate(
        json.loads(content.decode("utf-8"), object_pairs_hook=unique)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, type=Path)
    args = parser.parse_args()
    try:
        document = load_report(args.file)
        with connect("dp_app") as admin:
            receipt = import_report(admin, document)
        print(json.dumps(receipt))
        return 0
    except Exception as exc:
        print(json.dumps({"code": "REPORT_IMPORT_FAILED", "error_type": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
