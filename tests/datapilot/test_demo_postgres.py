"""Compare the product executor on an initialized demo_v1 database to raw fixture rows."""

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

from datapilot.contracts import ConfirmedSpec
from datapilot.data.fixtures import load_fixture
from tests.datapilot.cases import BASELINE, METRICS, envelope, plan_for
from tests.datapilot.semantic_oracle import answer

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
PROBE = Path(__file__).with_name("postgres_probe.py").resolve()


def weekly_by_category(metric: str, scope: str) -> ConfirmedSpec:
    return envelope(metric, scope, grain="week", group_by="category", comparison=BASELINE)


@pytest.fixture(scope="module")
def actual() -> dict[str, Any]:
    requests = {}
    for scope in ("east", "all"):
        for metric in METRICS:
            confirmed = weekly_by_category(metric, scope)
            requests[f"{scope}/{metric}"] = {
                "confirmed": confirmed.model_dump(mode="json"),
                "plan": plan_for(confirmed).model_dump(mode="json"),
            }
    # Only the probe goes inside the worker. Expected answers stay on the host.
    project = os.getenv("COMPOSE_PROJECT_NAME", "datapilot")
    cmd = [
        "docker",
        "compose",
        "-p",
        project,
        "--env-file",
        ".env.local",
        "-f",
        "apps/datapilot/compose.yaml",
        "--profile",
        "processing",
        "run",
        "--rm",
        "-T",
        "--no-deps",
        "-e",
        "DP_MODEL_MODE=none",
        "-e",
        "DEEPSEEK_API_KEY=",
        "-v",
        f"{PROBE}:/work/postgres_probe.py:ro",
        "worker",
        "python",
        "/work/postgres_probe.py",
    ]
    run = subprocess.run(
        cmd,
        cwd=ROOT,
        input=json.dumps(requests),
        text=True,
        encoding="utf-8",
        stdout=subprocess.PIPE,
        check=True,
        timeout=120,
    )
    return json.loads(run.stdout)


@pytest.mark.parametrize("scope", ["east", "all"])
@pytest.mark.parametrize("metric", METRICS)
def test_two_periods_match_source_rows(actual: dict[str, Any], scope: str, metric: str) -> None:
    source = load_fixture(ROOT / "fixtures/datapilot/demo_v1.source.json")
    spec = weekly_by_category(metric, scope).spec.model_dump(mode="json")
    expected = answer(source, spec, scope)
    result = actual[f"{scope}/{metric}"]
    assert result["role"] == f"analyst_{scope}"
    assert result["isolation"] == "repeatable read"
    assert result["read_only"] == "on"
    assert result["periods"] == expected["periods"]
    assert result["series"] == expected["series"]
