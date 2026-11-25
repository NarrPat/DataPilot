"""Recompute the published historical evidence. Run from any working directory."""

from __future__ import annotations

import hashlib
import json
import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
from tests.datapilot.semantic_oracle import answer  # noqa: E402


def read(name):
    return json.loads((HERE / name).read_bytes())


def digest(data):
    return hashlib.sha256(data).hexdigest()


def projection_digest(data):
    return digest(
        json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    )


def verify_model():
    runs = read("model-cases.json")
    frozen = read("expected-precall.json")
    expected = {r["id"]: r for r in frozen["expected"]}
    cases = {r["id"]: r for r in runs["cases"]}
    assert expected.keys() == cases.keys() and len(cases) == 12
    source_hash = runs["source_files_sha256"][
        "followup-validation/review/ordinary-live/expected-precall.json"
    ]
    assert digest((HERE / "expected-precall.json").read_bytes()) == source_hash
    raw = (ROOT / "fixtures/datapilot/demo_v1.source.json").read_bytes()
    # The frozen fixture hash was generated on Windows, with CRLF line endings.
    assert digest(raw.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")) == frozen["source_sha256"]
    demo = json.loads(raw)

    calls = 0
    repairs = 0
    for name, case in cases.items():
        trial = case["rerun"]
        reference = expected[name]
        assert (
            trial["status"] == reference["expected_status"]
            and trial["correct"]
            and not trial["errors"]
        ), name
        if trial["status"] == "completed":
            spec = deepcopy(trial["observed_spec"])
            # The frozen contract leaves the server-assigned reference timestamp open.
            assert (
                spec["time_range"]["reference_time"]
                and reference["spec"]["time_range"]["reference_time"] is None
            )
            spec["time_range"]["reference_time"] = None
            assert spec == reference["spec"], name
            assert (
                answer(demo, spec, case["scope"]) == reference["oracle"] == trial["actual_result"]
            ), name
        else:
            reason = trial["public_reason_check"]
            assert (
                name == "deny-west"
                and reason["actual"] == reason["expected"] == reference["reason_code"]
            )
            assert trial["database_attempts"] == 0 and trial["published_result"] is None
        calls += len(trial["model_stages"])
        repairs += trial["repair_count"]

    first_failures = [name for name, case in cases.items() if not case["initial"]["correct"]]
    assert first_failures == ["deny-west"] and runs["initial"]["correct"] == 11
    assert runs["initial"]["reason_check"] == {
        "expected": "SCOPE_FORBIDDEN",
        "actual": "FORBIDDEN_SCOPE",
    }
    assert runs["rerun"]["correct"] == len(cases) == 12
    assert calls == runs["rerun"]["provider"]["invocation_count"] == 24
    assert repairs == runs["rerun"]["total_repair_count"] == 0
    installation = runs["installation"]["checks"]
    assert len({row["id"] for row in installation}) == len(installation) == 19
    assert all(
        row["equal"] if "equal" in row else row["actual"] == row["expected"] for row in installation
    )
    statuses = Counter(case["rerun"]["status"] for case in cases.values())
    print(
        f"Model: initial {len(cases) - len(first_failures)}/{len(cases)}; "
        f"rerun {statuses['completed']} source-row matches + {statuses['rejected']} refusal; "
        f"{calls} calls, {repairs} repairs."
    )
    print(f"Installation: {len(installation)} verified new-volume checks.")


def verify_capacity(runs):
    raw = read("capacity-verified.json")
    source_hash = runs["source_files_sha256"]["followup-validation/capacity-verified.json"]
    assert digest((HERE / "capacity-verified.json").read_bytes()) == source_hash
    matrix = runs["capacity"]
    groups = {g["configuration_id"]: g for g in matrix["configurations"]}
    assert len(groups) == len(matrix["configurations"]) == raw["configuration_count"] == 96
    formals = {}
    warmups = 0
    for name, group in groups.items():
        expect = {(True, 1)} | {(False, i) for i in range(1, 6)}
        assert {(r["warmup"], r["repetition"]) for r in group["runs"]} == expect and len(
            group["runs"]
        ) == 6, name
        for row in group["runs"]:
            assert row["status"] == "completed_correct" and row["oracle_equal"], name
            assert row["matched_projection_sha256"] == group["frozen_answer_sha256"], name
            if row["warmup"]:
                warmups += 1
            else:
                formals[name, row["repetition"]] = row
    assert len(formals) == raw["formal_correct"] == raw["formal_planned"] == 480
    assert warmups == raw["warmup_planned"] == 96 and raw["provider_attempts"] == 0
    pairs = matrix["wide_aov_before_after"]
    expected_pairs = {
        key
        for key in formals
        if groups[key[0]]["orders"] == 100000
        and groups[key[0]]["sql_id"] == "DP-SQL-04"
        and groups[key[0]]["window"] == "wide"
    }
    assert (
        len(pairs) == 20
        and {(p["configuration_id"], p["repetition"]) for p in pairs} == expected_pairs
    )
    for pair in pairs:
        row = formals[pair["configuration_id"], pair["repetition"]]
        assert (
            pair["before"]["status"] == "timed_out"
            and pair["before"]["error_code"] == "STATEMENT_TIMEOUT"
        )
        assert all(
            pair["after"][k] == row[k]
            for k in ("status", "row_count", "oracle_equal", "result_hash")
        )
    sample = matrix["wide_aov_example"]
    assert sample["expected_result"] == sample["actual_result"]
    assert (
        projection_digest(sample["actual_result"])
        == groups[sample["configuration_id"]]["frozen_answer_sha256"]
    )
    assert len(matrix["paired_inputs"]) == 16 and all(
        p["unchanged"] for p in matrix["paired_inputs"]
    )
    print(
        f"Capacity: {len(pairs)} prior timeouts with correct paired results; "
        f"{len(formals)} correct formal runs + {warmups} correct warmups."
    )


def verify_steady_and_ci(runs):
    steady = runs["steady"]
    total = sum(m["submitted"] for m in steady["minutes"])
    assert (
        sum(m["completed_correct"] for m in steady["minutes"]) == steady["correct"] == total == 889
    )
    outcomes = Counter()
    for row in steady["native_outcomes_by_query"]:
        outcomes[row["status"]] += row["count"]
    assert (
        outcomes == {"completed_correct": total}
        and steady["provider_attempts"] == steady["unfinished"] == 0
    )
    assert round(steady["measured_seconds"], 3) == 600.007
    ci = runs["development_ci"]
    assert (
        len({u["file"] for u in ci["units"]}) == len(ci["units"]) == ci["default_test_files"] == 70
    )
    assert sum(u["native_nodes"] for u in ci["units"]) == ci["unique_native_pytest_nodes"] == 1777
    assert (
        sum(u["integration_marker_nodes"] for u in ci["units"])
        == ci["declared_integration_marker_nodes"]
        == 589
    )
    assert sum(u["failed"] + u["skipped"] for u in ci["units"]) == ci["not_run"] == 0
    assert len(set(ci["frontend_state_passes"])) == ci["frontend_state_nodes"] == 8
    print(f"Steady: {steady['correct']}/{total} correct in {steady['measured_seconds']:.3f} s.")
    print(
        f"Historical CI: {ci['unique_native_pytest_nodes']:,} Python nodes "
        f"({ci['declared_integration_marker_nodes']} integration), "
        f"{ci['frontend_state_nodes']} frontend state tests."
    )


if __name__ == "__main__":
    if not __debug__:
        raise SystemExit("Run without -O so evidence checks are enabled.")
    verify_model()
    data = read("sql-runs.json")
    verify_capacity(data)
    verify_steady_and_ci(data)
