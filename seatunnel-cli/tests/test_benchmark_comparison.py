#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Offline acceptance cases for comparing saved benchmark runs."""

import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmark.compare import compare_results, render_markdown  # noqa: E402


def trial(passed, index=0, first_pass=0):
    attempts = []
    for round_number in range(first_pass + 1):
        success = passed and round_number == first_pass
        attempts.append(
            {
                "round": round_number,
                "seconds": 1.0,
                "layers": {
                    "l1": {"passed": success},
                    "l2": None,
                    "l3": None,
                    "skipped_layers": [],
                    "all_gates_executed": True,
                },
            }
        )
    return {
        "trial": index,
        "attempts": attempts,
        "first_pass_round": first_pass if passed else None,
    }


def results(outcomes):
    return {
        "cli": {"cli_version": "0.1.0", "cli_commit": "baseline"},
        "levels": ["l1"],
        "trials": 1,
        "max_repairs": 3,
        "models": [
            {
                "name": "model-a",
                "config": {
                    "provider": "openai",
                    "model": "model-a",
                    "fast_model": "fast-a",
                },
                "tasks": [
                    {
                        "task_id": name,
                        "tier": 1,
                        "category": "smoke",
                        "task_sha256": "a" * 64,
                        "trials": [trial(passed)],
                    }
                    for name, passed in outcomes.items()
                ],
            }
        ],
    }


def test_aggregate_gain_does_not_hide_task_regression(tmp_path):
    from benchmark.report import _write_markdown

    before = results({"lost": True, "gained-a": False, "gained-b": False})
    after = results({"lost": False, "gained-a": True, "gained-b": True})
    after["cli"]["cli_commit"] = "candidate"

    # The existing single-run summary has aggregate rates, but no paired
    # identities that reveal the lost task.
    _write_markdown(before, tmp_path / "baseline.md")
    _write_markdown(after, tmp_path / "candidate.md")
    assert "lost" not in (tmp_path / "baseline.md").read_text()
    assert "lost" not in (tmp_path / "candidate.md").read_text()

    report = render_markdown(compare_results(before, after))

    assert "33.3%" in report and "66.7%" in report and "+33.3 pp" in report
    assert "pass→fail" in report and "fail→pass" in report
    assert "lost" in report and "gained-a" in report and "gained-b" in report
    assert "baseline" in report and "candidate" in report


def test_identical_runs_are_unchanged_and_inputs_are_immutable():
    before = results({"passed": True, "failed": False})
    original = copy.deepcopy(before)
    comparison = compare_results(before, before)
    assert comparison["rows"][0]["first"] == "fail→fail"
    assert comparison["rows"][1]["first"] == "pass→pass"
    assert before == original
    assert render_markdown(comparison) == render_markdown(
        compare_results(before, before)
    )


@pytest.mark.parametrize(
    "key,value",
    [
        ("levels", ["l1", "l2"]),
        ("trials", 2),
        ("max_repairs", 5),
    ],
)
def test_incompatible_run_settings_are_not_scored(key, value):
    before = results({"a": True})
    after = copy.deepcopy(before)
    after[key] = value
    comparison = compare_results(before, after)
    assert comparison["issues"]
    assert not comparison["rows"]
    assert "No comparable trials" in render_markdown(comparison)


@pytest.mark.parametrize(
    "change,reason",
    [
        (lambda task: task.pop("task_sha256"), "fingerprint"),
        (lambda task: task.update(task_sha256="b" * 64), "definition"),
        (lambda task: task.update(trials=[]), "trial IDs"),
        (
            lambda task: task["trials"].append(copy.deepcopy(task["trials"][0])),
            "duplicate",
        ),
    ],
)
def test_incompatible_task_or_trial_metadata_is_excluded(change, reason):
    before = results({"a": True})
    after = copy.deepcopy(before)
    change(after["models"][0]["tasks"][0])
    rows = compare_results(before, after)["rows"]
    assert len(rows) == 1 and reason in rows[0]["reason"]
    assert rows[0]["first"] is None


@pytest.mark.parametrize(
    "layers",
    [
        {"l1": {"passed": True}, "l2": None},
        {
            "l1": {"passed": True},
            "l2": {"passed": None},
            "all_gates_executed": False,
            "skipped_layers": ["l2"],
        },
        {
            "l1": {"passed": True},
            "l2": {"passed": True},
            "all_gates_executed": False,
            "skipped_layers": [],
        },
    ],
)
def test_absent_or_skipped_requested_gates_are_not_regressions(layers):
    before = results({"a": True})
    before["levels"] = ["l1", "l2"]
    before["models"][0]["tasks"][0]["trials"][0]["attempts"][0]["layers"]["l2"] = {
        "passed": True
    }
    after = copy.deepcopy(before)
    after["models"][0]["tasks"][0]["trials"][0]["attempts"][0]["layers"] = layers
    row = compare_results(before, after)["rows"][0]
    assert row["first"] is None and "gate" in row["reason"]


def test_missing_models_and_tasks_are_visible_not_failed():
    before = results({"removed": True, "same": True})
    after = results({"added": True, "same": True})
    extra = copy.deepcopy(after["models"][0])
    extra["name"] = "new-model"
    after["models"].append(extra)
    report = render_markdown(compare_results(before, after))
    assert "missing baseline task" in report and "missing candidate task" in report
    assert "missing baseline model" in report and "new-model" in report


def test_model_config_change_is_excluded_without_printing_config():
    before = results({"a": True})
    after = copy.deepcopy(before)
    after["models"][0]["config"]["base_url"] = "https://private.example/secret"
    report = render_markdown(compare_results(before, after))
    assert "model configuration differs" in report
    assert "private.example" not in report and "secret" not in report


def test_generation_failure_is_real_failure_but_empty_trial_is_incomplete():
    before = results({"a": True, "b": True})
    after = copy.deepcopy(before)
    for task in after["models"][0]["tasks"]:
        task["trials"][0].update(attempts=[], first_pass_round=None)
    after["models"][0]["tasks"][0]["trials"][0]["generation_error"] = "provider failed"
    rows = compare_results(before, after)["rows"]
    assert rows[0]["first"] == "pass→fail"
    assert rows[1]["first"] is None


def test_repair_regression_is_separate_from_first_attempt():
    before = results({"a": True})
    after = copy.deepcopy(before)
    after["models"][0]["tasks"][0]["trials"] = [trial(True, first_pass=2)]
    row = compare_results(before, after)["rows"][0]
    assert row["first"] == "pass→fail"
    assert row["repaired"] == "pass→pass"


def test_cli_is_offline_and_does_not_overwrite_inputs_or_reports(tmp_path):
    before = tmp_path / "before.json"
    after = tmp_path / "after.json"
    before.write_text(json.dumps(results({"a": True})))
    after.write_text(json.dumps(results({"a": False})))
    original = before.read_bytes(), after.read_bytes()
    report = tmp_path / "comparison.md"
    command = [
        sys.executable,
        "-S",
        "-m",
        "benchmark.compare",
        str(before),
        str(after),
        "--out",
        str(report),
    ]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert completed.returncode == 0, completed.stderr
    assert "pass→fail" in report.read_text()
    assert (before.read_bytes(), after.read_bytes()) == original
    existing = report.read_bytes()
    repeated = subprocess.run(command, capture_output=True, text=True, timeout=10)
    assert repeated.returncode == 2
    assert report.read_bytes() == existing


def test_runner_saved_results_compare_without_changing_legacy_reports(
    tmp_path, monkeypatch
):
    from benchmark.runner import run_benchmark
    from benchmark.report import write_reports

    monkeypatch.setattr(
        "benchmark.runner.collect_cli_fingerprint",
        lambda: {"cli_commit": "test-revision"},
    )
    monkeypatch.setattr(
        "seatunnel_cli.llm_provider.create_provider", lambda _: object()
    )
    monkeypatch.setattr(
        "benchmark.runner.run_task_with_repairs", lambda *args: trial(True)
    )
    tasks = [
        {
            "id": "a",
            "tier": 1,
            "prompt": "Generate a batch job",
            "expect": {"mode": "BATCH"},
        }
    ]
    models = [{"name": "model-a", "provider": "openai", "model": "model-a"}]
    original_tasks = copy.deepcopy(tasks)
    first = run_benchmark(models, tasks, ["l1"], 3, 2, tmp_path / "before")
    second = run_benchmark(models, tasks, ["l1"], 3, 2, tmp_path / "after")
    before = json.loads((tmp_path / "before/results.json").read_text())
    after = json.loads((tmp_path / "after/results.json").read_text())
    rows = compare_results(before, after)["rows"]
    assert len(rows) == 2 and all(row["first"] == "pass→pass" for row in rows)
    assert tasks == original_tasks

    # An additive raw-result field must not change the published reports.
    legacy = copy.deepcopy(first)
    for task in legacy["models"][0]["tasks"]:
        del task["task_sha256"]
    write_reports(first, tmp_path / "new-reports")
    write_reports(legacy, tmp_path / "old-reports")
    for filename in ("summary.md", "summary.csv"):
        assert (tmp_path / "new-reports" / filename).read_bytes() == (
            tmp_path / "old-reports" / filename
        ).read_bytes()
    assert "fingerprint" in compare_results(legacy, second)["rows"][0]["reason"]

    tasks[0]["prompt"] += " with different semantics"
    changed = run_benchmark(models, tasks, ["l1"], 3, 2, tmp_path / "changed")
    assert (
        compare_results(first, changed)["rows"][0]["reason"]
        == "task definition differs"
    )


@pytest.mark.parametrize("field", ["models", "tasks"])
def test_duplicate_identities_are_rejected(field):
    before = results({"a": True})
    records = before["models"] if field == "models" else before["models"][0]["tasks"]
    records.append(copy.deepcopy(records[0]))
    with pytest.raises(ValueError, match="duplicate"):
        compare_results(before, results({"a": True}))


def test_task_and_model_order_does_not_change_report():
    before = results({"b": True, "a": False})
    extra = copy.deepcopy(before["models"][0])
    extra["name"] = "model-b"
    before["models"].append(extra)
    after = copy.deepcopy(before)
    after["models"].reverse()
    for model in after["models"]:
        model["tasks"].reverse()
    assert render_markdown(compare_results(before, after)) == render_markdown(
        compare_results(after, before)
    )


@pytest.mark.parametrize(
    "mutate,reason",
    [
        (
            lambda run: run["models"][0].update(error="SECRET initialization failure"),
            "did not run",
        ),
        (lambda run: run["models"][0]["config"].pop("model"), "metadata missing"),
        (
            lambda run: run["models"][0]["tasks"][0]["trials"][0].update(
                first_pass_round=2
            ),
            "contradicts",
        ),
        (
            lambda run: run["models"][0]["tasks"][0]["trials"][0].pop(
                "first_pass_round"
            ),
            "missing",
        ),
        (
            lambda run: run["models"][0]["tasks"][0]["trials"][0]["attempts"][0].update(
                round=2
            ),
            "rounds invalid",
        ),
    ],
)
def test_incomplete_or_contradictory_evidence_is_excluded(mutate, reason):
    before = results({"a": True})
    after = copy.deepcopy(before)
    mutate(after)
    report = render_markdown(compare_results(before, after))
    assert reason in report and "No comparable trials" in report
    assert "SECRET" not in report


def test_short_circuited_gate_failure_is_not_missing_coverage():
    before = results({"a": False})
    before["levels"] = ["l1", "l2", "l3"]
    row = compare_results(before, before)["rows"][0]
    assert not row["reason"] and row["first"] == "fail→fail"


@pytest.mark.parametrize("cli", [None, [], {}, {"cli_commit": True}])
def test_missing_revision_metadata_is_not_an_unlabeled_delta(cli):
    before = results({"a": True})
    after = copy.deepcopy(before)
    after["cli"] = cli
    report = render_markdown(compare_results(before, after))
    assert "revision metadata missing" in report and "No comparable trials" in report


def test_report_escapes_untrusted_labels():
    before = results({"<script>|[label](url)\nnext": True})
    report = render_markdown(compare_results(before, before))
    assert "<script>" not in report
    assert "&#124;" in report and "&lt;script&gt;" in report
    assert "\\[label\\]" in report


@pytest.mark.parametrize(
    "response",
    [
        {"type": "config", "config": ""},
        {"type": "config"},
        {"type": "explanation"},
        RuntimeError("provider unavailable"),
    ],
)
def test_actual_runner_generation_failures_remain_failures(
    tmp_path, monkeypatch, response
):
    from unittest.mock import patch
    from benchmark.runner import run_benchmark

    monkeypatch.setattr(
        "benchmark.runner.collect_cli_fingerprint",
        lambda: {"cli_commit": "test-revision"},
    )
    monkeypatch.setattr(
        "seatunnel_cli.llm_provider.create_provider", lambda _: object()
    )
    tasks = [{"id": "a", "tier": 1, "prompt": "Generate a batch job"}]
    with patch("seatunnel_cli.agents.Orchestrator") as orchestrator:
        if isinstance(response, Exception):
            orchestrator.return_value.process_user_input.side_effect = response
        else:
            orchestrator.return_value.process_user_input.return_value = response
        run_benchmark(
            [{"name": "a", "provider": "openai", "model": "a"}],
            tasks,
            ["l1"],
            3,
            1,
            tmp_path,
        )
    saved = json.loads((tmp_path / "results.json").read_text())
    row = compare_results(saved, saved)["rows"][0]
    assert row["first"] == "fail→fail" and not row["reason"]


def test_actual_runner_outer_harness_failure_remains_failure(tmp_path, monkeypatch):
    from benchmark.runner import run_benchmark

    def crash(*args):
        raise RuntimeError("scoring crashed")

    monkeypatch.setattr(
        "benchmark.runner.collect_cli_fingerprint",
        lambda: {"cli_commit": "test-revision"},
    )
    monkeypatch.setattr(
        "seatunnel_cli.llm_provider.create_provider", lambda _: object()
    )
    monkeypatch.setattr("benchmark.runner.run_task_with_repairs", crash)
    run_benchmark(
        [{"name": "a", "provider": "openai", "model": "a"}],
        [{"id": "a", "tier": 1}],
        ["l1"],
        3,
        1,
        tmp_path,
    )
    saved = json.loads((tmp_path / "results.json").read_text())
    assert saved["models"][0]["tasks"][0]["trials"][0]["attempts"] == []
    row = compare_results(saved, saved)["rows"][0]
    assert row["first"] == "fail→fail" and not row["reason"]


def test_declared_trial_count_does_not_allocate_missing_records():
    before = results({"a": True})
    before["trials"] = 10**12
    row = compare_results(before, before)["rows"][0]
    assert "trial IDs" in row["reason"] and row["first"] is None


def test_actual_static_gate_results_can_be_compared(tmp_path, monkeypatch):
    from unittest.mock import patch
    from benchmark.runner import run_benchmark

    monkeypatch.setattr(
        "benchmark.runner.collect_cli_fingerprint",
        lambda: {"cli_commit": "test-revision"},
    )
    monkeypatch.setattr(
        "seatunnel_cli.llm_provider.create_provider", lambda _: object()
    )
    config = """
    env { parallelism = 1
      job.mode = "BATCH" }
    source { FakeSource { row.num = 5
      schema { fields { id = "bigint" } }
      plugin_output = "f" } }
    sink { Console { plugin_input = "f" } }
    """
    tasks = [
        {
            "id": "a",
            "tier": 1,
            "prompt": "Generate a batch job",
            "expect": {
                "source": ["FakeSource"],
                "sink": ["Console"],
                "job_mode": "BATCH",
            },
        }
    ]
    with patch("seatunnel_cli.agents.Orchestrator") as orchestrator:
        orchestrator.return_value.process_user_input.return_value = {
            "type": "config",
            "config": config,
        }
        run_benchmark(
            [{"name": "a", "provider": "openai", "model": "a"}],
            tasks,
            ["l1"],
            0,
            1,
            tmp_path,
        )
    saved = json.loads((tmp_path / "results.json").read_text())
    row = compare_results(saved, saved)["rows"][0]
    assert row["first"] == "pass→pass" and not row["reason"]


@pytest.mark.parametrize("passed", [True, False])
@pytest.mark.parametrize("flag", ["all_passed", "full_gate_passed"])
def test_contradictory_aggregate_gate_flags_are_excluded(passed, flag):
    before = results({"a": passed})
    after = copy.deepcopy(before)
    after["models"][0]["tasks"][0]["trials"][0]["attempts"][0]["layers"][
        flag
    ] = not passed
    row = compare_results(before, after)["rows"][0]
    assert row["first"] is None and f"{flag} contradicts" in row["reason"]


@pytest.mark.parametrize(
    "flag", ["all_passed", "full_gate_passed", "all_gates_executed"]
)
def test_gate_flags_require_boolean_values_when_present(flag):
    before = results({"a": True})
    after = copy.deepcopy(before)
    after["models"][0]["tasks"][0]["trials"][0]["attempts"][0]["layers"][flag] = "false"
    row = compare_results(before, after)["rows"][0]
    assert row["first"] is None and row["reason"]
