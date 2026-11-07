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

"""Offline contracts for the optional, public alternative-wording suite."""

import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmark import paraphrases, runner  # noqa: E402
from benchmark.compare import compare_results  # noqa: E402
from benchmark.report import write_reports  # noqa: E402
from benchmark.scoring import score_task  # noqa: E402

VARIANTS = json.loads(paraphrases.PARAPHRASES_PATH.read_text(encoding="utf-8"))[
    "paraphrases"
]


def replace_variants(monkeypatch, tmp_path, variants):
    path = tmp_path / "paraphrase.json"
    path.write_text(json.dumps({"paraphrases": variants}), encoding="utf-8")
    monkeypatch.setattr(paraphrases, "PARAPHRASES_PATH", path)


def fingerprint(task):
    return hashlib.sha256(
        json.dumps(
            task, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


STREAM_CONFIG = """
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}
source {
  FakeSource {
    schema { fields { id = "bigint", name = "string" } }
  }
}
sink { Console {} }
"""


def test_baseline_stays_identical_and_does_not_load_paraphrases(monkeypatch, tmp_path):
    replace_variants(monkeypatch, tmp_path, [{"invalid": "ignored"}])
    tasks = runner.load_tasks([1, 2, 3])
    assert len(tasks) == 100
    assert tasks == runner.load_tasks([1, 2, 3], suite="baseline")
    # Baseline loading must preserve every canonical field and its ordering,
    # without adding variant metadata that changes saved-result fingerprints.
    canonical = []
    for tier, filename in runner.TIER_FILES.items():
        data = json.loads((runner.TASKS_DIR / filename).read_text(encoding="utf-8"))
        canonical.extend(dict(task, tier=tier) for task in data["tasks"])
    assert tasks == canonical
    assert fingerprint(tasks) == fingerprint(canonical)
    assert all("parent_id" not in task for task in tasks)
    assert runner.load_tasks([1], ["t1_probe_parquet_conditional_p1"]) == []
    assert len(runner.load_tasks([1], [])) == 20


@pytest.mark.parametrize("variant", VARIANTS, ids=lambda item: item["parent_id"])
def test_variants_inherit_the_complete_task_without_aliasing(variant):
    parents = runner.load_tasks([1, 2, 3])
    original = copy.deepcopy(parents)
    parent = next(task for task in parents if task["id"] == variant["parent_id"])
    task = next(
        task
        for task in paraphrases.load_paraphrases(parents)
        if task["parent_id"] == parent["id"]
    )
    assert task["id"] == parent["id"] + "_p1"
    assert task["prompt"] != parent["prompt"]
    assert task["parent_sha256"] == fingerprint(parent)
    assert fingerprint(task) != fingerprint(parent)
    inherited = {
        key: value
        for key, value in task.items()
        if key not in ("id", "prompt", "parent_id", "parent_sha256")
    }
    assert inherited == {
        key: value for key, value in parent.items() if key not in ("id", "prompt")
    }
    assert task["expect"]["source"] and task["expect"]["sink"]
    assert task["execution"]["l3"] in ("run", "skip")
    assert (task["expect"]["job_mode"] == "STREAMING") == (
        task["execution"]["mode"] == "streaming"
    )
    for pattern in task["expect"].get("must_match", []) + task["expect"].get(
        "must_not_match", []
    ):
        re.compile(pattern)
    task["expect"]["source"].clear()
    task["execution"]["services"].append("additional-service")
    assert parents == original


def test_selection_is_opt_in_and_tier_filtered():
    tasks = runner.load_tasks([1, 2, 3], suite="paraphrase")
    assert len(tasks) == len({task["id"] for task in tasks}) == 12
    assert [
        len(runner.load_tasks([tier], suite="paraphrase")) for tier in (1, 2, 3)
    ] == [2, 6, 4]
    assert not (
        {task["id"] for task in tasks}
        & {task["id"] for task in runner.load_tasks([1, 2, 3])}
    )
    assert runner.load_tasks([1], [tasks[0]["id"]], "paraphrase") == [tasks[0]]


@pytest.mark.parametrize(
    "tiers,ids",
    [
        ([1], []),
        ([1], ["unknown"]),
        ([1], ["t1_probe_parquet_conditional"]),
        ([1], ["t1_probe_parquet_conditional_p1", "unknown"]),
        ([1], ["t1_probe_parquet_conditional_p1"] * 2),
        ([2], ["t1_probe_parquet_conditional_p1"]),
        ([1, 1], None),
        ([], None),
        ([4], None),
    ],
)
def test_invalid_paraphrase_selection_fails_closed(tiers, ids):
    with pytest.raises(ValueError):
        runner.load_tasks(tiers, ids, "paraphrase")


@pytest.mark.parametrize(
    "field", ["prompt", "expect", "execution", "tier", "category", "name"]
)
def test_parent_changes_require_review_and_repinning(field):
    parents = runner.load_tasks([1, 2, 3])
    parent = next(task for task in parents if task["id"] == VARIANTS[0]["parent_id"])
    parent[field] = "changed"
    with pytest.raises(ValueError, match="review and repin"):
        paraphrases.load_paraphrases(parents)


@pytest.mark.parametrize(
    "change",
    [
        lambda variants: variants.append(copy.deepcopy(variants[0])),
        lambda variants: variants[0].update(parent_id="unknown"),
        lambda variants: variants[0].update(parent_id=[]),
        lambda variants: variants[0].update(parent_sha256="0" * 64),
        lambda variants: variants[0].update(prompt=" "),
        lambda variants: variants[0].update(prompt=None),
        lambda variants: variants[0].update(expect={}),
        lambda variants: variants[0].pop("parent_sha256"),
    ],
)
def test_invalid_variant_definitions_are_rejected(change, monkeypatch, tmp_path):
    variants = copy.deepcopy(VARIANTS)
    change(variants)
    replace_variants(monkeypatch, tmp_path, variants)
    with pytest.raises(ValueError):
        runner.load_tasks([1, 2, 3], suite="paraphrase")


def test_identical_prompt_and_duplicate_parent_are_rejected(monkeypatch, tmp_path):
    parents = runner.load_tasks([1, 2, 3])
    with pytest.raises(ValueError, match="Duplicate baseline"):
        paraphrases.load_paraphrases(parents + [parents[0]])
    variants = copy.deepcopy(VARIANTS)
    variants[0]["prompt"] = next(
        task["prompt"] for task in parents if task["id"] == variants[0]["parent_id"]
    )
    replace_variants(monkeypatch, tmp_path, variants)
    with pytest.raises(ValueError, match="distinct"):
        paraphrases.load_paraphrases(parents)


@pytest.mark.parametrize(
    "content",
    [
        "{",
        "[]",
        "{}",
        '{"paraphrases": []}',
        '{"paraphrases": {}}',
        '{"paraphrases": [null]}',
    ],
)
def test_malformed_or_empty_corpus_fails_closed(content, monkeypatch, tmp_path):
    path = tmp_path / "paraphrase.json"
    path.write_text(content, encoding="utf-8")
    monkeypatch.setattr(paraphrases, "PARAPHRASES_PATH", path)
    with pytest.raises(ValueError):
        runner.load_tasks([1, 2, 3], suite="paraphrase")


@pytest.mark.parametrize(
    "config", [STREAM_CONFIG, STREAM_CONFIG.replace("STREAMING", "BATCH"), "invalid {"]
)
def test_same_config_receives_same_verdict_for_every_family(config):
    parents = {task["id"]: task for task in runner.load_tasks([1, 2, 3])}
    for task in runner.load_tasks([1, 2, 3], suite="paraphrase"):
        original = score_task(parents[task["parent_id"]], config).to_dict()
        alternate = score_task(task, config).to_dict()
        original.pop("task_id")
        alternate.pop("task_id")
        assert original == alternate
    assert score_task(parents["t1_probe_streaming_checkpoint"], STREAM_CONFIG).passed


def test_only_variant_wording_reaches_generation_and_skipped_gate_is_not_pass():
    task = runner.load_tasks([1], ["t1_probe_streaming_checkpoint_p1"], "paraphrase")[0]
    with mock.patch("seatunnel_cli.agents.Orchestrator") as factory, mock.patch(
        "benchmark.execution.run_execute",
        return_value={"passed": None, "detail": "SKIPPED: fixture"},
    ):
        orchestrator = factory.return_value
        orchestrator.process_user_input.return_value = {
            "type": "config",
            "config": STREAM_CONFIG,
        }
        record = runner.run_task_with_repairs(object(), task, ["l1", "l3"], 2)
    orchestrator.process_user_input.assert_called_once_with(task["prompt"])
    orchestrator._run_fix.assert_not_called()
    assert record["first_pass_round"] is None
    assert record["attempts"][0]["layers"]["skipped_layers"] == ["l3"]


def test_cli_dispatches_selected_suite_before_provider_setup(monkeypatch, tmp_path):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "benchmark.runner",
            "--suite",
            "paraphrase",
            "--tiers",
            "1",
            "--tasks",
            "t1_probe_streaming_checkpoint_p1",
            "--level",
            "l1",
        ],
    )
    with mock.patch.object(
        runner, "build_models_from_args", return_value=[]
    ) as models, mock.patch.object(
        runner, "resolve_levels", return_value=["l1"]
    ), mock.patch.object(
        runner, "run_benchmark", return_value={}
    ) as run, mock.patch(
        "benchmark.report.print_summary"
    ), mock.patch(
        "benchmark.report.write_reports"
    ), mock.patch.object(
        runner.tempfile, "mkdtemp", return_value=str(tmp_path)
    ):
        runner.main()
        selected = run.call_args.args[1]
        assert [task["id"] for task in selected] == ["t1_probe_streaming_checkpoint_p1"]
        assert run.call_args.kwargs["suite"] == "paraphrase"
        models.assert_called_once()
        sys.argv[-1] = "l3"
        sys.argv[6] = "unknown"
        models.reset_mock()
        with pytest.raises(SystemExit) as error:
            runner.main()
        assert error.value.code == 2
        models.assert_not_called()


@pytest.mark.parametrize(
    "args", [["--suite", "unknown"], ["--suite", "paraphrase", "--tasks", "unknown"]]
)
def test_invalid_suite_cli_needs_no_third_party_packages_or_provider(args):
    completed = subprocess.run(
        [sys.executable, "-S", "-m", "benchmark.runner", *args],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert completed.returncode == 2
    assert "Traceback" not in completed.stderr
    assert "error:" in completed.stderr


def test_saved_variants_compare_by_identity_and_keep_provenance(tmp_path, monkeypatch):
    monkeypatch.setattr(
        runner, "collect_cli_fingerprint", lambda: {"cli_commit": "test-revision"}
    )
    monkeypatch.setattr(
        "seatunnel_cli.llm_provider.create_provider", lambda _: object()
    )
    monkeypatch.setattr(
        runner,
        "run_task_with_repairs",
        lambda *args: {
            "attempts": [
                {
                    "round": 0,
                    "seconds": 0.1,
                    "layers": {
                        "l1": {"passed": True},
                        "l2": None,
                        "l3": None,
                        "skipped_layers": [],
                        "all_gates_executed": True,
                    },
                }
            ],
            "first_pass_round": 0,
        },
    )
    tasks = runner.load_tasks([1, 2, 3], suite="paraphrase")
    original = copy.deepcopy(tasks)
    models = [{"name": "fixture", "provider": "openai", "model": "fixture"}]
    first = runner.run_benchmark(
        models, tasks, ["l1"], 0, 1, tmp_path / "before", suite="paraphrase"
    )
    runner.run_benchmark(
        models, tasks, ["l1"], 0, 1, tmp_path / "after", suite="paraphrase"
    )
    saved = json.loads((tmp_path / "after/results.json").read_text())
    assert first["suite"] == saved["suite"] == "paraphrase"
    rows = compare_results(first, saved)["rows"]
    assert len(rows) == 12 and all(row["first"] == "pass→pass" for row in rows)
    assert tasks == original
    for task, entry in zip(tasks, saved["models"][0]["tasks"]):
        assert entry["task_sha256"] == fingerprint(task)
        assert entry["parent_id"] == task["parent_id"]
        assert entry["parent_sha256"] == task["parent_sha256"]
    without_provenance = copy.deepcopy(saved)
    for entry in without_provenance["models"][0]["tasks"]:
        del entry["parent_id"], entry["parent_sha256"]
    write_reports(saved, tmp_path / "reports")
    write_reports(without_provenance, tmp_path / "legacy-reports")
    for filename in ("summary.md", "summary.csv"):
        assert (tmp_path / "reports" / filename).read_bytes() == (
            tmp_path / "legacy-reports" / filename
        ).read_bytes()
    tasks[0]["prompt"] += " Revised wording."
    changed = runner.run_benchmark(
        models, tasks, ["l1"], 0, 1, tmp_path / "changed", suite="paraphrase"
    )
    assert (
        sum(
            row["reason"] == "task definition differs"
            for row in compare_results(first, changed)["rows"]
        )
        == 1
    )


@pytest.mark.parametrize("suite", ["unknown", "baseline"])
def test_runner_rejects_invalid_suite_before_creating_results(tmp_path, suite):
    tasks = runner.load_tasks([1], suite="paraphrase")
    output = tmp_path / "invalid"
    with pytest.raises(ValueError, match="suite"):
        runner.run_benchmark([], tasks, ["l1"], 0, 1, output, suite=suite)
    assert not output.exists()
