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

"""Compare two saved benchmark results without running providers or gates."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from benchmark.report import _cli_stamp


def _index(items: list, key: str) -> dict:
    if not isinstance(items, list):
        raise ValueError(f"expected a list of {key} records")
    indexed = {}
    for item in items:
        if (
            not isinstance(item, dict)
            or not isinstance(item.get(key), str)
            or not item[key]
        ):
            raise ValueError(f"missing or invalid {key}")
        if item[key] in indexed:
            raise ValueError(f"duplicate {key}; comparison would be ambiguous")
        indexed[item[key]] = item
    return indexed


def _run_issues(baseline: dict, candidate: dict) -> list[str]:
    issues = []
    for name, run in (("baseline", baseline), ("candidate", candidate)):
        if not isinstance(run, dict):
            raise ValueError(f"{name} results must be an object")
        if (
            not isinstance(run.get("cli"), dict)
            or not isinstance(run["cli"].get("cli_commit"), str)
            or not run["cli"]["cli_commit"]
        ):
            issues.append(f"{name}: CLI revision metadata missing")
        levels = run.get("levels")
        if levels not in (["l1"], ["l1", "l2"], ["l1", "l3"], ["l1", "l2", "l3"]):
            issues.append(f"{name}: requested gate metadata missing or invalid")
        for field, minimum in (("trials", 1), ("max_repairs", 0)):
            value = run.get(field)
            if type(value) is not int or value < minimum:
                issues.append(f"{name}: {field} missing or invalid")
    for field in ("levels", "trials", "max_repairs"):
        if baseline.get(field) != candidate.get(field):
            issues.append(f"{field} differs between runs")
    return issues


def _model_issue(before: dict, after: dict) -> str:
    for name, model in (("baseline", before), ("candidate", after)):
        if model.get("error"):
            return f"{name} model did not run"
        config = model.get("config")
        if not isinstance(config, dict) or any(
            not isinstance(config.get(key), str) or not config[key]
            for key in ("provider", "model")
        ):
            return f"{name} explicit provider/model metadata missing"
    # Names identify report rows, not provider settings. Never print config
    # values: saved endpoints and provider errors can contain credentials.
    configs = [
        {key: value for key, value in model["config"].items() if key != "name"}
        for model in (before, after)
    ]
    return "model configuration differs" if configs[0] != configs[1] else ""


def _task_issue(before: dict, after: dict, trials: int) -> str:
    for name, task in (("baseline", before), ("candidate", after)):
        fingerprint = task.get("task_sha256")
        if not isinstance(fingerprint, str) or not re.fullmatch(
            "[0-9a-f]{64}", fingerprint
        ):
            return f"{name} task fingerprint missing or invalid; rerun to record task definitions"
        records = task.get("trials")
        if not isinstance(records, list) or any(
            not isinstance(record, dict) or type(record.get("trial")) is not int
            for record in records
        ):
            return f"{name} trial IDs missing or invalid"
        ids = [record["trial"] for record in records]
        if len(ids) != len(set(ids)):
            return f"{name} duplicate trial IDs"
        if len(ids) != trials or sorted(ids) != list(range(len(ids))):
            return f"{name} trial IDs do not match declared trial count"
    if before["task_sha256"] != after["task_sha256"]:
        return "task definition differs"
    return ""


def _trial_outcome(trial: dict, levels: list[str], max_repairs: int) -> tuple:
    """Return first/final success, or an exclusion reason for incomplete evidence.

    A failed gate legitimately short-circuits later gates. A missing/skipped
    gate without a preceding failure is not a measured failure or success.
    """
    if "first_pass_round" not in trial:
        return None, None, "first_pass_round missing"
    attempts = trial.get("attempts")
    if not isinstance(attempts, list):
        return None, None, "attempt records missing"
    first_pass = None
    if not attempts:
        if trial.get("generation_error") and trial.get("first_pass_round") is None:
            return False, False, ""
        return None, None, "attempt records incomplete"
    for number, attempt in enumerate(attempts):
        if (
            not isinstance(attempt, dict)
            or type(attempt.get("round")) is not int
            or attempt["round"] != number
            or number > max_repairs
        ):
            return None, None, "attempt rounds invalid"
        layers = attempt.get("layers")
        # The runner records one explicit null-layer attempt when generation
        # produces no config, including an empty config without an exception.
        if (
            "layers" in attempt
            and layers is None
            and number == 0
            and len(attempts) == 1
        ):
            continue
        if not isinstance(layers, dict):
            return None, None, "gate results missing"
        if (
            "all_gates_executed" in layers
            and type(layers["all_gates_executed"]) is not bool
        ):
            return None, None, "gate coverage flag invalid"
        if layers.get("skipped_layers") or layers.get("all_gates_executed") is False:
            return None, None, "requested gate skipped"
        passed = True
        for level in levels:
            info = layers.get(level)
            if not isinstance(info, dict) or type(info.get("passed")) is not bool:
                return None, None, "requested gate result missing"
            if info["passed"] is False:
                passed = False
                break
        for flag in ("all_passed", "full_gate_passed"):
            if flag in layers and (
                type(layers[flag]) is not bool or layers[flag] != passed
            ):
                return None, None, f"{flag} contradicts gate results"
        if passed:
            first_pass = number
            if number != len(attempts) - 1:
                return None, None, "attempts continue after success"
    recorded = trial.get("first_pass_round")
    if (recorded is not None and type(recorded) is not int) or recorded != first_pass:
        return None, None, "first_pass_round contradicts gate results"
    return first_pass == 0, first_pass is not None, ""


def compare_results(baseline: dict, candidate: dict) -> dict:
    """Pair model/task/trial identities, excluding incompatible evidence.

    Only the paired, complete trials contribute to both denominators. Input
    dictionaries and existing single-run reports are never modified.
    """
    issues = _run_issues(baseline, candidate)
    before_models = _index(baseline.get("models"), "name")
    after_models = _index(candidate.get("models"), "name")
    comparison = {
        "baseline": (
            baseline.get("cli") if isinstance(baseline.get("cli"), dict) else {}
        ),
        "candidate": (
            candidate.get("cli") if isinstance(candidate.get("cli"), dict) else {}
        ),
        "max_repairs": baseline.get("max_repairs"),
        "issues": issues,
        "rows": [],
    }
    if issues:
        return comparison
    rows = comparison["rows"]

    def excluded(model: str, task: str, reason: str) -> None:
        rows.append(
            {
                "model": model,
                "task": task,
                "trial": None,
                "first": None,
                "repaired": None,
                "reason": reason,
            }
        )

    for name in sorted(before_models.keys() | after_models.keys()):
        if name not in before_models or name not in after_models:
            side = "baseline" if name not in before_models else "candidate"
            excluded(name, "—", f"missing {side} model")
            continue
        before, after = before_models[name], after_models[name]
        issue = _model_issue(before, after)
        if issue:
            excluded(name, "—", issue)
            continue
        before_tasks = _index(before.get("tasks"), "task_id")
        after_tasks = _index(after.get("tasks"), "task_id")
        for task_id in sorted(before_tasks.keys() | after_tasks.keys()):
            if task_id not in before_tasks or task_id not in after_tasks:
                side = "baseline" if task_id not in before_tasks else "candidate"
                excluded(name, task_id, f"missing {side} task")
                continue
            before_task, after_task = before_tasks[task_id], after_tasks[task_id]
            issue = _task_issue(before_task, after_task, baseline["trials"])
            if issue:
                excluded(name, task_id, issue)
                continue
            before_trials = {trial["trial"]: trial for trial in before_task["trials"]}
            after_trials = {trial["trial"]: trial for trial in after_task["trials"]}
            for index in sorted(before_trials):
                outcomes = [
                    _trial_outcome(
                        records[index], baseline["levels"], baseline["max_repairs"]
                    )
                    for records in (before_trials, after_trials)
                ]
                reason = "; ".join(
                    f"{side}: {outcome[2]}"
                    for side, outcome in zip(("baseline", "candidate"), outcomes)
                    if outcome[2]
                )
                row = {
                    "model": name,
                    "task": task_id,
                    "trial": index,
                    "first": None,
                    "repaired": None,
                    "reason": reason,
                }
                if not reason:
                    for metric, position in (("first", 0), ("repaired", 1)):
                        row[metric] = "→".join(
                            "pass" if value[position] else "fail" for value in outcomes
                        )
                rows.append(row)
    return comparison


def _cell(value) -> str:
    text = (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("|", "&#124;")
    )
    text = re.sub(r"[\x00-\x1f\x7f]", " ", text)
    return re.sub(r"([\\`*\[\]_])", r"\\\1", text)


def render_markdown(comparison: dict) -> str:
    """Render a deterministic, descriptive report, not a CI acceptance gate."""
    lines = [
        "# SeaTunnel AI CLI Benchmark — Revision Comparison",
        "",
        f"Baseline: {_cell(_cli_stamp({'cli': comparison['baseline']}))}",
        f"Candidate: {_cell(_cli_stamp({'cli': comparison['candidate']}))}",
        "",
        "Only paired trials with compatible recorded settings and complete verdicts are scored.",
        "Missing or incompatible evidence is excluded from both denominators, not counted as a failure.",
        "Model serving state, environment variables and engine/data state are not captured; "
        "hold them constant when collecting runs. Trial numbers are independent samples, not shared random seeds.",
        "",
    ]
    if comparison["issues"]:
        lines += (
            ["## Incompatible runs", ""]
            + [f"- {_cell(issue)}" for issue in comparison["issues"]]
            + [""]
        )
    comparable = [row for row in comparison["rows"] if not row["reason"]]
    lines += ["## Paired-trial summary", ""]
    if not comparable:
        lines += ["No comparable trials. No accuracy delta is reported.", ""]
    else:
        lines += [
            "| Model | Metric | Paired trials | Baseline | Candidate | Delta | Regressions | Improvements |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for name in sorted({row["model"] for row in comparable}):
            model_rows = [row for row in comparable if row["model"] == name]
            count = len(model_rows)
            for metric, label in (
                ("first", "pass@1"),
                ("repaired", f"pass@≤{comparison['max_repairs']} repairs"),
            ):
                outcomes = [row[metric] for row in model_rows]
                before = sum(value.startswith("pass") for value in outcomes) / count
                after = sum(value.endswith("pass") for value in outcomes) / count
                lines.append(
                    f"| {_cell(name)} | {label} | {count} | {before:.1%} | {after:.1%} | "
                    f"{(after - before) * 100:+.1f} pp | {outcomes.count('pass→fail')} | {outcomes.count('fail→pass')} |"
                )
        lines += [""]
    lines += [
        "## Task/trial transitions and exclusions",
        "",
        "Exclusions make this a partial comparison; do not treat the paired subset as the full suite.",
        "",
        "| Model | Task | Trial (zero-based) | pass@1 | Within repair budget | Exclusion |",
        "|---|---|---|---|---|---|",
    ]
    for row in comparison["rows"]:
        lines.append(
            "| "
            + " | ".join(
                _cell(row[field]) if row[field] is not None else "—"
                for field in ("model", "task", "trial", "first", "repaired", "reason")
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path, help="Baseline results.json")
    parser.add_argument("candidate", type=Path, help="Candidate results.json")
    parser.add_argument(
        "--out", type=Path, help="Create a new Markdown file; defaults to stdout"
    )
    args = parser.parse_args()
    try:
        runs = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (args.baseline, args.candidate)
        ]
        report = render_markdown(compare_results(*runs))
        if args.out:
            with args.out.open("x", encoding="utf-8") as output:
                output.write(report)
        else:
            print(report, end="")
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
