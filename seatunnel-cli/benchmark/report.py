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

"""Reports for the layered benchmark.

Per model:
  Layer funnel   — % of first attempts (trial 0) that passed L1 / L2 / L3
  Repair curve   — pass@1, pass@<=3, pass@<=max (success within k repairs,
                   averaged over trials)
  pass^k         — % of tasks where ALL trials succeeded (reliability)
  Category view  — pass@<=max by ETL scenario category (failure_probe
                   category isolates rule-knowledge probes)
  Tier view      — pass@1 / pass@<=max by complexity tier

Outputs: console summary, summary.csv (one row per model×task×trial),
summary.md (comparison matrices), stamped with the CLI version under test.
"""

from __future__ import annotations

import csv
from pathlib import Path


def _trial_layer_pass(trial_rec: dict, layer: str) -> bool | None:
    """Did this trial's FIRST attempt pass the given layer? None = not run."""
    attempts = trial_rec.get("attempts") or []
    if not attempts:
        return False
    layers = attempts[0].get("layers")
    if not layers:
        return False  # generation failed → all layers fail
    info = layers.get(layer)
    if info is None:
        return None
    passed = info.get("passed")
    if passed is None:
        return None
    return bool(passed)


def _trial_pass_within(trial_rec: dict, k: int) -> bool:
    fp = trial_rec.get("first_pass_round")
    return fp is not None and fp <= k


def _rate(hits: float, total: int) -> float | None:
    return hits / total if total else None


def _fmt(rate: float | None) -> str:
    return f"{rate:.1%}" if rate is not None else "—"


def _aggregate(results: dict) -> list[dict]:
    """Compute per-model metrics from raw results.

    Denominators: layer-funnel rates use only trials where that layer
    actually executed (None = skipped is excluded); repair-curve rates
    average over all trials of every task; incomplete_gate_trials counts
    trials whose last attempt had requested-but-skipped layers.
    """
    max_repairs = results.get("max_repairs", 5)
    trials_n = results.get("trials", 1)
    rows = []
    for model in results["models"]:
        if model.get("error"):
            rows.append({"name": model["name"], "error": model["error"]})
            continue

        tasks = model["tasks"]
        n = len(tasks)

        # Layer funnel: first attempt of trial 0 only (uncontaminated)
        funnel = {}
        for layer in ("l1", "l2", "l3"):
            outcomes = [
                _trial_layer_pass(t["trials"][0], layer)
                for t in tasks if t["trials"]
            ]
            applicable = [o for o in outcomes if o is not None]
            funnel[layer] = {
                "rate": _rate(sum(applicable), len(applicable)),
                "n": len(applicable),
            }

        # Repair curve: fraction of trials passing within k, averaged per task
        def curve_at(k: int) -> float | None:
            vals = []
            for t in tasks:
                if not t["trials"]:
                    continue
                vals.append(
                    sum(_trial_pass_within(tr, k) for tr in t["trials"])
                    / len(t["trials"])
                )
            return _rate(sum(vals), len(vals))

        curve = {k: curve_at(k) for k in (0, 3, max_repairs)}

        # pass^k reliability: every trial of the task succeeded
        pass_all = _rate(
            sum(bool(t.get("pass_all_trials")) for t in tasks), n)

        # Per-tier (task passes tier stats when ALL trials pass within k)
        tiers: dict[int, dict] = {}
        for t in tasks:
            b = tiers.setdefault(t["tier"], {"n": 0, "p1": 0.0, "pk": 0.0})
            b["n"] += 1
            if t["trials"]:
                b["p1"] += sum(_trial_pass_within(tr, 0) for tr in t["trials"]) / len(t["trials"])
                b["pk"] += sum(_trial_pass_within(tr, max_repairs) for tr in t["trials"]) / len(t["trials"])

        # Per-category
        categories: dict[str, dict] = {}
        for t in tasks:
            cat = t.get("category") or "uncategorized"
            b = categories.setdefault(cat, {"n": 0, "pk": 0.0})
            b["n"] += 1
            if t["trials"]:
                b["pk"] += sum(_trial_pass_within(tr, max_repairs) for tr in t["trials"]) / len(t["trials"])

        # Gate-coverage accounting: trials where a requested layer was
        # skipped (missing backend/service) must be visible in reports.
        all_trials_pre = [tr for t in tasks for tr in t["trials"]]
        incomplete = sum(
            1 for tr in all_trials_pre
            if tr["attempts"] and not
            (tr["attempts"][-1].get("layers") or {}).get("all_gates_executed", True)
        )

        # Telemetry across all trials
        all_trials = [tr for t in tasks for tr in t["trials"]]
        total_seconds = sum(
            (tr["attempts"][-1]["seconds"] if tr["attempts"] else 0.0)
            for tr in all_trials
        )
        repair_rounds = [
            tr["first_pass_round"] for tr in all_trials
            if (tr.get("first_pass_round") or 0) > 0
        ]

        rows.append({
            "name": model["name"],
            "n": n,
            "trials_n": trials_n,
            "funnel": funnel,
            "curve": curve,
            "pass_all": pass_all,
            "max_repairs": max_repairs,
            "tiers": dict(sorted(tiers.items())),
            "categories": dict(sorted(categories.items())),
            "avg_seconds": total_seconds / len(all_trials) if all_trials else 0.0,
            "avg_repairs_when_needed": (
                sum(repair_rounds) / len(repair_rounds) if repair_rounds else 0.0
            ),
            "incomplete_gate_trials": incomplete,
            "clarifications": sum(
                bool(tr.get("clarification_asked")) for tr in all_trials),
            "generation_errors": sum(
                bool(tr.get("generation_error")) for tr in all_trials),
        })
    return rows


def _cli_stamp(results: dict) -> str:
    cli = results.get("cli", {})
    stamp = f"seatunnel-cli v{cli.get('cli_version', '?')}"
    if cli.get("cli_commit"):
        stamp += f" @ {cli['cli_commit']} ({cli.get('cli_commit_date', '')})"
    if cli.get("cli_dirty"):
        stamp += " +local-changes"
    return stamp


def print_summary(results: dict) -> None:
    rows = _aggregate(results)
    trials_n = results.get("trials", 1)
    print("\n================ Benchmark Summary ================")
    print(f"CLI under test: {_cli_stamp(results)}")
    for row in rows:
        if row.get("error"):
            print(f"\n{row['name']}: SKIPPED ({row['error']})")
            continue
        mk = row["max_repairs"]
        f = row["funnel"]
        print(f"\n{row['name']}  ({row['n']} tasks × {row['trials_n']} trial(s))")
        print("  First-attempt layer funnel:")
        print(f"    L1 static    : {_fmt(f['l1']['rate'])}  (n={f['l1']['n']})")
        print(f"    L2 dry-run   : {_fmt(f['l2']['rate'])}  (n={f['l2']['n']})")
        print(f"    L3 execution : {_fmt(f['l3']['rate'])}  (n={f['l3']['n']})")
        print("  Repair curve (all enabled gates):")
        print(f"    pass@1       : {_fmt(row['curve'][0])}")
        print(f"    pass@<=3     : {_fmt(row['curve'][3])}")
        print(f"    pass@<={mk}     : {_fmt(row['curve'][mk])}")
        if trials_n > 1:
            print(f"    pass^{trials_n} (all trials): {_fmt(row['pass_all'])}")
        print(f"  By tier (pass@1 / pass@<={mk}):")
        for tier, b in row["tiers"].items():
            print(f"    tier {tier} ({b['n']:2d}) : "
                  f"{_fmt(_rate(b['p1'], b['n']))} / {_fmt(_rate(b['pk'], b['n']))}")
        print(f"  By category (pass@<={mk}):")
        for cat, b in row["categories"].items():
            print(f"    {cat:<20s}: {_fmt(_rate(b['pk'], b['n']))}  ({b['n']} tasks)")
        print(f"  avg wall time: {row['avg_seconds']:.1f}s/trial; "
              f"avg repairs when needed: {row['avg_repairs_when_needed']:.1f}")
        if row.get("incomplete_gate_trials"):
            print(f"  ⚠ trials with requested-but-skipped gates: "
                  f"{row['incomplete_gate_trials']} (see skipped_layers in CSV; "
                  f"these did NOT execute every requested layer)")
        if row["clarifications"]:
            print(f"  clarifications: {row['clarifications']}")
        if row["generation_errors"]:
            print(f"  generation errors: {row['generation_errors']}")
    print("\n===================================================")


def write_reports(results: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(results, out_dir / "summary.csv")
    _write_markdown(results, out_dir / "summary.md")
    print(f"Reports written to {out_dir}/summary.csv and {out_dir}/summary.md")


def _write_csv(results: dict, path: Path) -> None:
    fieldnames = [
        "model", "task_id", "tier", "category", "trial",
        "l1_first", "l2_first", "l3_first",
        "first_pass_round", "internal_repair_rounds", "attempts", "passed",
        "skipped_layers", "all_gates_executed",
        "total_seconds", "clarification_asked", "generation_error",
        "failed_layer_last", "error_detail_last",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for model in results["models"]:
            if model.get("error"):
                continue
            for t in model["tasks"]:
                for tr in t["trials"]:
                    last = tr["attempts"][-1] if tr["attempts"] else {}
                    last_layers = last.get("layers") or {}
                    writer.writerow({
                        "model": model["name"],
                        "task_id": t["task_id"],
                        "tier": t["tier"],
                        "category": t.get("category", ""),
                        "trial": tr.get("trial", 0),
                        "l1_first": _trial_layer_pass(tr, "l1"),
                        "l2_first": _trial_layer_pass(tr, "l2"),
                        "l3_first": _trial_layer_pass(tr, "l3"),
                        "first_pass_round": tr.get("first_pass_round"),
                        "internal_repair_rounds": tr.get("internal_repair_rounds", 0),
                        "attempts": len(tr["attempts"]),
                        "passed": tr.get("first_pass_round") is not None,
                        "skipped_layers": ";".join(
                            (tr["attempts"][-1].get("layers") or {}).get("skipped_layers", [])
                            if tr["attempts"] else []),
                        "all_gates_executed": bool(
                            (tr["attempts"][-1].get("layers") or {}).get("all_gates_executed", True)
                            if tr["attempts"] else False),
                        "total_seconds": last.get("seconds", ""),
                        "clarification_asked": tr.get("clarification_asked", False),
                        "generation_error": tr.get("generation_error") or "",
                        "failed_layer_last": last_layers.get("failed_layer") or "",
                        "error_detail_last": (last_layers.get("error_detail") or "")[:300],
                    })


def _write_markdown(results: dict, path: Path) -> None:
    rows = _aggregate(results)
    mk = results.get("max_repairs", 5)
    trials_n = results.get("trials", 1)

    lines = ["# SeaTunnel AI CLI Benchmark — Layered Cross-Model Comparison",
             "", f"CLI under test: **{_cli_stamp(results)}**", ""]

    lines += ["## First-attempt layer funnel", ""]
    lines.append("| Model | L1 static | L2 dry-run | L3 execution |")
    lines.append("|---|---|---|---|")
    for row in rows:
        if row.get("error"):
            lines.append(f"| {row['name']} | SKIPPED: {row['error']} | | |")
            continue
        f = row["funnel"]
        lines.append(
            f"| {row['name']} | {_fmt(f['l1']['rate'])} | "
            f"{_fmt(f['l2']['rate'])} | {_fmt(f['l3']['rate'])} |"
        )

    lines += ["", "## Repair curve (success within k auto-repair rounds)", ""]
    header = f"| Model | pass@1 | pass@≤3 | pass@≤{mk} |"
    sep = "|---|---|---|---|"
    if trials_n > 1:
        header += f" pass^{trials_n} |"
        sep += "---|"
    header += " avg repairs when needed |"
    sep += "---|"
    lines.append(header)
    lines.append(sep)
    for row in rows:
        if row.get("error"):
            continue
        cells = (f"| {row['name']} | {_fmt(row['curve'][0])} | "
                 f"{_fmt(row['curve'][3])} | {_fmt(row['curve'][mk])} |")
        if trials_n > 1:
            cells += f" {_fmt(row['pass_all'])} |"
        cells += f" {row['avg_repairs_when_needed']:.1f} |"
        lines.append(cells)

    all_cats = sorted({
        cat for row in rows if not row.get("error") for cat in row["categories"]
    })
    if all_cats:
        lines += ["", f"## By ETL category (pass@≤{mk})", ""]
        lines.append("| Model | " + " | ".join(all_cats) + " |")
        lines.append("|---" * (len(all_cats) + 1) + "|")
        for row in rows:
            if row.get("error"):
                continue
            cells = [row["name"]]
            for cat in all_cats:
                b = row["categories"].get(cat)
                cells.append(_fmt(_rate(b["pk"], b["n"])) if b else "—")
            lines.append("| " + " | ".join(cells) + " |")

    all_tiers = sorted({
        tier for row in rows if not row.get("error") for tier in row["tiers"]
    })
    if all_tiers:
        lines += ["", f"## By complexity tier (pass@1 / pass@≤{mk})", ""]
        lines.append("| Model | " + " | ".join(f"Tier {t}" for t in all_tiers) + " |")
        lines.append("|---" * (len(all_tiers) + 1) + "|")
        for row in rows:
            if row.get("error"):
                continue
            cells = [row["name"]]
            for tier in all_tiers:
                b = row["tiers"].get(tier)
                if b:
                    cells.append(f"{_fmt(_rate(b['p1'], b['n']))} / "
                                 f"{_fmt(_rate(b['pk'], b['n']))}")
                else:
                    cells.append("—")
            lines.append("| " + " | ".join(cells) + " |")

    lines += [
        "",
        "Gates: **L1** static validation (HOCON parse, connector metadata, "
        "regex assertions) → **L2** engine dry-run (`seatunnel.sh --dry-run "
        "static`: plugin loadability, OptionRule, unknown keys, types, DAG) → "
        "**L3** real execution against the Docker data environment "
        "(batch: exit 0; streaming: healthy for 60s; optional sink row-count "
        "verification). A task passes when every enabled gate passes; "
        "`pass@k` counts success within k auto-repair rounds; "
        "`pass^k` requires every independent trial to succeed. "
        "The `failure_probe` category contains rule-knowledge probes that "
        "target known LLM error patterns (conditional options, mode "
        "inference, routing labels).",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")
