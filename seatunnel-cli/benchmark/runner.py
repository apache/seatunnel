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

"""Benchmark runner: layered evaluation across models with a repair loop.

Per task, per model:
  1. Generate config via the real CLI Orchestrator (Planner → Config →
     Validator → internal fix loop).
  2. Evaluate through three strict gates:
       L1 static   (scoring.py — offline, always runs)
       L2 dry-run  (seatunnel.sh --dry-run static; needs SEATUNNEL_HOME)
       L3 execute  (seatunnel.sh -m local vs the Docker data env; needs
                    SEATUNNEL_HOME + docker compose services)
  3. On failure, feed the failing layer's error to the repair agent and
     retry — up to --max-repairs rounds (default 5).

Recorded per attempt: which layers passed, error details, wall time.
`first_pass_round` = 0 means first-try success (pass@1); k means success
after k repairs — reports derive pass@1 / pass@≤3 / pass@≤5 from it.

Usage:
    cd seatunnel-cli
    # data env (optional, enables L3):
    docker compose -f benchmark/docker/docker-compose.yml up -d --wait
    export SEATUNNEL_HOME=/path/to/apache-seatunnel   # enables L2+L3
    python -m benchmark.runner --models benchmark/models.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TASKS_DIR = Path(__file__).resolve().parent / "tasks"
TIER_FILES = {
    1: "tier1_simple.json",
    2: "tier2_medium.json",
    3: "tier3_complex.json",
}

CLARIFICATION_REPLY = (
    "Use sensible defaults for anything unspecified. Do not ask further "
    "questions — generate the config now."
)


def load_tasks(tiers: list[int], task_ids: list[str] | None = None) -> list[dict]:
    tasks = []
    for tier in tiers:
        path = TASKS_DIR / TIER_FILES[tier]
        data = json.loads(path.read_text(encoding="utf-8"))
        for task in data["tasks"]:
            task["tier"] = tier
            tasks.append(task)
    if task_ids:
        wanted = set(task_ids)
        tasks = [t for t in tasks if t["id"] in wanted]
    return tasks


def apply_model_env(model_cfg: dict) -> dict[str, str | None]:
    """Set provider/model env vars for a model config; return previous values."""
    mapping = {
        "AI_PROVIDER": model_cfg["provider"],
    }
    provider = model_cfg["provider"].lower()
    if provider in ("openai", "bedrock-mantle"):
        mapping["OPENAI_MODEL"] = model_cfg.get("model")
        mapping["OPENAI_SMALL_FAST_MODEL"] = model_cfg.get("fast_model")
        if model_cfg.get("base_url"):
            mapping["OPENAI_BASE_URL"] = model_cfg["base_url"]
    else:  # bedrock / anthropic share the ANTHROPIC_* override vars
        mapping["ANTHROPIC_MODEL"] = model_cfg.get("model")
        mapping["ANTHROPIC_SMALL_FAST_MODEL"] = model_cfg.get("fast_model")

    previous: dict[str, str | None] = {}
    for key, value in mapping.items():
        previous[key] = os.environ.get(key)
        if value:
            os.environ[key] = value
        elif key in os.environ and key != "AI_PROVIDER":
            del os.environ[key]
    return previous


def restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


# ─── Layered evaluation ───

def evaluate_layers(task: dict, config: str, levels: list[str]) -> dict:
    """Run the requested gate layers in order; stop at the first failure.

    Returns:
        {
          "l1": {"passed": bool, "detail": ...},
          "l2": {"passed": bool|None, ...} | None,   # None = not reached
          "l3": {...} | None,
          "all_passed": bool,      # every executed layer passed or was skipped
          "failed_layer": str|None,
          "error_detail": str,     # feed to the repair agent
        }
    """
    from benchmark.scoring import score_task
    from benchmark.execution import run_dry_run, run_execute

    result = {"l1": None, "l2": None, "l3": None,
              "all_passed": True, "failed_layer": None, "error_detail": "",
              # Layers requested but not executed (backend/service missing or
              # task-level skip). all_passed still holds for executed layers,
              # but reports must not treat such a trial as full-gate coverage.
              "skipped_layers": [], "all_gates_executed": True}

    # L1 static
    score = score_task(task, config)
    result["l1"] = {"passed": score.passed, "score": score.to_dict()}
    if not score.passed:
        failed = [name for name, ok in score.checks.items() if not ok]
        details = "; ".join(
            f"{k}: {v}" for k, v in score.details.items() if k in failed
        )
        result["all_passed"] = False
        result["failed_layer"] = "l1"
        result["error_detail"] = (
            f"Static validation failed on: {', '.join(failed)}. {details}"
        )
        return result

    # L2 engine dry-run
    if "l2" in levels:
        dry = run_dry_run(config)
        result["l2"] = dry
        if dry["passed"] is False:
            result["all_passed"] = False
            result["failed_layer"] = "l2"
            result["error_detail"] = f"Engine dry-run failed:\n{dry['detail']}"
            return result
        if dry["passed"] is None:
            result["skipped_layers"].append("l2")
            result["all_gates_executed"] = False

    # L3 real execution
    if "l3" in levels:
        execu = run_execute(config, task)
        result["l3"] = execu
        if execu["passed"] is False:
            result["all_passed"] = False
            result["failed_layer"] = "l3"
            result["error_detail"] = f"Job execution failed:\n{execu['detail']}"
            return result
        if execu["passed"] is None:
            result["skipped_layers"].append("l3")
            result["all_gates_executed"] = False

    # full_gate_passed drives every advertised pass@k/pass^k metric:
    # every requested gate must have EXECUTED and passed. all_passed
    # ("executed layers passed") is kept for repair-loop control only —
    # a skipped gate is not a failure to repair, but it is not a success
    # either.
    result["full_gate_passed"] = (
        result["all_passed"] and result["all_gates_executed"]
    )
    return result


def run_task_with_repairs(client, task: dict, levels: list[str],
                          max_repairs: int) -> dict:
    """Generate + evaluate + repair loop. Returns the full task record."""
    from seatunnel_cli.agents import Orchestrator

    record = {
        "task_id": task["id"],
        "tier": task["tier"],
        "category": task.get("category", ""),
        "attempts": [],
        "first_pass_round": None,   # 0 = first CLI-delivered config;
                                    # k = after k benchmark-level repairs.
                                    # The CLI may additionally run up to 3
                                    # internal fix rounds before delivering —
                                    # see internal_repair_rounds.
        "internal_repair_rounds": 0,
        "clarification_asked": False,
        "generation_error": None,
    }

    # The CLI's process_user_input runs its own validate/fix loop (up to 3
    # rounds) before returning — that is the product behavior this benchmark
    # measures, so "round 0" means "the CLI's first delivered config", not
    # "the model's raw first sample". Count those internal rounds via the
    # status callback so reports can expose both timelines.
    internal_repairs = {"count": 0}

    def _count_fixing(kind, _msg=""):
        if kind == "fixing":
            internal_repairs["count"] += 1

    orchestrator = Orchestrator(client, on_status=_count_fixing)
    start = time.monotonic()

    # ── Initial generation ──
    config = None
    try:
        result = orchestrator.process_user_input(task["prompt"])
        if result.get("type") == "question":
            record["clarification_asked"] = True
            result = orchestrator.process_user_input(CLARIFICATION_REPLY)
        if result.get("type") == "config":
            config = result.get("config")
        else:
            record["generation_error"] = f"non-config result: {result.get('type')}"
    except Exception as e:
        record["generation_error"] = f"{type(e).__name__}: {e}"
        traceback.print_exc()

    record["internal_repair_rounds"] = internal_repairs["count"]

    if not config:
        record["attempts"].append({
            "round": 0, "config": None, "layers": None,
            "seconds": round(time.monotonic() - start, 2),
        })
        return record

    # ── Evaluate / repair loop ──
    for round_num in range(max_repairs + 1):
        layers = evaluate_layers(task, config, levels)
        record["attempts"].append({
            "round": round_num,
            "config": config,
            "layers": layers,
            "seconds": round(time.monotonic() - start, 2),
        })
        if layers.get("full_gate_passed"):
            record["first_pass_round"] = round_num
            break
        if layers["all_passed"]:
            # Executed layers passed but a requested gate was skipped:
            # nothing to repair, but not a full-gate success either.
            break
        if round_num == max_repairs:
            break

        # Repair via the same fix agent the CLI uses
        try:
            fix = orchestrator._run_fix(config, layers["error_detail"])
            fixed = fix.get("config")
            if not fixed or fixed == config:
                break  # repair agent gave up or made no change
            config = fixed
        except Exception as e:
            record["generation_error"] = f"repair crashed: {type(e).__name__}: {e}"
            break

    return record


def collect_cli_fingerprint() -> dict:
    """Record exactly which seatunnel-cli build was benchmarked.

    Benchmark results are only comparable across runs when tied to a specific
    CLI version — this is the baseline stamp for evaluating CLI improvements.
    """
    import subprocess
    fingerprint = {}
    try:
        from seatunnel_cli import __version__
        fingerprint["cli_version"] = __version__
    except Exception:
        fingerprint["cli_version"] = "unknown"
    repo_root = Path(__file__).resolve().parent.parent
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "log", "-1",
             "--format=%h %cs", "--", "seatunnel_cli"],
            capture_output=True, text=True, timeout=10,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            commit, date = proc.stdout.strip().split(" ", 1)
            fingerprint["cli_commit"] = commit
            fingerprint["cli_commit_date"] = date
        proc = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain",
             "--", "seatunnel_cli"],
            capture_output=True, text=True, timeout=10,
        )
        fingerprint["cli_dirty"] = bool(proc.stdout.strip())
    except Exception:
        pass
    fingerprint["seatunnel_home"] = os.environ.get("SEATUNNEL_HOME", "")
    return fingerprint


def run_benchmark(models: list[dict], tasks: list[dict], levels: list[str],
                  max_repairs: int, trials: int, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    configs_dir = out_dir / "configs"
    configs_dir.mkdir(exist_ok=True)

    all_results = {
        "levels": levels,
        "max_repairs": max_repairs,
        "trials": trials,
        "cli": collect_cli_fingerprint(),
        "models": [],
    }
    print(f"CLI under test: v{all_results['cli'].get('cli_version')} "
          f"@ {all_results['cli'].get('cli_commit', '?')}"
          f"{' (dirty)' if all_results['cli'].get('cli_dirty') else ''}")

    for model_cfg in models:
        model_name = model_cfg.get("name") or \
            f"{model_cfg['provider']}:{model_cfg.get('model', 'default')}"
        print(f"\n=== Model: {model_name} ===", flush=True)
        previous_env = apply_model_env(model_cfg)
        model_result = {"name": model_name, "config": model_cfg, "tasks": []}

        try:
            from seatunnel_cli.llm_provider import create_provider
            client = create_provider(model_cfg["provider"])
        except Exception as e:
            print(f"  SKIPPED — provider init failed: {e}", file=sys.stderr)
            model_result["error"] = str(e)
            all_results["models"].append(model_result)
            restore_env(previous_env)
            continue

        try:
            for task in tasks:
                task_entry = {
                    "task_id": task["id"],
                    "tier": task["tier"],
                    "category": task.get("category", ""),
                    "trials": [],
                }
                for trial in range(trials):
                    label = task["id"] + (f" trial {trial + 1}/{trials}" if trials > 1 else "")
                    print(f"  [{label}] ...", end="", flush=True)
                    # Per-task boundary: an unexpected harness error (scoring
                    # crash, setup-file I/O, subprocess construction, ...)
                    # becomes one structured failed trial instead of aborting
                    # the whole multi-hour model comparison.
                    try:
                        record = run_task_with_repairs(
                            client, task, levels, max_repairs)
                    except Exception:
                        import traceback as _tb
                        record = {
                            "attempts": [],
                            "first_pass_round": None,
                            "internal_repair_rounds": 0,
                            "clarification_asked": False,
                            "generation_error":
                                f"harness error:\n{_tb.format_exc(limit=8)}",
                        }

                    # Persist every attempted config
                    for attempt in record["attempts"]:
                        if attempt.get("config"):
                            fname = (f"{_slug(model_name)}__{task['id']}"
                                     f"__t{trial}_r{attempt['round']}.conf")
                            (configs_dir / fname).write_text(
                                attempt["config"], encoding="utf-8")
                            attempt["config_file"] = fname
                        attempt.pop("config", None)  # keep results.json compact

                    record["trial"] = trial
                    fp = record["first_pass_round"]
                    if fp is None:
                        status = f"FAIL after {len(record['attempts'])} attempt(s)"
                    elif fp == 0:
                        status = "PASS@1"
                    else:
                        status = f"PASS after {fp} repair(s)"
                    print(f" {status}", flush=True)
                    task_entry["trials"].append(record)

                # pass^k: every trial must succeed (first_pass_round set)
                task_entry["pass_all_trials"] = all(
                    t["first_pass_round"] is not None for t in task_entry["trials"]
                )
                model_result["tasks"].append(task_entry)

            all_results["models"].append(model_result)
        finally:
            restore_env(previous_env)

    results_path = out_dir / "results.json"
    results_path.write_text(
        json.dumps(all_results, indent=2, ensure_ascii=False), encoding="utf-8",
    )
    print(f"\nRaw results written to {results_path}")
    return all_results


def _slug(text: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in text)


def resolve_levels(args_levels: str, tasks: list[dict]) -> list[str]:
    """Determine which gate layers can actually run; warn about the rest."""
    from benchmark.execution import engine_backend, check_services_up

    wanted = ["l1"]
    backend = engine_backend()
    if args_levels in ("l2", "l3"):
        if backend:
            wanted.append("l2")
            print(f"Engine backend for L2/L3: {backend}")
        else:
            print("WARN: no engine available (need SEATUNNEL_HOME dist or "
                  "docker + apache/seatunnel image) — L2/L3 disabled, "
                  "running static-only.", file=sys.stderr)
            return wanted
    if args_levels == "l3":
        needed = sorted({
            s for t in tasks for s in t.get("execution", {}).get("services", [])
        })
        up, detail = check_services_up(needed)
        if up:
            wanted.append("l3")
        else:
            # Core services may be up while heavy optional ones (doris,
            # starrocks) are not — enable L3 and let run_execute skip only
            # the tasks whose services are missing.
            core_up, core_detail = check_services_up(
                [s for s in needed if s not in ("doris", "starrocks")])
            if core_up:
                wanted.append("l3")
                print(f"NOTE: {detail} — affected tasks will skip L3; "
                      f"all other tasks run L3.", file=sys.stderr)
            else:
                print(f"WARN: Docker data env incomplete ({core_detail}) — "
                      f"L3 disabled. Start it with: docker compose -f "
                      f"benchmark/docker/docker-compose.yml up -d --wait",
                      file=sys.stderr)
    return wanted


def build_models_from_args(args) -> list[dict]:
    """Build the model list either from --models JSON or from direct CLI flags.

    Direct mode lets a user go from "here is my API key" to a report in one
    command:
        python -m benchmark.runner --provider openai --model gpt-4o \
            --api-key sk-... [--base-url https://.../v1]
    """
    if args.models:
        return json.loads(Path(args.models).read_text(encoding="utf-8"))["models"]

    if not args.provider:
        print("ERROR: provide either --models <file> or --provider/--model "
              "(with --api-key).", file=sys.stderr)
        sys.exit(2)

    # Push credentials from flags into the env vars the providers read.
    if args.api_key:
        env_key = {
            "openai": "OPENAI_API_KEY",
            "anthropic": "ANTHROPIC_API_KEY",
        }.get(args.provider)
        if env_key:
            os.environ[env_key] = args.api_key
        else:
            print("NOTE: --api-key is ignored for bedrock; it uses AWS "
                  "credentials (env/profile/IAM).", file=sys.stderr)

    model_cfg = {
        "name": args.model or f"{args.provider}-default",
        "provider": args.provider,
    }
    if args.model:
        model_cfg["model"] = args.model
    if args.fast_model:
        model_cfg["fast_model"] = args.fast_model
    if args.base_url:
        model_cfg["base_url"] = args.base_url
    return [model_cfg]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SeaTunnel AI CLI accuracy benchmark",
        epilog="Quick start (single model, no JSON file): "
               "python -m benchmark.runner --provider openai "
               "--model gpt-4o --api-key sk-...",
    )
    parser.add_argument("--models", default=None,
                        help="Path to models JSON for multi-model comparison "
                             "(see models.example.json)")
    parser.add_argument("--provider", default=None,
                        choices=["openai", "anthropic", "bedrock"],
                        help="Single-model mode: LLM provider")
    parser.add_argument("--model", default=None,
                        help="Single-model mode: model id (e.g. gpt-4o, "
                             "claude-sonnet-4-20250514, deepseek-chat)")
    parser.add_argument("--fast-model", default=None,
                        help="Single-model mode: fast/small model id "
                             "(defaults to provider default)")
    parser.add_argument("--api-key", default=None,
                        help="Single-model mode: API key (openai/anthropic; "
                             "bedrock uses AWS credentials)")
    parser.add_argument("--base-url", default=None,
                        help="Single-model mode: OpenAI-compatible endpoint "
                             "(DeepSeek, Azure, local vLLM, ...)")
    parser.add_argument("--tiers", type=int, nargs="+", default=[1, 2, 3],
                        choices=[1, 2, 3])
    parser.add_argument("--tasks", nargs="*", default=None,
                        help="Optional task id filter")
    parser.add_argument("--level", default="l3", choices=["l1", "l2", "l3"],
                        help="Deepest gate to run: l1=static only, l2=+engine "
                             "dry-run, l3=+real execution (default; degrades "
                             "gracefully if env is missing)")
    parser.add_argument("--max-repairs", type=int, default=5,
                        help="Max auto-repair rounds after a failed attempt")
    parser.add_argument("--trials", type=int, default=1,
                        help="Independent trials per task; pass^k requires "
                             "ALL trials to succeed (reliability metric)")
    parser.add_argument("--out", default="benchmark/results")
    args = parser.parse_args()

    models = build_models_from_args(args)
    tasks = load_tasks(args.tiers, args.tasks)
    if not tasks:
        print("No tasks selected.", file=sys.stderr)
        sys.exit(1)

    levels = resolve_levels(args.level, tasks)
    print(f"Running {len(tasks)} tasks × {len(models)} models × "
          f"{args.trials} trial(s), gates: {' → '.join(levels)}, "
          f"max repairs: {args.max_repairs}")

    # Isolate CLI state from the user's real .data dir
    os.environ["SEATUNNEL_CLI_DATA"] = tempfile.mkdtemp(prefix="st_bench_")

    # Export the benchmark environment credentials so L1 validation treats
    # ${MYSQL_PASSWORD}-style placeholders as resolved (matches real usage,
    # where the user exports them before running seatunnel.sh).
    from benchmark.execution import CREDENTIALS
    for key, value in CREDENTIALS.items():
        if value:
            os.environ.setdefault(key, value)

    results = run_benchmark(models, tasks, levels, args.max_repairs,
                            args.trials, Path(args.out))

    from benchmark.report import print_summary, write_reports
    print_summary(results)
    write_reports(results, Path(args.out))


if __name__ == "__main__":
    main()
