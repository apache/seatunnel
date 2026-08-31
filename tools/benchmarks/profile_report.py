#!/usr/bin/env python3
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

"""Build a standalone report for benchmark profiling artifacts."""

import argparse
import datetime
import json
import math
import pathlib
import re
import subprocess


SCHEMA = "seatunnel-profile/v1"
GC_COLUMNS = (
    ("gc.alloc.rate.norm", "Alloc/op", "B/op"),
    ("gc.alloc.rate", "Alloc rate", "MB/sec"),
    ("gc.count", "GC count", "counts"),
    ("gc.time", "GC time", "ms"),
)
PROFILE_MODES = ("cpu", "wall", "lock", "gc")
MODE_DETAILS = {
    "cpu": {
        "directory": "profile-cpu",
        "label": "CPU",
        "contents": "Flame graphs, async-profiler JFR and text summary",
    },
    "wall": {
        "directory": "profile-wall",
        "label": "Wall",
        "contents": "Flame graphs, async-profiler JFR and text summary",
    },
    "lock": {
        "directory": "profile-lock",
        "label": "Lock",
        "contents": "Contention flame graphs, JFR and text summary",
    },
    "gc": {
        "directory": "profile-gc",
        "label": "GC",
        "contents": "Allocation and garbage-collection metrics",
    },
    "jfr": {
        "directory": "capture-jfr",
        "label": "JFR",
        "contents": "JVM Flight Recorder capture",
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="report_command", required=True)

    mode_parser = commands.add_parser("mode", help="Build one profiler report.")
    mode_parser.add_argument("--mode", required=True)
    mode_parser.add_argument(
        "--command", required=True, choices=("profile", "capture")
    )
    mode_parser.add_argument("--jmh", required=True, type=pathlib.Path)
    mode_parser.add_argument("--artifact-dir", required=True, type=pathlib.Path)
    mode_parser.add_argument("--output-json", required=True, type=pathlib.Path)
    mode_parser.add_argument("--output-md", required=True, type=pathlib.Path)
    mode_parser.add_argument("--status", required=True, type=int)

    summary_parser = commands.add_parser(
        "summary", help="Build the combined GitHub Actions summary."
    )
    summary_parser.add_argument(
        "--diagnostics-dir", required=True, type=pathlib.Path
    )
    summary_parser.add_argument(
        "--profile", required=True, choices=("none", "cpu", "wall", "lock", "gc", "all")
    )
    summary_parser.add_argument(
        "--capture-jfr", required=True, choices=("true", "false")
    )
    summary_parser.add_argument("--repository", required=True, type=pathlib.Path)
    summary_parser.add_argument("--ref", required=True)
    summary_parser.add_argument("--pr-number", default="")
    summary_parser.add_argument("--benchmark", required=True)
    summary_parser.add_argument("--java", required=True)
    summary_parser.add_argument("--jmh-args", default="")
    summary_parser.add_argument("--artifacts-url", required=True)
    summary_parser.add_argument("--run-id", required=True)
    summary_parser.add_argument("--run-attempt", required=True)
    return parser.parse_args()


def finite_or_none(value):
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def load_jmh(path):
    if not path.is_file():
        return []
    try:
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return []


def load_profile_report(path):
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as handle:
            report = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return report if isinstance(report, dict) else None


def resolve_commit(repository):
    try:
        result = subprocess.run(
            ["git", "-C", str(repository), "rev-parse", "--short=12", "HEAD"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return result.stdout.strip() or "unknown"


def benchmark_name(result):
    return result.get("benchmark", "unknown")


def compact_params(params):
    if not params:
        return "default"
    return ", ".join("{}={}".format(key, params[key]) for key in sorted(params))


def gc_metrics(results):
    metrics = []
    for result in results:
        secondary = result.get("secondaryMetrics", {})
        values = {}
        for name, metric in secondary.items():
            if not name.startswith("gc."):
                continue
            values[name] = {
                "value": finite_or_none(metric.get("score")),
                "error": finite_or_none(metric.get("scoreError")),
                "unit": metric.get("scoreUnit"),
            }
        if values:
            metrics.append(
                {
                    "benchmark": benchmark_name(result),
                    "params": result.get("params", {}),
                    "metrics": values,
                }
            )
    return metrics


def human_size(size):
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024.0 or unit == "TiB":
            return "{:.1f} {}".format(value, unit)
        value /= 1024.0
    return "{} B".format(size)


def artifacts(directory, excluded):
    values = []
    if not directory.is_dir():
        return values
    excluded_paths = {path.resolve() for path in excluded}
    for path in sorted(directory.rglob("*")):
        if not path.is_file() or path.resolve() in excluded_paths:
            continue
        values.append(
            {
                "path": str(path.relative_to(directory)),
                "size_bytes": path.stat().st_size,
            }
        )
    return values


def async_sample_count(directory):
    counts = []
    if directory.is_dir():
        for summary in directory.rglob("summary-*.txt"):
            match = re.search(
                r"^Total samples\s*:\s*(\d+)",
                summary.read_text(encoding="utf-8"),
                flags=re.MULTILINE,
            )
            if match:
                counts.append(int(match.group(1)))
    return sum(counts) if counts else None


def format_number(value):
    if value is None:
        return "n/a"
    if abs(value) >= 1000:
        return "{:,.2f}".format(value)
    if abs(value) >= 1:
        return "{:.3f}".format(value)
    return "{:.6f}".format(value)


def gc_report_lines(metrics):
    """Return GC/allocation Markdown lines, or no section when metrics are unavailable."""
    if not metrics:
        return []
    lines = [
        "### GC and allocation metrics",
        "",
        "These values come from JMH's GC profiler and cover measurement iterations only. "
        "The primary JMH Score from a profiled run is diagnostic and is not compared with "
        "normal benchmark results.",
        "",
        "- **Alloc/op (B/op):** average bytes allocated per benchmark operation. This is the "
        "most direct allocation-efficiency metric; lower is generally better.",
        "- **Alloc rate (MB/sec):** average allocation throughput. It depends on both bytes "
        "allocated per operation and operations per second, so a high value alone does not "
        "prove a regression.",
        "- **GC count (counts):** total garbage collections observed across the measured "
        "iterations and forks. It is not normalized per operation.",
        "- **GC time (ms):** accumulated collection time reported by the JVM across the "
        "measured iterations and forks. It is JVM- and collector-dependent and is not an "
        "exact stop-the-world pause-time measurement.",
        "",
        "Compare these values only when the JVM, collector, heap settings, benchmark selector, "
        "and JMH fork/warmup/measurement settings are the same. Capture JFR separately when "
        "collection phases and pause details are needed.",
        "",
        "| Benchmark | Parameters | Alloc/op | Alloc rate | GC count | GC time |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for result in metrics:
        row = []
        for name, _, unit in GC_COLUMNS:
            metric = result["metrics"].get(name)
            row.append(
                "{} {}".format(format_number(metric["value"]), unit) if metric else "n/a"
            )
        lines.append(
            "| `{}` | `{}` | {} | {} | {} | {} |".format(
                result["benchmark"].rsplit(".", 1)[-1],
                compact_params(result["params"]),
                *row,
            )
        )
    return lines


def artifact_report_lines(values):
    if not values:
        return ["### Artifacts", "", "No diagnostic artifacts were produced."]
    lines = [
        "### Artifacts",
        "",
        "Open these files locally, or download the workflow artifact for offline analysis.",
        "",
        "| File | Size |",
        "| --- | ---: |",
    ]
    for artifact in values:
        lines.append(
            "| `{}` | {} |".format(
                artifact["path"], human_size(artifact["size_bytes"])
            )
        )
    return lines


def report_status(report):
    if report is None:
        return "not produced"
    status = report.get("status")
    if status == 0:
        return "passed"
    return "failed ({})".format(status if status is not None else "unknown")


def build_report(args):
    jmh_results = load_jmh(args.jmh)
    gc = gc_metrics(jmh_results)
    generated_artifacts = artifacts(
        args.artifact_dir, (args.output_json, args.output_md)
    )
    return {
        "schema": SCHEMA,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "command": args.command,
        "mode": args.mode,
        "status": args.status,
        "score_comparable": False,
        "async_samples": async_sample_count(args.artifact_dir),
        "gc_metrics": gc,
        "artifacts": generated_artifacts,
    }


def render_markdown(report):
    lines = [
        "## {} {} diagnostics".format(
            report["mode"].upper(), report["command"]
        ),
        "",
        "- Status: `{}`".format(report_status(report)),
        "- Schema: `{}`".format(report["schema"]),
        "- Score comparable with normal benchmarks: `no`",
    ]
    if report["mode"] in ("cpu", "wall", "lock"):
        sample_count = report.get("async_samples")
        lines.extend(
            [
                "- Async-profiler samples: `{}`".format(
                    sample_count if sample_count is not None else "unknown"
                ),
                "- `secondaryMetrics.async` in the raw JMH JSON is a file-profiler marker; "
                "its `NaN` Score is expected and is not a missing performance metric.",
            ]
        )
        if sample_count == 0:
            lines.extend(
                [
                    "",
                    "> No matching {} events were observed, so no flame graph was generated.".format(
                        report["mode"]
                    ),
                ]
            )
    for section in (
        gc_report_lines(report["gc_metrics"]),
        artifact_report_lines(report["artifacts"]),
    ):
        if section:
            lines.extend([""] + section)
    return "\n".join(lines) + "\n"


def selected_modes(profile, capture_jfr):
    modes = []
    if profile == "all":
        modes.extend(PROFILE_MODES)
    elif profile != "none":
        modes.append(profile)
    if capture_jfr:
        modes.append("jfr")
    return modes


def mode_report(root, mode):
    directory = root / MODE_DETAILS[mode]["directory"]
    return load_profile_report(directory / "profile-report.json")


def workflow_mode_lines(mode, report):
    details = MODE_DETAILS[mode]
    lines = [
        "### {} diagnostics".format(details["label"]),
        "",
        "- Status: `{}`".format(report_status(report)),
    ]
    if report is None:
        return lines
    if mode in ("cpu", "wall", "lock"):
        sample_count = report.get("async_samples")
        lines.append(
            "- Async-profiler samples: `{}`".format(
                sample_count if sample_count is not None else "unknown"
            )
        )
        if sample_count == 0:
            lines.extend(
                [
                    "",
                    "> No matching {} events were observed, so no flame graph was generated.".format(
                        mode
                    ),
                ]
            )
    if mode == "gc":
        gc_lines = gc_report_lines(report.get("gc_metrics", []))
        if gc_lines:
            gc_lines[0] = "#### GC and allocation metrics"
            lines.extend([""] + gc_lines)
    return lines


def workflow_artifact_name(mode, java, run_id, run_attempt):
    if mode == "jfr":
        prefix = "seatunnel-benchmark-capture-jfr"
    else:
        prefix = "seatunnel-benchmark-profile-{}".format(mode)
    return "{}-java{}-{}-{}".format(prefix, java, run_id, run_attempt)


def render_workflow_summary(
    diagnostics_dir,
    profile,
    capture_jfr,
    target_ref,
    pr_number,
    commit,
    benchmark,
    java,
    jmh_args,
    artifacts_url,
    run_id,
    run_attempt,
):
    """Return the combined GitHub Actions summary for the selected diagnostic modes."""
    modes = selected_modes(profile, capture_jfr)
    reports = {mode: mode_report(diagnostics_dir, mode) for mode in modes}
    target = "PR #{}".format(pr_number) if pr_number else target_ref
    settings = jmh_args or "benchmark annotations"
    lines = [
        "## Benchmark diagnostics",
        "",
        "- Target: `{}` at `{}`".format(target, commit),
        "- Benchmark: `{}`".format(benchmark),
        "- Java: `{}`".format(java),
        "- JMH settings: `{}`".format(settings),
        "- Profiled scores comparable with normal benchmarks: `no`",
    ]
    if any(mode in ("cpu", "wall", "lock") for mode in modes):
        lines.extend(
            [
                "- `secondaryMetrics.async` is a file-profiler marker; its `NaN` Score "
                "is expected.",
            ]
        )
    for mode in modes:
        lines.extend([""] + workflow_mode_lines(mode, reports[mode]))

    lines.extend(
        [
            "",
            "### Downloads",
            "",
            "Download each selected mode independently from the "
            "[run artifacts]({}).".format(artifacts_url),
            "",
            "| Mode | Status | Artifact | Contents |",
            "| --- | --- | --- | --- |",
        ]
    )
    for mode in modes:
        details = MODE_DETAILS[mode]
        directory = diagnostics_dir / details["directory"]
        artifact = (
            workflow_artifact_name(mode, java, run_id, run_attempt)
            if directory.is_dir()
            else "not produced"
        )
        lines.append(
            "| {} | `{}` | `{}` | {} |".format(
                details["label"],
                report_status(reports[mode]),
                artifact,
                details["contents"],
            )
        )
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    if args.report_command == "summary":
        print(
            render_workflow_summary(
                diagnostics_dir=args.diagnostics_dir,
                profile=args.profile,
                capture_jfr=args.capture_jfr == "true",
                target_ref=args.ref,
                pr_number=args.pr_number,
                commit=resolve_commit(args.repository),
                benchmark=args.benchmark,
                java=args.java,
                jmh_args=args.jmh_args,
                artifacts_url=args.artifacts_url,
                run_id=args.run_id,
                run_attempt=args.run_attempt,
            ),
            end="",
        )
        return

    report = build_report(args)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with args.output_json.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")
    args.output_md.write_text(render_markdown(report), encoding="utf-8")


if __name__ == "__main__":
    main()
