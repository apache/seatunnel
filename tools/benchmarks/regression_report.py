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

"""Render normalized SeaTunnel benchmark results as a Markdown report."""

import argparse
import json
import pathlib
import statistics


PIPELINE_COLUMNS = (
    ("throughput_rows_per_second", "Throughput", "rows/s"),
    ("event_time_latency_p50_ms", "P50", "ms"),
    ("event_time_latency_p95_ms", "P95", "ms"),
    ("event_time_latency_p99_ms", "P99", "ms"),
    ("event_time_latency_max_ms", "Max", "ms"),
    ("latency_growth_ratio", "Growth", "ratio"),
)
PERCENTILE_METRICS = {
    "event_time_latency_p50_ms",
    "event_time_latency_p95_ms",
    "event_time_latency_p99_ms",
}
PARAMETER_ORDER = (
    "offeredRatePerSecond",
    "parallelism",
    "payloadSize",
    "transformOperations",
    "rowCount",
)
PARAMETER_LABELS = {
    "offeredRatePerSecond": "rate",
    "parallelism": "p",
    "payloadSize": "payload",
    "transformOperations": "work",
    "rowCount": "rows",
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, action="append", type=pathlib.Path)
    parser.add_argument("--baseline", action="append", default=[], type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    return parser.parse_args()


def format_number(value):
    if value is None:
        return "n/a"
    absolute = abs(value)
    if absolute >= 1000:
        return "{:,.2f}".format(value)
    if absolute >= 1:
        return "{:.3f}".format(value)
    return "{:.6f}".format(value)


def format_pipeline_number(field, value, clamped=False):
    if value is None:
        return "n/a"
    if field in PERCENTILE_METRICS and clamped:
        return ">{:,.0f}".format(value - 1)
    if field.startswith("event_time_latency_"):
        return "{:,.0f}".format(value)
    if field == "throughput_rows_per_second":
        return "{:,.2f}".format(value)
    if field == "latency_growth_ratio":
        return "{:.3f}".format(value)
    return format_number(value)


def load_report(path):
    with path.open(encoding="utf-8") as handle:
        report = json.load(handle)
    if report.get("schema_version") != 1:
        raise ValueError("Unsupported benchmark report schema in {}".format(path))
    return report


def format_percent(value, signed=True):
    if value is None:
        return "n/a"
    if abs(value) < 0.005:
        value = 0.0
    return ("{:+.2f}%" if signed else "{:.2f}%").format(value)


def compact_rate(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return str(value)
    if number and number % 1_000_000 == 0:
        return "{}m".format(number // 1_000_000)
    if number and number % 1_000 == 0:
        return "{}k".format(number // 1_000)
    return str(number)


def compact_params(params, omitted=()):
    ordered = [name for name in PARAMETER_ORDER if name in params]
    ordered.extend(sorted(set(params) - set(ordered)))
    values = []
    for name in ordered:
        if name in omitted:
            continue
        value = params[name]
        if name == "offeredRatePerSecond":
            value = compact_rate(value)
        values.append("{}={}".format(PARAMETER_LABELS.get(name, name), value))
    return ", ".join(values) or "default"


def short_benchmark_name(metric):
    benchmark = metric.get("benchmark") or metric["name"].split("[", 1)[0]
    parts = benchmark.rsplit(".", 2)
    if len(parts) < 2:
        return benchmark
    class_name, method_name = parts[-2:]
    class_name = {
        "SeaTunnelPipelineBenchmark": "Pipeline",
        "SeaTunnelRowBenchmark": "SeaTunnelRow",
    }.get(class_name, class_name[:-9] if class_name.endswith("Benchmark") else class_name)
    return "{}.{}".format(class_name, method_name)


def parameter_sort_value(value):
    try:
        return 0, int(value)
    except (TypeError, ValueError):
        try:
            return 1, float(value)
        except (TypeError, ValueError):
            return 2, str(value)


def jmh_sort_key(metric):
    params = metric.get("params", {})
    ordered = tuple(
        parameter_sort_value(params.get(name, "")) for name in PARAMETER_ORDER
    )
    return short_benchmark_name(metric), ordered


def relative_error(metric):
    value = metric.get("value")
    relative = metric.get("relative_score_error")
    if relative is None and value not in (None, 0.0) and metric.get("score_error") is not None:
        relative = metric["score_error"] / abs(value)
    return None if relative is None else relative * 100.0


def coefficient_of_variation(metric):
    value = metric.get("value")
    deviation = metric.get("sample_standard_deviation")
    if value in (None, 0.0) or deviation is None:
        return None
    return deviation / abs(value) * 100.0


def pipeline_identity(metric):
    prefix, _, field = metric["name"].rpartition(".")
    pipeline = metric.get("pipeline") or prefix.split("[", 1)[0]
    return pipeline, metric.get("metric") or field, metric.get("params", {})


def canonical_params(params):
    return tuple(sorted((name, str(value)) for name, value in params.items()))


def pipeline_correctness_key(pipeline, params):
    suffix = ",".join("{}={}".format(name, params[name]) for name in sorted(params))
    return pipeline + ("[{}]".format(suffix) if suffix else "")


def find_correctness(report, pipeline, params):
    correctness = report.get("pipeline_correctness", {})
    direct = correctness.get(pipeline_correctness_key(pipeline, params))
    if direct is not None:
        return direct
    expected = canonical_params(params)
    for key, values in correctness.items():
        value_pipeline = values.get("pipeline") or key.split("[", 1)[0]
        value_params = values.get("params")
        if value_params is not None and value_pipeline == pipeline:
            if canonical_params(value_params) == expected:
                return values
    return None


def validity_text(values):
    if not values:
        return "n/a"
    samples = values["sample_count"]
    complete = values["complete_samples"]
    sustainable = values["sustainable_samples"]
    overflow = values["latency_overflow_rows"]
    if complete == samples and sustainable == samples and overflow == 0:
        return "✅ {}/{}".format(samples, samples)
    return "C {}/{} · S {}/{} · O {}".format(
        complete, samples, sustainable, samples, overflow
    )


def pipeline_groups(metrics):
    groups = {}
    for metric in sorted(metrics, key=jmh_sort_key):
        pipeline, field, params = pipeline_identity(metric)
        group_params = {name: value for name, value in params.items() if name != "payloadSize"}
        key = (pipeline, canonical_params(group_params))
        payload = params.get("payloadSize", "default")
        row = groups.setdefault(key, {}).setdefault(payload, {"params": params})
        row[field] = metric
    return groups


def payload_sort_key(payload):
    try:
        return 0, int(payload)
    except (TypeError, ValueError):
        return 1, str(payload)


def jmh_report_lines(metrics):
    if not metrics:
        return []
    lines = [
        "### JMH results",
        "",
        "- `Score`: JMH's estimate in the displayed unit.",
        "- `Error`: the confidence-interval half-width as a percentage of Score; the interval is "
        "approximately `Score × (1 ± Error%)`.",
        "- `CV`: sample standard deviation divided by the sample mean; lower values indicate "
        "more stable samples.",
        "",
        "| Benchmark | Parameters | Score | Error | CV | Unit |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for metric in metrics:
        lines.append(
            "| `{}` | `{}` | {} | {} | {} | {} |".format(
                short_benchmark_name(metric),
                compact_params(metric.get("params", {})),
                format_number(metric["value"]),
                format_percent(relative_error(metric), signed=False),
                format_percent(coefficient_of_variation(metric), signed=False),
                metric["unit"],
            )
        )
    return lines


def pipeline_report_lines(report, metrics):
    groups = pipeline_groups(metrics)
    if not groups:
        return []
    lines = [
        "### Pipeline results",
        "",
        "Pipeline values are medians. Detailed samples and standard deviations remain in the "
        "JSON artifact.",
        "",
        "- `Payload`: characters per row.",
        "- `Growth`: unitless `(second-half P99 + 1) / (first-half P99 + 1)` ratio.",
        "- Percentiles beyond the measurable range are shown as lower bounds, for example "
        "`>60,000`.",
        "- `Valid`: measurement samples only; warmup samples are excluded. `✅ x/y` means all "
        "samples are complete, sustainable, and have no latency overflow. Otherwise `C/S/O` "
        "show complete samples, sustainable samples, and overflow rows.",
    ]
    for (pipeline, group_params_tuple), rows in sorted(groups.items()):
        group_params = dict(group_params_tuple)
        lines.extend(
            [
                "",
                "#### `{}`".format(pipeline),
                "",
                "Parameters: `{}`".format(compact_params(group_params)),
                "",
                "| Payload (chars) | Throughput (rows/s) | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) | Growth (ratio) | Valid (samples) |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for payload, row in sorted(rows.items(), key=lambda item: payload_sort_key(item[0])):
            values = []
            for field, _, _ in PIPELINE_COLUMNS:
                metric = row.get(field)
                values.append(
                    format_pipeline_number(
                        field, metric["value"], metric.get("clamped", False)
                    )
                    if metric
                    else "n/a"
                )
            validity = validity_text(find_correctness(report, pipeline, row["params"]))
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    payload, *values, validity
                )
            )
    return lines


def report_lines(report):
    environment = report["environment"]
    lines = [
        "## SeaTunnel benchmark report",
        "",
        "- Ref: `{}`".format(report["source"]["ref"]),
        "- Commit: `{}`".format(report["source"]["commit"]),
        "- Suite: `{}`".format(report["source"].get("suite") or "custom"),
        "- Java: `{}`".format(
            environment.get("jdk_version", environment["java_requested"])
        ),
        "- Environment: `{}`".format(environment["name"]),
        "- Runner image: `{}`".format(environment.get("runner_image", "unknown")),
        "- CPU: `{}`".format(environment.get("cpu_model", "unknown")),
        "",
        "> This report is observational. GitHub-hosted runner performance varies between hosts, "
        "so a single run is not a performance regression gate.",
    ]
    jmh = [metric for metric in report["metrics"] if metric["kind"] == "jmh"]
    pipeline = [metric for metric in report["metrics"] if metric["kind"] == "pipeline"]
    for section in (jmh_report_lines(jmh), pipeline_report_lines(report, pipeline)):
        if section:
            lines.extend([""] + section)
    return lines


def metric_index(reports):
    indexed = {}
    for report in reports:
        for metric in report["metrics"]:
            indexed.setdefault(metric["name"], []).append(metric)
    return indexed


def convert_unit(value, source_unit, target_unit):
    if value is None or source_unit == target_unit:
        return value
    rates = {"ops/s": 1.0, "ops/ms": 1_000.0, "ops/us": 1_000_000.0, "ops/ns": 1_000_000_000.0}
    if source_unit in rates and target_unit in rates:
        return value * rates[source_unit] / rates[target_unit]
    raise ValueError("Cannot compare {} with {}".format(source_unit, target_unit))


def median_value(metrics, target_unit=None):
    values = [
        convert_unit(metric["value"], metric["unit"], target_unit or metric["unit"])
        for metric in metrics
        if metric["value"] is not None
    ]
    return statistics.median(values) if values else None


def adjusted_change(baseline, candidate, direction):
    if baseline in (None, 0.0) or candidate is None:
        return None
    raw = (candidate / baseline - 1.0) * 100.0
    return -raw if direction == "lower" else raw


def source_summary(reports):
    refs = list(dict.fromkeys(report["source"]["ref"] for report in reports))
    commits = list(dict.fromkeys(report["source"]["commit"] for report in reports))
    return ", ".join(refs), ", ".join(commits)


def jmh_comparison_lines(baselines, candidates):
    baseline_metrics = metric_index(baselines)
    candidate_metrics = metric_index(candidates)
    names = sorted(
        name
        for name in set(baseline_metrics) | set(candidate_metrics)
        if (candidate_metrics.get(name) or baseline_metrics[name])[0]["kind"] == "jmh"
    )
    if not names:
        return []
    lines = [
        "### JMH comparison",
        "",
        "| Benchmark | Parameters | Baseline | Candidate | Change | Unit |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    names.sort(
        key=lambda name: jmh_sort_key(
            (candidate_metrics.get(name) or baseline_metrics[name])[0]
        )
    )
    for name in names:
        baseline_group = baseline_metrics.get(name, [])
        candidate_group = candidate_metrics.get(name, [])
        metric = (candidate_group or baseline_group)[0]
        target_unit = (candidate_group or baseline_group)[0]["unit"]
        baseline = median_value(baseline_group, target_unit)
        candidate = median_value(candidate_group, target_unit)
        lines.append(
            "| `{}` | `{}` | {} | {} | {} | {} |".format(
                short_benchmark_name(metric),
                compact_params(metric.get("params", {})),
                format_number(baseline),
                format_number(candidate),
                format_percent(adjusted_change(baseline, candidate, metric["direction"])),
                target_unit,
            )
        )
    return lines


def pipeline_metric_index(reports):
    indexed = {}
    for report in reports:
        for metric in report["metrics"]:
            if metric["kind"] != "pipeline":
                continue
            pipeline, field, params = pipeline_identity(metric)
            key = (pipeline, canonical_params(params), field)
            indexed.setdefault(key, []).append(metric)
    return indexed


def aggregate_correctness(reports, pipeline, params):
    aggregated = {
        "sample_count": 0,
        "complete_samples": 0,
        "sustainable_samples": 0,
        "latency_percentiles_clamped_samples": 0,
        "latency_overflow_rows": 0,
    }
    found = False
    for report in reports:
        values = find_correctness(report, pipeline, params)
        if not values:
            continue
        found = True
        for field in aggregated:
            aggregated[field] += values.get(field, 0)
    return aggregated if found else None


def median_metric_is_clamped(metrics):
    return sum(bool(metric.get("clamped", False)) for metric in metrics) > len(metrics) / 2


def metric_comparison_cell(field, baseline_metrics, candidate_metrics):
    if not baseline_metrics and not candidate_metrics:
        return "n/a"
    metric = (candidate_metrics or baseline_metrics)[0]
    target_unit = metric["unit"]
    baseline = median_value(baseline_metrics, target_unit)
    candidate = median_value(candidate_metrics, target_unit)
    change = adjusted_change(baseline, candidate, metric["direction"])
    return "{} → {} ({})".format(
        format_pipeline_number(
            field, baseline, median_metric_is_clamped(baseline_metrics)
        ),
        format_pipeline_number(
            field, candidate, median_metric_is_clamped(candidate_metrics)
        ),
        format_percent(change),
    )


def pipeline_comparison_lines(baselines, candidates):
    baseline_index = pipeline_metric_index(baselines)
    candidate_index = pipeline_metric_index(candidates)
    keys = set(baseline_index) | set(candidate_index)
    groups = {}
    for pipeline, params_tuple, field in keys:
        params = dict(params_tuple)
        group_params = {name: value for name, value in params.items() if name != "payloadSize"}
        group_key = (pipeline, canonical_params(group_params))
        payload = params.get("payloadSize", "default")
        groups.setdefault(group_key, {}).setdefault(payload, {"params": params})[field] = (
            baseline_index.get((pipeline, params_tuple, field), []),
            candidate_index.get((pipeline, params_tuple, field), []),
        )
    if not groups:
        return []
    lines = [
        "### Pipeline comparison",
        "",
        "Cells show `baseline → candidate (adjusted change)`; positive change is favorable.",
        "`Payload` is characters per row and `Growth` is a unitless ratio. `Valid` includes "
        "measurement samples only; warmup samples are excluded.",
        "Percentiles beyond the measurable range are shown as lower bounds, for example "
        "`>60,000`.",
    ]
    for (pipeline, group_params_tuple), rows in sorted(groups.items()):
        group_params = dict(group_params_tuple)
        lines.extend(
            [
                "",
                "#### `{}`".format(pipeline),
                "",
                "Parameters: `{}`".format(compact_params(group_params)),
                "",
                "| Payload (chars) | Throughput (rows/s) | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) | Growth (ratio) | Valid (samples) |",
                "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for payload, row in sorted(rows.items(), key=lambda item: payload_sort_key(item[0])):
            cells = []
            for field, _, _ in PIPELINE_COLUMNS:
                baseline_group, candidate_group = row.get(field, ([], []))
                cells.append(metric_comparison_cell(field, baseline_group, candidate_group))
            baseline_validity = validity_text(
                aggregate_correctness(baselines, pipeline, row["params"])
            )
            candidate_validity = validity_text(
                aggregate_correctness(candidates, pipeline, row["params"])
            )
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} → {} |".format(
                    payload, *cells, baseline_validity, candidate_validity
                )
            )
    return lines


def comparison_lines(baselines, candidates):
    baseline_ref, baseline_commit = source_summary(baselines)
    candidate_ref, candidate_commit = source_summary(candidates)
    environment = candidates[0]["environment"]
    suite = candidates[0]["source"].get("suite") or "custom"
    lines = [
        "## SeaTunnel benchmark comparison",
        "",
        "- Baseline: `{}` at `{}` ({} runs)".format(
            baseline_ref, baseline_commit, len(baselines)
        ),
        "- Candidate: `{}` at `{}` ({} runs)".format(
            candidate_ref, candidate_commit, len(candidates)
        ),
        "- Suite: `{}`".format(suite),
        "- Java: `{}`".format(
            environment.get("jdk_version", environment["java_requested"])
        ),
        "- Runner image: `{}`".format(environment.get("runner_image", "unknown")),
        "- CPU: `{}`".format(environment.get("cpu_model", "unknown")),
        "",
        "> Baseline and candidate ran alternately on the same worker. Positive adjusted change is "
        "favorable, but this observational report does not enforce a regression threshold.",
    ]
    for section in (
        jmh_comparison_lines(baselines, candidates),
        pipeline_comparison_lines(baselines, candidates),
    ):
        if section:
            lines.extend([""] + section)
    return lines


def main():
    args = parse_args()
    reports = [load_report(path) for path in args.input]
    baselines = [load_report(path) for path in args.baseline]
    if baselines and not reports:
        raise ValueError("Candidate benchmark reports are required for comparison")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    lines = comparison_lines(baselines, reports) if baselines else report_lines(reports[0])
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
