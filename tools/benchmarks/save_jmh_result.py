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

"""Save JMH and Zeta pipeline results in a stable, versioned JSON format."""

import argparse
import datetime
import json
import math
import pathlib
import statistics


SCHEMA_VERSION = 1
PIPELINE_METRICS = (
    ("throughput_rows_per_second", "rows/s", "higher"),
    ("event_time_latency_p50_ms", "ms", "lower"),
    ("event_time_latency_p95_ms", "ms", "lower"),
    ("event_time_latency_p99_ms", "ms", "lower"),
    ("event_time_latency_max_ms", "ms", "lower"),
    ("latency_growth_ratio", "ratio", "lower"),
)
PERCENTILE_FIELDS = (
    "event_time_latency_p50_ms",
    "event_time_latency_p95_ms",
    "event_time_latency_p99_ms",
    "first_half_p99_ms",
    "second_half_p99_ms",
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jmh", required=True, type=pathlib.Path)
    parser.add_argument("--pipeline-dir", type=pathlib.Path)
    parser.add_argument("--output-json", required=True, type=pathlib.Path)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--java", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--runner-os")
    parser.add_argument("--runner-arch")
    parser.add_argument("--runner-name")
    parser.add_argument("--runner-image")
    parser.add_argument("--kernel")
    parser.add_argument("--cpu-model")
    parser.add_argument("--cpu-count", type=int)
    parser.add_argument("--memory-kib", type=int)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--suite")
    parser.add_argument("--timestamp")
    return parser.parse_args()


def finite_or_none(value):
    if value is None:
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def flatten(values):
    return [value for fork in values for value in fork]


def benchmark_name(result):
    params = result.get("params", {})
    suffix = ",".join("{}={}".format(key, params[key]) for key in sorted(params))
    return result["benchmark"] + ("[{}]".format(suffix) if suffix else "")


def jmh_metrics(results):
    metrics = []
    for result in results:
        primary = result["primaryMetric"]
        score = finite_or_none(primary.get("score"))
        error = finite_or_none(primary.get("scoreError"))
        samples = flatten(primary.get("rawData", []))
        metrics.append(
            {
                "name": benchmark_name(result),
                "benchmark": result["benchmark"],
                "kind": "jmh",
                "value": score,
                "score_error": error,
                "sample_standard_deviation": sample_standard_deviation(samples),
                "relative_score_error": (
                    error / abs(score) if score not in (None, 0.0) and error is not None else None
                ),
                "unit": primary["scoreUnit"],
                "direction": "higher" if result["mode"] == "thrpt" else "lower",
                "mode": result["mode"],
                "params": result.get("params", {}),
                "forks": result["forks"],
                "samples": samples,
            }
        )
    return metrics


def pipeline_name(run_id):
    parts = run_id.rsplit("-", 2)
    if len(parts) != 3 or not parts[0] or not parts[1].isdigit() or not parts[2].isdigit():
        raise ValueError("Invalid pipeline run id: {}".format(run_id))
    return parts[0]


def pipeline_params(sample):
    field_names = (
        ("offered_rate_rows_per_second", "offeredRatePerSecond"),
        ("parallelism", "parallelism"),
        ("payload_size", "payloadSize"),
        ("transform_operations", "transformOperations"),
    )
    return {output: sample[field] for field, output in field_names if field in sample}


def pipeline_metric_prefix(name, params):
    suffix = ",".join("{}={}".format(key, params[key]) for key in sorted(params))
    return name + ("[{}]".format(suffix) if suffix else "")


def sample_standard_deviation(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


def percentile_is_clamped(sample, field):
    if not sample.get("latency_percentiles_clamped", False):
        return False
    overflow_bucket = max(sample[name] for name in PERCENTILE_FIELDS)
    return sample[field] == overflow_bucket


def median_percentile_is_clamped(samples, field):
    clamped_samples = sum(percentile_is_clamped(sample, field) for sample in samples)
    return clamped_samples > len(samples) / 2


def pipeline_metrics(pipeline_dir):
    if pipeline_dir is None or not pipeline_dir.is_dir():
        return [], {}

    grouped = {}
    for result_file in sorted(pipeline_dir.glob("*.json")):
        with result_file.open(encoding="utf-8") as handle:
            sample = json.load(handle)
        name = pipeline_name(sample["run_id"])
        params = pipeline_params(sample)
        grouped.setdefault((name, tuple(sorted(params.items()))), []).append(sample)

    metrics = []
    correctness = {}
    for (name, params_tuple), samples in sorted(grouped.items()):
        params = dict(params_tuple)
        metric_prefix = pipeline_metric_prefix(name, params)
        complete = [sample["processed_rows"] == sample["expected_rows"] for sample in samples]
        correctness[metric_prefix] = {
            "pipeline": name,
            "params": params,
            "sample_count": len(samples),
            "complete_samples": sum(complete),
            "sustainable_samples": sum(bool(sample["sustainable"]) for sample in samples),
            "latency_percentiles_clamped_samples": sum(
                bool(sample.get("latency_percentiles_clamped", False)) for sample in samples
            ),
            "latency_overflow_rows": sum(sample["latency_overflow_rows"] for sample in samples),
        }
        for field, unit, direction in PIPELINE_METRICS:
            values = [float(sample[field]) for sample in samples]
            metric = {
                "name": "{}.{}".format(metric_prefix, field),
                "pipeline": name,
                "metric": field,
                "kind": "pipeline",
                "value": statistics.median(values),
                "score_error": None,
                "sample_standard_deviation": sample_standard_deviation(values),
                "relative_score_error": None,
                "unit": unit,
                "direction": direction,
                "mode": "median",
                "params": params,
                "forks": None,
                "samples": values,
            }
            if field in PERCENTILE_FIELDS:
                metric["clamped"] = median_percentile_is_clamped(samples, field)
            metrics.append(metric)
    return metrics, correctness


def environment_metadata(jmh_results, args):
    first = jmh_results[0] if jmh_results else {}
    metadata = {
        "name": args.environment,
        "java_requested": args.java,
        "jdk_version": first.get("jdkVersion"),
        "vm_name": first.get("vmName"),
        "vm_version": first.get("vmVersion"),
        "jvm": first.get("jvm"),
        "jvm_args": first.get("jvmArgs", []),
        "jmh_version": first.get("jmhVersion"),
        "runner_os": args.runner_os,
        "runner_arch": args.runner_arch,
        "runner_name": args.runner_name,
        "runner_image": args.runner_image,
        "kernel": args.kernel,
        "cpu_model": args.cpu_model,
        "cpu_count": args.cpu_count,
        "memory_kib": args.memory_kib,
    }
    return {key: value for key, value in metadata.items() if value is not None}


def main():
    args = parse_args()
    with args.jmh.open(encoding="utf-8") as handle:
        jmh_results = json.load(handle)

    pipeline, correctness = pipeline_metrics(args.pipeline_dir)
    timestamp = args.timestamp or datetime.datetime.now(datetime.timezone.utc).isoformat()
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": timestamp,
        "source": {
            "ref": args.ref,
            "commit": args.commit,
            "run_id": args.run_id,
            "suite": args.suite,
        },
        "environment": environment_metadata(jmh_results, args),
        "metrics": jmh_metrics(jmh_results) + pipeline,
        "pipeline_correctness": correctness,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    with args.output_json.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
