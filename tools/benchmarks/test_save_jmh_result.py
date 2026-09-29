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

import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import save_jmh_result


class SaveJmhResultTest(unittest.TestCase):

    def test_normalizes_jmh_samples_and_uncertainty(self):
        metrics = save_jmh_result.jmh_metrics(
            [
                {
                    "benchmark": "org.apache.seatunnel.Queue.publish",
                    "mode": "thrpt",
                    "forks": 2,
                    "params": {"workers": "2", "capacity": "1024"},
                    "primaryMetric": {
                        "score": 100.0,
                        "scoreError": 5.0,
                        "scoreUnit": "ops/s",
                        "rawData": [[90.0, 110.0], [100.0]],
                    },
                }
            ]
        )

        self.assertEqual(1, len(metrics))
        metric = metrics[0]
        self.assertEqual(
            "org.apache.seatunnel.Queue.publish[capacity=1024,workers=2]",
            metric["name"],
        )
        self.assertEqual([90.0, 110.0, 100.0], metric["samples"])
        self.assertEqual(10.0, metric["sample_standard_deviation"])
        self.assertEqual(0.05, metric["relative_score_error"])
        self.assertEqual("higher", metric["direction"])

    def test_aggregates_pipeline_medians_correctness_and_clamping(self):
        with tempfile.TemporaryDirectory() as directory:
            pipeline_dir = pathlib.Path(directory)
            samples = [
                self.pipeline_sample("sourceSink-0-0", 1000.0, True, True, 1),
                self.pipeline_sample("sourceSink-0-1", 1200.0, False, False, 2),
            ]
            for index, sample in enumerate(samples):
                (pipeline_dir / "{}.json".format(index)).write_text(
                    json.dumps(sample), encoding="utf-8"
                )

            metrics, correctness = save_jmh_result.pipeline_metrics(pipeline_dir)

        throughput = next(
            metric
            for metric in metrics
            if metric["metric"] == "throughput_rows_per_second"
        )
        p50 = next(
            metric for metric in metrics if metric["metric"] == "event_time_latency_p50_ms"
        )
        p99 = next(
            metric for metric in metrics if metric["metric"] == "event_time_latency_p99_ms"
        )
        self.assertEqual(1100.0, throughput["value"])
        self.assertFalse(p50["clamped"])
        self.assertTrue(p99["clamped"])

        values = next(iter(correctness.values()))
        self.assertEqual(2, values["sample_count"])
        self.assertEqual(1, values["complete_samples"])
        self.assertEqual(1, values["sustainable_samples"])
        self.assertEqual(2, values["latency_percentiles_clamped_samples"])
        self.assertEqual(3, values["latency_overflow_rows"])

    def test_main_writes_versioned_report_and_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            jmh = root / "jmh.json"
            output = root / "nested" / "report.json"
            jmh.write_text(
                json.dumps(
                    [
                        {
                            "benchmark": "org.apache.seatunnel.Queue.publish",
                            "mode": "thrpt",
                            "forks": 1,
                            "params": {},
                            "jdkVersion": "1.8.0_472",
                            "vmName": "OpenJDK 64-Bit Server VM",
                            "jmhVersion": "1.37",
                            "primaryMetric": {
                                "score": 10.0,
                                "scoreError": 1.0,
                                "scoreUnit": "ops/s",
                                "rawData": [[10.0]],
                            },
                        }
                    ]
                ),
                encoding="utf-8",
            )
            arguments = [
                "save_jmh_result.py",
                "--jmh",
                str(jmh),
                "--output-json",
                str(output),
                "--ref",
                "dev",
                "--commit",
                "abc123",
                "--java",
                "8",
                "--environment",
                "local",
                "--run-id",
                "42",
                "--timestamp",
                "2026-08-31T00:00:00+00:00",
            ]

            with mock.patch.object(sys, "argv", arguments):
                save_jmh_result.main()

            report = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(save_jmh_result.SCHEMA_VERSION, report["schema_version"])
        self.assertEqual("2026-08-31T00:00:00+00:00", report["generated_at"])
        self.assertEqual("abc123", report["source"]["commit"])
        self.assertEqual("1.8.0_472", report["environment"]["jdk_version"])
        self.assertEqual(1, len(report["metrics"]))

    @staticmethod
    def pipeline_sample(run_id, throughput, complete, sustainable, overflow_rows):
        expected_rows = 100
        return {
            "run_id": run_id,
            "offered_rate_rows_per_second": 1000,
            "parallelism": 2,
            "payload_size": 256,
            "transform_operations": 64,
            "processed_rows": expected_rows if complete else expected_rows - 1,
            "expected_rows": expected_rows,
            "sustainable": sustainable,
            "latency_percentiles_clamped": True,
            "latency_overflow_rows": overflow_rows,
            "throughput_rows_per_second": throughput,
            "event_time_latency_p50_ms": 10.0,
            "event_time_latency_p95_ms": 100.0,
            "event_time_latency_p99_ms": 60001.0,
            "event_time_latency_max_ms": 70000.0,
            "first_half_p99_ms": 500.0,
            "second_half_p99_ms": 60001.0,
            "latency_growth_ratio": 2.0,
        }


if __name__ == "__main__":
    unittest.main()
