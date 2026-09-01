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

import regression_report


class RegressionReportTest(unittest.TestCase):

    def test_rejects_unknown_report_schema(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "report.json"
            path.write_text(json.dumps({"schema_version": 2}), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Unsupported benchmark report schema"):
                regression_report.load_report(path)

    def test_jmh_comparison_uses_medians_and_converts_units(self):
        baselines = [
            self.report("dev", self.jmh_metric(1000.0, "ops/s")),
            self.report("dev", self.jmh_metric(1.0, "ops/ms")),
        ]
        candidates = [
            self.report("pr", self.jmh_metric(1.1, "ops/ms")),
            self.report("pr", self.jmh_metric(1100.0, "ops/s")),
        ]

        markdown = "\n".join(
            regression_report.jmh_comparison_lines(baselines, candidates)
        )

        self.assertIn("1.000", markdown)
        self.assertIn("1.100", markdown)
        self.assertIn("+10.00%", markdown)
        self.assertIn("ops/ms", markdown)

    def test_lower_is_better_change_is_reported_as_positive(self):
        metric = self.jmh_metric(10.0, "ms/op", direction="lower")
        baseline = self.report("dev", metric)
        candidate = self.report(
            "pr", self.jmh_metric(8.0, "ms/op", direction="lower")
        )

        markdown = "\n".join(
            regression_report.jmh_comparison_lines([baseline], [candidate])
        )

        self.assertIn("+20.00%", markdown)

    def test_report_explains_score_error_and_cv(self):
        metric = self.jmh_metric(100.0, "ops/s")
        metric.update({"score_error": 5.0, "sample_standard_deviation": 10.0})

        markdown = "\n".join(regression_report.jmh_report_lines([metric]))

        self.assertIn("confidence-interval half-width", markdown)
        self.assertIn("sample standard deviation", markdown)
        self.assertIn("5.00%", markdown)
        self.assertIn("10.00%", markdown)

    def test_main_writes_markdown_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = root / "report.json"
            output = root / "nested" / "report.md"
            source.write_text(
                json.dumps(self.report("dev", self.jmh_metric(100.0, "ops/s"))),
                encoding="utf-8",
            )
            arguments = [
                "regression_report.py",
                "--input",
                str(source),
                "--output",
                str(output),
            ]

            with mock.patch.object(sys, "argv", arguments):
                regression_report.main()

            markdown = output.read_text(encoding="utf-8")

        self.assertIn("## SeaTunnel benchmark report", markdown)
        self.assertIn("Queue.publish", markdown)
        self.assertTrue(markdown.endswith("\n"))

    @staticmethod
    def jmh_metric(value, unit, direction="higher"):
        return {
            "name": "org.apache.seatunnel.QueueBenchmark.publish[capacity=1024]",
            "benchmark": "org.apache.seatunnel.QueueBenchmark.publish",
            "kind": "jmh",
            "value": value,
            "score_error": None,
            "sample_standard_deviation": None,
            "relative_score_error": None,
            "unit": unit,
            "direction": direction,
            "mode": "thrpt" if direction == "higher" else "avgt",
            "params": {"capacity": "1024"},
            "forks": 1,
            "samples": [value],
        }

    @staticmethod
    def report(ref, metric):
        return {
            "schema_version": 1,
            "generated_at": "2026-08-31T00:00:00+00:00",
            "source": {
                "ref": ref,
                "commit": "{}-commit".format(ref),
                "run_id": "42",
                "suite": "test",
            },
            "environment": {
                "name": "local",
                "java_requested": "8",
                "jdk_version": "1.8.0_472",
            },
            "metrics": [metric],
            "pipeline_correctness": {},
        }


if __name__ == "__main__":
    unittest.main()
