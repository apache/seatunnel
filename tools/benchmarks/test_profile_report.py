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

import pathlib
import tempfile
import unittest

import profile_report


class ProfileReportTest(unittest.TestCase):

    def test_ignores_incomplete_jmh_result(self):
        with tempfile.TemporaryDirectory() as directory:
            result = pathlib.Path(directory) / "result.json"
            result.write_text("[", encoding="utf-8")

            self.assertEqual([], profile_report.load_jmh(result))

    def test_extracts_gc_secondary_metrics(self):
        results = [
            {
                "benchmark": "org.apache.seatunnel.Queue.publish",
                "params": {"capacity": "1024"},
                "secondaryMetrics": {
                    "gc.alloc.rate.norm": {
                        "score": 8.0,
                        "scoreError": 0.1,
                        "scoreUnit": "B/op",
                    },
                    "gc.count": {
                        "score": 2.0,
                        "scoreError": 0.0,
                        "scoreUnit": "counts",
                    },
                    "unrelated": {
                        "score": 1.0,
                        "scoreError": 0.0,
                        "scoreUnit": "x",
                    },
                },
            }
        ]

        metrics = profile_report.gc_metrics(results)

        self.assertEqual(1, len(metrics))
        self.assertEqual(8.0, metrics[0]["metrics"]["gc.alloc.rate.norm"]["value"])
        self.assertNotIn("unrelated", metrics[0]["metrics"])

    def test_builds_artifact_list(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            raw = root / "raw" / "benchmark"
            raw.mkdir(parents=True)
            (raw / "profile.jfr").write_bytes(b"jfr")
            artifacts = profile_report.artifacts(root, ())

            self.assertEqual(
                ["raw/benchmark/profile.jfr"],
                [artifact["path"] for artifact in artifacts],
            )

    def test_renders_diagnostic_warning(self):
        markdown = profile_report.render_markdown(
            {
                "mode": "cpu",
                "command": "profile",
                "status": 0,
                "schema": profile_report.SCHEMA,
                "gc_metrics": [],
                "artifacts": [],
            }
        )

        self.assertIn("Score comparable with normal benchmarks: `no`", markdown)
        self.assertIn("No diagnostic artifacts were produced", markdown)

    def test_reports_async_sample_count_and_nan_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            summary = root / "raw" / "benchmark" / "summary-cpu.txt"
            summary.parent.mkdir(parents=True)
            summary.write_text(
                "--- Execution profile ---\nTotal samples       : 42\n",
                encoding="utf-8",
            )

            self.assertEqual(42, profile_report.async_sample_count(root))

        markdown = profile_report.render_markdown(
            {
                "mode": "cpu",
                "command": "profile",
                "status": 0,
                "schema": profile_report.SCHEMA,
                "async_samples": 42,
                "gc_metrics": [],
                "artifacts": [],
            }
        )
        self.assertIn("Async-profiler samples: `42`", markdown)
        self.assertIn("its `NaN` Score is expected", markdown)

    def test_explains_gc_metrics(self):
        markdown = "\n".join(
            profile_report.gc_report_lines(
                [
                    {
                        "benchmark": "org.apache.seatunnel.Queue.publish",
                        "params": {},
                        "metrics": {},
                    }
                ]
            )
        )

        self.assertIn("Alloc/op (B/op)", markdown)
        self.assertIn("high value alone does not prove a regression", markdown)
        self.assertIn("not normalized per operation", markdown)
        self.assertIn("not an exact stop-the-world pause-time measurement", markdown)
        self.assertIn("Capture JFR separately", markdown)


if __name__ == "__main__":
    unittest.main()
