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
import tempfile
import unittest

import profile_report


class ProfileReportTest(unittest.TestCase):

    @staticmethod
    def write_profile_report(root, directory, **values):
        report_directory = root / directory
        report_directory.mkdir(parents=True)
        report = {
            "schema": profile_report.SCHEMA,
            "mode": directory,
            "command": "profile",
            "status": 0,
            "gc_metrics": [],
            "artifacts": [],
        }
        report.update(values)
        (report_directory / "profile-report.json").write_text(
            json.dumps(report), encoding="utf-8"
        )

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

    def test_renders_compact_workflow_summary_for_cpu_and_jfr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.write_profile_report(root, "profile-cpu", async_samples=9453)
            self.write_profile_report(
                root, "capture-jfr", mode="jfr", command="capture"
            )

            markdown = profile_report.render_workflow_summary(
                diagnostics_dir=root,
                profile="cpu",
                capture_jfr=True,
                target_ref="dev",
                pr_number="",
                commit="0123456789ab",
                benchmark="IntermediateQueueBenchmark.disruptorRecordHandoff$",
                java="11",
                jmh_args="-f 1 -wi 1 -i 1 -w 1s -r 1s",
                artifacts_url="https://example.test/actions/runs/42#artifacts",
                run_id="42",
                run_attempt="2",
            )

        self.assertIn("Target: `dev` at `0123456789ab`", markdown)
        self.assertIn("Async-profiler samples: `9453`", markdown)
        self.assertLess(
            markdown.index("### CPU diagnostics"),
            markdown.index("### JFR diagnostics"),
        )
        self.assertIn("seatunnel-benchmark-profile-cpu-java11-42-2", markdown)
        self.assertIn("seatunnel-benchmark-capture-jfr-java11-42-2", markdown)
        self.assertIn("https://example.test/actions/runs/42#artifacts", markdown)
        self.assertNotIn("raw/", markdown)

    def test_renders_all_modes_in_order_and_marks_missing_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.write_profile_report(root, "profile-cpu", async_samples=1)
            self.write_profile_report(root, "profile-wall", async_samples=2)
            self.write_profile_report(root, "profile-lock", async_samples=3)
            self.write_profile_report(root, "capture-jfr", mode="jfr")

            markdown = profile_report.render_workflow_summary(
                diagnostics_dir=root,
                profile="all",
                capture_jfr=True,
                target_ref="dev",
                pr_number="12021",
                commit="fedcba987654",
                benchmark="Queue.method$",
                java="8",
                jmh_args="",
                artifacts_url="https://example.test/artifacts",
                run_id="7",
                run_attempt="1",
            )

        headings = [
            "### CPU diagnostics",
            "### Wall diagnostics",
            "### Lock diagnostics",
            "### GC diagnostics",
            "### JFR diagnostics",
        ]
        positions = [markdown.index(heading) for heading in headings]
        self.assertEqual(sorted(positions), positions)
        self.assertIn("Target: `PR #12021` at `fedcba987654`", markdown)
        self.assertIn("| GC | `not produced` | `not produced` |", markdown)


if __name__ == "__main__":
    unittest.main()
