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

import os
import pathlib
import subprocess
import tempfile
import unittest


SCRIPT = pathlib.Path(__file__).with_name("run_benchmarks.sh")
SUITE = pathlib.Path(__file__).with_name("suites") / "benchmarks_core.txt"
REPOSITORY = pathlib.Path(__file__).parents[2]
WORKFLOW = REPOSITORY / ".github" / "workflows" / "benchmarks.yml"


class RunBenchmarksTest(unittest.TestCase):

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.workspace = pathlib.Path(self.temporary_directory.name)
        (self.workspace / "baseline").mkdir()
        self.environment = os.environ.copy()
        for name in (
            "BENCHMARKS",
            "BENCHMARK_SUITE",
            "CUSTOM_BENCHMARKS",
            "PR_NUMBER",
        ):
            self.environment.pop(name, None)
        self.environment["GITHUB_WORKSPACE"] = str(self.workspace)

    def tearDown(self):
        self.temporary_directory.cleanup()

    def resolve(self, **overrides):
        environment = self.environment.copy()
        environment.update(overrides)
        result = subprocess.run(
            ["bash", str(SCRIPT), "--print-selection"],
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        selection = dict(
            line.split("=", 1) for line in result.stdout.splitlines() if "=" in line
        )
        return result, selection

    def test_default_dispatch_resolves_core_suite(self):
        result, selection = self.resolve()

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("suite", selection["benchmark_selection_source"])
        self.assertEqual("benchmarks_core", selection["benchmark_suite"])
        self.assertEqual("benchmarks_core", selection["benchmark_selector"])
        self.assertEqual(self.core_regex(), selection["benchmark_regex"])

    def test_scheduled_suite_overrides_the_unused_choice_selector(self):
        result, selection = self.resolve(
            BENCHMARK_SUITE="benchmarks_core", BENCHMARKS=".*"
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("suite", selection["benchmark_selection_source"])
        self.assertEqual("benchmarks_core", selection["benchmark_suite"])
        self.assertEqual(self.core_regex(), selection["benchmark_regex"])

    def test_manual_custom_selector_disables_the_suite(self):
        result, selection = self.resolve(
            BENCHMARK_SUITE="benchmarks_core",
            BENCHMARKS="IMapJobStorageBenchmark",
            CUSTOM_BENCHMARKS="IMapWalStorageBenchmark.appendHotKey$",
        )

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("custom selector", selection["benchmark_selection_source"])
        self.assertEqual("", selection["benchmark_suite"])
        self.assertEqual(
            "IMapWalStorageBenchmark.appendHotKey$",
            selection["benchmark_selector"],
        )
        self.assertEqual(
            "IMapWalStorageBenchmark.appendHotKey$", selection["benchmark_regex"]
        )

    def test_explicit_full_pr_comparison_warns_about_the_timeout(self):
        result, selection = self.resolve(BENCHMARKS=".*", PR_NUMBER="12044")

        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual("selector", selection["benchmark_selection_source"])
        self.assertEqual("", selection["benchmark_suite"])
        self.assertEqual(".*", selection["benchmark_regex"])
        self.assertIn("full-suite PR comparison", result.stderr)
        self.assertIn("240-minute", result.stderr)

    def test_workflow_uses_core_by_default_and_keeps_full_suite_custom_only(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        benchmark_input = workflow.split("      benchmarks:\n", 1)[1].split(
            "      custom_benchmarks:\n", 1
        )[0]

        self.assertIn("default: 'benchmarks_core'", benchmark_input)
        self.assertIn("- 'benchmarks_core'", benchmark_input)
        self.assertNotIn("- '.*'", benchmark_input)
        self.assertIn(".* may exceed 240 minutes", workflow)
        self.assertIn(
            "BENCHMARK_SUITE: ${{ github.event_name == 'schedule' && "
            "'benchmarks_core' || '' }}",
            workflow,
        )
        self.assertIn(
            "BENCHMARKS: ${{ github.event.inputs.benchmarks || "
            "'benchmarks_core' }}",
            workflow,
        )
        self.assertIn(
            "CUSTOM_BENCHMARKS: ${{ github.event.inputs.custom_benchmarks || '' }}",
            workflow,
        )

    def test_workflow_uses_its_own_driver_for_old_baseline_compatibility(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("- name: Checkout benchmark driver", workflow)
        self.assertIn("ref: ${{ github.workflow_sha }}", workflow)
        self.assertIn("path: benchmark-driver", workflow)
        self.assertIn(
            "run: bash benchmark-driver/tools/benchmarks/run_benchmarks.sh",
            workflow,
        )
        self.assertNotIn(
            "run: bash baseline/tools/benchmarks/run_benchmarks.sh", workflow
        )

    def test_driver_does_not_load_tools_or_suites_from_baseline(self):
        script = SCRIPT.read_text(encoding="utf-8")

        self.assertIn('benchmark_tools_dir=$(cd -- "$(dirname -- ', script)
        self.assertIn(
            'benchmark_suite_file="${benchmark_tools_dir}/suites/', script
        )
        self.assertIn('"${benchmark_tools_dir}/save_jmh_result.py"', script)
        self.assertIn('"${benchmark_tools_dir}/regression_report.py"', script)
        self.assertNotIn("baseline/tools/benchmarks", script)

    @staticmethod
    def core_regex():
        return "|".join(
            line
            for line in SUITE.read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")
        )


if __name__ == "__main__":
    unittest.main()
