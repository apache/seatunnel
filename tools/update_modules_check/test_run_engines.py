#!/usr/bin/env python3
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


import re
import subprocess
import sys
import unittest
from pathlib import Path

from run_engines import GROUP_ENGINES, run_engines_for_groups


WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "backend.yml"
)


class RunEnginesTest(unittest.TestCase):
    """Verify that the E2E engine scope follows the changed path groups."""

    def test_translation_base_change_runs_flink_and_spark(self) -> None:
        self.assertEqual(["flink", "spark"], run_engines_for_groups(["translation-base"]))

    def test_translation_flink_change_runs_flink_only(self) -> None:
        self.assertEqual(["flink"], run_engines_for_groups(["translation-flink"]))

    def test_translation_spark_change_runs_spark_only(self) -> None:
        self.assertEqual(["spark"], run_engines_for_groups(["translation-spark"]))

    def test_translation_base_and_flink_change_run_both_engines(self) -> None:
        self.assertEqual(
            ["flink", "spark"],
            run_engines_for_groups(["translation-base", "translation-flink"]),
        )

    def test_engine_change_runs_zeta_only(self) -> None:
        self.assertEqual(["zeta"], run_engines_for_groups(["engine"]))

    def test_engine_e2e_change_runs_zeta_only(self) -> None:
        self.assertEqual(["zeta"], run_engines_for_groups(["engine-e2e"]))

    def test_engine_and_translation_change_run_every_engine(self) -> None:
        self.assertEqual(
            ["flink", "spark", "zeta"],
            run_engines_for_groups(["engine", "translation-base"]),
        )

    def test_engine_and_connector_change_run_every_engine(self) -> None:
        self.assertEqual(
            ["flink", "spark", "zeta"],
            run_engines_for_groups(["engine", "connectors"]),
        )

    def test_shared_changes_run_every_engine(self) -> None:
        for group in ("shared", "connectors", "connector-e2e", "dist", "standalone"):
            with self.subTest(group=group):
                self.assertEqual(
                    ["flink", "spark", "zeta"], run_engines_for_groups([group])
                )

    def test_unknown_change_runs_every_engine(self) -> None:
        self.assertEqual(["flink", "spark", "zeta"], run_engines_for_groups([]))


class RunEnginesWorkflowTest(unittest.TestCase):
    """Verify the workflow wiring and the CLI stay in step with the mapping table."""

    def run_helper(self, *args: str) -> str:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve().parent / "run_engines.py"), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def test_flink_only_translation_change_prints_flink(self) -> None:
        self.assertEqual(
            '["flink"]',
            self.run_helper(
                "--translation-base",
                "false",
                "--translation-flink",
                "true",
                "--translation-spark",
                "false",
            ),
        )

    def test_no_group_prints_every_engine(self) -> None:
        self.assertEqual('["flink","spark","zeta"]', self.run_helper())

    def test_workflow_passes_every_group_flag(self) -> None:
        invocation = re.search(
            r"run_engines\.py(.*?)\); then", WORKFLOW_PATH.read_text(), re.DOTALL
        ).group(1)
        passed_groups = set(re.findall(r"--([a-z0-9-]+)\s+\"\$\{", invocation))
        self.assertEqual(set(GROUP_ENGINES), passed_groups)

    def test_translation_probes_stay_narrow(self) -> None:
        # The broad `seatunnel-translation/**` glob also matches the Flink and Spark
        # sub-modules, which would make the flink-only/spark-only scoping unreachable.
        base_probe = re.search(
            r"translation_base_files=`([^`]*)`", WORKFLOW_PATH.read_text()
        ).group(1)
        self.assertIn("seatunnel-translation-base/**", base_probe)
        self.assertNotIn('"seatunnel-translation/**"', base_probe)


if __name__ == "__main__":
    unittest.main()
