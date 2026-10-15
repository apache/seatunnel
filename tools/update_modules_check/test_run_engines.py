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


import unittest

from run_engines import run_engines_for_groups


class RunEnginesTest(unittest.TestCase):
    """Verify that the E2E engine scope follows the changed path groups."""

    def test_translation_change_runs_flink_and_spark(self) -> None:
        self.assertEqual(["flink", "spark"], run_engines_for_groups(["translation"]))

    def test_translation_flink_change_runs_flink_only(self) -> None:
        self.assertEqual(["flink"], run_engines_for_groups(["translation-flink"]))

    def test_translation_spark_change_runs_spark_only(self) -> None:
        self.assertEqual(["spark"], run_engines_for_groups(["translation-spark"]))

    def test_engine_change_runs_zeta_only(self) -> None:
        self.assertEqual(["zeta"], run_engines_for_groups(["engine"]))

    def test_engine_e2e_change_runs_zeta_only(self) -> None:
        self.assertEqual(["zeta"], run_engines_for_groups(["engine-e2e"]))

    def test_engine_and_translation_change_run_every_engine(self) -> None:
        self.assertEqual(
            ["flink", "spark", "zeta"],
            run_engines_for_groups(["engine", "translation"]),
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


if __name__ == "__main__":
    unittest.main()
