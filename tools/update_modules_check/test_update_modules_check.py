#
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
from collections import Counter
from pathlib import Path
import re

from update_modules_check import (
    ALL_CONNECTORS_DEDICATED_SHARD_MODULES,
    build_sub_it_modules,
)


class UpdateModulesCheckTest(unittest.TestCase):
    """
    Guard the all-connectors shard contract used by backend CI.
    """

    @staticmethod
    def parse_modules(modules):
        return [module.lstrip(":") for module in modules.split(",") if module]

    def test_regular_shards_keep_only_remaining_modules_once(self):
        """
        Regular all-connectors shards should keep each surviving module once.
        """
        expected_modules = {
            "connector-assert-e2e",
            "connector-cdc-sqlserver-e2e",
            "connector-http-e2e",
        }
        modules = ",".join(
            ["", *sorted(expected_modules), *ALL_CONNECTORS_DEDICATED_SHARD_MODULES]
        )

        shard_outputs = [build_sub_it_modules(modules, 7, shard) for shard in range(7)]
        shard_modules = [self.parse_modules(output) for output in shard_outputs]
        combined_counter = Counter(
            module for output_modules in shard_modules for module in output_modules
        )

        self.assertEqual(expected_modules, set(combined_counter))
        self.assertEqual(Counter(expected_modules), combined_counter)
        self.assertTrue(
            set(ALL_CONNECTORS_DEDICATED_SHARD_MODULES).isdisjoint(set(combined_counter))
        )
        for output_modules in shard_modules:
            self.assertNotIn("connector-iceberg-e2e", output_modules)
            self.assertNotIn("connector-hbase-e2e", output_modules)

    def test_regular_shards_fail_fast_when_dedicated_modules_disappear(self):
        """
        The all-connectors source list should fail loudly if a dedicated module drifts.
        """
        modules = ",".join(
            [
                "",
                "connector-assert-e2e",
                *[
                    module
                    for module in ALL_CONNECTORS_DEDICATED_SHARD_MODULES
                    if module != "connector-elasticsearch-e2e"
                ],
            ]
        )

        with self.assertRaisesRegex(ValueError, "connector-elasticsearch-e2e"):
            build_sub_it_modules(modules, 7, 0)

    def test_workflow_keeps_dedicated_jobs_for_excluded_modules(self):
        """
        Workflow job lists should continue covering modules excluded from regular shards.
        """
        workflow = (
            Path(__file__).resolve().parents[2] / ".github" / "workflows" / "backend.yml"
        ).read_text(encoding="utf-8")
        workflow_modules = set()
        for modules in re.findall(
            r"-pl\s+(:[A-Za-z0-9._-]+(?:,:[A-Za-z0-9._-]+)*)", workflow
        ):
            workflow_modules.update(
                module.lstrip(":") for module in modules.split(",") if module
            )

        expected_workflow_modules = set(ALL_CONNECTORS_DEDICATED_SHARD_MODULES)
        expected_workflow_modules.remove("connector-jdbc-e2e")
        expected_workflow_modules.update(
            {
                "connector-jdbc-e2e-part-1",
                "connector-jdbc-e2e-part-2",
                "connector-jdbc-e2e-part-3",
                "connector-jdbc-e2e-part-4",
                "connector-jdbc-e2e-part-5",
                "connector-jdbc-e2e-part-6",
                "connector-jdbc-e2e-part-7",
                "connector-jdbc-e2e-ddl",
            }
        )

        self.assertFalse(
            expected_workflow_modules - workflow_modules,
            f"Missing dedicated workflow modules: {sorted(expected_workflow_modules - workflow_modules)}",
        )


if __name__ == "__main__":
    unittest.main()
