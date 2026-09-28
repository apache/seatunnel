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

"""Regression tests for connector E2E module sharding."""

import io
import re
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path

from update_modules_check import (
    ALL_CONNECTORS_DEDICATED_SHARD_MODULES,
    ALL_CONNECTORS_OPTIONAL_DEDICATED_SHARD_MODULES,
    ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES,
    build_sub_it_modules,
    get_sub_it_modules,
    get_sub_update_it_modules,
    modules_to_json,
    split_full_connector_it_modules,
)


class ConnectorItShardingTest(unittest.TestCase):
    """Verify connector sharding remains stable as modules change."""

    @staticmethod
    def parse_modules(modules):
        return [module.lstrip(":") for module in modules.split(",") if module]

    @staticmethod
    def workflow_text():
        return (
            Path(__file__).resolve().parents[2] / ".github" / "workflows" / "backend.yml"
        ).read_text(encoding="utf-8")

    def test_modules_to_json_preserves_exact_module_tokens(self) -> None:
        self.assertEqual(
            '["connector-kafka-e2e", "connector-iceberg-e2e"]',
            modules_to_json(":connector-kafka-e2e,:connector-iceberg-e2e"),
        )
        self.assertEqual("[]", modules_to_json(""))

    def test_module_outputs_use_json_empty_array(self) -> None:
        workflow = self.workflow_text()
        self.assertIn(
            "ut-modules: ${{ steps.ut-modules.outputs.modules || '[]' }}", workflow
        )
        self.assertIn(
            "it-modules: ${{ steps.it-modules.outputs.modules || '[]' }}", workflow
        )
        self.assertIn("needs.changes.outputs.ut-modules != '[]'", workflow)
        self.assertIn("needs.changes.outputs.it-modules != '[]'", workflow)

    def test_every_module_is_assigned_once(self) -> None:
        modules = ["connector-a-e2e", "connector-b-e2e", "connector-c-e2e"]

        shards = split_full_connector_it_modules(modules, 7)

        assigned_modules = [module for shard in shards for module in shard]
        self.assertCountEqual(modules, assigned_modules)
        self.assertEqual(len(modules), len(assigned_modules))

    def test_unknown_module_does_not_reshuffle_existing_modules(self) -> None:
        modules = ["connector-a-e2e", "connector-b-e2e", "connector-c-e2e"]
        original_shards = split_full_connector_it_modules(modules, 7)

        shards_with_new_module = split_full_connector_it_modules(
            modules + ["new-connector-e2e"], 7
        )

        for original_shard, new_shard in zip(original_shards, shards_with_new_module):
            self.assertEqual(
                original_shard,
                [module for module in new_shard if module != "new-connector-e2e"],
            )

    def test_sharding_is_independent_of_module_order(self) -> None:
        modules = ["connector-a-e2e", "connector-b-e2e", "connector-c-e2e"]

        self.assertEqual(
            split_full_connector_it_modules(modules, 7),
            split_full_connector_it_modules(list(reversed(modules)), 7),
        )

    def test_historical_seed_assignments_are_preserved(self) -> None:
        modules = [
            "connector-file-hadoop-e2e",
            "connector-cdc-mongodb-e2e",
            "connector-clickhouse-e2e",
            "connector-typesense-e2e",
            "connector-file-ftp-e2e",
            "connector-databend-e2e",
            "connector-http-e2e",
        ]

        self.assertEqual(
            split_full_connector_it_modules(modules, 7),
            [
                ["connector-file-hadoop-e2e"],
                ["connector-cdc-mongodb-e2e"],
                ["connector-clickhouse-e2e"],
                ["connector-typesense-e2e"],
                ["connector-file-ftp-e2e"],
                ["connector-databend-e2e"],
                ["connector-http-e2e"],
            ],
        )

    def test_full_and_updated_paths_apply_their_ownership_rules(self) -> None:
        self.assertIn(
            "connector-google-pubsub-e2e",
            ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES,
        )
        connector_modules = [
            "connector-normal-e2e",
            *ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES,
            *ALL_CONNECTORS_OPTIONAL_DEDICATED_SHARD_MODULES,
            "connector-iceberg-hadoop3-e2e",
            "connector-iceberg-s3-e2e",
        ]
        updated_modules = connector_modules + [
            "seatunnel-engine-k8s-e2e",
        ]

        full_output = io.StringIO()
        with redirect_stdout(full_output):
            get_sub_it_modules("," + ",".join(connector_modules), 1, 0)
        self.assertEqual(
            full_output.getvalue(),
            ":connector-iceberg-hadoop3-e2e,"
            ":connector-iceberg-s3-e2e,"
            ":connector-normal-e2e\n",
        )

        updated_output = io.StringIO()
        with redirect_stdout(updated_output):
            get_sub_update_it_modules(
                modules_to_json(":" + ",:".join(updated_modules)), 1, 0
            )
        self.assertEqual(
            updated_output.getvalue(),
            ":connector-normal-e2e,"
            ":connector-jdbc-e2e,"
            ":connector-iceberg-hadoop3-e2e,"
            ":connector-iceberg-s3-e2e\n",
        )

    def test_regular_shards_keep_only_remaining_modules_once(self) -> None:
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

    def test_regular_shards_fail_fast_when_dedicated_modules_disappear(self) -> None:
        modules = ",".join(
            [
                "",
                "connector-assert-e2e",
                *[
                    module
                    for module in ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES
                    if module != "connector-elasticsearch-e2e"
                ],
            ]
        )

        with self.assertRaisesRegex(ValueError, "connector-elasticsearch-e2e"):
            build_sub_it_modules(modules, 7, 0)

    def test_regular_shards_allow_optional_dedicated_modules_to_be_absent(
        self,
    ) -> None:
        modules = ",".join(
            ["", "connector-assert-e2e", *ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES]
        )

        shard_outputs = [build_sub_it_modules(modules, 7, shard) for shard in range(7)]
        combined_modules = {
            module
            for output in shard_outputs
            for module in self.parse_modules(output)
        }

        self.assertEqual({"connector-assert-e2e"}, combined_modules)

    def test_workflow_keeps_dedicated_jobs_for_excluded_modules(self) -> None:
        workflow_modules = set()
        for modules in re.findall(
            r"-pl\s+(:[A-Za-z0-9._-]+(?:,:[A-Za-z0-9._-]+)*)",
            self.workflow_text(),
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

    def test_dedicated_job_conditions_match_json_module_tokens(self) -> None:
        workflow = self.workflow_text()
        condition_modules = set(ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES) - {
            "connector-jdbc-e2e"
        }
        condition_modules.update(
            {"seatunnel-edge-agent-e2e", "seatunnel-engine-k8s-e2e"}
        )
        for module in sorted(condition_modules):
            with self.subTest(module=module):
                self.assertIn(
                    "contains(fromJSON(needs.changes.outputs.it-modules), "
                    f"'{module}')",
                    workflow,
                )

    def test_full_shard_rejects_non_positive_shard_count(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "total shard count must be positive, got 0"
        ):
            build_sub_it_modules("connector-normal-e2e", 0, 0)

    def test_full_shard_rejects_out_of_range_index(self) -> None:
        for current_num in (-1, 7):
            with self.subTest(current_num=current_num):
                with self.assertRaisesRegex(
                    ValueError,
                    f"shard index {current_num} out of range \\[0, 7\\)",
                ):
                    build_sub_it_modules("connector-normal-e2e", 7, current_num)

if __name__ == "__main__":
    unittest.main()
