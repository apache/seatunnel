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

import unittest

from update_modules_check import (
    DEDICATED_CONNECTOR_IT_MODULES,
    FULL_CONNECTOR_IT_EXCLUDED_MODULES,
    NON_SHARED_IT_MODULES,
    UPDATED_CONNECTOR_IT_EXCLUDED_MODULES,
    split_connector_it_modules,
)


class ConnectorItShardingTest(unittest.TestCase):
    """Verify connector sharding remains stable as modules change."""

    def test_every_module_is_assigned_once(self) -> None:
        modules = ["connector-a-e2e", "connector-b-e2e", "connector-c-e2e"]

        shards = split_connector_it_modules(modules, 7)

        assigned_modules = [module for shard in shards for module in shard]
        self.assertCountEqual(modules, assigned_modules)
        self.assertEqual(len(modules), len(assigned_modules))

    def test_unknown_module_does_not_reshuffle_existing_modules(self) -> None:
        modules = ["connector-a-e2e", "connector-b-e2e", "connector-c-e2e"]
        original_shards = split_connector_it_modules(modules, 7)

        shards_with_new_module = split_connector_it_modules(
            modules + ["new-connector-e2e"], 7
        )

        for original_shard, new_shard in zip(original_shards, shards_with_new_module):
            self.assertEqual(
                original_shard,
                [module for module in new_shard if module != "new-connector-e2e"],
            )

    def test_dedicated_iceberg_and_hbase_are_excluded_from_shared_shards(
        self,
    ) -> None:
        for module in ("connector-iceberg-e2e", "connector-hbase-e2e"):
            with self.subTest(module=module):
                self.assertIn(module, DEDICATED_CONNECTOR_IT_MODULES)
                self.assertIn(module, FULL_CONNECTOR_IT_EXCLUDED_MODULES)
                self.assertIn(module, UPDATED_CONNECTOR_IT_EXCLUDED_MODULES)

    def test_all_dedicated_modules_are_excluded_from_shared_shards(self) -> None:
        self.assertLessEqual(
            DEDICATED_CONNECTOR_IT_MODULES, FULL_CONNECTOR_IT_EXCLUDED_MODULES
        )
        self.assertLessEqual(
            DEDICATED_CONNECTOR_IT_MODULES, UPDATED_CONNECTOR_IT_EXCLUDED_MODULES
        )
        self.assertLessEqual(NON_SHARED_IT_MODULES, FULL_CONNECTOR_IT_EXCLUDED_MODULES)
        self.assertLessEqual(
            NON_SHARED_IT_MODULES, UPDATED_CONNECTOR_IT_EXCLUDED_MODULES
        )

    def test_path_specific_exclusions_are_preserved(self) -> None:
        self.assertIn("connector-jdbc-e2e", FULL_CONNECTOR_IT_EXCLUDED_MODULES)
        self.assertNotIn("connector-jdbc-e2e", UPDATED_CONNECTOR_IT_EXCLUDED_MODULES)
        self.assertNotIn(
            "seatunnel-engine-k8s-e2e", FULL_CONNECTOR_IT_EXCLUDED_MODULES
        )
        self.assertIn(
            "seatunnel-engine-k8s-e2e", UPDATED_CONNECTOR_IT_EXCLUDED_MODULES
        )

    def test_sensorsdata_is_owned_by_its_dedicated_job(self) -> None:
        self.assertIn(
            "connector-sensorsdata-e2e", DEDICATED_CONNECTOR_IT_MODULES
        )
        self.assertIn("connector-sensorsdata-e2e", FULL_CONNECTOR_IT_EXCLUDED_MODULES)
        self.assertIn(
            "connector-sensorsdata-e2e", UPDATED_CONNECTOR_IT_EXCLUDED_MODULES
        )

    def test_sharding_is_independent_of_module_order(self) -> None:
        modules = ["connector-a-e2e", "connector-b-e2e", "connector-c-e2e"]

        self.assertEqual(
            split_connector_it_modules(modules, 7),
            split_connector_it_modules(list(reversed(modules)), 7),
        )


if __name__ == "__main__":
    unittest.main()
