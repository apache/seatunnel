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
import unittest
from contextlib import redirect_stdout

from update_modules_check import (
    build_sub_it_modules,
    get_sub_it_modules,
    get_sub_update_it_modules,
    split_full_connector_it_modules,
)


class ConnectorItShardingTest(unittest.TestCase):
    """Verify connector sharding remains stable as modules change."""

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
        connector_modules = [
            "connector-normal-e2e",
            "connector-jdbc-e2e",
            "connector-seatunnel-e2e-base",
            "connector-console-seatunnel-e2e",
            "seatunnel-edge-agent-e2e",
            "connector-iceberg-e2e",
            "connector-hbase-e2e",
            "connector-sensorsdata-e2e",
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
            get_sub_update_it_modules(":" + ",:".join(updated_modules), 1, 0)
        self.assertEqual(
            updated_output.getvalue(),
            ":connector-normal-e2e,"
            ":connector-jdbc-e2e,"
            ":connector-iceberg-hadoop3-e2e,"
            ":connector-iceberg-s3-e2e\n",
        )

    def test_dedicated_heavy_suites_do_not_stay_in_regular_shards(self):
        """Iceberg and HBase should run only in their dedicated jobs."""
        modules = ",".join(
            [
                "",
                "connector-assert-e2e",
                "connector-jdbc-e2e",
                "connector-redis-e2e",
                "connector-cdc-sqlserver-e2e",
                "connector-kafka-e2e",
                "connector-iceberg-e2e",
                "connector-hbase-e2e",
                "connector-http-e2e",
                "connector-rocketmq-e2e",
                "connector-kudu-e2e",
                "connector-amazonsqs-e2e",
                "connector-doris-e2e",
                "connector-paimon-e2e",
                "connector-cdc-oracle-e2e",
                "connector-file-local-e2e",
                "connector-file-sftp-e2e",
                "connector-sensorsdata-e2e",
            ]
        )

        shard_outputs = [build_sub_it_modules(modules, 7, shard) for shard in range(7)]
        combined_outputs = ",".join(shard_outputs)

        self.assertIn("connector-assert-e2e", combined_outputs)
        for shard_modules in shard_outputs:
            self.assertNotIn("connector-iceberg-e2e", shard_modules)
            self.assertNotIn("connector-hbase-e2e", shard_modules)

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
