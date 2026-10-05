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
import json
import re
import unittest
from collections import Counter
from contextlib import redirect_stdout
from pathlib import Path

from update_modules_check import (
    ALL_CONNECTORS_DEDICATED_SHARD_MODULES,
    ALL_CONNECTORS_OPTIONAL_DEDICATED_SHARD_MODULES,
    ALL_CONNECTORS_REQUIRED_DEDICATED_SHARD_MODULES,
    STANDALONE_MODULE_PATHS,
    build_standalone_modules,
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

    @staticmethod
    def repo_root():
        return Path(__file__).resolve().parents[2]

    @staticmethod
    def pom_artifact_id(pom):
        text = re.sub(r"<parent>.*?</parent>", "", pom.read_text(encoding="utf-8"), flags=re.S)
        return re.search(r"<artifactId>([^<]+)</artifactId>", text).group(1)

    def workflow_pl_modules(self):
        modules = set()
        for pl in re.findall(
            r"-pl\s+(:[A-Za-z0-9._-]+(?:,:[A-Za-z0-9._-]+)*)", self.workflow_text()
        ):
            modules.update(module.lstrip(":") for module in pl.split(",") if module)
        return modules

    def test_standalone_paths_map_to_their_test_modules(self) -> None:
        self.assertEqual(
            ["seatunnel-trace-analyzer", "seatunnel-starter-e2e"],
            build_standalone_modules(
                json.dumps(
                    [
                        "seatunnel-trace/seatunnel-trace-analyzer/src/main/java/A.java",
                        "seatunnel-trace/pom.xml",
                        "seatunnel-e2e/seatunnel-core-e2e/seatunnel-starter-e2e/pom.xml",
                    ]
                )
            ),
        )
        self.assertEqual([], build_standalone_modules("[]"))

    def test_standalone_modules_exist_and_match_workflow_filter(self) -> None:
        workflow = self.workflow_text()
        filter_line = next(
            line for line in workflow.splitlines() if line.strip().startswith("standalone_files=")
        )
        self.assertEqual(
            [prefix + "**" for prefix, _ in STANDALONE_MODULE_PATHS],
            re.findall(r'"([^"]+)"', filter_line),
        )
        for path_prefix, module in STANDALONE_MODULE_PATHS:
            with self.subTest(module=module):
                poms = [
                    pom
                    for pom in (self.repo_root() / path_prefix).rglob("pom.xml")
                    if "target" not in pom.parts
                ]
                self.assertIn(module, {self.pom_artifact_id(pom) for pom in poms})

    def test_engine_changes_run_the_k8s_integration_test(self) -> None:
        job = re.search(
            r"^  engine-k8s-it:\n(.*?)(?=^  \S)", self.workflow_text(), re.M | re.S
        ).group(1)
        self.assertTrue(
            "needs.changes.outputs.engine == 'true'" in job,
            "engine-k8s-it must run for engine changes",
        )

    def test_every_non_connector_e2e_it_module_has_a_workflow_job(self) -> None:
        e2e_root = self.repo_root() / "seatunnel-e2e"
        workflow_modules = self.workflow_pl_modules()
        for pom in sorted(e2e_root.rglob("pom.xml")):
            relative = pom.relative_to(e2e_root).parts
            if relative[0] in ("seatunnel-connector-v2-e2e", "seatunnel-e2e-common") or "target" in relative:
                continue
            test_root = pom.parent / "src" / "test" / "java"
            if not test_root.is_dir() or not any(test_root.rglob("*IT.java")):
                continue
            module = self.pom_artifact_id(pom)
            with self.subTest(module=module):
                self.assertIn(module, workflow_modules)

if __name__ == "__main__":
    unittest.main()
