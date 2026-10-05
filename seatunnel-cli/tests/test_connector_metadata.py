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

import unittest
from unittest.mock import patch

from seatunnel_cli import connectors
from seatunnel_cli.skills import PipelineSlot, SkillExecutor, StructuredPlan


def _runtime_transform_entry():
    return {
        "name": "Sql",
        "type": "transform",
        "required": [
            {
                "key": "query",
                "type": "string",
                "defaultValue": None,
                "description": "SQL query",
                "category": "absolutely_required",
            }
        ],
        "optional": [],
        "conditionRules": [],
        "valueConstraints": [
            {
                "expression": "query must not be blank",
                "conditionTree": {
                    "key": "query",
                    "expectValue": "must not be blank",
                    "compareOperator": "extension",
                    "conditionOperator": "EXTENSION",
                    "conditionOperatorCategory": "EXTENSION",
                },
            }
        ],
    }


def _api_transform_response():
    return {
        "pluginName": "Sql",
        "pluginType": "transform",
        "optionRule": {
            "requiredOptions": [
                {
                    "ruleType": "absolutely_required",
                    "options": [
                        {
                            "key": "query",
                            "type": "java.lang.String",
                            "defaultValue": None,
                            "description": "SQL query",
                        }
                    ],
                }
            ],
            "optionalOptions": [],
            "conditionRules": [],
            "valueConstraints": [
                {
                    "expression": "query must not be blank",
                    "conditionTree": {
                        "option": {"key": "query"},
                        "expectValue": "must not be blank",
                        "compareOperator": "extension",
                        "conditionOperator": "EXTENSION",
                        "conditionOperatorCategory": "EXTENSION",
                    },
                }
            ],
        },
    }


class ConnectorMetadataSyncTest(unittest.TestCase):
    def test_api_response_preserves_value_constraints(self):
        detail = connectors._api_response_to_detail(_api_transform_response())

        self.assertEqual(
            detail["value_constraints"],
            [
                {
                    "expression": "query must not be blank",
                    "key": "query",
                    "expect": "must not be blank",
                    "operator": "extension",
                    "condition_operator": "EXTENSION",
                }
            ],
        )

    def test_runtime_metadata_preserves_value_constraints(self):
        detail = connectors._runtime_metadata_to_detail(_runtime_transform_entry())

        self.assertEqual(detail["value_constraints"][0]["key"], "query")
        self.assertEqual(detail["value_constraints"][0]["operator"], "extension")
        self.assertEqual(detail["value_constraints"][0]["expect"], "must not be blank")

    def test_prompt_includes_value_constraints(self):
        detail = connectors._runtime_metadata_to_detail(_runtime_transform_entry())

        prompt = connectors.format_metadata_for_prompt(detail, "Sql", "transform")

        self.assertIn("Value Constraints", prompt)
        self.assertIn("`query` must not be blank", prompt)
        self.assertNotIn("`query` extension must not be blank", prompt)

    def test_value_constraint_tolerates_missing_or_malformed_condition_tree(self):
        constraints = [
            {"expression": "query must not be blank"},
            {"expression": "query must not be blank", "conditionTree": None},
            {
                "expression": "query must not be blank",
                "conditionTree": ["not", "a", "dict"],
            },
        ]

        for constraint in constraints:
            with self.subTest(constraint=constraint):
                self.assertEqual(
                    connectors._value_constraint_to_dict(constraint),
                    {"expression": "query must not be blank"},
                )

    def test_value_constraint_formats_complex_expect_value_as_json(self):
        constraint = connectors._value_constraint_to_dict(
            {
                "expression": "mode should be in the allowed list",
                "conditionTree": {
                    "key": "mode",
                    "expectValue": ["batch", "streaming"],
                    "compareOperator": "in",
                },
            }
        )

        self.assertEqual(
            connectors._format_value_constraint(constraint),
            '- `mode` in ["batch", "streaming"] (mode should be in the allowed list)',
        )

    def test_connector_detail_can_fetch_transform_metadata_from_runtime_api(self):
        calls = []

        def fake_fetch(plugin_type, plugin_name):
            calls.append((plugin_type, plugin_name))
            if plugin_type == "transform" and plugin_name == "Sql":
                return _api_transform_response()
            return None

        with patch.object(connectors, "_fetch_option_rules", side_effect=fake_fetch):
            with patch.object(connectors, "_load_runtime_metadata", return_value={}):
                detail = connectors.get_connector_detail(
                    "Sql", connector_type="transform"
                )

        self.assertEqual(calls, [("transform", "Sql")])
        self.assertIn("[source: runtime API]", detail)
        self.assertIn("Types: transform", detail)
        self.assertIn("Value Constraints", detail)

    def test_connector_detail_queries_transform_when_type_is_not_specified(self):
        calls = []

        def fake_fetch(plugin_type, plugin_name):
            calls.append((plugin_type, plugin_name))
            if plugin_type == "transform" and plugin_name == "Sql":
                return _api_transform_response()
            return None

        with patch.object(connectors, "_fetch_option_rules", side_effect=fake_fetch):
            detail = connectors.get_connector_detail("Sql")

        self.assertEqual(
            calls,
            [("source", "Sql"), ("sink", "Sql"), ("transform", "Sql")],
        )
        self.assertIn("Types: transform", detail)

    def test_list_connector_names_includes_runtime_transforms(self):
        runtime_meta = {
            "source:FakeSource": {"name": "FakeSource", "type": "source"},
            "sink:Console": {"name": "Console", "type": "sink"},
            "transform:DataValidator": {"name": "DataValidator", "type": "transform"},
        }

        with patch.object(
            connectors, "_load_runtime_metadata", return_value=runtime_meta
        ):
            names = connectors.list_connector_names()

        self.assertIn("DataValidator", names["transforms"])
        self.assertIn("Sql", names["transforms"])

    def test_skill_executor_fetches_transform_metadata_from_plan(self):
        plan = StructuredPlan(
            pipelines=[
                PipelineSlot(
                    "pipeline_1",
                    source_connector="FakeSource",
                    sink_connector="Console",
                    transform="Sql",
                )
            ]
        )
        executor = SkillExecutor(plan, skills=[])
        calls = []

        def fake_fetch(name, connector_type):
            calls.append((name, connector_type))
            return {
                "name": name,
                "required": [],
                "optional": [],
                "conditional": [],
                "source": "test",
            }

        with patch(
            "seatunnel_cli.connectors.fetch_connector_metadata", side_effect=fake_fetch
        ):
            metadata = executor.fetch_all_metadata(lambda *_args: None)

        self.assertEqual(
            calls,
            [
                ("FakeSource", "source"),
                ("Console", "sink"),
                ("Sql", "transform"),
            ],
        )
        self.assertIn("### Sql (TRANSFORM)", metadata)

    def test_skill_executor_fill_and_check_includes_transform_required_options(self):
        plan = StructuredPlan(
            pipelines=[
                PipelineSlot(
                    "pipeline_1",
                    source_connector="FakeSource",
                    sink_connector="Console",
                    transform={"connector": "Sql"},
                )
            ]
        )
        executor = SkillExecutor(plan, skills=[])
        collect_calls = []

        def fake_collect(connector, connector_type):
            collect_calls.append((connector, connector_type))
            if connector_type == "transform":
                return [
                    {
                        "key": "query",
                        "type": "string",
                        "description": "SQL query",
                        "connector": connector,
                        "connector_type": connector_type,
                    }
                ]
            return []

        with patch(
            "seatunnel_cli.skills._collect_required_options", side_effect=fake_collect
        ):
            with patch(
                "seatunnel_cli.skills.llm_check_missing_info",
                return_value=["Please provide query"],
            ) as check_missing:
                missing = executor.fill_and_check(
                    "Filter rows with a SQL transform", client=object()
                )

        self.assertEqual(
            collect_calls,
            [
                ("FakeSource", "source"),
                ("Console", "sink"),
                ("Sql", "transform"),
            ],
        )
        self.assertEqual(missing, ["Please provide query"])
        required_options = check_missing.call_args[0][2]
        self.assertEqual(required_options[0]["key"], "query")
        self.assertEqual(required_options[0]["connector_type"], "transform")


if __name__ == "__main__":
    unittest.main()
