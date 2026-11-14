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

"""An empty plan must stop the exchange, not fall through to generation.

`fetch_all_metadata` iterates the plan's pipelines, so a plan with none yields
an empty connector-option block. Generating from that asks the model to recall
option keys from memory, which is what the retrieved rules exist to replace.
"""

import unittest
from unittest import mock

from seatunnel_cli.agents import Orchestrator
from seatunnel_cli.skills import SkillExecutor, StructuredPlan, parse_structured_plan


class EmptyPlanIsReachableTest(unittest.TestCase):
    """The parser produces this state on ordinary input, not just on garbage."""

    def test_legacy_plan_without_a_sink_has_no_pipeline(self):
        plan = parse_structured_plan("source: Jdbc\nmode: BATCH")
        self.assertEqual(plan.pipelines, [])

    def test_legacy_plan_without_a_source_has_no_pipeline(self):
        plan = parse_structured_plan("sink: Console\nmode: BATCH")
        self.assertEqual(plan.pipelines, [])

    def test_unparseable_planner_output_has_no_pipeline(self):
        plan = parse_structured_plan("I am not sure what you want.")
        self.assertEqual(plan.pipelines, [])

    def test_an_empty_plan_fetches_no_connector_option_rules(self):
        skill = SkillExecutor(StructuredPlan(pipelines=[]), [])
        self.assertEqual(skill.fetch_all_metadata(lambda *a: None), "")


class EmptyPlanStopsGenerationTest(unittest.TestCase):
    def test_question_is_returned_and_generator_is_not_called(self):
        orchestrator = Orchestrator(client=mock.MagicMock())

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan", "content": "source: Jdbc\nmode: BATCH"},
        ), mock.patch.object(orchestrator, "_run_config_generator") as generator:
            result = orchestrator.process_user_input("Move my data somewhere.")

        self.assertEqual(result["type"], "question")
        self.assertIsNone(result["config"])
        generator.assert_not_called()

    def test_the_question_asks_for_the_two_missing_facts(self):
        orchestrator = Orchestrator(client=mock.MagicMock())

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan", "content": "nothing parseable here"},
        ):
            result = orchestrator.process_user_input("Do the thing.")

        self.assertIn("where the data comes from", result["content"])
        self.assertIn("where it should end up", result["content"])

    def test_pending_state_lets_the_reply_keep_the_original_request(self):
        orchestrator = Orchestrator(client=mock.MagicMock())

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan", "content": "source: Jdbc"},
        ):
            orchestrator.process_user_input("Copy the users table.")

        self.assertIsNotNone(orchestrator.pending_question)
        merged = orchestrator._effective_request("From MySQL to Console.")
        self.assertIn("Copy the users table.", merged)
        self.assertIn("From MySQL to Console.", merged)

    def test_a_plan_with_a_pipeline_still_reaches_generation(self):
        orchestrator = Orchestrator(client=mock.MagicMock())
        skill = mock.MagicMock()
        skill.fill_and_check.return_value = []
        skill.fetch_all_metadata.return_value = "## Connector Option Rules\n"
        skill.build_enriched_prompt.return_value = "## User Request:\nx"

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan",
                          "content": "source: Jdbc\nsink: Console\nmode: BATCH"},
        ), mock.patch("seatunnel_cli.skills.SkillExecutor", return_value=skill), \
             mock.patch("seatunnel_cli.skills.SkillRouter.match", return_value=[]), \
             mock.patch.object(
                 orchestrator, "_run_config_generator",
                 return_value={"config": "env {}\nsource {}\nsink {}",
                               "explanation": ""}) as generator, \
             mock.patch.object(orchestrator, "_run_validator", return_value="PASS"), \
             mock.patch("seatunnel_cli.agents.dry_run_config",
                        return_value={"valid": True, "phase1_local": "VALID",
                                      "phase2_check": "PASS", "phase3_api": None,
                                      "summary": ""}):
            result = orchestrator.process_user_input(
                "Copy users from MySQL at localhost:3306 to the console.")

        self.assertEqual(result["type"], "config")
        generator.assert_called_once()


if __name__ == "__main__":
    unittest.main()
