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

"""A clarifying question must not drop the requirement that preceded it.

Only the planner reads the conversation history. Skill matching, slot filling
and the enriched prompt are all built from the latest message, so before this
was fixed a reply such as "use sensible defaults" became the whole request and
the generator never saw what the user had actually asked for.
"""

import unittest
from unittest import mock

from seatunnel_cli.agents import Orchestrator


REQUEST = (
    "Sync the users table from MySQL at localhost:3306, database shop, "
    "user root into the existing users_replica table in the same database."
)
REPLY = (
    "Use sensible defaults for anything unspecified. Do not ask further "
    "questions - generate the config now."
)


class EffectiveRequestTest(unittest.TestCase):
    def setUp(self):
        self.orchestrator = Orchestrator(client=mock.MagicMock())

    def test_plain_message_is_the_request(self):
        self.assertEqual(self.orchestrator._effective_request(REQUEST), REQUEST)

    def test_reply_to_a_question_is_added_not_substituted(self):
        self.orchestrator._effective_request(REQUEST)
        self.orchestrator.pending_question = "Which table should I write to?"

        merged = self.orchestrator._effective_request(REPLY)

        # The requirement has to survive; the reply is extra detail, and on its
        # own it names no source, no sink and no table.
        self.assertIn("users_replica", merged)
        self.assertIn("localhost:3306", merged)
        self.assertIn(REPLY, merged)

    def test_several_rounds_keep_accumulating(self):
        self.orchestrator._effective_request(REQUEST)
        self.orchestrator.pending_question = "q1"
        self.orchestrator._effective_request("The target table already exists.")
        self.orchestrator.pending_question = "q2"

        merged = self.orchestrator._effective_request("Batch mode is fine.")

        self.assertIn("users_replica", merged)
        self.assertIn("The target table already exists.", merged)
        self.assertIn("Batch mode is fine.", merged)

    def test_a_new_message_after_delivery_starts_over(self):
        self.orchestrator._effective_request(REQUEST)
        # Delivering a config clears the exchange.
        self.orchestrator.pending_question = None
        self.orchestrator._original_request = None

        fresh = self.orchestrator._effective_request("Print the orders table.")

        self.assertEqual(fresh, "Print the orders table.")
        self.assertNotIn("users_replica", fresh)


class EnrichedPromptAfterClarificationTest(unittest.TestCase):
    """End to end over `process_user_input`, which is where the loss happened.

    The planner is stubbed because the point under test is what the *generator*
    receives, not how the plan is produced.
    """

    def _orchestrator_with_stubs(self, seen):
        orchestrator = Orchestrator(client=mock.MagicMock())

        skill = mock.MagicMock()
        skill.fill_and_check.return_value = []
        skill.fetch_all_metadata.return_value = "## Connector Option Rules\n"

        def build_enriched_prompt(request, on_status, memory_store=None):
            seen.append(request)
            return f"## User Request:\n{request}"

        skill.build_enriched_prompt.side_effect = build_enriched_prompt
        return orchestrator, skill

    def test_generator_still_sees_the_original_request(self):
        seen: list[str] = []
        orchestrator, skill = self._orchestrator_with_stubs(seen)

        with mock.patch.object(
            orchestrator, "_run_planner",
            side_effect=[
                {"type": "question", "content": "Which database?",
                 "config": None, "explanation": None},
                {"type": "plan", "content": "source: Jdbc\nsink: Jdbc\nmode: BATCH"},
            ],
        ), mock.patch("seatunnel_cli.skills.SkillExecutor", return_value=skill), \
             mock.patch("seatunnel_cli.skills.SkillRouter.match", return_value=[]), \
             mock.patch("seatunnel_cli.skills.parse_structured_plan") as parse_plan, \
             mock.patch.object(
                 orchestrator, "_run_config_generator",
                 return_value={"config": "env {}\nsource {}\nsink {}",
                               "explanation": ""}), \
             mock.patch.object(orchestrator, "_run_validator", return_value="PASS"), \
             mock.patch("seatunnel_cli.agents.dry_run_config",
                        return_value={"valid": True, "phase1_local": "VALID",
                                      "phase2_check": "PASS", "phase3_api": None,
                                      "summary": ""}):
            parse_plan.return_value = mock.MagicMock(pipelines=[mock.MagicMock()])

            first = orchestrator.process_user_input(REQUEST)
            self.assertEqual(first["type"], "question")

            second = orchestrator.process_user_input(REPLY)
            self.assertEqual(second["type"], "config")

        self.assertEqual(len(seen), 1, "the generator should run once, after the reply")
        prompt_request = seen[0]
        self.assertIn("users_replica", prompt_request)
        self.assertIn("localhost:3306", prompt_request)

    def test_question_records_pending_state_and_config_clears_it(self):
        seen: list[str] = []
        orchestrator, skill = self._orchestrator_with_stubs(seen)

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "question", "content": "Which database?",
                          "config": None, "explanation": None},
        ):
            orchestrator.process_user_input(REQUEST)

        self.assertEqual(orchestrator.pending_question, "Which database?")
        self.assertEqual(orchestrator._original_request, REQUEST)


if __name__ == "__main__":
    unittest.main()
