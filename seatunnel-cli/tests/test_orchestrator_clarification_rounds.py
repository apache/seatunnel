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

"""Clarifying questions must not repeat forever.

A planner that keeps failing to resolve a pipeline and a caller that keeps
answering produce an endless exchange, and because each reply is accumulated
onto the requirement, the text sent to the model grows with every round.
"""

import unittest
from unittest import mock

from seatunnel_cli.agents import Orchestrator


class RoundsAreBoundedTest(unittest.TestCase):
    def setUp(self):
        self.orchestrator = Orchestrator(client=mock.MagicMock())

    def _always_asks(self):
        return mock.patch.object(
            self.orchestrator, "_run_planner",
            return_value={"type": "question", "content": "Which database?",
                          "config": None, "explanation": None},
        )

    def test_the_first_rounds_still_ask(self):
        with self._always_asks():
            for _ in range(Orchestrator.MAX_CLARIFICATION_ROUNDS):
                result = self.orchestrator.process_user_input("something vague")
                self.assertEqual(result["type"], "question")

    def test_the_next_round_gives_up_instead_of_asking_again(self):
        with self._always_asks():
            for _ in range(Orchestrator.MAX_CLARIFICATION_ROUNDS):
                self.orchestrator.process_user_input("something vague")
            result = self.orchestrator.process_user_input("still vague")

        self.assertEqual(result["type"], "error")
        self.assertIsNone(result["config"])

    def test_giving_up_says_what_is_actually_needed(self):
        with self._always_asks():
            for _ in range(Orchestrator.MAX_CLARIFICATION_ROUNDS + 1):
                result = self.orchestrator.process_user_input("vague")

        # Telling the user the two facts only they have is the whole point of
        # stopping rather than generating something ungrounded.
        self.assertIn("the source", result["content"])
        self.assertIn("the destination", result["content"])
        self.assertIn("Which database?", result["content"])

    def test_giving_up_clears_the_exchange(self):
        with self._always_asks():
            for _ in range(Orchestrator.MAX_CLARIFICATION_ROUNDS + 1):
                self.orchestrator.process_user_input("vague")

        self.assertIsNone(self.orchestrator.pending_question)
        self.assertIsNone(self.orchestrator._original_request)
        self.assertEqual(self.orchestrator._clarification_rounds, 0)

    def test_the_requirement_stops_growing_once_we_give_up(self):
        """Each reply is appended, so an unbounded loop grows the prompt."""
        lengths = []
        with self._always_asks():
            for _ in range(Orchestrator.MAX_CLARIFICATION_ROUNDS + 1):
                self.orchestrator.process_user_input("a" * 200)
                lengths.append(len(self.orchestrator._original_request or ""))

        self.assertEqual(lengths[-1], 0, "the exchange should have been reset")
        self.assertLessEqual(
            max(lengths),
            200 * (Orchestrator.MAX_CLARIFICATION_ROUNDS + 1) + 500,
            "accumulated requirement grew beyond the round bound",
        )


class CounterResetsTest(unittest.TestCase):
    """A served or abandoned requirement must not leave the count behind.

    Otherwise a user who needed one clarification earlier gets fewer, or none,
    on their next unrelated request.
    """

    def setUp(self):
        self.orchestrator = Orchestrator(client=mock.MagicMock())
        self.skill = mock.MagicMock()
        self.skill.fill_and_check.return_value = []
        self.skill.fetch_all_metadata.return_value = "## Connector Option Rules\n"
        self.skill.build_enriched_prompt.return_value = "## User Request:\nx"

    def _ask_once(self):
        with mock.patch.object(
            self.orchestrator, "_run_planner",
            return_value={"type": "question", "content": "Which database?",
                          "config": None, "explanation": None},
        ):
            self.orchestrator.process_user_input("vague")
        self.assertEqual(self.orchestrator._clarification_rounds, 1)

    def test_a_delivered_config_resets_the_count(self):
        self._ask_once()

        with mock.patch.object(
            self.orchestrator, "_run_planner",
            return_value={"type": "plan",
                          "content": "source: Jdbc\nsink: Console\nmode: BATCH"},
        ), mock.patch("seatunnel_cli.skills.SkillExecutor", return_value=self.skill), \
             mock.patch("seatunnel_cli.skills.SkillRouter.match", return_value=[]), \
             mock.patch.object(
                 self.orchestrator, "_run_config_generator",
                 return_value={"config": "env {}\nsource {}\nsink {}",
                               "explanation": ""}), \
             mock.patch.object(self.orchestrator, "_run_validator",
                               return_value="PASS"), \
             mock.patch("seatunnel_cli.agents.dry_run_config",
                        return_value={"valid": True, "phase1_local": "VALID",
                                      "phase2_check": "PASS", "phase3_api": None,
                                      "summary": ""}):
            result = self.orchestrator.process_user_input("MySQL to console.")

        self.assertEqual(result["type"], "config")
        self.assertEqual(self.orchestrator._clarification_rounds, 0)

    def test_a_chat_reply_resets_the_count(self):
        self._ask_once()

        with mock.patch.object(
            self.orchestrator, "_run_planner",
            return_value={"type": "chat", "content": "SeaTunnel is an ETL tool."},
        ):
            self.orchestrator.process_user_input("What is SeaTunnel?")

        self.assertEqual(self.orchestrator._clarification_rounds, 0)

    def test_resuming_a_session_resets_the_count(self):
        self._ask_once()

        self.orchestrator.load_history([
            {"role": "user", "content": [{"text": "an earlier session"}]},
        ])

        self.assertEqual(self.orchestrator._clarification_rounds, 0)


if __name__ == "__main__":
    unittest.main()
