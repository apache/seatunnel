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

"""A valid config that answers the wrong question must not be delivered.

Choosing Jdbc where the plan resolved to MySQL-CDC produces HOCON that parses,
loads and runs, so every existing gate passes it. The generator's connector
choice was therefore never revisited, and the repair prompt held only the
config and the validation errors - nothing to compare the config against.
"""

import unittest
from unittest import mock

from seatunnel_cli.agents import Orchestrator, _plan_conformance_errors
from seatunnel_cli.skills import PipelineSlot, StructuredPlan


CDC_TO_CONSOLE = StructuredPlan(
    pipelines=[PipelineSlot("pipeline_1", "MySQL-CDC", "Console", mode="STREAMING")],
    raw_plan_text="source: MySQL-CDC\nsink: Console\nmode: STREAMING",
)

WRONG_SOURCE = """env {
  job.mode = "STREAMING"
}
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/shop"
    query = "select * from users"
  }
}
sink {
  Console {}
}
"""

RIGHT_SOURCE = """env {
  job.mode = "STREAMING"
}
source {
  MySQL-CDC {
    base-url = "jdbc:mysql://localhost:3306/shop"
    table-names = ["shop.users"]
  }
}
sink {
  Console {}
}
"""


class ConformanceCheckTest(unittest.TestCase):
    def test_wrong_source_connector_is_reported(self):
        errors = _plan_conformance_errors(CDC_TO_CONSOLE, WRONG_SOURCE)
        self.assertEqual(len(errors), 1)
        self.assertIn("mysql-cdc", errors[0])
        self.assertIn("jdbc", errors[0])

    def test_matching_connectors_report_nothing(self):
        self.assertEqual(_plan_conformance_errors(CDC_TO_CONSOLE, RIGHT_SOURCE), [])

    def test_case_differences_are_not_drift(self):
        plan = StructuredPlan(
            pipelines=[PipelineSlot("p", "jdbc", "console")])
        config = "source {\n  Jdbc {}\n}\nsink {\n  Console {}\n}\n"
        self.assertEqual(_plan_conformance_errors(plan, config), [])

    def test_an_extra_sink_is_allowed(self):
        """A plan for one pipeline may legitimately gain an Assert sink."""
        plan = StructuredPlan(
            pipelines=[PipelineSlot("p", "Jdbc", "Console")])
        config = ("source {\n  Jdbc {}\n}\n"
                  "sink {\n  Console {}\n  Assert {}\n}\n")
        self.assertEqual(_plan_conformance_errors(plan, config), [])

    def test_no_plan_means_no_opinion(self):
        self.assertEqual(
            _plan_conformance_errors(StructuredPlan(pipelines=[]), WRONG_SOURCE), [])

    def test_a_planned_name_the_engine_does_not_register_is_ignored(self):
        """The plan is model output; an invented alias must not condemn a config.

        A planner that writes ``S3`` where the plugin is ``S3File`` would
        otherwise spend every repair round moving a correct config away from
        the right connector.
        """
        plan = StructuredPlan(pipelines=[PipelineSlot("p", "S3", "Console")])
        config = "source {\n  S3File {}\n}\nsink {\n  Console {}\n}\n"

        with mock.patch("seatunnel_cli.connectors.list_connector_names",
                        return_value={"sources": ["S3File", "Jdbc"],
                                      "sinks": ["Console"], "transforms": []}):
            self.assertEqual(_plan_conformance_errors(plan, config), [])

    def test_a_registered_name_is_still_enforced(self):
        plan = StructuredPlan(pipelines=[PipelineSlot("p", "MySQL-CDC", "Console")])

        with mock.patch("seatunnel_cli.connectors.list_connector_names",
                        return_value={"sources": ["MySQL-CDC", "Jdbc"],
                                      "sinks": ["Console"], "transforms": []}):
            errors = _plan_conformance_errors(plan, WRONG_SOURCE)

        self.assertEqual(len(errors), 1)
        self.assertIn("mysql-cdc", errors[0])

    def test_an_unavailable_registry_falls_back_to_comparing_as_given(self):
        with mock.patch("seatunnel_cli.connectors.list_connector_names",
                        side_effect=RuntimeError("no metadata exported")):
            errors = _plan_conformance_errors(CDC_TO_CONSOLE, WRONG_SOURCE)

        self.assertEqual(len(errors), 1)

    def test_unrecognisable_config_is_left_to_the_hocon_checks(self):
        self.assertEqual(_plan_conformance_errors(CDC_TO_CONSOLE, "not hocon"), [])


class DriftReachesTheRepairLoopTest(unittest.TestCase):
    def _run(self, generated_configs):
        orchestrator = Orchestrator(client=mock.MagicMock())
        skill = mock.MagicMock()
        skill.fill_and_check.return_value = []
        skill.fetch_all_metadata.return_value = "## Connector Option Rules\n"
        skill.build_enriched_prompt.return_value = "## User Request:\nx"

        fix_prompts: list[tuple[str, str]] = []

        def run_fix(config, validation_errors, request=None, plan_text=""):
            fix_prompts.append((validation_errors, f"{request}|{plan_text}"))
            return {"config": generated_configs.pop(0), "explanation": ""}

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan", "content": CDC_TO_CONSOLE.raw_plan_text},
        ), mock.patch("seatunnel_cli.skills.SkillExecutor", return_value=skill), \
             mock.patch("seatunnel_cli.skills.SkillRouter.match", return_value=[]), \
             mock.patch("seatunnel_cli.skills.parse_structured_plan",
                        return_value=CDC_TO_CONSOLE), \
             mock.patch.object(
                 orchestrator, "_run_config_generator",
                 return_value={"config": generated_configs.pop(0),
                               "explanation": ""}), \
             mock.patch.object(orchestrator, "_run_validator", return_value="PASS"), \
             mock.patch.object(orchestrator, "_run_fix", side_effect=run_fix), \
             mock.patch("seatunnel_cli.agents.dry_run_config",
                        return_value={"valid": True, "phase1_local": "VALID",
                                      "phase2_check": "PASS", "phase3_api": None,
                                      "summary": ""}):
            result = orchestrator.process_user_input(
                "Watch the users table in MySQL at localhost:3306 and show "
                "changes as they happen.")
        return result, fix_prompts

    def test_a_valid_but_wrong_config_is_sent_for_repair(self):
        # The validator says PASS, so before this the loop delivered it.
        result, fix_prompts = self._run([WRONG_SOURCE, RIGHT_SOURCE])

        self.assertEqual(len(fix_prompts), 1, "drift should trigger one repair")
        errors, context = fix_prompts[0]
        self.assertIn("mysql-cdc", errors)
        self.assertEqual(result["type"], "config")
        self.assertIn("MySQL-CDC", result["config"])

    def test_the_repair_is_told_the_request_and_the_plan(self):
        _result, fix_prompts = self._run([WRONG_SOURCE, RIGHT_SOURCE])

        _errors, context = fix_prompts[0]
        self.assertIn("show changes as they happen", context)
        self.assertIn("MySQL-CDC", context)

    def test_a_conforming_config_is_delivered_without_repair(self):
        result, fix_prompts = self._run([RIGHT_SOURCE])

        self.assertEqual(fix_prompts, [])
        self.assertEqual(result["type"], "config")


class DriftMergedWithValidatorOutputTest(unittest.TestCase):
    """Drift must not displace the validator's own findings.

    The validator's contract is "FAIL: <issue list>", so its first line carries
    a real issue. An earlier version sliced that line off when both drift and a
    validation failure occurred, removing a genuine defect from the repair
    prompt in exactly the case where two things were wrong at once.
    """

    def _validation_seen_by_repair(self, validator_output):
        orchestrator = Orchestrator(client=mock.MagicMock())
        skill = mock.MagicMock()
        skill.fill_and_check.return_value = []
        skill.fetch_all_metadata.return_value = "## Connector Option Rules\n"
        skill.build_enriched_prompt.return_value = "## User Request:\nx"
        seen: list[str] = []

        def run_fix(config, validation_errors, request=None, plan_text=""):
            seen.append(validation_errors)
            return {"config": RIGHT_SOURCE, "explanation": ""}

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan", "content": CDC_TO_CONSOLE.raw_plan_text},
        ), mock.patch("seatunnel_cli.skills.SkillExecutor", return_value=skill), \
             mock.patch("seatunnel_cli.skills.SkillRouter.match", return_value=[]), \
             mock.patch("seatunnel_cli.skills.parse_structured_plan",
                        return_value=CDC_TO_CONSOLE), \
             mock.patch.object(
                 orchestrator, "_run_config_generator",
                 return_value={"config": WRONG_SOURCE, "explanation": ""}), \
             mock.patch.object(orchestrator, "_run_validator",
                               return_value=validator_output), \
             mock.patch.object(orchestrator, "_run_fix", side_effect=run_fix), \
             mock.patch("seatunnel_cli.agents.dry_run_config",
                        return_value={"valid": True, "phase1_local": "VALID",
                                      "phase2_check": "PASS", "phase3_api": None,
                                      "summary": ""}):
            orchestrator.process_user_input("Watch the users table for changes.")
        return seen[0]

    def test_every_validator_issue_survives_the_merge(self):
        merged = self._validation_seen_by_repair(
            "FAIL: missing env block\nplugin_input is not wired\nunknown option foo")

        self.assertIn("missing env block", merged)
        self.assertIn("plugin_input is not wired", merged)
        self.assertIn("unknown option foo", merged)
        self.assertIn("mysql-cdc", merged)

    def test_drift_alone_when_the_validator_passed(self):
        merged = self._validation_seen_by_repair("PASS")

        self.assertIn("mysql-cdc", merged)
        self.assertNotIn("PASS", merged)


class RepairPromptRedactionTest(unittest.TestCase):
    """The request is free text the user typed and can hold a password.

    `_run_fix` already swaps credentials out of the config before calling the
    model; adding the request and the plan unredacted would route around that.
    These assert that the existing scrubber is applied to the new context, so
    the request path has the same protection as the config path.

    How much `redact_credentials` itself catches is a separate matter: it keys
    on `key = value`, `key: value` and "password is X", so a bare "password X"
    in prose is not matched. Widening it would change a helper that session
    persistence and memory also use, and would start redacting phrases like
    "password rotation policy". That belongs in its own change, not here.
    """

    def test_a_password_in_the_request_does_not_reach_the_model(self):
        orchestrator = Orchestrator(client=mock.MagicMock())
        captured = {}

        def chat(messages, system="", **kwargs):
            captured["text"] = "".join(
                block.get("text", "")
                for message in messages for block in message.get("content", []))
            return {"output": {"message": {"content": [
                {"text": "```hocon\nenv {}\nsource {}\nsink {}\n```"}]}},
                "stopReason": "end_turn"}

        orchestrator.client.chat.side_effect = chat

        orchestrator._run_fix(
            "source {\n  Jdbc {\n    url = \"jdbc:mysql://h/db\"\n  }\n}",
            "FAIL: missing driver",
            request=("Copy users from MySQL at localhost:3306. The password "
                     "is Hunter2Secret! and the user is root."),
            plan_text="source: Jdbc\nsink: Console",
        )

        self.assertNotIn("Hunter2Secret!", captured["text"])
        # The rest of the request must survive, or the repair loses the context
        # that lets it tell a wrong connector from a right one.
        self.assertIn("Copy users from MySQL", captured["text"])
        self.assertIn("localhost:3306", captured["text"])
        self.assertIn("source: Jdbc", captured["text"])

    def test_a_key_value_secret_in_the_plan_is_redacted_too(self):
        orchestrator = Orchestrator(client=mock.MagicMock())
        captured = {}

        def chat(messages, system="", **kwargs):
            captured["text"] = "".join(
                block.get("text", "")
                for message in messages for block in message.get("content", []))
            return {"output": {"message": {"content": [
                {"text": "```hocon\nenv {}\nsource {}\nsink {}\n```"}]}},
                "stopReason": "end_turn"}

        orchestrator.client.chat.side_effect = chat

        orchestrator._run_fix(
            "source {\n  Jdbc {}\n}",
            "FAIL: missing url",
            request="Load the table.",
            plan_text="source: Jdbc\nsecret_key=AKIAIOSFODNN7EXAMPLE",
        )

        self.assertNotIn("AKIAIOSFODNN7EXAMPLE", captured["text"])
        self.assertIn("source: Jdbc", captured["text"])


if __name__ == "__main__":
    unittest.main()
