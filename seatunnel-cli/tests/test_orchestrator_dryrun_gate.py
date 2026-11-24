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

"""The engine's verdict on a config has to be able to change the outcome.

The engine `--check` ran only after the LLM validator had already passed the
config, and its result was appended to the explanation as a note while the
config was returned as a normal result. So the one gate that actually loads
the config could not stop it being delivered, and `_run_fix` was never given
an engine error to work from -- it only ever saw the validator's text.
"""

import unittest
from unittest import mock

from seatunnel_cli.agents import Orchestrator, _engine_rejection


CONFIG_A = """env {
  job.mode = "BATCH"
}
source {
  FakeSource {}
}
sink {
  Console {}
}
"""

CONFIG_B = CONFIG_A.replace('job.mode = "BATCH"', 'job.mode = "BATCH"\n  parallelism = 1')


def _dryrun(phase2, valid=True, local="VALID"):
    return {
        "valid": valid,
        "phase1_local": local,
        "phase2_check": phase2,
        "phase3_api": None,
        "summary": "",
    }


class EngineRejectionTest(unittest.TestCase):
    """`phase2_check` has four shapes and only one of them is a rejection."""

    def test_an_explicit_failure_is_a_rejection(self):
        self.assertEqual(
            _engine_rejection(_dryrun("FAIL: ConfigRuntimeException: no such option")),
            "FAIL: ConfigRuntimeException: no such option",
        )

    def test_a_pass_is_not_a_rejection(self):
        self.assertIsNone(_engine_rejection(_dryrun("PASS")))

    def test_a_missing_engine_is_not_a_rejection(self):
        """`None` means nothing was checked, not that the config is bad.

        Gating on it would stop every host without the engine installed from
        ever receiving a config.
        """
        self.assertIsNone(_engine_rejection(_dryrun(None)))

    def test_a_check_that_could_not_run_is_not_a_rejection(self):
        """A subprocess timeout says nothing about the config.

        Treating anything that is not PASS as a rejection would send a good
        config into a rewrite because the engine was slow.
        """
        self.assertIsNone(_engine_rejection(_dryrun("TIMEOUT (30s)")))
        self.assertIsNone(_engine_rejection(_dryrun("ERROR: boom")))


class EngineRejectionReachesRepairTest(unittest.TestCase):
    def _run(self, dryruns, generated=None):
        """Drive the loop with a scripted sequence of dry-run outcomes."""
        orchestrator = Orchestrator(client=mock.MagicMock())
        skill = mock.MagicMock()
        skill.fill_and_check.return_value = []
        skill.fetch_all_metadata.return_value = "## Connector Option Rules\n"
        skill.build_enriched_prompt.return_value = "## User Request:\nx"

        configs = list(generated or [CONFIG_B])
        fix_calls: list[str] = []

        def run_fix(config, validation_errors, request=None, plan_text=""):
            fix_calls.append(validation_errors)
            if not configs:
                return {"config": None, "explanation": ""}
            return {"config": configs.pop(0), "explanation": ""}

        plan = mock.MagicMock(pipelines=[mock.MagicMock()], raw_plan_text="")

        with mock.patch.object(
            orchestrator, "_run_planner",
            return_value={"type": "plan", "content": "source: FakeSource\nsink: Console"},
        ), mock.patch("seatunnel_cli.skills.SkillExecutor", return_value=skill), \
             mock.patch("seatunnel_cli.skills.SkillRouter.match", return_value=[]), \
             mock.patch("seatunnel_cli.skills.parse_structured_plan", return_value=plan), \
             mock.patch("seatunnel_cli.agents._plan_conformance_errors", return_value=[]), \
             mock.patch.object(
                 orchestrator, "_run_config_generator",
                 return_value={"config": CONFIG_A, "explanation": ""}), \
             mock.patch.object(orchestrator, "_run_validator", return_value="PASS"), \
             mock.patch.object(orchestrator, "_run_fix", side_effect=run_fix), \
             mock.patch("seatunnel_cli.agents.dry_run_config",
                        side_effect=list(dryruns)):
            result = orchestrator.process_user_input("print the fake table")
        return result, fix_calls

    def test_a_rejected_config_is_repaired_instead_of_delivered(self):
        """Before this the validator's PASS delivered it with a footnote."""
        result, fix_calls = self._run(
            [_dryrun("FAIL: ConfigRuntimeException: option 'x' not recognised",
                     valid=False),
             _dryrun("PASS")]
        )

        self.assertEqual(len(fix_calls), 1, "the engine rejection should drive one repair")
        self.assertEqual(result["type"], "config")
        self.assertEqual(result["config"], CONFIG_B, "the repaired config is delivered")

    def test_the_repair_is_given_the_engine_error_itself(self):
        _result, fix_calls = self._run(
            [_dryrun("FAIL: ConfigRuntimeException: option 'x' not recognised",
                     valid=False),
             _dryrun("PASS")]
        )

        self.assertIn("the engine rejected this config", fix_calls[0])
        self.assertIn("option 'x' not recognised", fix_calls[0])

    def test_an_accepted_config_is_delivered_without_repair(self):
        result, fix_calls = self._run([_dryrun("PASS")])

        self.assertEqual(fix_calls, [])
        self.assertEqual(result["config"], CONFIG_A)
        self.assertIn("**Dry-run:** PASSED", result["explanation"])

    def test_a_host_without_the_engine_still_gets_its_config(self):
        result, fix_calls = self._run([_dryrun(None)])

        self.assertEqual(fix_calls, [], "a missing engine must not trigger repair")
        self.assertEqual(result["config"], CONFIG_A)

    def test_a_check_that_timed_out_does_not_trigger_repair(self):
        result, fix_calls = self._run([_dryrun("TIMEOUT (30s)", valid=False)])

        self.assertEqual(fix_calls, [])
        self.assertEqual(result["config"], CONFIG_A)
        self.assertIn("TIMEOUT (30s)", result["explanation"])

    def test_an_unresolved_rejection_is_reported_not_called_a_warning(self):
        """The repair budget running out is not a style note.

        Three rounds all rejected, and the repair keeps returning a config, so
        the loop exhausts its budget and takes the best-effort exit.
        """
        rejection = _dryrun("FAIL: ConfigRuntimeException: still wrong", valid=False)
        result, fix_calls = self._run(
            [rejection, rejection, rejection],
            generated=[CONFIG_B, CONFIG_A, CONFIG_B],
        )

        self.assertEqual(len(fix_calls), 3)
        self.assertEqual(result["type"], "config")
        self.assertIn("validation did not pass", result["content"])
        self.assertIn("still wrong", result["content"])
        self.assertIn("last_dry_run", result)


if __name__ == "__main__":
    unittest.main()
