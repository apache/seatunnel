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

"""Regression tests for benchmark gate-skip accounting and repair-round
semantics (review findings: skipped L2/L3 must not count as full-gate
passes; product-internal repair rounds must be observable)."""

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent.parent))

from benchmark.runner import evaluate_layers  # noqa: E402
from benchmark.report import _aggregate  # noqa: E402

VALID_CONFIG = """
env { parallelism = 1
  job.mode = "BATCH" }
source { FakeSource { row.num = 5
  schema { fields { id = "bigint" } }
  plugin_output = "f" } }
sink { Console { plugin_input = "f" } }
"""

TASK = {
    "id": "t_test", "tier": 1, "category": "smoke", "name": "t", 
    "prompt": "p",
    "expect": {"source": ["FakeSource"], "sink": ["Console"],
               "job_mode": "BATCH", "must_match": [], "must_not_match": []},
    "execution": {"services": [], "mode": "batch", "l3": "run"},
}


def test_skipped_l2_l3_marked_not_executed_not_passed():
    with mock.patch("benchmark.execution.run_dry_run",
                    return_value={"passed": None, "detail": "SKIPPED: x",
                                  "seconds": 0.0}), \
         mock.patch("benchmark.execution.run_execute",
                    return_value={"passed": None, "detail": "SKIPPED: y",
                                  "seconds": 0.0}):
        result = evaluate_layers(TASK, VALID_CONFIG, ["l1", "l2", "l3"])
    assert result["all_passed"] is True          # executed layers all passed
    assert result["skipped_layers"] == ["l2", "l3"]
    assert result["all_gates_executed"] is False  # but coverage is partial


def test_executed_l3_failure_still_fails():
    with mock.patch("benchmark.execution.run_dry_run",
                    return_value={"passed": True, "detail": "PASS",
                                  "seconds": 0.1}), \
         mock.patch("benchmark.execution.run_execute",
                    return_value={"passed": False, "detail": "boom",
                                  "seconds": 0.1}):
        result = evaluate_layers(TASK, VALID_CONFIG, ["l1", "l2", "l3"])
    assert result["all_passed"] is False
    assert result["failed_layer"] == "l3"
    assert result["all_gates_executed"] is True


def _trial(first_pass_round, all_gates_executed=True, internal=0):
    return {
        "attempts": [{"round": 0, "seconds": 1.0, "layers": {
            "l1": {"passed": True}, "l2": None, "l3": None,
            "all_passed": first_pass_round is not None,
            "failed_layer": None, "error_detail": "",
            "skipped_layers": [] if all_gates_executed else ["l3"],
            "all_gates_executed": all_gates_executed,
        }}],
        "first_pass_round": first_pass_round,
        "internal_repair_rounds": internal,
        "clarification_asked": False,
        "generation_error": None,
        "trial": 0,
    }


def test_aggregate_counts_incomplete_gate_trials():
    results = {
        "levels": ["l1", "l2", "l3"], "max_repairs": 3, "trials": 1,
        "cli": {}, "models": [{
            "name": "m", "config": {}, "tasks": [
                {"task_id": "a", "tier": 1, "category": "smoke",
                 "trials": [_trial(0, all_gates_executed=False)],
                 "pass_all_trials": True},
                {"task_id": "b", "tier": 1, "category": "smoke",
                 "trials": [_trial(0, all_gates_executed=True)],
                 "pass_all_trials": True},
            ],
        }],
    }
    row = _aggregate(results)[0]
    assert row["incomplete_gate_trials"] == 1


def test_internal_repair_rounds_preserved_in_trial_record():
    t = _trial(0, internal=2)
    assert t["internal_repair_rounds"] == 2
