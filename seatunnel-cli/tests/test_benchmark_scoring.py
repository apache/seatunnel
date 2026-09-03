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

"""Tests for the benchmark task suite, scoring, and layered evaluation."""

import re

from benchmark.runner import load_tasks, evaluate_layers
from benchmark.scoring import score_task, WEIGHTS
from benchmark.execution import CREDENTIALS, _variable_args


GOOD_CONFIG = """
env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    query = "SELECT * FROM users"
    plugin_output = "jdbc_out"
  }
}
sink {
  Console {
    plugin_input = "jdbc_out"
  }
}
"""


def test_weights_sum_to_one():
    assert abs(sum(WEIGHTS.values()) - 1.0) < 1e-9


def test_all_tiers_load_with_execution_metadata():
    tasks = load_tasks([1, 2, 3])
    assert len(tasks) >= 25
    ids = [t["id"] for t in tasks]
    assert len(ids) == len(set(ids)), "duplicate task ids"
    for task in tasks:
        assert task["prompt"].strip()
        assert task.get("category"), f"{task['id']} missing category"
        expect = task["expect"]
        assert expect["source"] and expect["sink"]
        assert expect["job_mode"] in ("BATCH", "STREAMING")
        for pattern in expect.get("must_match", []) + expect.get("must_not_match", []):
            re.compile(pattern)
        execution = task["execution"]
        assert execution["mode"] in ("batch", "streaming")
        assert execution["l3"] in ("run", "skip")
        assert isinstance(execution["services"], list)
        # streaming tasks must be declared as streaming execution and vice versa
        assert (expect["job_mode"] == "STREAMING") == (execution["mode"] == "streaming")


def test_tier_filter_and_task_filter():
    tier1 = load_tasks([1])
    assert all(t["tier"] == 1 for t in tier1)
    one = load_tasks([1], task_ids=["t1_mysql_console"])
    assert len(one) == 1


def test_good_config_passes_l1():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    result = score_task(task, GOOD_CONFIG)
    assert result.passed
    assert result.score == 1.0


def test_wrong_connector_fails():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    bad = GOOD_CONFIG.replace("Console", "Clickhouse")
    result = score_task(task, bad)
    assert not result.checks["connector_match"]
    assert not result.passed


def test_wrong_job_mode_fails():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    bad = GOOD_CONFIG.replace('"BATCH"', '"STREAMING"')
    result = score_task(task, bad)
    assert not result.checks["job_mode_match"]


def test_broken_hocon_fails_parse():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    result = score_task(task, GOOD_CONFIG + "\n}")
    assert not result.checks["parse_success"]


def test_empty_config_scores_zero():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    for empty in (None, "", "   "):
        result = score_task(task, empty)
        assert result.score == 0.0
        assert not result.passed


def test_evaluate_layers_l1_only():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    layers = evaluate_layers(task, GOOD_CONFIG, levels=["l1"])
    assert layers["all_passed"]
    assert layers["l1"]["passed"]
    assert layers["l2"] is None and layers["l3"] is None


def test_evaluate_layers_stops_at_first_failure():
    task = load_tasks([1], task_ids=["t1_mysql_console"])[0]
    bad = GOOD_CONFIG.replace("Console", "Clickhouse")
    layers = evaluate_layers(task, bad, levels=["l1", "l2", "l3"])
    assert not layers["all_passed"]
    assert layers["failed_layer"] == "l1"
    assert "connector_match" in layers["error_detail"]
    assert layers["l2"] is None  # never reached


def test_variable_args_resolves_known_placeholders():
    config = 'password = "${MYSQL_PASSWORD}"\nuser = "${MYSQL_USER}"'
    args = _variable_args(config)
    assert "-i" in args
    joined = " ".join(args)
    assert f"MYSQL_PASSWORD={CREDENTIALS['MYSQL_PASSWORD']}" in joined
    assert f"MYSQL_USER={CREDENTIALS['MYSQL_USER']}" in joined


def test_variable_args_ignores_unknown_placeholders():
    args = _variable_args('x = "${TOTALLY_UNKNOWN_VAR_XYZ}"')
    assert args == []
