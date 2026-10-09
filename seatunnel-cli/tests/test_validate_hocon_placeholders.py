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

"""Regression tests for field-aware engine placeholder handling in validate_hocon."""

import os
from unittest import mock

import pytest
from rich.console import Console

from seatunnel_cli.agents import validate_hocon
from seatunnel_cli.cli import THEME, SeaTunnelCLI, classify_run_env


def _config_with_sink(sink_body: str) -> str:
    return f"""
env {{ parallelism = 1
  job.mode = "BATCH" }}
source {{ FakeSource {{ row.num = 5
  schema {{ fields {{ id = "bigint" }} }}
  plugin_output = "f" }} }}
sink {{ LocalFile {{ plugin_input = "f"
  path = "/tmp/out"
  {sink_body} }} }}
"""


# ── engine placeholders accepted only in their own fields ──

def test_file_name_expression_engine_placeholders_accepted():
    for expr in ("${now}", "${uuid}", "${transactionId}", "out_${uuid}_${now}"):
        config = _config_with_sink(f'file_name_expression = "{expr}"')
        result = validate_hocon(config)
        assert "Unresolved environment variables" not in result, expr


def test_partition_dir_expression_engine_placeholders_accepted():
    config = _config_with_sink(
        'partition_dir_expression = "${k0}=${v0}/${k1}=${v1}"')
    result = validate_hocon(config)
    assert "Unresolved environment variables" not in result


# ── the same names in unrelated fields are still diagnosed ──

@pytest.mark.parametrize("field_line, var", [
    ('url = "jdbc:mysql://${now}:3306/db"', "now"),
    ('password = "${uuid}"', "uuid"),
    ('topic = "${transactionId}"', "transactionId"),
    ('path = "/data/${k0}"', "k0"),
])
def test_engine_placeholder_names_warned_outside_their_fields(field_line, var, monkeypatch):
    monkeypatch.delenv(var, raising=False)
    config = _config_with_sink(field_line)
    result = validate_hocon(config)
    assert result.startswith("VALID")
    assert "Unresolved environment variables" in result
    assert "WARNING:" in result
    assert var in result
    strict = validate_hocon(config, strict_env=True)
    assert strict.startswith("INVALID")
    assert "ERROR:" in strict
    assert var in strict


def test_unset_env_var_still_diagnosed_in_expression_fields(monkeypatch):
    # A non-engine placeholder inside file_name_expression is still an env var
    monkeypatch.delenv("MY_UNSET_PREFIX", raising=False)
    config = _config_with_sink(
        'custom_filename = true\n  '
        'file_name_expression = "${MY_UNSET_PREFIX}_${now}"')
    result = validate_hocon(config)
    assert result.startswith("VALID")
    assert "Unresolved environment variables" in result
    assert "MY_UNSET_PREFIX" in result
    assert validate_hocon(config, strict_env=True).startswith("INVALID")


def test_set_env_var_accepted_anywhere():
    with mock.patch.dict(os.environ, {"MYSQL_PASSWORD": "x"}):
        config = _config_with_sink('password = "${MYSQL_PASSWORD}"')
        result = validate_hocon(config)
        assert "Unresolved environment variables" not in result

# ── HOCON colon separator (key : value) is equally valid ──

def test_colon_separator_engine_placeholders_accepted():
    config = _config_with_sink('file_name_expression: "${now}"')
    result = validate_hocon(config)
    assert "Unresolved environment variables" not in result

    config = _config_with_sink('partition_dir_expression: "${k0}=${v0}"')
    result = validate_hocon(config)
    assert "Unresolved environment variables" not in result


def test_colon_separator_still_diagnoses_env_vars_elsewhere(monkeypatch):
    monkeypatch.delenv("now", raising=False)
    config = _config_with_sink('topic: "${now}"')
    result = validate_hocon(config)
    assert result.startswith("VALID")
    assert "Unresolved environment variables" in result
    assert validate_hocon(config, strict_env=True).startswith("INVALID")


def test_credential_placeholders_are_warnings_by_default(monkeypatch):
    monkeypatch.delenv("MYSQL_USER", raising=False)
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    config = _config_with_sink(
        'user = "${MYSQL_USER}"\n  password = "${MYSQL_PASSWORD}"'
    )
    result = validate_hocon(config)
    assert result.startswith("VALID (with warnings)")
    assert "MYSQL_USER" in result
    assert "MYSQL_PASSWORD" in result
    assert not result.startswith("INVALID")

    strict = validate_hocon(config, strict_env=True)
    assert strict.startswith("INVALID")
    assert "ERROR:" in strict


# ── transform-mediated routing (regression for transform blocks being ──
# ── invisible to _validate_routing_pairs)                             ──

def test_sink_consuming_transform_output_is_valid():
    config = """
env { parallelism = 1
  job.mode = "BATCH" }
source { FakeSource {
    row.num = 5
    schema { fields { id = "bigint" } }
    plugin_output = "raw" } }
transform {
  Sql {
    plugin_input = "raw"
    plugin_output = "filtered"
    query = "SELECT * FROM raw WHERE id > 1"
  }
}
sink { Console { plugin_input = "filtered" } }
"""
    result = validate_hocon(config)
    assert "has no matching plugin_output" not in result


def test_split_via_parallel_transforms_is_valid():
    config = """
env { parallelism = 1
  job.mode = "BATCH" }
source { FakeSource {
    row.num = 5
    schema { fields { id = "bigint" } }
    plugin_output = "raw" } }
transform {
  Sql {
    plugin_input = "raw"
    plugin_output = "big"
    query = "SELECT * FROM raw WHERE id > 100"
  }
  Sql {
    plugin_input = "raw"
    plugin_output = "small"
    query = "SELECT * FROM raw WHERE id <= 100"
  }
}
sink {
  Console { plugin_input = "big" }
  Console { plugin_input = "small" }
}
"""
    result = validate_hocon(config)
    assert "has no matching plugin_output" not in result


def test_genuinely_unmatched_plugin_input_still_rejected():
    config = """
env { parallelism = 1
  job.mode = "BATCH" }
source { FakeSource {
    row.num = 5
    schema { fields { id = "bigint" } }
    plugin_output = "raw" } }
sink { Console { plugin_input = "nonexistent_label" } }
"""
    result = validate_hocon(config)
    assert "nonexistent_label" in result
    assert "has no matching plugin_output" in result


def test_routing_errors_attribute_transform_blocks_correctly():
    # A dangling plugin_input on a TRANSFORM must be reported against the
    # transform block, not misattributed to a source or sink.
    config = """
env { parallelism = 1
  job.mode = "BATCH" }
source { FakeSource {
    row.num = 5
    schema { fields { id = "bigint" } }
    plugin_output = "raw" } }
transform {
  Sql {
    plugin_input = "wrong_label"
    plugin_output = "filtered"
    query = "SELECT * FROM wrong_label"
  }
}
sink { Console { plugin_input = "filtered" } }
"""
    result = validate_hocon(config)
    assert 'transform.Sql: plugin_input "wrong_label"' in result


def test_duplicate_output_across_transforms_reports_transform_location():
    config = """
env { parallelism = 1
  job.mode = "BATCH" }
source { FakeSource {
    row.num = 5
    schema { fields { id = "bigint" } }
    plugin_output = "raw" } }
transform {
  Sql {
    plugin_input = "raw"
    plugin_output = "same_label"
    query = "SELECT * FROM raw WHERE id > 1"
  }
  Sql {
    plugin_input = "raw"
    plugin_output = "same_label"
    query = "SELECT * FROM raw WHERE id <= 1"
  }
}
sink { Console { plugin_input = "same_label" } }
"""
    result = validate_hocon(config)
    assert 'in transform.Sql' in result
    assert 'already used by transform.Sql' in result


def test_benchmark_parse_success_ignores_unset_placeholders(monkeypatch):
    from benchmark.scoring import score_task

    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    result = score_task(
        {"id": "unset-env", "expect": {}},
        _config_with_sink('password = "${MYSQL_PASSWORD}"'),
    )
    assert result.checks["parse_success"]


def _credential_config() -> str:
    return _config_with_sink(
        'user = "${MYSQL_USER}"\n  password = "${MYSQL_PASSWORD}"'
    )


def test_classify_run_env_blocks_local_cli_and_warns_for_rest(monkeypatch):
    monkeypatch.delenv("MYSQL_USER", raising=False)
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    config = _credential_config()

    action, lines = classify_run_env(config, submit_via_rest=False)
    assert action == "block"
    assert lines
    assert "MYSQL_USER" in lines[0]
    assert "MYSQL_PASSWORD" in lines[0]

    action, lines = classify_run_env(config, submit_via_rest=True)
    assert action == "warn"
    assert "MYSQL_USER" in lines[0]


def test_classify_run_env_ok_when_placeholders_are_set(monkeypatch):
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASSWORD", "secret")
    action, lines = classify_run_env(_credential_config(), submit_via_rest=False)
    assert action == "ok"
    assert lines == []


def _cli(monkeypatch, tmp_path) -> tuple[SeaTunnelCLI, Console]:
    monkeypatch.setenv("SEATUNNEL_CLI_DATA", str(tmp_path))
    console = Console(record=True, theme=THEME, width=120, force_terminal=False)
    return SeaTunnelCLI(console), console


def test_run_blocks_before_confirm_on_local_cli(monkeypatch, tmp_path):
    monkeypatch.delenv("MYSQL_USER", raising=False)
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    cli, buf = _cli(monkeypatch, tmp_path)
    cli.last_config = _credential_config()
    prompted = {"called": False}

    def fake_prompt(*_args, **_kwargs):
        prompted["called"] = True
        return "no"

    with mock.patch("seatunnel_cli.connectors._check_engine", return_value=False), \
            mock.patch("seatunnel_cli.cli.pt_prompt", side_effect=fake_prompt):
        cli._run_config()

    assert prompted["called"] is False
    text = buf.export_text()
    assert "Cannot /run" in text
    assert "MYSQL_USER" in text


def test_run_warns_and_still_confirms_for_rest(monkeypatch, tmp_path):
    monkeypatch.delenv("MYSQL_USER", raising=False)
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    cli, buf = _cli(monkeypatch, tmp_path)
    cli.last_config = _credential_config()

    with mock.patch("seatunnel_cli.connectors._check_engine", return_value=True), \
            mock.patch("seatunnel_cli.cli.pt_prompt", return_value="no"):
        cli._run_config()

    text = buf.export_text()
    assert "Cannot /run" not in text
    assert "engine" in text
    assert "MYSQL_PASSWORD" in text
    assert "Execution cancelled" in text


def test_run_continues_to_confirm_when_env_is_set(monkeypatch, tmp_path):
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASSWORD", "secret")
    cli, buf = _cli(monkeypatch, tmp_path)
    cli.last_config = _credential_config()

    with mock.patch("seatunnel_cli.connectors._check_engine", return_value=False), \
            mock.patch("seatunnel_cli.cli.pt_prompt", return_value="no"):
        cli._run_config()

    text = buf.export_text()
    assert "Cannot /run" not in text
    assert "Execution cancelled" in text


def test_check_config_prints_unset_env_warnings(monkeypatch, tmp_path):
    monkeypatch.delenv("MYSQL_USER", raising=False)
    monkeypatch.delenv("MYSQL_PASSWORD", raising=False)
    cli, buf = _cli(monkeypatch, tmp_path)
    cli.last_config = _credential_config()

    with mock.patch("seatunnel_cli.agents._find_seatunnel_sh", return_value=None), \
            mock.patch("seatunnel_cli.connectors._check_engine", return_value=False):
        cli._check_config()

    text = buf.export_text()
    assert "with warnings" in text
    assert "WARNING:" in text
    assert "MYSQL_USER" in text
    assert "MYSQL_PASSWORD" in text
