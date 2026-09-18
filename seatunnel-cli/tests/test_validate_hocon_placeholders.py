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

from seatunnel_cli.agents import validate_hocon


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
def test_engine_placeholder_names_rejected_outside_their_fields(field_line, var):
    assert var not in os.environ
    config = _config_with_sink(field_line)
    result = validate_hocon(config)
    assert "Unresolved environment variables" in result
    assert var in result


def test_unset_env_var_still_rejected_in_expression_fields():
    # A non-engine placeholder inside file_name_expression is still an env var
    assert "MY_UNSET_PREFIX" not in os.environ
    config = _config_with_sink(
        'file_name_expression = "${MY_UNSET_PREFIX}_${now}"')
    result = validate_hocon(config)
    assert "Unresolved environment variables" in result
    assert "MY_UNSET_PREFIX" in result


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


def test_colon_separator_still_rejects_env_vars_elsewhere():
    assert "now" not in os.environ
    config = _config_with_sink('topic: "${now}"')
    result = validate_hocon(config)
    assert "Unresolved environment variables" in result


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
