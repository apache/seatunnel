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
