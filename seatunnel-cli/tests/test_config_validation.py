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

"""Tests for environment-variable placeholder validation."""

import os

from seatunnel_cli.agents import validate_hocon


CONFIG = """
env {
  job.mode = "BATCH"
}
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "${ST_TEST_PASSWORD}"
    query = "SELECT * FROM users"
    plugin_output = "rows"
  }
}
sink {
  Console {
    plugin_input = "rows"
  }
}
"""

VAR = "ST_TEST_PASSWORD"


def _validate_with(value):
    """Run validation with VAR set to value, or removed when value is None."""
    previous = os.environ.get(VAR)
    had_previous = VAR in os.environ
    try:
        if value is None:
            os.environ.pop(VAR, None)
        else:
            os.environ[VAR] = value
        return validate_hocon(CONFIG)
    finally:
        if had_previous:
            os.environ[VAR] = previous
        else:
            os.environ.pop(VAR, None)


def test_unset_variable_is_reported():
    assert VAR in _validate_with(None)


def test_set_variable_is_accepted():
    assert VAR not in _validate_with("Test@123")


def test_empty_variable_counts_as_resolved():
    # A passwordless account is normal: Doris and StarRocks default to root
    # with no password, Elasticsearch to no auth. `export VAR=` is a resolved
    # value, so it must not be reported as still needing an export.
    assert VAR not in _validate_with("")
