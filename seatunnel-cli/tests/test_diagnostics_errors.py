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

"""Tests for structured failure parsing."""

from seatunnel_cli.diagnostics import parse_error


# Shape taken from SeaTunnelErrorCode#getErrorMessage plus the wrapper chain
# Zeta puts around a connector failure.
JDBC_AUTH_TRACE = """
org.apache.seatunnel.core.starter.exception.CommandExecuteException: SeaTunnel job executed failed
\tat org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand.execute(ClientExecuteCommand.java:191)
Caused by: org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: ErrorCode:[JDBC-05], ErrorDescription:[Connect to database failed]
\tat org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat.open(JdbcOutputFormat.java:142)
Caused by: java.sql.SQLException: Access denied for user 'bench'@'10.0.0.4' (using password: YES)
\tat com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:129)
"""

MISSING_DRIVER_TRACE = """
java.lang.RuntimeException: Failed to create sink writer
Caused by: java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver
\tat java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:445)
"""

CONFIG_TRACE = (
    "org.apache.seatunnel.api.configuration.util.OptionValidationException: "
    "ErrorCode:[API-02], ErrorDescription:[Option item validate failed]"
)

NESTED_BRACKET_TRACE = (
    "ErrorCode:[COMMON-22], "
    "ErrorDescription:[SeaTunnel write file [/tmp/out/part-0] failed]"
)


def test_extracts_code_root_cause_and_prefers_concrete_category():
    parsed = parse_error(JDBC_AUTH_TRACE)
    assert parsed.code == "JDBC-05"
    assert parsed.namespace == "JDBC"
    assert parsed.component == "JDBC"
    assert parsed.description == "Connect to database failed"
    # The innermost cause, not the outer CommandExecuteException.
    assert parsed.exception == "java.sql.SQLException"
    assert "Access denied for user" in parsed.root_cause
    # "auth" explains the failure; "connector" (from the JDBC namespace) does
    # not, so the concrete text match must win.
    assert parsed.category == "auth"
    assert parsed.hint and "user/password" in parsed.hint


def test_headline_leads_with_code_and_description():
    assert parse_error(JDBC_AUTH_TRACE).headline() == (
        "JDBC-05: Connect to database failed"
    )


def test_classifies_missing_driver_without_an_error_code():
    parsed = parse_error(MISSING_DRIVER_TRACE)
    assert parsed.code is None
    assert parsed.category == "missing_dependency"
    assert parsed.exception == "java.lang.ClassNotFoundException"
    assert parsed.root_cause == "com.mysql.cj.jdbc.Driver"
    assert parsed.hint and "plugins" in parsed.hint


def test_infrastructure_namespace_maps_to_config_not_connector():
    parsed = parse_error(CONFIG_TRACE)
    assert parsed.code == "API-02"
    assert parsed.category == "config"
    # API is not a connector, so no component is claimed.
    assert parsed.component is None
    assert parsed.hint and "option rule" in parsed.hint


def test_description_may_contain_brackets():
    parsed = parse_error(NESTED_BRACKET_TRACE)
    assert parsed.code == "COMMON-22"
    assert parsed.description == "SeaTunnel write file [/tmp/out/part-0] failed"


def test_unknown_input_admits_it_rather_than_guessing():
    parsed = parse_error("something went sideways")
    assert parsed.category == "unknown"
    assert parsed.hint is None
    assert parsed.code is None


def test_blank_input_is_safe():
    for blank in (None, "", "   \n\t "):
        parsed = parse_error(blank)
        assert parsed.category == "unknown"
        assert parsed.signature == "empty"


def test_signature_collapses_volatile_detail():
    first = parse_error(
        "Caused by: java.sql.SQLException: Access denied for user 'a'@'10.0.0.4'"
    )
    second = parse_error(
        "Caused by: java.sql.SQLException: Access denied for user 'b'@'10.0.0.9'"
    )
    assert first.signature == second.signature
    # A different failure must not collapse into the same key.
    assert parse_error(MISSING_DRIVER_TRACE).signature != first.signature


def test_all_codes_are_retained_in_order():
    parsed = parse_error(
        "ErrorCode:[JDBC-05], ErrorDescription:[Connect to database failed]\n"
        "Caused by: ErrorCode:[COMMON-01], ErrorDescription:[read file failed]"
    )
    assert parsed.code == "JDBC-05"
    assert parsed.codes == ("JDBC-05", "COMMON-01")
    assert parsed.as_dict()["codes"] == ["JDBC-05", "COMMON-01"]


def test_as_dict_omits_empty_fields():
    data = parse_error("something went sideways").as_dict()
    assert "hint" not in data
    assert "code" not in data
    assert data["category"] == "unknown"
