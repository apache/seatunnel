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

"""Tests for the failure excerpt handed to the config-repair prompt."""

from seatunnel_cli.cli import (
    _ERROR_EXCERPT_LIMIT,
    _OMITTED_MARKER,
    _build_failure_report,
)


def _long_trace() -> str:
    """A trace longer than the excerpt limit whose root cause is at the very end.

    This is the realistic shape: Zeta wraps the connector failure, so the
    generic wrapper is printed first and the specific `Caused by` last. Long
    traces are produced by deep frame chains, not by a long first line.
    """
    head = (
        "org.apache.seatunnel.core.starter.exception.CommandExecuteException: "
        "SeaTunnel job executed failed\n"
    )
    filler = "".join(
        "\tat org.apache.seatunnel.engine.server.task.SeaTunnelTask."
        f"stateProcess(SeaTunnelTask.java:{line})\n"
        for line in range(100, 140)
    )
    while len(head + filler) <= _ERROR_EXCERPT_LIMIT:
        filler += filler
    tail = (
        "Caused by: org.apache.seatunnel.common.exception.SeaTunnelRuntimeException: "
        "ErrorCode:[JDBC-05], ErrorDescription:[Connect to database failed]\n"
        "Caused by: java.sql.SQLException: Access denied for user 'bench'\n"
    )
    return head + filler + tail


def test_root_cause_is_parsed_from_the_end_of_an_oversized_trace():
    # The whole point of parsing before truncating: the error code lives past
    # the excerpt limit, so a parser fed the truncated text would see nothing.
    trace = _long_trace()
    assert len(trace) > _ERROR_EXCERPT_LIMIT

    parsed, _ = _build_failure_report(trace)

    assert parsed.code == "JDBC-05"
    assert parsed.description == "Connect to database failed"
    assert parsed.component == "JDBC"


def test_excerpt_keeps_the_tail_rather_than_the_head():
    trace = _long_trace()

    _, excerpt = _build_failure_report(trace)

    assert excerpt.startswith(_OMITTED_MARKER)
    assert len(excerpt) == len(_OMITTED_MARKER) + _ERROR_EXCERPT_LIMIT
    # The root cause must survive; the outer wrapper is what may be dropped.
    assert "Access denied for user" in excerpt
    assert "ErrorCode:[JDBC-05]" in excerpt
    assert "CommandExecuteException" not in excerpt


def test_short_trace_is_forwarded_unchanged_and_unmarked():
    trace = "java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver\n"

    parsed, excerpt = _build_failure_report(trace)

    assert excerpt == trace
    assert _OMITTED_MARKER not in excerpt
    assert parsed.exception == "java.lang.ClassNotFoundException"


def test_a_trace_exactly_at_the_limit_is_not_marked_as_cut():
    trace = "x" * _ERROR_EXCERPT_LIMIT

    _, excerpt = _build_failure_report(trace)

    assert excerpt == trace


def test_empty_and_non_string_input_do_not_raise():
    # This runs while the CLI is already reporting a failure; raising here would
    # replace the user's real error with ours.
    for value in ("", None):
        parsed, excerpt = _build_failure_report(value)
        assert excerpt == ""
        assert parsed.signature == "empty"
