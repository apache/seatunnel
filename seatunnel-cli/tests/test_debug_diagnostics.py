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

"""Tests for CLI --debug / SEATUNNEL_CLI_DEBUG pipeline diagnostics."""

import os
from unittest import mock

import pytest

from seatunnel_cli.agents import Orchestrator
from seatunnel_cli.debug import (
    DEBUG_ENV,
    enable_debug,
    error_message,
    format_debug_line,
    is_debug_enabled,
    redact_and_truncate,
    redact_text,
)


@pytest.fixture(autouse=True)
def _clear_debug_env(monkeypatch):
    monkeypatch.delenv(DEBUG_ENV, raising=False)


def test_is_debug_enabled_and_enable():
    assert is_debug_enabled() is False
    enable_debug()
    assert is_debug_enabled() is True


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1", True),
        ("true", True),
        ("YES", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("", False),
    ],
)
def test_is_debug_enabled_env_values(monkeypatch, value, expected):
    monkeypatch.setenv(DEBUG_ENV, value)
    assert is_debug_enabled() is expected


def test_redact_and_truncate_secrets_and_length():
    text = "Authorization: Bearer " + ("a" * 50) + " password=supersecret"
    redacted = redact_text(text)
    assert "supersecret" not in redacted
    assert "***REDACTED***" in redacted
    # Use spaced text so the generic long-token redactor does not collapse it.
    long_text = ("word " * 400).strip()
    truncated = redact_and_truncate(long_text, max_len=50)
    assert len(truncated) == 50
    assert truncated.endswith("...")


def test_first_line_reason():
    from seatunnel_cli.debug import first_line_reason

    assert first_line_reason("FAIL: missing sink\nmore") == "FAIL: missing sink"
    assert "password=" not in first_line_reason("FAIL password=secret value")


def test_error_message_known_and_fallback():
    assert "HOCON" in error_message("no_hocon_block")
    assert error_message("custom_code") == "custom code"


def test_redact_keeps_hocon_prose():
    """Long HOCON-like lines must not be wiped by an over-aggressive token regex."""
    text = 'url = "jdbc:mysql://mysql-host:3306/mydb?useSSL=false&serverTimezone=UTC"'
    assert "jdbc:mysql" in redact_text(text)


def test_format_debug_line_includes_stage_fields():
    line = format_debug_line(
        "generator",
        model="glm-5.3",
        stop_reason="end_turn",
        tools=0,
        outcome="no_hocon_block",
    )
    assert line.startswith("[debug] stage=generator")
    assert "model=glm-5.3" in line
    assert "outcome=no_hocon_block" in line


def test_parse_config_response_error_codes():
    assert Orchestrator._parse_config_response("")["error_code"] == "empty_response"
    parsed = Orchestrator._parse_config_response("sorry I cannot help")
    assert parsed["config"] is None
    assert parsed["error_code"] == "no_hocon_block"
    assert "cannot help" in parsed["raw_text"]

    ok = Orchestrator._parse_config_response(
        "```hocon\nenv {}\nsource {}\ntransform {}\nsink {}\n```\n"
    )
    assert ok["config"]
    assert "error_code" not in ok


def test_cli_debug_flag_enables_env():
    from seatunnel_cli import cli

    class _Stop(Exception):
        pass

    def fake_console(*a, **k):
        raise _Stop()

    with mock.patch.object(cli.sys, "argv", ["seatunnel", "--debug"]), \
            mock.patch.object(cli, "Console", side_effect=fake_console), \
            pytest.raises(_Stop):
        cli.main()
    assert is_debug_enabled() is True


def test_process_user_input_soft_fail_includes_reason_and_debug_chain():
    """Planner ok + generator text without HOCON → soft error with reason; debug fires."""

    from seatunnel_cli.skills import PipelineSlot, StructuredPlan

    class FakeClient:
        provider_name = "openai"
        model_id = "test-model"
        fast_model_id = "test-fast"

        def chat_stream(self, messages, system="", tools=None, **kwargs):
            if not hasattr(self, "_n"):
                self._n = 0
            self._n += 1
            if self._n == 1:
                yield {
                    "type": "text_delta",
                    "text": "PLAN:\nJdbc source to Console sink",
                }
                yield {"type": "message_stop", "stop_reason": "end_turn"}
            else:
                yield {"type": "text_delta", "text": "I refuse to emit a code block."}
                yield {"type": "message_stop", "stop_reason": "end_turn"}

        def quick_chat(self, prompt, system="", use_fast_model=True):
            return "[]"

    enable_debug()
    events = []

    def on_debug(stage, **fields):
        events.append((stage, fields))

    real_plan = StructuredPlan(
        pipelines=[
            PipelineSlot(
                pipeline_id="p1",
                source_connector="Jdbc",
                sink_connector="Console",
                tables=["t"],
            )
        ]
    )

    orch = Orchestrator(client=FakeClient(), on_debug=on_debug)
    with mock.patch(
        "seatunnel_cli.skills.SkillRouter.match",
        return_value=[],
    ), mock.patch(
        "seatunnel_cli.skills.SkillExecutor.fill_and_check",
        return_value=[],
    ), mock.patch(
        "seatunnel_cli.skills.SkillExecutor.fetch_all_metadata",
        return_value="",
    ), mock.patch(
        "seatunnel_cli.skills.SkillExecutor.build_enriched_prompt",
        return_value="## User Request:\nhello",
    ), mock.patch(
        "seatunnel_cli.skills.parse_structured_plan",
        return_value=real_plan,
    ):
        result = orch.process_user_input("生成 mysql 到 console")

    assert result["type"] == "error"
    assert result["error_code"] == "no_hocon_block"
    assert "no HOCON code block" in result["content"]
    stages = [s for s, _ in events]
    assert stages[0] == "start"
    assert "planner" in stages
    assert "generator" in stages
    assert stages[-1] == "result"
    gen = next(f for s, f in events if s == "generator")
    assert gen.get("outcome") == "no_hocon_block"
    assert "refuse" in (gen.get("snippet") or "")
