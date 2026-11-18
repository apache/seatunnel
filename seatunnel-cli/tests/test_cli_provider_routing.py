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

"""Regression tests for CLI --provider/--model routing of the
bedrock-mantle provider family (issue: mantle models were routed to
ANTHROPIC_MODEL and the provider was missing from argparse choices)."""

import os
import sys
from unittest import mock

import pytest


def _run_main_until_provider(argv):
    """Run cli.main() with argv, stopping right after env routing."""
    from seatunnel_cli import cli

    captured = {}

    class _Stop(Exception):
        pass

    def fake_console(*a, **k):
        # capture env state at the point the CLI would build the console
        captured["AI_PROVIDER"] = os.environ.get("AI_PROVIDER")
        captured["OPENAI_MODEL"] = os.environ.get("OPENAI_MODEL")
        captured["ANTHROPIC_MODEL"] = os.environ.get("ANTHROPIC_MODEL")
        captured["ORCAROUTER_MODEL"] = os.environ.get("ORCAROUTER_MODEL")
        captured["UBERLLM_MODEL"] = os.environ.get("UBERLLM_MODEL")
        captured["UBERLLM_SMALL_FAST_MODEL"] = os.environ.get(
            "UBERLLM_SMALL_FAST_MODEL")
        captured["ANTHROPIC_SMALL_FAST_MODEL"] = os.environ.get(
            "ANTHROPIC_SMALL_FAST_MODEL")
        raise _Stop()

    with mock.patch.object(sys, "argv", ["seatunnel"] + argv), \
            mock.patch.object(cli, "Console", side_effect=fake_console), \
            pytest.raises(_Stop):
        cli.main()
    return captured


@pytest.fixture(autouse=True)
def _clean_env():
    saved = {k: os.environ.pop(k, None) for k in
             ("AI_PROVIDER", "OPENAI_MODEL", "ANTHROPIC_MODEL",
              "OPENAI_SMALL_FAST_MODEL", "ANTHROPIC_SMALL_FAST_MODEL",
              "ORCAROUTER_MODEL", "ORCAROUTER_SMALL_FAST_MODEL",
              "UBERLLM_MODEL", "UBERLLM_SMALL_FAST_MODEL")}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_bedrock_mantle_accepted_by_argparse_and_routes_openai_model():
    captured = _run_main_until_provider(
        ["--provider", "bedrock-mantle", "--model", "openai.gpt-5.6-sol", "hi"])
    assert captured["AI_PROVIDER"] == "bedrock-mantle"
    assert captured["OPENAI_MODEL"] == "openai.gpt-5.6-sol"
    assert captured["ANTHROPIC_MODEL"] is None


def test_bedrock_still_routes_anthropic_model():
    captured = _run_main_until_provider(
        ["--provider", "bedrock", "--model", "us.anthropic.claude-sonnet-5", "hi"])
    assert captured["ANTHROPIC_MODEL"] == "us.anthropic.claude-sonnet-5"
    assert captured["OPENAI_MODEL"] is None


def test_orcarouter_accepted_by_argparse_and_routes_own_model():
    captured = _run_main_until_provider(
        ["--provider", "orcarouter", "--model", "deepseek/deepseek-v4-pro", "hi"])
    assert captured["AI_PROVIDER"] == "orcarouter"
    assert captured["ORCAROUTER_MODEL"] == "deepseek/deepseek-v4-pro"
    assert captured["OPENAI_MODEL"] is None
    assert captured["ANTHROPIC_MODEL"] is None


def test_uberllm_accepted_by_argparse_and_routes_own_model():
    captured = _run_main_until_provider(
        ["--provider", "uberllm", "--model", "anthropic/claude-sonnet-4.6", "hi"])
    assert captured["AI_PROVIDER"] == "uberllm"
    assert captured["UBERLLM_MODEL"] == "anthropic/claude-sonnet-4.6"
    assert captured["ORCAROUTER_MODEL"] is None
    assert captured["OPENAI_MODEL"] is None
    assert captured["ANTHROPIC_MODEL"] is None


@pytest.fixture
def _isolated_config(tmp_path, monkeypatch):
    """Point the CLI config at an empty temp dir and drop every credential
    the auto-detection looks at, so only what the test sets is visible."""
    from seatunnel_cli import llm_provider

    path = tmp_path / "config.json"
    monkeypatch.setattr(llm_provider, "get_config_path", lambda: str(path))
    for var in ("ANTHROPIC_API_KEY", "ORCAROUTER_API_KEY", "UBERLLM_API_KEY",
                "OPENAI_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    return llm_provider


def test_uberllm_from_saved_config_gets_model_flags(_isolated_config):
    """--model/--fast-model must reach a provider chosen by config.json,
    not only one named with --provider."""
    _isolated_config.save_config({"provider": "uberllm"})
    captured = _run_main_until_provider(
        ["--model", "z-ai/glm-5.3", "--fast-model", "qwen/qwen3-coder", "hi"])
    assert captured["AI_PROVIDER"] is None
    assert captured["UBERLLM_MODEL"] == "z-ai/glm-5.3"
    assert captured["UBERLLM_SMALL_FAST_MODEL"] == "qwen/qwen3-coder"
    assert captured["ANTHROPIC_MODEL"] is None
    assert captured["ANTHROPIC_SMALL_FAST_MODEL"] is None
    assert captured["OPENAI_MODEL"] is None


def test_uberllm_auto_detected_gets_model_flags(_isolated_config, monkeypatch):
    """Same when the provider is auto-detected from UBERLLM_API_KEY."""
    monkeypatch.setenv("UBERLLM_API_KEY", "ull_test")
    captured = _run_main_until_provider(
        ["--model", "z-ai/glm-5.3", "--fast-model", "qwen/qwen3-coder", "hi"])
    assert captured["AI_PROVIDER"] is None
    assert captured["UBERLLM_MODEL"] == "z-ai/glm-5.3"
    assert captured["UBERLLM_SMALL_FAST_MODEL"] == "qwen/qwen3-coder"
    assert captured["ANTHROPIC_MODEL"] is None
    assert captured["ANTHROPIC_SMALL_FAST_MODEL"] is None


def test_provider_flag_still_wins_over_saved_config(_isolated_config):
    _isolated_config.save_config({"provider": "uberllm"})
    captured = _run_main_until_provider(
        ["--provider", "bedrock", "--model", "us.anthropic.claude-sonnet-5", "hi"])
    assert captured["ANTHROPIC_MODEL"] == "us.anthropic.claude-sonnet-5"
    assert captured["UBERLLM_MODEL"] is None


def test_resolve_provider_name_matches_create_provider_order(
        _isolated_config, monkeypatch):
    resolve = _isolated_config.resolve_provider_name
    assert resolve() is None or resolve() == "bedrock"  # ambient AWS files only
    monkeypatch.setenv("UBERLLM_API_KEY", "ull_test")
    assert resolve() == "uberllm"                       # auto-detect
    _isolated_config.save_config({"provider": "openai"})
    assert resolve() == "openai"                        # saved config
    monkeypatch.setenv("AI_PROVIDER", "Anthropic ")
    assert resolve() == "anthropic"                     # env, normalized
    assert resolve("orcarouter") == "orcarouter"        # explicit


def test_uberllm_setup_key_prompt_is_password_mode(_isolated_config):
    """The setup wizard must not echo the UberLLM key while it is typed,
    and must not print it afterwards."""
    import io

    from rich.console import Console

    from seatunnel_cli import cli

    secret = "ull_secret_value_1234567890"
    calls = []

    def fake_prompt(message, **kwargs):
        calls.append((message, kwargs))
        if "choice" in message:
            return "6"
        if "API key" in message:
            return secret
        return ""

    out = io.StringIO()
    console = Console(file=out, theme=cli.THEME, width=200)
    with mock.patch.object(cli, "pt_prompt", side_effect=fake_prompt), \
            mock.patch.dict("sys.modules", {"openai": mock.MagicMock()}):
        cli.SeaTunnelCLI.run_init(console)

    key_prompts = [kw for msg, kw in calls if "API key" in msg]
    assert key_prompts == [{"is_password": True}]
    assert secret not in out.getvalue()
    assert os.environ.pop("UBERLLM_API_KEY", None) == secret


def test_unknown_provider_rejected():
    from seatunnel_cli import cli
    with mock.patch.object(sys, "argv",
                           ["seatunnel", "--provider", "nonsense", "hi"]), \
            pytest.raises(SystemExit):
        cli.main()
