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
              "ORCAROUTER_MODEL", "ORCAROUTER_SMALL_FAST_MODEL")}
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


def test_unknown_provider_rejected():
    from seatunnel_cli import cli
    with mock.patch.object(sys, "argv",
                           ["seatunnel", "--provider", "nonsense", "hi"]), \
            pytest.raises(SystemExit):
        cli.main()
