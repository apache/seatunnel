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

import unittest
from types import SimpleNamespace
from unittest import mock

from seatunnel_cli.llm_provider import (
    LLMProvider,
    OrcaRouterProvider,
)


class _FakeOrcaRouterCompletions:
    def __init__(self, stream):
        self.stream = stream
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return self.stream


class _FakeOrcaRouterClient:
    def __init__(self, stream):
        self.completions = _FakeOrcaRouterCompletions(stream)
        self.chat = SimpleNamespace(completions=self.completions)


def _chunk(delta, finish_reason=None):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                delta=delta,
                finish_reason=finish_reason,
            )
        ]
    )


class OrcaRouterProviderTest(unittest.TestCase):
    def test_default_model_and_base_url(self):
        provider = OrcaRouterProvider.__new__(OrcaRouterProvider)
        self.assertEqual(provider.provider_name, "orcarouter")
        self.assertEqual(OrcaRouterProvider.DEFAULT_BASE_URL, "https://api.orcarouter.ai/v1")
        self.assertEqual(OrcaRouterProvider.DEFAULT_MODEL, "orcarouter/auto")

    def test_init_requires_api_key(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            from seatunnel_cli import llm_provider
            with self.assertRaises(ValueError):
                llm_provider.OrcaRouterProvider()

    def test_echo_reasoning_content_env_override(self):
        with mock.patch.dict(
            "os.environ",
            {"ORCAROUTER_API_KEY": "orc_test", "ORCAROUTER_ECHO_REASONING_CONTENT": "false"},
            clear=False,
        ):
            from seatunnel_cli import llm_provider
            provider = llm_provider.OrcaRouterProvider()
            self.assertFalse(provider._echo_reasoning_content)

        with mock.patch.dict(
            "os.environ",
            {"ORCAROUTER_API_KEY": "orc_test", "ORCAROUTER_ECHO_REASONING_CONTENT": "true"},
            clear=False,
        ):
            from seatunnel_cli import llm_provider
            provider = llm_provider.OrcaRouterProvider()
            self.assertTrue(provider._echo_reasoning_content)

        # Defaults to True when unset (parity with OPENAI_ECHO_REASONING_CONTENT).
        with mock.patch.dict("os.environ", {"ORCAROUTER_API_KEY": "orc_test"}, clear=False):
            from seatunnel_cli import llm_provider
            provider = llm_provider.OrcaRouterProvider()
            self.assertTrue(provider._echo_reasoning_content)

    def test_stream_uses_openai_protocol_and_collects_text(self):
        provider = OrcaRouterProvider.__new__(OrcaRouterProvider)
        provider._model_id = "deepseek/deepseek-v4-pro"
        provider._client = _FakeOrcaRouterClient(
            [
                _chunk(SimpleNamespace(content="Sync ", tool_calls=None)),
                _chunk(SimpleNamespace(content="users to S3", tool_calls=None)),
                _chunk(
                    SimpleNamespace(content=None, tool_calls=None),
                    finish_reason="stop",
                ),
            ]
        )

        events = list(
            provider.chat_stream(
                messages=[
                    {
                        "role": "user",
                        "content": [{"text": "sync mysql to s3"}],
                    }
                ]
            )
        )
        response = LLMProvider.collect_stream(events)

        self.assertEqual(
            response["output"]["message"]["content"],
            [{"text": "Sync users to S3"}],
        )
        # The default model is used when no override is passed.
        self.assertEqual(
            provider._client.completions.kwargs["model"], "deepseek/deepseek-v4-pro"
        )

    def test_chat_round_trip(self):
        provider = OrcaRouterProvider.__new__(OrcaRouterProvider)
        provider._model_id = "orcarouter/auto"
        response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content="PLAN: use Jdbc",
                        tool_calls=None,
                    ),
                    finish_reason="stop",
                )
            ]
        )
        provider._client = _FakeOrcaRouterClient(response)

        result = provider.chat(
            messages=[
                {
                    "role": "user",
                    "content": [{"text": "sync oracle to iceberg"}],
                }
            ]
        )

        self.assertEqual(
            result["output"]["message"]["content"],
            [{"text": "PLAN: use Jdbc"}],
        )
        self.assertEqual(result["stopReason"], "end_turn")

    def test_config_model_override_is_independent(self):
        """Config-file model overrides apply even when unrelated model env
        vars (ANTHROPIC_MODEL/OPENAI_MODEL) are set elsewhere."""
        import os
        import tempfile

        from seatunnel_cli import llm_provider

        tmp = tempfile.TemporaryDirectory()
        original = llm_provider.get_config_path

        def fake_get_config_path():
            return os.path.join(tmp.name, "config.json")

        llm_provider.get_config_path = fake_get_config_path
        try:
            with mock.patch.dict(
                "os.environ",
                {"ORCAROUTER_API_KEY": "orc_test", "ANTHROPIC_MODEL": "claude-other"},
                clear=False,
            ):
                llm_provider.save_config(
                    {
                        "provider": "orcarouter",
                        "models": {
                            "orcarouter": {
                                "model": "deepseek/deepseek-v4-pro",
                                "fast_model": "qwen/qwen3.8-flash",
                            }
                        },
                    }
                )
                provider = llm_provider.create_provider()
                self.assertEqual(provider.model_id, "deepseek/deepseek-v4-pro")
                self.assertEqual(provider.fast_model_id, "qwen/qwen3.8-flash")
        finally:
            llm_provider.get_config_path = original
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
