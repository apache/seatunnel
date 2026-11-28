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
    MartLLMProvider,
)


class _FakeMartLLMCompletions:
    def __init__(self, stream):
        self.stream = stream
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return self.stream


class _FakeMartLLMClient:
    def __init__(self, stream):
        self.completions = _FakeMartLLMCompletions(stream)
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


class _fake_openai:
    """Replace the openai SDK module so no real client (and no connection
    pool) is created, and the SDK does not have to be installed."""

    def __enter__(self):
        self.module = mock.MagicMock()
        self._patch = mock.patch.dict("sys.modules", {"openai": self.module})
        self._patch.start()
        return self.module

    def __exit__(self, *exc):
        self._patch.stop()
        return False


class MartLLMProviderTest(unittest.TestCase):
    def test_default_model_and_base_url(self):
        provider = MartLLMProvider.__new__(MartLLMProvider)
        self.assertEqual(provider.provider_name, "martllm")
        self.assertEqual(MartLLMProvider.DEFAULT_BASE_URL, "https://api.martllm.com/v1")
        self.assertEqual(MartLLMProvider.DEFAULT_MODEL, "deepseek/deepseek-v4-pro")
        self.assertEqual(MartLLMProvider.DEFAULT_FAST_MODEL, "deepseek/deepseek-v4.1-flash")

    def test_init_requires_api_key(self):
        with mock.patch.dict("os.environ", {}, clear=True), _fake_openai():
            with self.assertRaises(ValueError):
                MartLLMProvider()

    def test_init_does_not_fall_back_to_openai_key_or_base_url(self):
        env = {"OPENAI_API_KEY": "sk-other", "OPENAI_BASE_URL": "https://example.invalid/v1"}
        with mock.patch.dict("os.environ", env, clear=True), _fake_openai():
            with self.assertRaises(ValueError):
                MartLLMProvider()

        env["MARTLLM_API_KEY"] = "ull_test"
        with mock.patch.dict("os.environ", env, clear=True), _fake_openai() as fake:
            MartLLMProvider()
            fake.OpenAI.assert_called_once_with(
                api_key="ull_test", base_url="https://api.martllm.com/v1"
            )

    def test_model_precedence(self):
        """MARTLLM_* wins, then OPENAI_*, then the class defaults."""
        key = {"MARTLLM_API_KEY": "ull_test"}
        with mock.patch.dict("os.environ", key, clear=True), _fake_openai():
            provider = MartLLMProvider()
            self.assertEqual(provider.model_id, MartLLMProvider.DEFAULT_MODEL)
            self.assertEqual(provider.fast_model_id, MartLLMProvider.DEFAULT_FAST_MODEL)

        openai_only = dict(key, OPENAI_MODEL="o-main", OPENAI_SMALL_FAST_MODEL="o-fast")
        with mock.patch.dict("os.environ", openai_only, clear=True), _fake_openai():
            provider = MartLLMProvider()
            self.assertEqual(provider.model_id, "o-main")
            self.assertEqual(provider.fast_model_id, "o-fast")

        both = dict(openai_only, MARTLLM_MODEL="u-main", MARTLLM_SMALL_FAST_MODEL="u-fast")
        with mock.patch.dict("os.environ", both, clear=True), _fake_openai():
            provider = MartLLMProvider()
            self.assertEqual(provider.model_id, "u-main")
            self.assertEqual(provider.fast_model_id, "u-fast")

    def test_echo_reasoning_content_env_override(self):
        key = {"MARTLLM_API_KEY": "ull_test"}
        with mock.patch.dict(
            "os.environ", dict(key, MARTLLM_ECHO_REASONING_CONTENT="false"), clear=True
        ), _fake_openai():
            self.assertFalse(MartLLMProvider()._echo_reasoning_content)

        with mock.patch.dict(
            "os.environ", dict(key, MARTLLM_ECHO_REASONING_CONTENT="true"), clear=True
        ), _fake_openai():
            self.assertTrue(MartLLMProvider()._echo_reasoning_content)

        # Defaults to True when unset (parity with OPENAI_ECHO_REASONING_CONTENT).
        with mock.patch.dict("os.environ", key, clear=True), _fake_openai():
            self.assertTrue(MartLLMProvider()._echo_reasoning_content)

    def test_stream_uses_openai_protocol_and_collects_text(self):
        provider = MartLLMProvider.__new__(MartLLMProvider)
        provider._model_id = "deepseek/deepseek-v4-pro"
        provider._client = _FakeMartLLMClient(
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
        provider = MartLLMProvider.__new__(MartLLMProvider)
        provider._model_id = "anthropic/claude-sonnet-4.6"
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
        provider._client = _FakeMartLLMClient(response)

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
                {"MARTLLM_API_KEY": "ull_test", "ANTHROPIC_MODEL": "claude-other"},
                clear=True,
            ), _fake_openai():
                llm_provider.save_config(
                    {
                        "provider": "martllm",
                        "models": {
                            "martllm": {
                                "model": "deepseek/deepseek-v4-pro",
                                "fast_model": "deepseek/deepseek-v4.1-flash",
                            }
                        },
                    }
                )
                provider = llm_provider.create_provider()
                self.assertEqual(provider.model_id, "deepseek/deepseek-v4-pro")
                self.assertEqual(provider.fast_model_id, "deepseek/deepseek-v4.1-flash")
        finally:
            llm_provider.get_config_path = original
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
