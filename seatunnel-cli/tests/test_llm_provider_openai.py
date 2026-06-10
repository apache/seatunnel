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

from seatunnel_cli.llm_provider import (
    LLMProvider,
    OpenAIProvider,
    format_llm_error,
)


class _FakeOpenAICompletions:
    def __init__(self, stream):
        self.stream = stream
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return self.stream


class _FakeOpenAIClient:
    def __init__(self, stream):
        self.completions = _FakeOpenAICompletions(stream)
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


class OpenAIProviderReasoningContentTest(unittest.TestCase):
    def test_openai_stream_preserves_reasoning_content_delta(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)
        provider._model_id = "reasoning-model"
        provider._client = _FakeOpenAIClient(
            [
                _chunk(
                    SimpleNamespace(
                        content=None,
                        reasoning_content="inspect connector metadata",
                        tool_calls=None,
                    )
                ),
                _chunk(SimpleNamespace(content="PLAN: use Jdbc", tool_calls=None)),
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
                        "content": [{"text": "sync oracle to iceberg"}],
                    }
                ]
            )
        )
        response = LLMProvider.collect_stream(events)

        self.assertEqual(
            response["output"]["message"]["content"],
            [
                {"reasoningContent": "inspect connector metadata"},
                {"text": "PLAN: use Jdbc"},
            ],
        )

    def test_openai_messages_send_reasoning_content_back_to_api(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)

        messages = [
            {
                "role": "assistant",
                "content": [
                    {"reasoningContent": "need source and sink connector metadata"},
                    {"text": "PLAN: inspect connectors"},
                    {
                        "toolUse": {
                            "toolUseId": "tool-1",
                            "name": "get_connector_info",
                            "input": {"name": "Jdbc", "type": "source"},
                        }
                    },
                ],
            }
        ]

        self.assertEqual(
            provider._to_openai_messages(messages),
            [
                {
                    "role": "assistant",
                    "content": "PLAN: inspect connectors",
                    "reasoning_content": "need source and sink connector metadata",
                    "tool_calls": [
                        {
                            "id": "tool-1",
                            "type": "function",
                            "function": {
                                "name": "get_connector_info",
                                "arguments": '{"name": "Jdbc", "type": "source"}',
                            },
                        }
                    ],
                }
            ],
        )

    def test_openai_messages_skip_empty_reasoning_content_for_tool_calls(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)

        messages = [
            {
                "role": "assistant",
                "content": [
                    {
                        "toolUse": {
                            "toolUseId": "tool-1",
                            "name": "get_connector_info",
                            "input": {"name": "Doris", "connector_type": "sink"},
                        }
                    },
                ],
            }
        ]

        self.assertEqual(
            provider._to_openai_messages(messages),
            [
                {
                    "role": "assistant",
                    "tool_calls": [
                        {
                            "id": "tool-1",
                            "type": "function",
                            "function": {
                                "name": "get_connector_info",
                                "arguments": '{"name": "Doris", "connector_type": "sink"}',
                            },
                        }
                    ],
                }
            ],
        )

    def test_openai_messages_can_disable_reasoning_content_echo(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)
        provider._echo_reasoning_content = False

        messages = [
            {
                "role": "assistant",
                "content": [
                    {"reasoningContent": "provider-specific hidden state"},
                    {"text": "done"},
                ],
            }
        ]

        self.assertEqual(
            provider._to_openai_messages(messages),
            [{"role": "assistant", "content": "done"}],
        )

    def test_openai_messages_ignore_bedrock_reasoning_content_blocks(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)

        messages = [
            {
                "role": "assistant",
                "content": [
                    {
                        "reasoningContent": {
                            "reasoningText": {
                                "text": "provider-specific hidden state",
                                "signature": "sig-1",
                            }
                        }
                    },
                    {"text": "done"},
                ],
            }
        ]

        self.assertEqual(
            provider._to_openai_messages(messages),
            [{"role": "assistant", "content": "done"}],
        )

    def test_openai_response_preserves_reasoning_content_from_extra_fields(self):
        message = SimpleNamespace(
            content="done",
            model_extra={"reasoning_content": "checked config shape"},
            tool_calls=None,
        )
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="stop")]
        )

        self.assertEqual(
            OpenAIProvider._from_openai_response(response),
            {
                "output": {
                    "message": {
                        "role": "assistant",
                        "content": [
                            {"reasoningContent": "checked config shape"},
                            {"text": "done"},
                        ],
                    }
                },
                "stopReason": "end_turn",
            },
        )

    def test_reasoning_content_error_gets_actionable_hint(self):
        err = Exception(
            "Error code: 400 - Param Incorrect: "
            "The reasoning_content in the thinking mode must be passed back to the API."
        )

        message = format_llm_error(err)

        self.assertIn("reasoning_content", message)
        self.assertIn("OpenAI-compatible reasoning model", message)
        self.assertIn("OPENAI_ECHO_REASONING_CONTENT", message)


if __name__ == "__main__":
    unittest.main()
