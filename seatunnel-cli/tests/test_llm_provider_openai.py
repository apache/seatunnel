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
    TruncatedOutputError,
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

    def test_openai_stream_accepts_the_reasoning_field_alias(self):
        """vLLM's reasoning parser names the field `reasoning` from 0.34 on.

        Reading only `reasoning_content` dropped the whole block, and for a
        server that requires it to be replayed after a tool call that became a
        400 on the following request.
        """
        provider = OpenAIProvider.__new__(OpenAIProvider)
        provider._model_id = "reasoning-model"
        provider._client = _FakeOpenAIClient(
            [
                _chunk(SimpleNamespace(
                    content=None, tool_calls=None, model_extra={"reasoning": "weighing sinks"},
                )),
                _chunk(SimpleNamespace(content="env {}", tool_calls=None), finish_reason="stop"),
            ]
        )

        events = list(provider.chat_stream([{"role": "user", "content": [{"text": "hi"}]}]))

        self.assertIn({"type": "reasoning_delta", "text": "weighing sinks"}, events)
        self.assertIn({"type": "text_delta", "text": "env {}"}, events)

    def test_openai_response_accepts_the_reasoning_field_alias(self):
        message = SimpleNamespace(
            content="done", model_extra={"reasoning": "checked"}, tool_calls=None,
        )
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="stop")]
        )

        self.assertEqual(
            OpenAIProvider._from_openai_response(response)["output"]["message"]["content"],
            [{"reasoningContent": "checked"}, {"text": "done"}],
        )

    def test_reasoning_content_wins_when_a_server_sends_both_names(self):
        message = SimpleNamespace(
            content="done",
            model_extra={"reasoning_content": "canonical", "reasoning": "alias"},
            tool_calls=None,
        )
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason="stop")]
        )

        self.assertEqual(
            OpenAIProvider._from_openai_response(response)["output"]["message"]["content"],
            [{"reasoningContent": "canonical"}, {"text": "done"}],
        )


class OpenAIProviderStopReasonTest(unittest.TestCase):
    """Every finish reason used to collapse to `end_turn` except tool_calls.

    That made a response truncated at the token limit indistinguishable from a
    completed one, so the caller went on to parse a config that was cut in half
    -- or absent entirely -- and reported a generic failure instead of the real
    cause.
    """

    @staticmethod
    def _response(finish_reason, content="env {}", tool_calls=None, reasoning=None):
        message = SimpleNamespace(
            content=content,
            tool_calls=tool_calls,
            model_extra={"reasoning_content": reasoning} if reasoning else {},
        )
        return SimpleNamespace(
            choices=[SimpleNamespace(message=message, finish_reason=finish_reason)]
        )

    def test_finish_reasons_are_translated_rather_than_collapsed(self):
        for finish_reason, expected in [
            ("stop", "end_turn"),
            ("length", "max_tokens"),
            ("content_filter", "content_filtered"),
        ]:
            with self.subTest(finish_reason=finish_reason):
                result = OpenAIProvider._from_openai_response(
                    self._response(finish_reason)
                )
                self.assertEqual(result["stopReason"], expected)

    def test_tool_calls_still_map_to_tool_use(self):
        tool_call = SimpleNamespace(
            id="t1",
            function=SimpleNamespace(name="get_connector_detail", arguments="{}"),
        )
        result = OpenAIProvider._from_openai_response(
            self._response("tool_calls", content=None, tool_calls=[tool_call])
        )

        self.assertEqual(result["stopReason"], "tool_use")

    def test_an_unknown_finish_reason_still_ends_the_turn(self):
        """OpenAI-compatible servers add their own values.

        An unfamiliar one is not a reason to fail a call that returned usable
        content, so the previous default is kept.
        """
        result = OpenAIProvider._from_openai_response(self._response("eos_token"))

        self.assertEqual(result["stopReason"], "end_turn")

    def test_truncated_with_no_content_raises_rather_than_returning_an_empty_turn(self):
        with self.assertRaises(TruncatedOutputError) as caught:
            OpenAIProvider._from_openai_response(
                self._response("length", content=None, reasoning="thinking at length")
            )

        self.assertIn("reasoning", str(caught.exception))

    def test_truncated_with_partial_content_is_reported_not_raised(self):
        """A half-written config is still the caller's to judge.

        Only the case with nothing at all to act on is failed here, so this
        change cannot turn a usable short answer into an error.
        """
        result = OpenAIProvider._from_openai_response(
            self._response("length", content="env {\n  parallelism = 1")
        )

        self.assertEqual(result["stopReason"], "max_tokens")
        self.assertEqual(
            result["output"]["message"]["content"], [{"text": "env {\n  parallelism = 1"}]
        )

    def test_streaming_truncation_with_no_content_raises(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)
        provider._model_id = "reasoning-model"
        provider._client = _FakeOpenAIClient(
            [
                _chunk(SimpleNamespace(
                    content=None, tool_calls=None, model_extra={"reasoning": "..."},
                )),
                _chunk(SimpleNamespace(content=None, tool_calls=None), finish_reason="length"),
            ]
        )

        with self.assertRaises(TruncatedOutputError):
            list(provider.chat_stream([{"role": "user", "content": [{"text": "hi"}]}]))

    def test_streaming_truncation_after_text_reports_max_tokens_without_raising(self):
        provider = OpenAIProvider.__new__(OpenAIProvider)
        provider._model_id = "m"
        provider._client = _FakeOpenAIClient(
            [
                _chunk(SimpleNamespace(content="env {", tool_calls=None)),
                _chunk(SimpleNamespace(content=None, tool_calls=None), finish_reason="length"),
            ]
        )

        events = list(provider.chat_stream([{"role": "user", "content": [{"text": "hi"}]}]))

        self.assertEqual(events[-1], {"type": "message_stop", "stop_reason": "max_tokens"})

    def test_truncated_output_error_gets_an_actionable_hint(self):
        message = format_llm_error(
            TruncatedOutputError("The model stopped at the output token limit.")
        )

        self.assertIn("max_tokens", message)
        self.assertIn("disable thinking", message)


if __name__ == "__main__":
    unittest.main()
