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

"""Tests for BedrockMantleProvider message/tool format conversion and the
chat/chat_stream response mapping (Responses API <-> internal format)."""

from types import SimpleNamespace
from unittest import mock

import pytest

from seatunnel_cli.llm_provider import BedrockMantleProvider


def _make_provider():
    provider = BedrockMantleProvider.__new__(BedrockMantleProvider)
    provider._region = "us-east-1"
    provider._model_id = "openai.gpt-5.6-terra"
    provider._fast_model_id = "openai.gpt-5.6-terra"
    provider._client = mock.MagicMock()
    provider._token_born = float("inf")  # never refresh in tests
    return provider


# ── input conversion: internal (Converse-shaped) -> Responses API items ──

def test_to_responses_input_text_and_tool_roundtrip():
    messages = [
        {"role": "user", "content": [{"text": "hi"}]},
        {"role": "assistant", "content": [
            {"text": "let me check"},
            {"toolUse": {"toolUseId": "call_1", "name": "calc",
                         "input": {"expr": "6*7"}}},
        ]},
        {"role": "user", "content": [
            {"toolResult": {"toolUseId": "call_1",
                            "content": [{"text": "42"}]}},
        ]},
    ]
    items = BedrockMantleProvider._to_responses_input(messages)
    assert items[0] == {"role": "user", "content": "hi"}
    assert items[1] == {"role": "assistant", "content": "let me check"}
    assert items[2]["type"] == "function_call"
    assert items[2]["call_id"] == "call_1"
    assert items[2]["name"] == "calc"
    assert items[3] == {"type": "function_call_output",
                        "call_id": "call_1", "output": "42"}


def test_to_responses_tools_conversion():
    tools = [{"toolSpec": {
        "name": "calc", "description": "calculate",
        "inputSchema": {"json": {"type": "object",
                                 "properties": {"expr": {"type": "string"}}}},
    }}]
    converted = BedrockMantleProvider._to_responses_tools(tools)
    assert converted == [{
        "type": "function", "name": "calc", "description": "calculate",
        "parameters": {"type": "object",
                       "properties": {"expr": {"type": "string"}}},
    }]


# ── chat: Responses output -> internal format ──

def _resp(output_items):
    return SimpleNamespace(output=output_items)


def _message_item(text):
    return SimpleNamespace(type="message",
                           content=[SimpleNamespace(text=text)])


def _function_call_item(call_id, name, arguments):
    return SimpleNamespace(type="function_call", call_id=call_id,
                           name=name, arguments=arguments)


def test_chat_maps_text_response():
    provider = _make_provider()
    provider._client.responses.create.return_value = _resp(
        [_message_item("OK")])
    resp = provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert resp["stopReason"] == "end_turn"
    assert resp["output"]["message"]["content"] == [{"text": "OK"}]
    # temperature must never be sent (unsupported by these models)
    kwargs = provider._client.responses.create.call_args.kwargs
    assert "temperature" not in kwargs


def test_chat_maps_tool_call_and_bad_json_input():
    provider = _make_provider()
    provider._client.responses.create.return_value = _resp([
        _function_call_item("call_9", "calc", '{"expr": "6*7"}'),
        _function_call_item("call_x", "calc", "NOT JSON"),
    ])
    resp = provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert resp["stopReason"] == "tool_use"
    tool_uses = [b["toolUse"] for b in resp["output"]["message"]["content"]]
    assert tool_uses[0] == {"toolUseId": "call_9", "name": "calc",
                            "input": {"expr": "6*7"}}
    assert tool_uses[1]["input"] == {}  # malformed arguments degrade to {}


def test_chat_passes_system_as_instructions():
    provider = _make_provider()
    provider._client.responses.create.return_value = _resp(
        [_message_item("OK")])
    provider.chat([{"role": "user", "content": [{"text": "hi"}]}],
                  system="be brief")
    kwargs = provider._client.responses.create.call_args.kwargs
    assert kwargs["instructions"] == "be brief"


# ── chat_stream: Responses stream events -> internal events ──

def _ev(type_, **attrs):
    return SimpleNamespace(type=type_, **attrs)


def test_chat_stream_maps_text_and_tool_events():
    provider = _make_provider()
    fc_item = SimpleNamespace(type="function_call", call_id="call_1",
                              name="calc")
    provider._client.responses.create.return_value = iter([
        _ev("response.output_text.delta", delta="hel"),
        _ev("response.output_text.delta", delta="lo"),
        _ev("response.output_item.added", item=fc_item),
        _ev("response.function_call_arguments.delta", delta='{"expr":'),
        _ev("response.function_call_arguments.delta", delta='"6*7"}'),
        _ev("response.output_item.done", item=fc_item),
        _ev("response.completed"),
    ])
    events = list(provider.chat_stream(
        [{"role": "user", "content": [{"text": "hi"}]}]))
    types = [e["type"] for e in events]
    assert types == ["text_delta", "text_delta", "tool_start",
                     "tool_input_delta", "tool_input_delta", "tool_stop",
                     "message_stop"]
    assert events[2] == {"type": "tool_start", "tool_use_id": "call_1",
                         "name": "calc"}
    assert events[-1]["stop_reason"] == "tool_use"


def test_chat_stream_plain_text_ends_with_end_turn():
    provider = _make_provider()
    provider._client.responses.create.return_value = iter([
        _ev("response.output_text.delta", delta="OK"),
        _ev("response.completed"),
    ])
    events = list(provider.chat_stream(
        [{"role": "user", "content": [{"text": "hi"}]}]))
    assert events[-1] == {"type": "message_stop", "stop_reason": "end_turn"}


# ── stream events integrate with LLMProvider.collect_stream ──

def test_stream_events_collect_to_internal_response():
    provider = _make_provider()
    fc_item = SimpleNamespace(type="function_call", call_id="call_1",
                              name="calc")
    provider._client.responses.create.return_value = iter([
        _ev("response.output_item.added", item=fc_item),
        _ev("response.function_call_arguments.delta", delta='{"expr": "1"}'),
        _ev("response.output_item.done", item=fc_item),
        _ev("response.completed"),
    ])
    events = list(provider.chat_stream(
        [{"role": "user", "content": [{"text": "hi"}]}]))
    resp = BedrockMantleProvider.collect_stream(events)
    assert resp["stopReason"] == "tool_use"
    tool_uses = BedrockMantleProvider.extract_tool_use(resp)
    assert tool_uses == [{"toolUseId": "call_1", "name": "calc",
                          "input": {"expr": "1"}}]


# ── endpoint construction ──

def test_client_uses_documented_mantle_openai_path():
    """These models are served on the `openai/v1` path of the bedrock-mantle
    endpoint — NOT the generic `v1` path used by other Responses-API models.
    See the AWS model card for gpt-5.6-terra ("available on the
    openai/v1/responses path ... different from the v1/responses path").
    Empirically, the generic /v1 path rejects these models with
    "does not support the '/v1/responses' API"."""
    provider = BedrockMantleProvider.__new__(BedrockMantleProvider)
    provider._region = "eu-west-3"
    provider._model_id = "m"
    provider._fast_model_id = "m"
    provider._client = None
    provider._token_born = 0.0

    fake_openai = mock.MagicMock()
    fake_generator = mock.MagicMock()
    fake_generator.provide_token.return_value = "tok"
    with mock.patch.dict("sys.modules", {
        "openai": fake_openai,
        "aws_bedrock_token_generator": fake_generator,
    }):
        provider._get_client()
    kwargs = fake_openai.OpenAI.call_args.kwargs
    assert kwargs["base_url"] == \
        "https://bedrock-mantle.eu-west-3.api.aws/openai/v1"
    assert kwargs["api_key"] == "tok"


# ── token refresh ──

def test_client_refreshes_after_ttl():
    provider = BedrockMantleProvider.__new__(BedrockMantleProvider)
    provider._region = "us-east-1"
    provider._model_id = "m"
    provider._fast_model_id = "m"
    provider._client = None
    provider._token_born = 0.0

    fake_openai = mock.MagicMock()
    fake_generator = mock.MagicMock()
    fake_generator.provide_token.return_value = "tok"
    with mock.patch.dict("sys.modules", {
        "openai": fake_openai,
        "aws_bedrock_token_generator": fake_generator,
    }):
        provider._get_client()
        assert fake_generator.provide_token.called
        first_client = provider._client
        # within TTL: same client reused
        provider._get_client()
        assert provider._client is first_client
        # expire TTL: new token requested
        provider._token_born = -10_000.0
        provider._get_client()
        assert fake_generator.provide_token.call_count == 2

# ── store=False on every request (Bedrock retains data by default) ──

def test_chat_sends_store_false():
    provider = _make_provider()
    provider._client.responses.create.return_value = _resp(
        [_message_item("OK")])
    provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    assert provider._client.responses.create.call_args.kwargs["store"] is False


def test_chat_stream_sends_store_false():
    provider = _make_provider()
    provider._client.responses.create.return_value = iter([
        _ev("response.completed"),
    ])
    list(provider.chat_stream([{"role": "user", "content": [{"text": "hi"}]}]))
    assert provider._client.responses.create.call_args.kwargs["store"] is False


# ── reasoning items are preserved and replayed in order ──

def _reasoning_item():
    item = mock.MagicMock()
    item.type = "reasoning"
    item.model_dump.return_value = {
        "type": "reasoning", "id": "rs_1",
        "summary": [], "content": None,
    }
    return item


def test_chat_preserves_reasoning_for_replay():
    provider = _make_provider()
    provider._client.responses.create.return_value = _resp([
        _reasoning_item(),
        _function_call_item("call_1", "calc", '{"expr": "1"}'),
    ])
    resp = provider.chat([{"role": "user", "content": [{"text": "hi"}]}])
    blocks = resp["output"]["message"]["content"]
    assert blocks[0] == {"mantleResponsesItem": {
        "type": "reasoning", "id": "rs_1", "summary": [], "content": None}}
    assert "toolUse" in blocks[1]

    # replay: history containing the reasoning block converts back verbatim,
    # in order, before the function_call item
    history = [
        {"role": "user", "content": [{"text": "hi"}]},
        resp["output"]["message"],
        {"role": "user", "content": [{"toolResult": {
            "toolUseId": "call_1", "content": [{"text": "1"}]}}]},
    ]
    items = BedrockMantleProvider._to_responses_input(history)
    assert items[1] == {"type": "reasoning", "id": "rs_1",
                        "summary": [], "content": None}
    assert items[2]["type"] == "function_call"
    assert items[3]["type"] == "function_call_output"


def test_chat_stream_forwards_reasoning_items():
    provider = _make_provider()
    r_item = mock.MagicMock()
    r_item.type = "reasoning"
    r_item.model_dump.return_value = {"type": "reasoning", "id": "rs_2"}
    provider._client.responses.create.return_value = iter([
        _ev("response.output_item.added", item=r_item),
        _ev("response.output_item.done", item=r_item),
        _ev("response.completed"),
    ])
    events = list(provider.chat_stream(
        [{"role": "user", "content": [{"text": "hi"}]}]))
    assert {"type": "mantle_responses_item",
            "item": {"type": "reasoning", "id": "rs_2"}} in events
    # and collect_stream lands it in history
    resp = BedrockMantleProvider.collect_stream(events)
    assert {"mantleResponsesItem": {"type": "reasoning", "id": "rs_2"}} \
        in resp["output"]["message"]["content"]


# ── terminal states must not become successful end_turns ──

def test_chat_incomplete_status_raises():
    provider = _make_provider()
    response = _resp([_message_item("truncated par")])
    response.status = "incomplete"
    response.incomplete_details = mock.MagicMock(reason="max_output_tokens")
    response.error = None
    provider._client.responses.create.return_value = response
    with pytest.raises(RuntimeError, match="incomplete.*max_output_tokens"):
        provider.chat([{"role": "user", "content": [{"text": "hi"}]}])


def test_chat_refusal_raises():
    provider = _make_provider()
    refusal_part = mock.MagicMock()
    refusal_part.type = "refusal"
    refusal_part.refusal = "cannot help with that"
    msg = mock.MagicMock()
    msg.type = "message"
    msg.content = [refusal_part]
    provider._client.responses.create.return_value = _resp([msg])
    with pytest.raises(RuntimeError, match="refused"):
        provider.chat([{"role": "user", "content": [{"text": "hi"}]}])


def test_chat_stream_failed_event_raises():
    provider = _make_provider()
    provider._client.responses.create.return_value = iter([
        _ev("response.output_text.delta", delta="par"),
        _ev("response.failed", response=None, message="internal model error"),
    ])
    with pytest.raises(RuntimeError, match="abnormally"):
        list(provider.chat_stream(
            [{"role": "user", "content": [{"text": "hi"}]}]))

# ── dependency contract: every extra bundling this provider must satisfy it ──

def test_extras_bundling_mantle_share_responses_capable_floor():
    """The provider calls client.responses.create, which needs a modern
    openai SDK. Any extra that installs this provider (bedrock-mantle, all,
    dev) must therefore pin the same floor — a lower one would install a
    broken provider (review finding)."""
    import re
    from pathlib import Path
    pyproject = Path(__file__).parent.parent / "pyproject.toml"
    extras = {}
    for line in pyproject.read_text().splitlines():
        m = re.match(r'^([\w-]+)\s*=\s*\[(.*)\]', line.strip())
        if m:
            extras[m.group(1)] = m.group(2)
    for extra in ("bedrock-mantle", "all", "dev"):
        assert extra in extras, f"extra '{extra}' missing"
        deps = extras[extra]
        m = re.search(r'openai>=([\d.]+)', deps)
        assert m, f"extra '{extra}' has no openai floor"
        version = tuple(int(x) for x in m.group(1).split("."))
        assert version >= (2, 45, 0), \
            f"extra '{extra}' allows openai {m.group(1)} < 2.45.0"
        assert "aws-bedrock-token-generator" in deps, \
            f"extra '{extra}' missing aws-bedrock-token-generator"
