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

"""LLM Provider abstraction layer.

Supports multiple backends while presenting a unified interface:
  - bedrock  : AWS Bedrock Converse API (Claude models)
  - anthropic: Anthropic Messages API (direct)
  - openai   : OpenAI Chat Completions API

All providers normalize their responses to a common internal format
so that the agent layer (agents.py) needs no provider-specific code.

Selection: set AI_PROVIDER env var, or run --init for interactive setup.
"""

import abc
import json
import logging
import os
from typing import Generator

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean environment flag."""
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def format_llm_error(error: Exception) -> str:
    """Return an actionable error message for common provider protocol issues."""
    message = str(error)
    lower = message.lower()
    if "reasoning_content" in lower and "passed back" in lower:
        return (
            f"{message}\n\n"
            "Hint: This looks like an OpenAI-compatible reasoning model "
            "requiring reasoning_content replay after a tool call. SeaTunnel CLI "
            "preserves and sends reasoning_content by default for the OpenAI "
            "provider. If this session was created before the fix, start a fresh "
            "session with /new. Also make sure OPENAI_ECHO_REASONING_CONTENT is "
            "not set to false."
        )
    if "thinking" in lower and "signature" in lower:
        return (
            f"{message}\n\n"
            "Hint: This looks like a Claude thinking blocks replay issue. "
            "SeaTunnel CLI preserves Anthropic thinking/signature blocks and "
            "Bedrock reasoningContent blocks in assistant history. If this "
            "session was created before the fix, start a fresh session with /new."
        )
    return message


# ─── Common internal message format ───
# We reuse the Bedrock Converse API message shape as our internal format:
#
#   messages = [
#       {"role": "user",      "content": [{"text": "..."}, ...]},
#       {"role": "assistant", "content": [{"text": "..."}, {"toolUse": {...}}, ...]},
#   ]
#
#   response = {
#       "output": {"message": {"role": "assistant", "content": [...]}},
#       "stopReason": "end_turn" | "tool_use",
#   }
#
# Provider-owned reasoning state is preserved as opaque replay data:
#   {"reasoningContent": "..."}                         # OpenAI-compatible
#   {"reasoningContent": {"reasoningText": {...}}}      # Bedrock Converse
#   {"anthropicThinking": {"thinking": "...", ...}}     # Anthropic Messages
#
# Tools follow the Bedrock Converse toolSpec shape:
#   {"toolSpec": {"name": "...", "description": "...", "inputSchema": {"json": {...}}}}


class LLMProvider(abc.ABC):
    """Abstract base for LLM providers."""

    @property
    @abc.abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider name."""
        ...

    @property
    @abc.abstractmethod
    def model_id(self) -> str:
        """Primary model identifier."""
        ...

    @property
    @abc.abstractmethod
    def fast_model_id(self) -> str:
        """Fast/small model identifier."""
        ...

    @abc.abstractmethod
    def chat(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> dict:
        """Send a chat request and return the full response (internal format)."""
        ...

    @abc.abstractmethod
    def chat_stream(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> Generator[dict, None, None]:
        """Send a chat request and stream response events."""
        ...

    def quick_chat(self, prompt: str, system: str = "", use_fast_model: bool = True) -> str:
        """Simple single-turn chat, returns text. Uses fast model by default."""
        model = self.fast_model_id if use_fast_model else None
        messages = [{"role": "user", "content": [{"text": prompt}]}]
        resp = self.chat(messages, system=system, model=model, max_tokens=2048)
        return self.extract_text(resp)

    @staticmethod
    def extract_text(response: dict) -> str:
        """Extract text content from internal-format response."""
        parts = []
        for block in response.get("output", {}).get("message", {}).get("content", []):
            if "text" in block:
                parts.append(block["text"])
        return "\n".join(parts)

    @staticmethod
    def extract_tool_use(response: dict) -> list[dict]:
        """Extract tool use blocks from internal-format response."""
        tool_uses = []
        for block in response.get("output", {}).get("message", {}).get("content", []):
            if "toolUse" in block:
                tool_uses.append(block["toolUse"])
        return tool_uses

    @staticmethod
    def collect_stream(events: list[dict]) -> dict:
        """Reconstruct a full internal-format response from collected stream events."""
        content: list[dict] = []
        current_text = ""
        current_reasoning = ""
        current_anthropic_thinking: dict | None = None
        current_bedrock_reasoning: dict | None = None
        current_tool: dict | None = None
        stop_reason = "end_turn"

        def flush_text() -> None:
            nonlocal current_text
            if current_text:
                content.append({"text": current_text})
                current_text = ""

        def flush_reasoning() -> None:
            nonlocal current_reasoning
            if current_reasoning:
                content.append({"reasoningContent": current_reasoning})
                current_reasoning = ""

        def flush_anthropic_thinking() -> None:
            nonlocal current_anthropic_thinking
            if current_anthropic_thinking:
                block = {
                    "thinking": current_anthropic_thinking.get("thinking", ""),
                }
                signature = current_anthropic_thinking.get("signature", "")
                if signature:
                    block["signature"] = signature
                if block["thinking"] or signature:
                    content.append({"anthropicThinking": block})
                current_anthropic_thinking = None

        def flush_bedrock_reasoning() -> None:
            nonlocal current_bedrock_reasoning
            if current_bedrock_reasoning:
                reasoning_text = current_bedrock_reasoning.get("reasoningText")
                if isinstance(reasoning_text, dict) and not reasoning_text.get("signature"):
                    reasoning_text.pop("signature", None)
                content.append({"reasoningContent": current_bedrock_reasoning})
                current_bedrock_reasoning = None

        def flush_model_state() -> None:
            flush_reasoning()
            flush_anthropic_thinking()
            flush_bedrock_reasoning()

        for event in events:
            etype = event.get("type", "")
            if etype == "text_delta":
                flush_model_state()
                current_text += event["text"]
            elif etype == "reasoning_delta":
                flush_text()
                flush_anthropic_thinking()
                flush_bedrock_reasoning()
                current_reasoning += event["text"]
            elif etype == "thinking_start":
                flush_text()
                flush_reasoning()
                flush_bedrock_reasoning()
                flush_anthropic_thinking()
                current_anthropic_thinking = {
                    "thinking": event.get("thinking", ""),
                    "signature": event.get("signature", ""),
                }
            elif etype == "thinking_delta":
                flush_text()
                flush_reasoning()
                flush_bedrock_reasoning()
                if current_anthropic_thinking is None:
                    current_anthropic_thinking = {"thinking": "", "signature": ""}
                current_anthropic_thinking["thinking"] += event.get("text", "")
            elif etype == "signature_delta":
                if current_anthropic_thinking is None:
                    current_anthropic_thinking = {"thinking": "", "signature": ""}
                current_anthropic_thinking["signature"] += event.get("signature", "")
            elif etype == "thinking_stop":
                flush_anthropic_thinking()
            elif etype == "redacted_thinking":
                flush_text()
                flush_model_state()
                content.append({
                    "anthropicRedactedThinking": {"data": event.get("data", "")}
                })
            elif etype == "bedrock_reasoning_delta":
                flush_text()
                flush_reasoning()
                flush_anthropic_thinking()
                redacted = event.get("redacted_content")
                if redacted is not None:
                    flush_bedrock_reasoning()
                    content.append({"reasoningContent": {"redactedContent": redacted}})
                else:
                    if (
                        current_bedrock_reasoning is None
                        or "reasoningText" not in current_bedrock_reasoning
                    ):
                        current_bedrock_reasoning = {
                            "reasoningText": {"text": "", "signature": ""}
                        }
                    reasoning_text = current_bedrock_reasoning["reasoningText"]
                    reasoning_text["text"] += event.get("text", "")
                    if event.get("signature"):
                        reasoning_text["signature"] += event["signature"]
            elif etype == "tool_start":
                flush_text()
                flush_model_state()
                current_tool = {
                    "toolUseId": event["tool_use_id"],
                    "name": event["name"],
                    "input_json": "",
                }
            elif etype == "tool_input_delta":
                if current_tool:
                    current_tool["input_json"] += event.get("delta", "")
            elif etype == "tool_stop":
                if current_tool:
                    try:
                        parsed = json.loads(current_tool["input_json"])
                    except (json.JSONDecodeError, ValueError):
                        parsed = {}
                    content.append({
                        "toolUse": {
                            "toolUseId": current_tool["toolUseId"],
                            "name": current_tool["name"],
                            "input": parsed,
                        }
                    })
                    current_tool = None
            elif etype == "message_stop":
                stop_reason = event.get("stop_reason", "end_turn")

        flush_text()
        flush_model_state()

        return {
            "output": {"message": {"role": "assistant", "content": content}},
            "stopReason": stop_reason,
        }


# ─── Bedrock Provider ───

class BedrockProvider(LLMProvider):
    """AWS Bedrock Converse API provider (original implementation)."""

    def __init__(self):
        import boto3

        region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        endpoint_url = os.environ.get("ANTHROPIC_BEDROCK_BASE_URL")

        self._model_id = os.environ.get("ANTHROPIC_MODEL", "us.anthropic.claude-sonnet-4-20250514-v1:0")
        self._fast_model_id = os.environ.get(
            "ANTHROPIC_SMALL_FAST_MODEL", "us.anthropic.claude-haiku-4-5-20251001-v1:0"
        )

        session = boto3.Session(region_name=region)
        client_kwargs = {"service_name": "bedrock-runtime", "region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self.client = session.client(**client_kwargs)

    @property
    def provider_name(self) -> str:
        return "bedrock"

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def fast_model_id(self) -> str:
        return self._fast_model_id

    def chat(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> dict:
        model_id = model or self._model_id
        kwargs = {
            "modelId": model_id,
            "messages": messages,
            "inferenceConfig": {"temperature": temperature, "maxTokens": max_tokens},
        }
        if system:
            kwargs["system"] = [{"text": system}]
        if tools:
            kwargs["toolConfig"] = {"tools": tools}

        response = self.client.converse(**kwargs)
        return response

    def chat_stream(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> Generator[dict, None, None]:
        model_id = model or self._model_id
        kwargs = {
            "modelId": model_id,
            "messages": messages,
            "inferenceConfig": {"temperature": temperature, "maxTokens": max_tokens},
        }
        if system:
            kwargs["system"] = [{"text": system}]
        if tools:
            kwargs["toolConfig"] = {"tools": tools}

        response = self.client.converse_stream(**kwargs)
        current_tool_id = None
        for event in response.get("stream", []):
            if "contentBlockStart" in event:
                start = event["contentBlockStart"].get("start", {})
                if "toolUse" in start:
                    tu = start["toolUse"]
                    current_tool_id = tu.get("toolUseId", "")
                    yield {"type": "tool_start", "tool_use_id": current_tool_id, "name": tu.get("name", "")}
            elif "contentBlockDelta" in event:
                delta = event["contentBlockDelta"].get("delta", {})
                if "reasoningContent" in delta:
                    rc = delta["reasoningContent"]
                    reasoning_text = rc.get("reasoningText", {})
                    if reasoning_text:
                        yield {
                            "type": "bedrock_reasoning_delta",
                            "text": reasoning_text.get("text", ""),
                            "signature": reasoning_text.get("signature", ""),
                        }
                    if "redactedContent" in rc:
                        yield {
                            "type": "bedrock_reasoning_delta",
                            "redacted_content": rc["redactedContent"],
                        }
                elif "text" in delta:
                    yield {"type": "text_delta", "text": delta["text"]}
                elif "toolUse" in delta:
                    yield {"type": "tool_input_delta", "tool_use_id": current_tool_id or "", "delta": delta["toolUse"].get("input", "")}
            elif "contentBlockStop" in event:
                if current_tool_id:
                    yield {"type": "tool_stop", "tool_use_id": current_tool_id}
                    current_tool_id = None
            elif "messageStop" in event:
                yield {"type": "message_stop", "stop_reason": event["messageStop"].get("stopReason", "end_turn")}


# ─── Anthropic Provider ───

class AnthropicProvider(LLMProvider):
    """Anthropic Messages API provider (direct, not via Bedrock)."""

    def __init__(self):
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "anthropic package required for AI_PROVIDER=anthropic. "
                "Install it: pip install anthropic"
            )

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable is required for AI_PROVIDER=anthropic")

        self._client = anthropic.Anthropic(api_key=api_key)
        self._model_id = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")
        self._fast_model_id = os.environ.get("ANTHROPIC_SMALL_FAST_MODEL", "claude-haiku-4-5-20251001")

    @property
    def provider_name(self) -> str:
        return "anthropic"

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def fast_model_id(self) -> str:
        return self._fast_model_id

    def chat(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> dict:
        model_id = model or self._model_id
        anthropic_messages = self._to_anthropic_messages(messages)
        kwargs = {
            "model": model_id,
            "messages": anthropic_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if system:
            kwargs["system"] = system
        if tools:
            kwargs["tools"] = self._to_anthropic_tools(tools)

        response = self._client.messages.create(**kwargs)
        return self._from_anthropic_response(response)

    def chat_stream(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> Generator[dict, None, None]:
        model_id = model or self._model_id
        anthropic_messages = self._to_anthropic_messages(messages)
        kwargs = {
            "model": model_id,
            "messages": anthropic_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if system:
            kwargs["system"] = system
        if tools:
            kwargs["tools"] = self._to_anthropic_tools(tools)

        current_tool_id = None
        block_types: dict[int, str] = {}
        with self._client.messages.stream(**kwargs) as stream:
            for event in stream:
                etype = getattr(event, "type", "")
                if etype == "content_block_start":
                    index = getattr(event, "index", 0)
                    block = getattr(event, "content_block", None)
                    block_type = getattr(block, "type", "") if block else ""
                    block_types[index] = block_type
                    if block_type == "tool_use":
                        current_tool_id = getattr(block, "id", "")
                        yield {"type": "tool_start", "tool_use_id": current_tool_id, "name": getattr(block, "name", "")}
                    elif block_type == "thinking":
                        yield {
                            "type": "thinking_start",
                            "thinking": getattr(block, "thinking", ""),
                            "signature": getattr(block, "signature", ""),
                        }
                    elif block_type == "redacted_thinking":
                        yield {
                            "type": "redacted_thinking",
                            "data": getattr(block, "data", ""),
                        }
                elif etype == "content_block_delta":
                    delta = getattr(event, "delta", None)
                    if delta:
                        dt = getattr(delta, "type", "")
                        if dt == "text_delta":
                            yield {"type": "text_delta", "text": getattr(delta, "text", "")}
                        elif dt == "input_json_delta":
                            yield {"type": "tool_input_delta", "tool_use_id": current_tool_id or "", "delta": getattr(delta, "partial_json", "")}
                        elif dt == "thinking_delta":
                            yield {"type": "thinking_delta", "text": getattr(delta, "thinking", "")}
                        elif dt == "signature_delta":
                            yield {"type": "signature_delta", "signature": getattr(delta, "signature", "")}
                elif etype == "content_block_stop":
                    index = getattr(event, "index", 0)
                    block_type = block_types.pop(index, "")
                    if block_type == "tool_use" and current_tool_id:
                        yield {"type": "tool_stop", "tool_use_id": current_tool_id}
                        current_tool_id = None
                    elif block_type == "thinking":
                        yield {"type": "thinking_stop"}
                elif etype == "message_stop":
                    msg = getattr(event, "message", None)
                    sr = getattr(msg, "stop_reason", "end_turn") if msg else "end_turn"
                    yield {"type": "message_stop", "stop_reason": sr}

    def _to_anthropic_messages(self, messages: list[dict]) -> list[dict]:
        """Convert internal message format to Anthropic API format."""
        result = []
        for msg in messages:
            role = msg["role"]
            content = msg.get("content", [])
            anthropic_content = []

            for block in content:
                if "text" in block:
                    anthropic_content.append({"type": "text", "text": block["text"]})
                elif "anthropicThinking" in block:
                    thinking = dict(block["anthropicThinking"])
                    anthropic_content.append({
                        "type": "thinking",
                        "thinking": thinking.get("thinking", ""),
                        "signature": thinking.get("signature", ""),
                    })
                elif "anthropicRedactedThinking" in block:
                    redacted = dict(block["anthropicRedactedThinking"])
                    anthropic_content.append({
                        "type": "redacted_thinking",
                        "data": redacted.get("data", ""),
                    })
                elif "toolUse" in block:
                    tu = block["toolUse"]
                    anthropic_content.append({
                        "type": "tool_use",
                        "id": tu["toolUseId"],
                        "name": tu["name"],
                        "input": tu.get("input", {}),
                    })
                elif "toolResult" in block:
                    tr = block["toolResult"]
                    text_parts = [c["text"] for c in tr.get("content", []) if "text" in c]
                    anthropic_content.append({
                        "type": "tool_result",
                        "tool_use_id": tr["toolUseId"],
                        "content": "\n".join(text_parts),
                    })

            result.append({"role": role, "content": anthropic_content})
        return result

    @staticmethod
    def _to_anthropic_tools(tools: list[dict]) -> list[dict]:
        """Convert internal tool format to Anthropic API format."""
        result = []
        for tool in tools:
            spec = tool.get("toolSpec", {})
            result.append({
                "name": spec["name"],
                "description": spec.get("description", ""),
                "input_schema": spec.get("inputSchema", {}).get("json", {}),
            })
        return result

    @staticmethod
    def _from_anthropic_response(response) -> dict:
        """Convert Anthropic API response to internal format."""
        content = []
        for block in response.content:
            if block.type == "thinking":
                content.append({
                    "anthropicThinking": {
                        "thinking": getattr(block, "thinking", ""),
                        "signature": getattr(block, "signature", ""),
                    }
                })
            elif block.type == "redacted_thinking":
                content.append({
                    "anthropicRedactedThinking": {
                        "data": getattr(block, "data", ""),
                    }
                })
            elif block.type == "text":
                content.append({"text": block.text})
            elif block.type == "tool_use":
                content.append({
                    "toolUse": {
                        "toolUseId": block.id,
                        "name": block.name,
                        "input": block.input,
                    }
                })

        stop_reason = "end_turn"
        if response.stop_reason == "tool_use":
            stop_reason = "tool_use"

        return {
            "output": {"message": {"role": "assistant", "content": content}},
            "stopReason": stop_reason,
        }



# ─── OpenAI Provider ───

class OpenAIProvider(LLMProvider):
    """OpenAI Chat Completions API provider."""

    def __init__(self):
        try:
            import openai
        except ImportError:
            raise ImportError(
                "openai package required for AI_PROVIDER=openai. "
                "Install it: pip install openai"
            )

        api_key = os.environ.get("OPENAI_API_KEY")
        base_url = os.environ.get("OPENAI_BASE_URL")  # Supports compatible APIs
        if not api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required for AI_PROVIDER=openai")

        client_kwargs = {"api_key": api_key}
        if base_url:
            client_kwargs["base_url"] = base_url
        self._client = openai.OpenAI(**client_kwargs)
        self._model_id = os.environ.get("OPENAI_MODEL", "gpt-4o")
        self._fast_model_id = os.environ.get("OPENAI_SMALL_FAST_MODEL", "gpt-4o-mini")
        self._echo_reasoning_content = _env_bool("OPENAI_ECHO_REASONING_CONTENT", True)

    @property
    def provider_name(self) -> str:
        return "openai"

    @property
    def model_id(self) -> str:
        return self._model_id

    @property
    def fast_model_id(self) -> str:
        return self._fast_model_id

    def chat(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> dict:
        model_id = model or self._model_id
        openai_messages = self._to_openai_messages(messages, system)
        kwargs = {
            "model": model_id,
            "messages": openai_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if tools:
            kwargs["tools"] = self._to_openai_tools(tools)

        response = self._client.chat.completions.create(**kwargs)
        return self._from_openai_response(response)

    def chat_stream(
        self,
        messages: list[dict],
        system: str = "",
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 4096,
        tools: list[dict] | None = None,
    ) -> Generator[dict, None, None]:
        model_id = model or self._model_id
        openai_messages = self._to_openai_messages(messages, system)
        kwargs = {
            "model": model_id,
            "messages": openai_messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
        }
        if tools:
            kwargs["tools"] = self._to_openai_tools(tools)

        stream = self._client.chat.completions.create(**kwargs)
        started_tools: dict[int, str] = {}  # index -> tool_use_id
        for chunk in stream:
            if not chunk.choices:
                continue
            choice = chunk.choices[0]
            delta = choice.delta

            reasoning_content = self._get_openai_field(delta, "reasoning_content")
            if reasoning_content:
                yield {"type": "reasoning_delta", "text": reasoning_content}

            if delta and delta.content:
                yield {"type": "text_delta", "text": delta.content}

            if delta and delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index
                    if idx not in started_tools and tc.id:
                        started_tools[idx] = tc.id
                        yield {"type": "tool_start", "tool_use_id": tc.id, "name": tc.function.name if tc.function else ""}
                    if tc.function and tc.function.arguments:
                        yield {"type": "tool_input_delta", "tool_use_id": started_tools.get(idx, ""), "delta": tc.function.arguments}

            if choice.finish_reason:
                for idx in sorted(started_tools):
                    yield {"type": "tool_stop", "tool_use_id": started_tools[idx]}
                sr = "tool_use" if choice.finish_reason == "tool_calls" else "end_turn"
                yield {"type": "message_stop", "stop_reason": sr}

    def _to_openai_messages(self, messages: list[dict], system: str = "") -> list[dict]:
        """Convert internal message format to OpenAI API format."""
        result = []
        echo_reasoning = getattr(self, "_echo_reasoning_content", True)
        if system:
            result.append({"role": "system", "content": system})

        for msg in messages:
            role = msg["role"]
            content = msg.get("content", [])

            # Check if this message contains tool results
            has_tool_results = any("toolResult" in block for block in content)

            if has_tool_results:
                for block in content:
                    if "toolResult" in block:
                        tr = block["toolResult"]
                        text_parts = [c["text"] for c in tr.get("content", []) if "text" in c]
                        result.append({
                            "role": "tool",
                            "tool_call_id": tr["toolUseId"],
                            "content": "\n".join(text_parts),
                        })
                continue

            # Check if this message contains tool use (assistant with tool calls)
            has_tool_use = any("toolUse" in block for block in content)

            if has_tool_use:
                text_parts = []
                reasoning_parts = []
                tool_calls = []
                for block in content:
                    if "text" in block:
                        text_parts.append(block["text"])
                    elif "reasoningContent" in block and isinstance(block["reasoningContent"], str):
                        reasoning_parts.append(block["reasoningContent"])
                    elif "toolUse" in block:
                        tu = block["toolUse"]
                        tool_calls.append({
                            "id": tu["toolUseId"],
                            "type": "function",
                            "function": {
                                "name": tu["name"],
                                "arguments": json.dumps(tu.get("input", {})),
                            },
                        })
                msg_dict = {"role": "assistant"}
                if text_parts:
                    msg_dict["content"] = "\n".join(text_parts)
                if echo_reasoning and reasoning_parts:
                    msg_dict["reasoning_content"] = "\n".join(reasoning_parts)
                if tool_calls:
                    msg_dict["tool_calls"] = tool_calls
                result.append(msg_dict)
                continue

            # Regular text message
            text_parts = [block["text"] for block in content if "text" in block]
            reasoning_parts = [
                block["reasoningContent"]
                for block in content
                if "reasoningContent" in block and isinstance(block["reasoningContent"], str)
            ]
            if text_parts:
                msg_dict = {"role": role, "content": "\n".join(text_parts)}
                if echo_reasoning and role == "assistant" and reasoning_parts:
                    msg_dict["reasoning_content"] = "\n".join(reasoning_parts)
                result.append(msg_dict)
            elif echo_reasoning and role == "assistant" and reasoning_parts:
                result.append({
                    "role": role,
                    "reasoning_content": "\n".join(reasoning_parts),
                })

        return result

    @staticmethod
    def _to_openai_tools(tools: list[dict]) -> list[dict]:
        """Convert internal tool format to OpenAI API format."""
        result = []
        for tool in tools:
            spec = tool.get("toolSpec", {})
            result.append({
                "type": "function",
                "function": {
                    "name": spec["name"],
                    "description": spec.get("description", ""),
                    "parameters": spec.get("inputSchema", {}).get("json", {}),
                },
            })
        return result

    @staticmethod
    def _from_openai_response(response) -> dict:
        """Convert OpenAI API response to internal format."""
        choice = response.choices[0]
        message = choice.message
        content = []

        reasoning_content = OpenAIProvider._get_openai_field(message, "reasoning_content")
        if reasoning_content:
            content.append({"reasoningContent": reasoning_content})

        if message.content:
            content.append({"text": message.content})

        if message.tool_calls:
            for tc in message.tool_calls:
                content.append({
                    "toolUse": {
                        "toolUseId": tc.id,
                        "name": tc.function.name,
                        "input": json.loads(tc.function.arguments),
                    }
                })

        stop_reason = "end_turn"
        if choice.finish_reason == "tool_calls":
            stop_reason = "tool_use"

        return {
            "output": {"message": {"role": "assistant", "content": content}},
            "stopReason": stop_reason,
        }

    @staticmethod
    def _get_openai_field(obj, field_name: str):
        """Read SDK fields, including provider-specific extra fields."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return obj.get(field_name)

        value = getattr(obj, field_name, None)
        if value is not None:
            return value

        model_extra = getattr(obj, "model_extra", None)
        if isinstance(model_extra, dict):
            return model_extra.get(field_name)

        return None


# ─── Config file ───


def get_config_path() -> str:
    """Return path to CLI config file (inside the CLI data directory)."""
    from . import get_data_dir
    return str(get_data_dir() / "config.json")


def load_config() -> dict:
    """Load CLI config from data directory. Returns {} if not found."""
    try:
        with open(get_config_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_config(config: dict) -> None:
    """Save CLI config to data directory."""
    path = get_config_path()
    config_dir = os.path.dirname(path)
    os.makedirs(config_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def _auto_detect_provider() -> str | None:
    """Auto-detect the best available provider from environment credentials.

    Returns provider name or None if no credentials found.
    """
    # 1. Anthropic API key
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"

    # 2. OpenAI API key
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"

    # 3. AWS credentials (for Bedrock)
    if os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_PROFILE"):
        return "bedrock"
    try:
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials is not None:
            return "bedrock"
    except Exception:
        pass

    return None


# ─── Factory ───

_PROVIDERS = {
    "bedrock": BedrockProvider,
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
}


def create_provider(provider: str | None = None) -> LLMProvider:
    """Create an LLM provider instance.

    Resolution order:
      1. Explicit `provider` argument (from --provider CLI flag)
      2. AI_PROVIDER environment variable
      3. <data_dir>/config.json "provider" field
      4. Auto-detect from available credentials

    Returns:
        An initialized LLMProvider instance.

    Raises:
        ValueError: If no provider can be determined or the name is unknown.
        ImportError: If the required SDK package is not installed.
    """
    available = ", ".join(sorted(_PROVIDERS.keys()))

    # 1. Explicit argument
    name = provider

    # 2. Environment variable
    if not name:
        name = os.environ.get("AI_PROVIDER")

    # 3. Config file
    config = load_config()
    if not name:
        name = config.get("provider")

    # NOTE: API keys are NEVER stored in config.json — only environment variables.
    # Only non-secret settings (base_url) can be loaded from config.
    non_secret_settings = config.get("settings", {})
    if non_secret_settings.get("openai_base_url"):
        os.environ.setdefault("OPENAI_BASE_URL", non_secret_settings["openai_base_url"])

    # Apply model overrides from config file
    if name and "models" in config:
        model_config = config["models"].get(name, {})
        if model_config.get("model") and not os.environ.get("ANTHROPIC_MODEL") and not os.environ.get("OPENAI_MODEL"):
            if name == "openai":
                os.environ.setdefault("OPENAI_MODEL", model_config["model"])
            else:
                os.environ.setdefault("ANTHROPIC_MODEL", model_config["model"])
        if model_config.get("fast_model") and not os.environ.get("ANTHROPIC_SMALL_FAST_MODEL") and not os.environ.get("OPENAI_SMALL_FAST_MODEL"):
            if name == "openai":
                os.environ.setdefault("OPENAI_SMALL_FAST_MODEL", model_config["fast_model"])
            else:
                os.environ.setdefault("ANTHROPIC_SMALL_FAST_MODEL", model_config["fast_model"])

    # 4. Auto-detect
    if not name:
        name = _auto_detect_provider()

    if not name:
        raise ValueError(
            f"No AI provider configured. Set up with one of:\n"
            f"  1. Run: seatunnel --init              (interactive setup)\n"
            f"  2. Set: export AI_PROVIDER=<{available}>\n"
            f"  3. Set provider credentials:\n"
            f"     - Anthropic: export ANTHROPIC_API_KEY=sk-ant-...\n"
            f"     - OpenAI:    export OPENAI_API_KEY=sk-...\n"
            f"     - Bedrock:   configure AWS credentials (aws configure)"
        )

    name = name.lower().strip()
    cls = _PROVIDERS.get(name)
    if cls is None:
        raise ValueError(f"Unknown AI provider '{name}'. Available: {available}")
    return cls()
