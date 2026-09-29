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

"""Opt-in debug diagnostics for the SeaTunnel CLI agent pipeline.

Enable with ``--debug`` or ``SEATUNNEL_CLI_DEBUG=1``. When enabled, each stage
of the planner → skill → generator → validator chain emits a one-line summary.
Failed stages may attach a redacted, truncated model-output snippet.
"""

from __future__ import annotations

import os
import re

DEBUG_ENV = "SEATUNNEL_CLI_DEBUG"

# Keep in sync with cli._SECRET_PATTERNS intent: never print raw credentials.
# Note: the generic "40+ alnum" token pattern is intentionally omitted from
# debug snippets — it collapses HOCON / validator prose into "***REDACTED***".
_SECRET_PATTERNS = re.compile(
    r"(sk-ant-[A-Za-z0-9_-]+)"
    r"|(sk-[A-Za-z0-9_-]{20,})"
    r"|(AKIA[A-Z0-9]{16})"
    r"|(api[_-]?key\s*[:=]\s*\S+)"
    r"|(password\s*[:=]\s*\S+)"
    r"|(secret\s*[:=]\s*\S+)"
    r"|(token\s*[:=]\s*\S+)"
    r"|(Bearer\s+\S+)",
    re.IGNORECASE,
)

ERROR_MESSAGES = {
    "empty_response": "empty model response",
    "no_hocon_block": "no HOCON code block in model response",
    "tool_loop_exhausted": "tool-use loop exhausted without a config",
}

DEFAULT_SNIPPET_LEN = 800


def is_debug_enabled() -> bool:
    """Return True when CLI debug diagnostics are enabled."""
    return os.environ.get(DEBUG_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def enable_debug() -> None:
    """Enable debug diagnostics for the current process."""
    os.environ[DEBUG_ENV] = "1"


def redact_text(text: str) -> str:
    """Redact credential-like substrings from diagnostic text."""
    if not text:
        return ""
    return _SECRET_PATTERNS.sub("***REDACTED***", text)


def truncate_text(text: str, max_len: int = DEFAULT_SNIPPET_LEN) -> str:
    """Truncate text for terminal display."""
    if text is None:
        return ""
    text = text.replace("\r\n", "\n")
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def redact_and_truncate(text: str, max_len: int = DEFAULT_SNIPPET_LEN) -> str:
    """Redact secrets then truncate for safe debug snippets."""
    return truncate_text(redact_text(text or ""), max_len=max_len)


def error_message(error_code: str | None) -> str:
    """Human-readable description for a soft-failure error code."""
    if not error_code:
        return "unknown generation failure"
    return ERROR_MESSAGES.get(error_code, error_code.replace("_", " "))


def format_debug_line(stage: str, **fields) -> str:
    """Format a single pipeline debug summary line (without snippet body)."""
    parts = [f"stage={stage}"]
    for key in (
        "provider",
        "model",
        "fast_model",
        "stop_reason",
        "tools",
        "chars",
        "outcome",
        "matched",
        "missing",
        "round",
        "type",
        "code",
        "phase2",
        "local",
        "reason",
    ):
        if key in fields and fields[key] is not None and fields[key] != "":
            parts.append(f"{key}={fields[key]}")
    return "[debug] " + " ".join(parts)


def first_line_reason(text: str, max_len: int = 160) -> str:
    """Extract a short one-line reason from multi-line diagnostic text."""
    if not text:
        return ""
    for line in str(text).splitlines():
        cleaned = line.strip()
        if cleaned:
            return truncate_text(redact_text(cleaned), max_len=max_len)
    return ""
