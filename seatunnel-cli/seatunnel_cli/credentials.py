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

"""Shared credential detection for CLI redaction and LLM placeholder swap.

Quoted ``key = "value"`` / JSON ``"key": "value"`` forms are handled by one
regex so debug snippets and config-repair placeholders cannot drift apart.
"""

from __future__ import annotations

import re

_CRED_KEYS = (
    r"secret[-_]?key|access[-_]?key|api[-_]?key|private[-_]?key|"
    r"auth[-_]?token|password|passwd|credential|secret|token"
)

# HOCON `password = "..."`, colon form, JSON `"password": "..."`, single quotes.
# Placeholders that already start with ${ are left untouched by callers.
CRED_KV_RE = re.compile(
    rf'(?P<prefix>(?<![A-Za-z0-9_])"?(?:{_CRED_KEYS})"?\s*[:=]\s*)'
    r'(?:"(?P<dvalue>[^"]*)"|\'(?P<svalue>[^\']*)\')',
    re.IGNORECASE,
)

# Unquoted `password=supersecret` / `secret_key: abc` (debug / logs only).
_UNQUOTED_CRED_RE = re.compile(
    rf'(?<![A-Za-z0-9_])"?(?:{_CRED_KEYS})"?\s*[:=]\s*(?!["\'])(?!\$\{{)\S+',
    re.IGNORECASE,
)

# Token-shaped secrets. The generic "40+ alnum" pattern is intentionally omitted
# here — it collapses HOCON / validator prose into "***REDACTED***".
_TOKEN_PATTERNS = re.compile(
    r"(sk-ant-[A-Za-z0-9_-]+)"
    r"|(sk-[A-Za-z0-9_-]{20,})"
    r"|(AKIA[A-Z0-9]{16})"
    r"|(Bearer\s+\S+)",
    re.IGNORECASE,
)

_REDACTED = "***REDACTED***"


def _quoted_value(match: re.Match) -> tuple[str, str]:
    if match.group("dvalue") is not None:
        return match.group("dvalue"), '"'
    return match.group("svalue"), "'"


def replace_creds_with_placeholders(config: str) -> tuple[str, dict[str, str]]:
    """Replace quoted credential values with ${_CRED_N_} placeholders.

    Returns (safe_config, mapping) where mapping can restore originals.
    """
    cred_map: dict[str, str] = {}
    counter = [0]

    def _replacer(match: re.Match) -> str:
        value, quote = _quoted_value(match)
        if value.startswith("${"):
            return match.group(0)
        counter[0] += 1
        placeholder = f"${{_CRED_{counter[0]}_}}"
        cred_map[placeholder] = value
        return f"{match.group('prefix')}{quote}{placeholder}{quote}"

    return CRED_KV_RE.sub(_replacer, config), cred_map


def restore_creds_from_placeholders(config: str, cred_map: dict[str, str]) -> str:
    """Restore original credential values from ${_CRED_N_} placeholders."""
    for placeholder, original in cred_map.items():
        config = config.replace(placeholder, original)
    return config


def redact_credential_values(text: str) -> str:
    """Redact quoted credential values, keeping ${...} placeholders intact."""

    def _replacer(match: re.Match) -> str:
        value, quote = _quoted_value(match)
        if value.startswith("${"):
            return match.group(0)
        return f"{match.group('prefix')}{quote}{_REDACTED}{quote}"

    return CRED_KV_RE.sub(_replacer, text)


def redact_text(text: str) -> str:
    """Redact credential-like substrings from diagnostic text."""
    if not text:
        return ""
    text = redact_credential_values(text)
    text = _UNQUOTED_CRED_RE.sub(_REDACTED, text)
    return _TOKEN_PATTERNS.sub(_REDACTED, text)
