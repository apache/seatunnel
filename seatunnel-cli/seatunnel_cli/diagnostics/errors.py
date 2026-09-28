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

"""Turn a raw SeaTunnel failure into structured fields.

The CLI currently feeds a truncated stack trace straight to the model, which
buries the two things that actually identify a failure -- the SeaTunnel error
code and the innermost ``Caused by`` -- under whatever happened to fit in the
character budget. This module extracts them with plain regexes so callers can
lead with the signal.

Everything here is a pure function over a string: no I/O, no network, no model
calls. Unknown input yields ``category="unknown"`` and ``hint=None`` rather
than a guess -- a wrong hint costs more than a missing one, because it sends
the model (and the user) down the wrong path with false confidence.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# org.apache.seatunnel.common.exception.SeaTunnelErrorCode#getErrorMessage
# formats every coded failure as:
#     ErrorCode:[COMMON-22], ErrorDescription:[SeaTunnel write file '...' failed]
# The description itself may contain bracketed text (paths, SQL), so allow one
# level of nesting before closing. Deeper nesting falls back to the shortest
# match, which still yields a usable prefix.
_ERROR_CODE_RE = re.compile(
    r"ErrorCode:\[(?P<ns>[A-Z][A-Z0-9_]*)-(?P<num>\d+)\],\s*"
    r"ErrorDescription:\[(?P<desc>(?:[^\[\]]|\[[^\[\]]*\])*)\]"
)

# "Caused by: org.apache.seatunnel.api.Foo: message" -- the last occurrence is
# the innermost cause, which is the one worth showing first.
_CAUSED_BY_RE = re.compile(
    r"^[ \t]*Caused by:[ \t]*(?P<cls>[\w.$]+(?:Exception|Error|Throwable))"
    r"(?::[ \t]*(?P<msg>.*))?$",
    re.MULTILINE,
)

# A bare "com.foo.BarException: message" line, used when there is no
# "Caused by" chain at all.
_EXCEPTION_LINE_RE = re.compile(
    r"^[ \t]*(?P<cls>[\w.$]+(?:Exception|Error|Throwable))"
    r"(?::[ \t]*(?P<msg>.*))?$",
    re.MULTILINE,
)

# Error-code namespaces that are not connectors. Everything else is treated as
# a connector namespace, so new connectors classify correctly without touching
# this module -- a hardcoded connector list would rot on every new connector.
_INFRASTRUCTURE_NAMESPACES = {
    "API": "config",
    "COMMON": "engine",
    "SE": "engine",
    "TRANSFORM_COMMON": "transform",
    "JSONPATH_ERROR_CODE": "transform",
}

# Failures that carry no SeaTunnel error code. These are JVM- and
# driver-level strings, matched case-insensitively and ordered most specific
# first: the first match wins, so a narrow pattern must precede a broad one.
_TEXT_PATTERNS: tuple[tuple[str, str, str, str], ...] = (
    (
        r"ClassNotFoundException|NoClassDefFoundError",
        "missing_dependency",
        "A class is missing from the classpath",
        "Install the connector plugin and its JDBC driver into "
        "$SEATUNNEL_HOME/plugins (or lib/) and restart the cluster.",
    ),
    (
        r"Access denied for user|password authentication failed"
        r"|authentication failed|Login failed for user"
        r"|SASL authentication failed",
        "auth",
        "The endpoint rejected the supplied credentials",
        "Check the user/password options for this plugin, and that the "
        "account may connect from the SeaTunnel host.",
    ),
    (
        r"UnknownHostException|Name or service not known"
        r"|Temporary failure in name resolution",
        "network",
        "A hostname in the config did not resolve",
        "Verify the host spelling; inside containers 'localhost' means the "
        "container itself, not the host machine.",
    ),
    (
        r"Connection refused|No route to host|ConnectException",
        "network",
        "The endpoint refused the connection",
        "Confirm the service is listening on that host and port and that no "
        "firewall or network policy blocks it.",
    ),
    (
        r"OutOfMemoryError|GC overhead limit exceeded"
        r"|unable to create new native thread",
        "resource",
        "The JVM ran out of memory or threads",
        "Raise the worker heap, or lower parallelism / batch size so less "
        "data is buffered at once.",
    ),
    (
        r"doesn't exist|does not exist|Unknown database|Unknown column"
        r"|Invalid object name|Table or view not found",
        "schema",
        "A referenced table, column, or database is absent",
        "Check the table and field names against the endpoint, including "
        "case, and whether the sink needs save_mode to create them.",
    ),
    (
        r"FileNotFoundException|No such file or directory|NoSuchFileException",
        "missing_path",
        "A path in the config does not exist",
        "Verify the path exists and is readable by the SeaTunnel process; "
        "for cluster mode it must exist on every node.",
    ),
    (
        r"SocketTimeoutException|TimeoutException|Read timed out"
        r"|timeout expired",
        "timeout",
        "An operation exceeded its time limit",
        "Check endpoint health and latency, then raise the plugin's timeout "
        "option if the endpoint is simply slow.",
    ),
    (
        r"Permission denied|AccessDeniedException|not authorized"
        r"|AccessDenied",
        "permission",
        "The endpoint accepted the identity but refused the operation",
        "Grant the account the required privileges on the target object "
        "(for CDC sources this includes replication privileges).",
    ),
)

_COMPILED_TEXT_PATTERNS = tuple(
    (re.compile(pattern, re.IGNORECASE), category, summary, hint)
    for pattern, category, summary, hint in _TEXT_PATTERNS
)

# Hints for coded failures, keyed by category rather than by individual code:
# the code set is large and changes, the remedy per class does not.
_CATEGORY_HINTS = {
    "config": "Validate the plugin's options against its documented option "
              "rule -- required options, and options that only apply to "
              "certain formats.",
    "transform": "Check the transform's field references and that the Zeta "
                 "SQL engine supports the expression used.",
}

# Substrings dropped from a signature so the same failure on different rows,
# hosts, or attempts collapses to one key.
_VOLATILE_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\b[0-9a-fA-F]{8,}\b|\d+")


@dataclass(frozen=True)
class ParsedError:
    """Structured view of one failure. Absent fields stay ``None``."""

    category: str = "unknown"
    code: str | None = None
    namespace: str | None = None
    description: str | None = None
    component: str | None = None
    exception: str | None = None
    root_cause: str | None = None
    summary: str | None = None
    hint: str | None = None
    signature: str | None = None
    codes: tuple[str, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict:
        """Drop empty fields so callers can render without None checks."""
        data = {
            "category": self.category,
            "code": self.code,
            "namespace": self.namespace,
            "description": self.description,
            "component": self.component,
            "exception": self.exception,
            "root_cause": self.root_cause,
            "summary": self.summary,
            "hint": self.hint,
            "signature": self.signature,
        }
        result = {k: v for k, v in data.items() if v}
        if len(self.codes) > 1:
            result["codes"] = list(self.codes)
        return result

    def headline(self) -> str:
        """One line naming the failure, for a compact report or tool result."""
        parts = [self.code or self.exception or "unknown failure"]
        detail = self.description or self.root_cause or self.summary
        if detail:
            parts.append(detail)
        return ": ".join(parts)


def _short_class(name: str) -> str:
    return name.rsplit(".", 1)[-1]


def _signature(code: str | None, exception: str | None, text: str) -> str:
    """Stable dedupe key: identity of the failure without its variable parts."""
    if code:
        base = code
    elif exception:
        base = _short_class(exception)
    else:
        base = next((line.strip() for line in text.splitlines() if line.strip()),
                    "empty")
    return _VOLATILE_RE.sub("?", base)[:120]


def parse_error(text: str | None) -> ParsedError:
    """Extract error code, root cause, and a category from a raw failure."""
    if not text or not text.strip():
        return ParsedError(signature="empty")

    codes: list[str] = []
    first_desc: str | None = None
    first_ns: str | None = None
    for match in _ERROR_CODE_RE.finditer(text):
        code = f"{match.group('ns')}-{match.group('num')}"
        if code not in codes:
            codes.append(code)
        if first_desc is None:
            first_ns = match.group("ns")
            first_desc = " ".join(match.group("desc").split()) or None

    causes = _CAUSED_BY_RE.findall(text)
    if causes:
        # findall returns (cls, msg) pairs; the last is the innermost cause.
        cause_cls, cause_msg = causes[-1]
        exception = cause_cls
        root_cause = " ".join(cause_msg.split()) or None
    else:
        exception, root_cause = None, None
        bare = _EXCEPTION_LINE_RE.search(text)
        if bare:
            exception = bare.group("cls")
            root_cause = " ".join((bare.group("msg") or "").split()) or None

    # Classify on the innermost cause when there is one: a generic
    # SeaTunnelRuntimeException wrapper says less than what it wrapped.
    category, summary, hint = "unknown", None, None
    for pattern, cat, text_summary, text_hint in _COMPILED_TEXT_PATTERNS:
        if pattern.search(text):
            category, summary, hint = cat, text_summary, text_hint
            break

    component = None
    if first_ns:
        if first_ns in _INFRASTRUCTURE_NAMESPACES:
            coded_category = _INFRASTRUCTURE_NAMESPACES[first_ns]
        else:
            coded_category = "connector"
            component = first_ns
        # A concrete text match (auth, network, ...) explains the failure
        # better than the namespace it surfaced in, so it takes precedence.
        if category == "unknown":
            category = coded_category
            hint = _CATEGORY_HINTS.get(category)

    return ParsedError(
        category=category,
        code=codes[0] if codes else None,
        namespace=first_ns,
        description=first_desc,
        component=component,
        exception=exception,
        root_cause=root_cause,
        summary=summary,
        hint=hint,
        signature=_signature(codes[0] if codes else None, exception, text),
        codes=tuple(codes),
    )
