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

"""One place where the CLI talks to the SeaTunnel engine REST API.

The same six lines of `urllib.request.Request` / `urlopen` / `json.loads` were
repeated at four call sites across three modules, each with its own timeout and
its own idea of what to do with a non-200. That made two things awkward: there
was nowhere to put behaviour that every engine call needs (the response size
bound below), and a caller that wanted an HTTP error *body* had to reach into
`urllib.error.HTTPError` itself.

Nothing here adds a dependency; it is the same `urllib` calls behind one door.
"""

import json
import logging
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# Engine responses are JSON status documents, not payloads. A job list on a busy
# cluster or an errorMsg carrying a full stack trace is the realistic upper end,
# so this is generous while still refusing to buffer an unbounded body into
# memory from a host the CLI does not control.
MAX_RESPONSE_BYTES = 8 * 1024 * 1024

DEFAULT_TIMEOUT = 10


class RestError(Exception):
    """An HTTP-level failure, carrying the body so callers can diagnose it.

    `_run_via_rest_api` needs the response body of a failed submit -- that is
    where the engine explains why a config was rejected -- so the body is read
    here rather than leaving callers to remember that `HTTPError` is also a
    readable file object.
    """

    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"HTTP {status} from {url}")
        self.status = status
        self.body = body
        self.url = url


def request_json(
    url: str,
    *,
    method: str = "GET",
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict:
    """Call the engine and return the decoded JSON body.

    Raises `RestError` for an HTTP error status; transport failures and
    undecodable bodies propagate as-is, because the callers here already treat
    "could not reach the engine" and "engine said no" differently.
    """
    req = urllib.request.Request(
        url, data=data, headers=headers or {}, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as e:
        body = e.read(MAX_RESPONSE_BYTES).decode("utf-8", errors="replace")
        raise RestError(e.code, body, url) from e

    if len(raw) > MAX_RESPONSE_BYTES:
        raise RestError(
            0,
            f"response from {url} exceeded {MAX_RESPONSE_BYTES} bytes",
            url,
        )
    return json.loads(raw.decode("utf-8"))


def is_reachable(url: str, *, timeout: float = 2) -> bool:
    """Whether the engine answered 200 at `url`. Never raises.

    Used for the liveness probe, where any failure -- refused, timed out, 500 --
    means the same thing to the caller: fall back to offline metadata.
    """
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False
