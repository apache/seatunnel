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

"""Tests for the shared engine REST helper.

These run against a real loopback HTTP server rather than a patched `urlopen`,
so they exercise the actual `urllib` behaviour the call sites depend on --
in particular that an error *body* survives, which is what the submit path
feeds into failure diagnosis.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from seatunnel_cli import rest


class _Handler(BaseHTTPRequestHandler):
    """Serves whatever the active test put in `self.server.script`."""

    def log_message(self, *args):  # keep pytest output clean
        pass

    def _respond(self):
        status, body = self.server.script(self)
        payload = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = _respond
    do_POST = _respond


@pytest.fixture
def server():
    httpd = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    httpd.base = f"http://127.0.0.1:{httpd.server_port}"
    try:
        yield httpd
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_get_returns_decoded_json(server):
    server.script = lambda h: (200, json.dumps({"jobStatus": "RUNNING"}))

    assert rest.request_json(f"{server.base}/job-info/7") == {"jobStatus": "RUNNING"}


def test_post_sends_body_and_headers(server):
    seen = {}

    def script(handler):
        seen["method"] = handler.command
        seen["type"] = handler.headers.get("Content-Type")
        length = int(handler.headers.get("Content-Length", 0))
        seen["body"] = handler.rfile.read(length).decode("utf-8")
        return 200, json.dumps({"jobId": "852"})

    server.script = script

    body = rest.request_json(
        f"{server.base}/submit-job?format=hocon",
        method="POST",
        data=b"env {}\n",
        headers={"Content-Type": "text/plain"},
        timeout=5,
    )

    assert body == {"jobId": "852"}
    assert seen["method"] == "POST"
    assert seen["type"] == "text/plain"
    assert seen["body"] == "env {}\n"


def test_http_error_keeps_status_and_body(server):
    # The submit path shows this body to the user and feeds it to diagnosis,
    # so losing it would turn a specific rejection into "Submit failed (400)".
    server.script = lambda h: (400, "ErrorCode:[API-02], ErrorDescription:[bad config]")

    with pytest.raises(rest.RestError) as excinfo:
        rest.request_json(f"{server.base}/submit-job")

    assert excinfo.value.status == 400
    assert "ErrorCode:[API-02]" in excinfo.value.body
    assert excinfo.value.url.endswith("/submit-job")


def test_oversized_response_is_refused_rather_than_buffered(server):
    big = json.dumps({"errorMsg": "x" * (rest.MAX_RESPONSE_BYTES + 64)})
    server.script = lambda h: (200, big)

    with pytest.raises(rest.RestError) as excinfo:
        rest.request_json(f"{server.base}/job-info/7")

    assert "exceeded" in excinfo.value.body


def test_a_response_at_the_limit_is_still_accepted(server):
    filler = "y" * (rest.MAX_RESPONSE_BYTES - len('{"errorMsg": ""}'))
    payload = json.dumps({"errorMsg": filler})
    assert len(payload) == rest.MAX_RESPONSE_BYTES
    server.script = lambda h: (200, payload)

    assert rest.request_json(f"{server.base}/job-info/7")["errorMsg"] == filler


def test_is_reachable_true_on_200(server):
    server.script = lambda h: (200, "[]")

    assert rest.is_reachable(f"{server.base}/running-jobs") is True


def test_is_reachable_false_on_error_status(server):
    server.script = lambda h: (500, "boom")

    assert rest.is_reachable(f"{server.base}/running-jobs") is False


def test_is_reachable_false_when_nothing_is_listening():
    # The liveness probe must not raise: every failure means "engine offline"
    # and the caller falls back to offline metadata.
    assert rest.is_reachable("http://127.0.0.1:1/running-jobs", timeout=1) is False
