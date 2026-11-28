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

"""Deterministic API-contract fixture, never a live Slack certification."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

LOCK = threading.Lock()
REQUESTS = []
RELEASE = threading.Event()


def page(messages, cursor="", **extra):
    return dict(ok=True, messages=messages,
                response_metadata={"next_cursor": cursor}, **extra)


FIRST = {"ts": "1700000000.000001", "user": "U1", "text": "世界🚀", "type": "message"}
SECOND = {"ts": "1700000000.000002"}
THIRD = {"ts": "1700000000.000003", "thread_ts": "1700000000.000001",
         "text": "café", "type": "message", "subtype": "bot_message"}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Do not log request headers or response bodies.
        pass

    def send(self, status, body, headers=None):
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            # The source deliberately aborts pending HTTP during stop/cancel and deadline expiry.
            pass

    def send_slow(self, body):
        """Keep socket reads active while exceeding the whole-attempt deadline."""
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            for value in data:
                self.wfile.write(bytes([value]))
                self.wfile.flush()
                time.sleep(0.1)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path == "/ready":
            self.send(200, {"ready": True})
            return
        if parsed.path == "/requests":
            with LOCK:
                records = list(REQUESTS)
            self.send(200, records)
            return
        if parsed.path == "/reset":
            with LOCK:
                REQUESTS.clear()
            RELEASE.clear()
            self.send(200, {})
            return
        if parsed.path == "/release":
            RELEASE.set()
            self.send(200, {})
            return
        scenario = parsed.path.split("/")[1]
        query = parse_qs(parsed.query)
        channel = query.get("channel", [""])[0]
        cursor = query.get("cursor", [""])[0]
        record = {"scenario": scenario, "channel": channel, "cursor": cursor,
                  "latest": query.get("latest", [""])[0],
                  "oldest": query.get("oldest", [""])[0],
                  "limit": query.get("limit", [""])[0],
                  "inclusive": query.get("inclusive", [""])[0],
                  "received_ms": time.monotonic_ns() // 1000000}
        with LOCK:
            REQUESTS.append(record)
            attempt = sum(item["scenario"] == scenario and item["channel"] == channel
                          and item["cursor"] == cursor for item in REQUESTS)
        if self.headers.get("Authorization") != "Bearer fixture-token":
            self.send(401, {"ok": False, "error": "invalid_auth"})
        elif scenario == "success":
            if channel == "C1" and cursor == "short" and attempt == 1:
                self.send(429, {"fixture": "rate limit"}, {"Retry-After": "1"})
            elif channel == "C2":
                self.send(200, page([THIRD]))
            elif cursor == "":
                self.send(200, page([FIRST], "short"))
            elif cursor == "short":
                self.send(200, page([], "last"))
            elif cursor == "last":
                self.send(200, page([SECOND]))
            else:
                self.send(400, {"ok": False, "error": "invalid_cursor"})
        elif scenario == "auth":
            self.send(401, {"secret": "fixture-response-secret"})
        elif scenario == "api-error":
            self.send(200, {"ok": False, "error": "invalid_auth", "secret": "fixture-response-secret"})
        elif scenario == "api-error-secret":
            self.send(200, {"ok": False, "error": "fixture-response-secret"})
        elif scenario == "malformed":
            self.send(200, {"ok": True, "messages": "fixture-response-secret"})
        elif scenario == "repeated":
            if cursor and not RELEASE.wait(240):
                self.send(503, {"error": "fixture wait expired"})
                return
            self.send(200, page([FIRST if not cursor else SECOND], "repeat"))
        elif scenario == "cycle":
            self.send(200, page([], {"": "a", "a": "b", "b": "a"}[cursor]))
        elif scenario == "has-more":
            self.send(200, {"ok": True, "messages": [], "has_more": True})
        elif scenario == "exhausted":
            self.send(503, {"secret": "fixture-response-secret"})
        elif scenario == "deadline-stall":
            # Each finite fixture wait is longer than the case-specific HTTP deadline.
            RELEASE.wait(5)
            self.send(200, page([FIRST]))
        elif scenario == "deadline-slow":
            self.send_slow(page([FIRST]))
        elif scenario == "retry-after":
            self.send(429, {}, {"Retry-After": "3600"})
        elif scenario == "oversized":
            self.send(200, page([dict(FIRST, text="x" * 2048)]))
        elif scenario == "page-limit":
            self.send(200, page([], "p" + str(attempt) if not cursor else cursor + "x"))
        elif scenario == "redirect":
            self.send(302, {}, {"Location": "http://slack-fixture:1080/leaked/conversations.history"})
        elif scenario == "restore":
            if channel == "C2":
                self.send(200, page([]))
            elif cursor == "":
                self.send(200, page([FIRST], "hold"))
            elif cursor == "hold":
                if not RELEASE.wait(240):
                    self.send(503, {"error": "fixture wait expired"})
                    return
                self.send(200, page([SECOND]))
            else:
                self.send(400, {"ok": False, "error": "invalid_cursor"})
        else:
            self.send(400, {"ok": False, "error": "fixture unexpected scenario"})


ThreadingHTTPServer(("0.0.0.0", 1080), Handler).serve_forever()
