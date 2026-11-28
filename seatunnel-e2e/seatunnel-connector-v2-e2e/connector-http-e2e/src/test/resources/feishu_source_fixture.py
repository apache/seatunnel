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

import json
import re
import threading
import time
from email.utils import formatdate
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from select import select
from socket import MSG_PEEK
from urllib.parse import parse_qs, urlsplit

AUTH = "/open-apis/auth/v3/tenant_access_token/internal"
SEARCH = re.compile(r"/open-apis/bitable/v1/apps/([\w-]+)/tables/([\w-]+)/records/search")
FIRST_CURSOR = "page +/=中&?"
END_CURSOR = "end+/="
ALPHA = (
    '{"record_id":"rec-alpha","id":"legacy-001","fields":{"文本":"你好🌊",'
    '"nested":[{"date":1760000000123,"empty":null}],'
    '"decimal":1234567890.12345678901},"created_time":1760000000456,'
    '"last_modified_time":1760000000789}'
)
BETA = '{"record_id":"rec-beta","id":null,"fields":null}'
LOCK = threading.Lock()
STATES = {}
RELEASES = {}


def state_for(name):
    return STATES.setdefault(name, {
        "auth": 0, "requests": [], "rows": [], "redirected": 0, "held": False, "released": False,
    })


class Provider(BaseHTTPRequestHandler):
    def log_message(self, message, *args):
        pass

    def reply(self, status, payload, headers=None):
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        try:
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def rows(self, items=(), cursor=None):
        payload = (
            '{"code":0,"data":{"has_more":' + json.dumps(cursor is not None)
            + ',"page_token":' + json.dumps(cursor) + ',"items":[' + ",".join(items) + ']}}'
        )
        self.reply(200, payload.encode())

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == "/health":
            self.reply(200, {})
            return
        if path.startswith("/state/"):
            with LOCK:
                payload = json.dumps(state_for(path.split("/")[-1])).encode()
            self.reply(200, payload)
            return
        if path.startswith("/trap/"):
            self.trap(path)
            return
        self.reply(404, {})

    def do_POST(self):
        path = urlsplit(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length)) if length else {}
        if path.path == AUTH:
            self.authenticate(body)
            return
        if path.path.startswith("/release/"):
            name = path.path.split("/")[-1]
            with LOCK:
                state_for(name)["released"] = True
                RELEASES.setdefault(name, threading.Event()).set()
            self.reply(200, {})
            return
        if path.path.startswith("/sink/"):
            with LOCK:
                state_for(path.path.split("/")[-1])["rows"].append(body)
            self.reply(200, {})
            return
        if path.path.startswith("/trap/"):
            self.trap(path.path)
            return
        search = SEARCH.fullmatch(path.path)
        if search:
            self.search(search.group(1), search.group(2),
                        parse_qs(path.query, keep_blank_values=True), body)
            return
        self.reply(404, {})

    def trap(self, path):
        with LOCK:
            state_for(path.split("/")[-1])["redirected"] += 1
        self.reply(200, {})

    def authenticate(self, body):
        if not isinstance(body, dict) or set(body) != {"app_id", "app_secret"}:
            self.reply(400, {"code": 10001})
            return
        name = body["app_id"]
        with LOCK:
            state = state_for(name)
            state["auth"] += 1
            attempt = state["auth"]
        mode = name.split("_")[0]
        if body["app_secret"] != "fixture-secret":
            self.reply(200, {"code": 10014, "msg": "fixture-private-body"})
        elif mode == "authredirect":
            self.reply(307, {}, {"Location": "/trap/" + name})
        else:
            self.reply(200, {
                "code": 0, "tenant_access_token": f"t-{name}-{attempt}",
                "expire": 1 if mode == "expiry" and attempt == 1 else 7200,
            })

    def search(self, base_token, name, query, body):
        mode = name.split("_")[0]
        cursor_values = query.get("page_token", [None])
        cursor = cursor_values[0]
        with LOCK:
            state = state_for(name)
            auth_attempts = state["auth"]
            state["requests"].append({"cursor": cursor, "time": time.monotonic()})
            attempt = len(state["requests"])
        bearer = self.headers.get("Authorization")
        if (base_token != "base-" + name + "-credential"
                or query.get("page_size") != ["2"] or len(cursor_values) != 1
                or set(query) - {"page_size", "page_token"} or body != {}):
            self.reply(200, {"code": 13000, "msg": "unexpected record search request"})
            return
        if bearer != f"Bearer t-{name}-{auth_attempts}":
            self.reply(200, {"code": 13001, "msg": "missing tenant bearer"})
            return

        if mode == "happy":
            if cursor is None:
                self.rows((ALPHA,), FIRST_CURSOR)
            elif cursor == FIRST_CURSOR and auth_attempts == 1:
                self.reply(200, {"code": 99991663})
            elif cursor == FIRST_CURSOR:
                self.rows(cursor=END_CURSOR)
            elif cursor == END_CURSOR:
                self.rows((BETA,))
            else:
                self.reply(200, {"code": 13002})
        elif mode == "expiry":
            if attempt == 1:
                self.reply(429, {}, {"Retry-After": "1"})
            elif auth_attempts == 2:
                self.rows((BETA,))
            else:
                self.reply(200, {"code": 13003})
        elif mode == "expiredforever":
            self.reply(200, {"code": 99991663})
        elif mode == "empty":
            self.reply(200, {"code": 0, "data": {"has_more": False}})
        elif mode == "permission":
            self.reply(200, {"code": 99991672, "msg": "fixture-private-body"})
        elif mode == "badcursor":
            self.reply(200, {"code": 0, "data": {"has_more": True, "page_token": False, "items": []}})
        elif mode == "cycle":
            self.rows(cursor=END_CURSOR if cursor == FIRST_CURSOR else FIRST_CURSOR)
        elif mode == "limit":
            self.rows((ALPHA,), FIRST_CURSOR)
        elif mode == "retry" and attempt < 4:
            if attempt == 1:
                self.reply(429, {}, {"Retry-After": "1"})
            elif attempt == 2:
                self.reply(503, {}, {"x-ogw-ratelimit-reset": "1"})
            else:
                self.reply(400, {"code": 99991400},
                           {"Retry-After": formatdate(time.time() + 2, usegmt=True)})
        elif mode == "retryexhaust":
            self.reply(503, {})
        elif mode == "retryhold":
            self.reply(429, {}, {"Retry-After": "60"})
            with LOCK:
                state_for(name)["held"] = True
        elif mode == "redirect":
            self.reply(303, {}, {"Location": "/trap/" + name})
        elif mode == "slow":
            self.slow_body(name)
        elif mode == "oversize":
            self.send_response(200)
            self.send_header("Content-Length", str(16 * 1024 * 1024 + 1))
            self.end_headers()
        elif mode == "stall":
            if self.hold(name, observe_disconnect=True):
                self.rows()
        elif mode == "hold":
            if cursor is None:
                self.rows((ALPHA,), FIRST_CURSOR)
            elif cursor == FIRST_CURSOR:
                self.hold(name)
                self.rows((BETA,))
            else:
                self.reply(200, {"code": 13004})
        else:
            self.rows((BETA,))

    def note_disconnect(self, name):
        with LOCK:
            state_for(name).setdefault("disconnected_at", time.monotonic())

    def peer_disconnected(self, name):
        if not select([self.connection], [], [], 0)[0]:
            return False
        try:
            if self.connection.recv(1, MSG_PEEK):
                return False
        except ConnectionResetError:
            pass
        self.note_disconnect(name)
        return True

    def slow_body(self, name):
        self.send_response(200)
        self.send_header("Content-Length", "100")
        self.end_headers()
        for sent in range(1, 101):
            if self.peer_disconnected(name):
                return
            try:
                self.wfile.write(b" ")
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                self.note_disconnect(name)
                return
            with LOCK:
                state_for(name)["trickle_bytes"] = sent
            time.sleep(0.1)

    def hold(self, name, observe_disconnect=False):
        with LOCK:
            state_for(name)["held"] = True
            release = RELEASES.setdefault(name, threading.Event())
        if not observe_disconnect:
            return release.wait(150)
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            if release.wait(0.05):
                return True
            if self.peer_disconnected(name):
                return False
        return False


class PrefixProvider(Provider):
    """Adds exact partial-page restore scenarios while preserving all original provider methods."""

    def search(self, base_token, name, query, body):
        mode = name.split("_")[0]
        if mode not in ("prefixstable", "prefixchanged"):
            return super().search(base_token, name, query, body)
        with LOCK:
            state = state_for(name)
            state["requests"].append({"cursor": query.get("page_token", [None])[0],
                                      "time": time.monotonic()})
            attempt = len(state["requests"])
            auth_attempts = state["auth"]
        if (base_token != "base-" + name + "-credential"
                or query != {"page_size": ["100"]} or body != {}
                or self.headers.get("Authorization") != f"Bearer t-{name}-{auth_attempts}"):
            self.reply(200, {"code": 13000, "msg": "unexpected prefix request"})
            return
        items = [{"record_id": f"rec-{index:03d}", "id": None,
                  "fields": {"text": f"row-{index:03d}"}} for index in range(100)]
        if mode == "prefixchanged" and attempt > 1:
            items[0]["fields"]["text"] = "changed-accepted-record"
        self.rows(tuple(json.dumps(item, separators=(",", ":")) for item in items))


ThreadingHTTPServer(("0.0.0.0", 8080), PrefixProvider).serve_forever()
