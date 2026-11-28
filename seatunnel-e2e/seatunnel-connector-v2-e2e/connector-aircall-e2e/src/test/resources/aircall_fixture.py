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
#/

import base64
import gzip
import json
import os
import select
import ssl
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

START = 1704067200
PRIVATE = "sensitive-provider-fixture-content"
AUTH = "Basic " + base64.b64encode(
    (os.environ["FIXTURE_USER"] + ":" + os.environ["FIXTURE_PASSWORD"]).encode("ascii")
).decode("ascii")
ROOTS = {
    "calls": ("calls", "calls", "page", 101),
    "company": ("company", "company", "object", 200),
    "contacts": ("contacts", "contacts", "page", 210),
    "numbers": ("numbers", "numbers", "page", 301),
    "tags": ("tags", "tags", "page", 310),
    "users/availabilities": ("user_availability", "users", "page", 320),
    "users": ("users", "users", "page", 401),
    "teams": ("teams", "teams", "page", 410),
    "webhooks": ("webhooks", "webhooks", "page", 420),
    "conversations": ("conversations", "conversations", "page", 501),
    "campaigns": ("campaigns", "campaigns", "cursor", 520),
    "campaign_outcomes": ("campaign_outcomes", "campaign_outcomes", "cursor", 530),
    "trackers": ("trackers", "trackers", "array", 540),
    "sms/templates": ("sms_templates", "templates", "page", 550),
}
CHILDREN = {
    ("conversations", "engagements"): ("conversation_engagements", "engagements", "page", 600),
    ("numbers", "templates"): ("whatsapp_templates", "templates", "wa", 610),
    ("users", "numbers"): ("user_numbers", "numbers", "page", 620),
    ("users", "dialer_campaign"): ("dialer_campaigns", None, "object", 630),
    ("users", "dialer_campaign/phone_numbers"): ("dialer_campaign_phone_numbers", "numbers", "array", 640),
    ("calls", "transcription"): ("call_transcriptions", "transcription", "object", 700),
    ("calls", "sentiments"): ("call_sentiments", "sentiment", "object", 710),
    ("calls", "topics"): ("call_topics", "topic", "object", 720),
    ("calls", "summary"): ("call_summaries", "summary", "object", 730),
    ("calls", "action_items"): ("call_action_items", "action_items", "array", 740),
    ("calls", "playbook_result"): ("call_playbook_results", None, "object", 750),
}
MODE = "catalog"
REQUESTS = []
GATE = threading.Event()
LOCK = threading.Lock()


def obj(name, identifier, started=None):
    key = "webhook_id" if name == "webhooks" else "id"
    if name == "contacts" and identifier == 210:
        identifier = 1234567890123456789012345678901234567890
    row = {key: identifier, "name": "café 雪"}
    if name == "calls":
        row["started_at"] = START + identifier - 101 if started is None else started
        row["recording"] = "https://redirect.aircall.test:8443/trap"
    if name == "conversations":
        stamp = START + identifier - 501
        row["last_engagement_at"] = datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.500Z")
    row["nested"] = {"nullable": None, "values": [True, 17, "FIXTURE_DECIMAL"]}
    return row


def encode(data):
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).replace(
        '"FIXTURE_DECIMAL"', "1234567890123456789.123456789012345678900"
    ).encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        try:
            self.route()
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def send(self, code, value, headers=None):
        body = value if isinstance(value, bytes) else encode(value)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, val in (headers or {}).items():
            self.send_header(name, val)
        self.end_headers()
        self.wfile.write(body)


class Control(Handler):
    def route(self):
        global MODE
        route = urlsplit(self.path)
        if route.path == "/ready":
            self.send(200, {"ready": True})
        elif route.path == "/select":
            GATE.set()
            with LOCK:
                REQUESTS.clear()
                MODE = parse_qs(route.query)["mode"][0]
                GATE.clear()
            self.send(200, {})
        elif route.path == "/calls":
            with LOCK:
                data = [call.copy() for call in REQUESTS]
            self.send(200, data)
        elif route.path == "/release":
            GATE.set()
            self.send(200, {})
        else:
            self.send(404, {})


class Provider(Handler):
    protocol_version = "HTTP/1.1"

    def route(self):
        self.close_connection = True
        request = urlsplit(self.path)
        query = parse_qs(request.query, strict_parsing=True)
        auth = self.headers.get_all("Authorization") == [AUTH]
        start = time.monotonic()
        call = {"path": request.path, "q": query, "auth": auth, "start_ms": int(start * 1000),
                "host": self.headers.get("Host"), "gzip": self.headers.get("Accept-Encoding") == "gzip"}
        with LOCK:
            REQUESTS.append(call)
            number = len(REQUESTS)
            mode = MODE
        if request.path == "/trap":
            self.send(200, {"followed": True})
            return
        if not auth:
            self.send(401, {"message": PRIVATE})
            return

        route = request.path.removeprefix("/v1/")
        parent = None
        resource = ROOTS.get(route)
        if resource is None and request.path.startswith("/v1/"):
            parts = route.split("/", 2)
            if len(parts) == 3 and parts[1].isdigit():
                parent = int(parts[1])
                resource = CHILDREN.get((parts[0], parts[2]))
        if resource is None:
            self.send(404, {"message": PRIVATE})
            return
        name, key, kind, first = resource
        values = {name: val[0] for name, val in query.items()}
        if not self.valid_query(name, kind, values) or any(len(v) != 1 for v in query.values()):
            self.send(400, {"message": PRIVATE})
            return
        if mode == "forbidden" or mode == "ai-forbidden" and parent is not None:
            self.send(403, {"message": "Forbidden", "details": PRIVATE})
            return
        if mode == "ai-unknown" and parent is not None:
            self.send(404, {"message": PRIVATE})
            return
        if mode == "redirect":
            self.send(302, {"message": PRIVATE}, {"Location": "https://redirect.aircall.test:8443/trap"})
            return
        if mode == "bad-json":
            self.send(200, b'{"' + PRIVATE.encode() + b'":')
            return
        if mode == "huge":
            huge = gzip.compress(encode({"calls": [{"id": 999, "note": "x" * 9000}]}))
            self.send(200, huge, {"Content-Encoding": "gzip"})
            return
        if mode == "trickle":
            self.trickle(start, call)
            return
        page = int(values.get("page", "1"))
        if mode in ("stall", "cancel") or mode == "hold-child" and parent == 101 and number == 2:
            if self.hold(start, call) and mode == "hold-child":
                self.data(request.path, values, resource, parent)
            return
        if mode in ("busy", "long-reset") or mode == "retry" and number <= 2:
            if mode == "long-reset" or mode == "retry" and number == 1:
                reset = int(time.time()) + (3600 if mode == "long-reset" else 2)
                self.send(429, {"message": PRIVATE}, {"X-AircallApi-Reset": str(reset), "Retry-After": "1"})
            else:
                self.send(503, {"message": PRIVATE})
            return
        if mode in ("saturated", "still-saturated", "cap-400"):
            lo, hi = int(values["from"]), int(values["to"])
            if mode == "cap-400" and hi > lo:
                self.send(400, {"message": "BAD_REQUEST", "errors": ["Your query is limited to 10k results."]})
            elif mode == "still-saturated" or hi - lo + 1 > 2:
                self.numbered(request.path, values, key, [], 10000)
            else:
                rows = [obj(name, 101 + offset) for offset in range(lo - START, hi - START + 1)]
                self.numbered(request.path, values, key, rows, len(rows))
            return
        if mode == "overlap":
            lo = int(values["from"])
            rows = [obj("calls", 101, START + 259200), obj("calls", 102, START + 259199),
                    obj("calls", 103, START + 259204)]
            self.numbered(request.path, values, key, rows, 3, override=lo)
            return
        if mode == "hold-offset" and page == 2 and number == 2:
            if self.hold(start, call):
                self.numbered(request.path, values, key, [obj(name, 103)], 3)
            return
        if mode == "hold-offset":
            rows = [obj(name, 101), obj(name, 102)] if page == 1 else [obj(name, 103)]
            self.numbered(request.path, values, key, rows, 3)
            return
        if mode == "long-page":
            self.numbered(request.path, values, key, [obj(name, i) for i in range(101, 151)], 50)
            return
        if mode in ("foreign", "wrong-path", "wrong-page"):
            link = ("https://redirect.aircall.test:8443/trap" if mode == "foreign"
                    else "/v1/numbers?page=2" if mode == "wrong-path" else "/v1/calls?page=1")
            self.numbered(request.path, values, key, [obj(name, 101)], 4, next_link=link)
            return
        if mode == "empty-continuation":
            rows = [] if page == 1 else [obj(name, 103)]
            self.numbered(request.path, values, key, rows, 3)
            return
        if mode == "cursor-cycle":
            self.send(200, {key: [obj(name, first + number)], "meta": {"count": 1, "next_cursor": "same"}})
            return
        if mode == "capped-child" and name == "conversation_engagements":
            self.numbered(request.path, values, key, [], 10000)
            return
        if mode == "no-child-filter" and parent is None and name == "conversations":
            self.numbered(request.path, values, key, [obj(name, 501), obj(name, 502)], 2)
            return
        self.data(request.path, values, resource, parent)

    def valid_query(self, resource, kind, query):
        expected = set()
        if kind == "page":
            expected |= {"page", "per_page"}
            size = query.get("per_page", "")
            page = query.get("page", "")
            if not size.isdigit() or not page.isdigit() or int(page) < 1:
                return False
            if not 1 <= int(size) <= (100 if resource == "sms_templates" else 50):
                return False
        elif kind == "cursor":
            expected.add("per_page")
            if "cursor" in query:
                expected.add("cursor")
        elif kind == "wa" and "token" in query:
            expected.add("token")
        if resource in ("calls", "contacts", "conversations"):
            expected |= {"from", "to", "order"}
        if resource in ("users", "user_availability", "numbers", "conversation_engagements"):
            expected.add("order")
        if resource == "calls":
            expected.add("fetch_call_timeline")
            if query.get("fetch_call_timeline") != "true":
                return False
        if resource == "call_playbook_results":
            expected.add("fetch_playbook")
            if query.get("fetch_playbook") != "true":
                return False
        return set(query) == expected

    def data(self, path, query, resource, parent):
        name, key, kind, first = resource
        if parent == 302 and name == "whatsapp_templates":
            self.send(403, {"message": "Line is not configured to use WhatsApp"})
            return
        if parent == 402 and name in ("dialer_campaigns", "dialer_campaign_phone_numbers"):
            self.send(404, {"message": "NOT_FOUND"})
            return
        if parent == 102 and name == "call_summaries":
            self.send(404, {"error": "NotFound", "troubleshoot": "Summary not found for this call"})
            return
        index = 0 if parent is None else parent % 100
        if kind == "object":
            val = obj(name, first + index)
            if name == "call_playbook_results":
                val["call_id"] = parent
            self.send(200, val if key is None else {key: val})
        elif kind == "array":
            self.send(200, {key: [obj(name, first + index)]})
        elif kind == "wa":
            current = int(query.get("token", "0"))
            self.send(200, {key: [obj(name, first + (0 if current == 0 else 1))],
                            "pageInfo": {"currentToken": current, "nextToken": 10 if current == 0 else None, "totalCount": 2}})
        elif kind == "cursor":
            current = query.get("cursor")
            token = "opaque+/=value" if current is None else None
            vals = {**query, "cursor": token}
            link = self.link(path, vals) if token else None
            self.send(200, {key: [obj(name, first + (0 if current is None else 1))],
                            "meta": {"count": 1, "next_cursor": token, "next_page_link": link}})
        else:
            count = 3 if name == "contacts" else 2 if name in ("calls", "numbers", "users", "conversations") else 1
            size, page = int(query["per_page"]), int(query["page"])
            rows = [obj(name, first + index + i) for i in range((page - 1) * size, min(count, page * size))]
            self.numbered(path, query, key, rows, count)

    def link(self, path, query):
        return "https://" + self.headers["Host"] + path + "?" + urlencode(query)

    def numbered(self, path, query, key, rows, total, next_link=None, override=None):
        page, size = int(query["page"]), int(query["per_page"])
        if override is not None and override != START:
            self.send(400, {"message": PRIVATE})
            return
        link = next_link if next_link is not None else self.link(path, {**query, "page": page + 1}) if page * size < total else None
        meta = {"count": len(rows), "total": total, "current_page": page, "per_page": size}
        if path != "/v1/sms/templates":
            meta["next_page_link"] = link
        payload = gzip.compress(encode({key: rows, "meta": meta}))
        self.send(200, payload, {"Content-Encoding": "gzip"})

    def observed_close(self, start, call):
        with LOCK:
            call["closed_ms"] = int((time.monotonic() - start) * 1000)

    def hold(self, start, call):
        with LOCK:
            call["held"] = True
        until = time.monotonic() + 80
        while time.monotonic() < until:
            if GATE.wait(0.05):
                with LOCK:
                    call["released"] = True
                return True
            if select.select([self.connection], [], [], 0)[0]:
                try:
                    self.connection.settimeout(0.05)
                    if not self.connection.recv(1):
                        self.observed_close(start, call)
                        return False
                except (ConnectionResetError, ssl.SSLError):
                    self.observed_close(start, call)
                    return False
                except TimeoutError:
                    continue
        return False

    def trickle(self, start, call):
        self.send_response(200)
        self.send_header("Content-Length", "300")
        self.end_headers()
        try:
            for _ in range(300):
                self.wfile.write(b" ")
                self.wfile.flush()
                time.sleep(0.075)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            self.observed_close(start, call)


if __name__ == "__main__":
    api = ThreadingHTTPServer(("0.0.0.0", 8443), Provider)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain("/aircall-cert.pem", "/aircall-key.pem")
    api.socket = tls.wrap_socket(api.socket, server_side=True)
    threading.Thread(target=api.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Control).serve_forever()
