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

import gzip
import json
import os
import select
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

RESOURCES = ("campaigns", "contacts", "lists", "deals", "segments", "forms")
TOKEN = os.environ["FIXTURE_TOKEN"]
PRIVATE = "sensitive-provider-diagnostic"
LOCK = threading.Lock()
RELEASE = threading.Event()
REQUESTS = []
MODE = "normal"
BIG_ID = 1234567890123456789012345678901234567890


def record(resource, offset):
    return {
        "id": BIG_ID if resource == "campaigns" and offset == 0 else f"{resource}-{offset}",
        "name": "café 雪",
        "nested": {"empty": None, "values": [True, 127, "DECIMAL_FIXTURE"]},
    }


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).replace(
        '"DECIMAL_FIXTURE"', "1234567890123456789.123456789012345678900"
    ).encode("utf-8")


class QuietHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, code, payload, headers=None):
        body = payload if isinstance(payload, bytes) else encoded(payload)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            self.dispatch()
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass


class Control(QuietHandler):
    def dispatch(self):
        global MODE
        route = urlsplit(self.path)
        if route.path == "/ready":
            self.respond(200, {"ready": True})
        elif route.path == "/select":
            RELEASE.set()
            with LOCK:
                MODE = parse_qs(route.query)["mode"][0]
                REQUESTS.clear()
                RELEASE.clear()
            self.respond(200, {"selected": True})
        elif route.path == "/release":
            RELEASE.set()
            self.respond(200, {"released": True})
        elif route.path == "/requests":
            with LOCK:
                events = [event.copy() for event in REQUESTS]
            self.respond(200, events)
        else:
            self.respond(404, {})


class Provider(QuietHandler):
    protocol_version = "HTTP/1.1"

    def dispatch(self):
        self.close_connection = True
        route = urlsplit(self.path)
        query = parse_qs(route.query, strict_parsing=True)
        auth = self.headers.get_all("Api-Token", [])
        now = time.monotonic()
        event = {
            "path": route.path,
            "query": query,
            "authorized": auth == [TOKEN],
            "host": self.headers.get("Host"),
            "gzip": self.headers.get("Accept-Encoding") == "gzip",
            "started_ms": int(now * 1000),
        }
        with LOCK:
            REQUESTS.append(event)
            mode = MODE
            call = len(REQUESTS)
        if route.path == "/foreign":
            self.respond(200, {"crossed_origin": True})
            return
        resource = route.path.removeprefix("/api/3/")
        if not route.path.startswith("/api/3/") or resource not in RESOURCES:
            self.respond(404, {"message": PRIVATE})
            return
        if auth != [TOKEN]:
            self.respond(403, {"message": PRIVATE})
            return
        if set(query) != {"limit", "offset"} or any(len(v) != 1 for v in query.values()):
            self.respond(422, {"errors": [{"title": PRIVATE}]})
            return
        limit = int(query["limit"][0])
        offset = int(query["offset"][0])
        if not 1 <= limit <= 100 or offset < 0 or offset % limit:
            self.respond(422, {"errors": [{"title": PRIVATE}]})
            return
        if mode in ("stall", "cancel") or mode == "early" and call == 1 or mode == "boundary" and offset == limit and call == 2:
            if not self.hold(now, event):
                return
        if mode == "trickle":
            self.trickle(now, event)
            return
        if mode == "redirect":
            self.respond(302, {"notice": PRIVATE}, {"Location": "https://foreign.activecampaign.test:8443/foreign"})
            return
        if mode in ("unauthorized", "payment", "missing", "invalid"):
            status = {"unauthorized": 401, "payment": 402, "missing": 404, "invalid": 422}[mode]
            self.respond(status, {"errors": [{"title": PRIVATE}]})
            return
        if mode == "retry" and call <= 2:
            self.respond(429 if call == 1 else 503, {"message": PRIVATE},
                         {"Retry-After": "1" if call == 1 else "invalid"})
            return
        if mode in ("long-retry", "busy"):
            self.respond(429 if mode == "long-retry" else 503, {"message": PRIVATE},
                         {"Retry-After": "300" if mode == "long-retry" else "0"})
            return
        if mode == "oversized":
            self.respond(200, gzip.compress(encoded({resource: [{"id": "size", "details": "x" * 5000}]})),
                         {"Content-Encoding": "gzip"})
            return
        if mode == "bad-gzip":
            self.respond(200, b"not-gzip", {"Content-Encoding": "gzip"})
            return
        if mode == "bad-json":
            self.respond(200, b'{"' + PRIVATE.encode() + b'":')
            return
        if mode == "provider-errors":
            self.respond(200, {resource: [], "errors": [{"title": PRIVATE}]})
            return
        if mode == "wrong-root":
            self.respond(200, {"unexpected": [{"id": PRIVATE}]})
            return
        if mode == "bad-meta":
            self.respond(200, {resource: [], "meta": {"total": "unknown"}})
            return
        if mode == "contradiction":
            self.respond(200, {resource: [record(resource, 0)], "meta": {"total": 0}})
            return
        if mode == "oversized-page":
            self.respond(200, {resource: [record(resource, i) for i in range(limit + 1)]})
            return
        if mode == "empty-loop":
            self.respond(200, {resource: [], "meta": {"total": "999999"}})
            return
        if mode == "cycle":
            values = [record(resource, i) for i in range(limit)]
            self.respond(200, {resource: list(reversed(values)) if offset else values,
                               "meta": {"total": 100}})
            return
        if mode == "normal" and resource == "contacts":
            result = [record(resource, offset)] if offset in (0, 6) else []
            self.respond(200, {resource: result, "meta": {"total": "7"}})
            return
        if mode == "normal" and resource == "forms":
            self.respond(200, {resource: [record(resource, i) for i in range(offset, 4)] if offset else
                               [record(resource, i) for i in range(limit)]})
            return
        if mode == "normal":
            content = {resource: [record(resource, i) for i in range(offset, min(offset + limit, 4))],
                       "meta": {"total": 4}}
            self.respond(200, gzip.compress(encoded(content)), {"Content-Encoding": "gzip"})
            return
        if mode == "boundary":
            if resource == "campaigns":
                self.respond(200, {resource: [record(resource, offset)], "meta": {"total": limit + 1}})
            else:
                self.respond(200, {resource: [], "meta": {"total": 0}})
            return
        if mode == "midpage" and resource == "campaigns":
            self.respond(200, {resource: [record(resource, i) for i in range(100)], "meta": {"total": 100}})
            return
        if mode in ("early", "midpage", "terminal") and resource != ("contacts" if mode == "terminal" else "campaigns"):
            self.respond(200, {resource: [], "meta": {"total": 0}})
            return
        self.respond(200, {resource: [record(resource, offset)], "meta": {"total": 1}})

    def closed(self, start, event):
        with LOCK:
            event["closed_ms"] = int((time.monotonic() - start) * 1000)

    def hold(self, start, event):
        with LOCK:
            event["held"] = True
        until = start + 85
        while time.monotonic() < until:
            if RELEASE.wait(0.05):
                with LOCK:
                    event["released"] = True
                return True
            if select.select([self.connection], [], [], 0)[0]:
                try:
                    self.connection.settimeout(0.05)
                    if not self.connection.recv(1):
                        self.closed(start, event)
                        return False
                except (ConnectionResetError, ssl.SSLError):
                    self.closed(start, event)
                    return False
                except TimeoutError:
                    continue
        return False

    def trickle(self, start, event):
        with LOCK:
            event["held"] = True
        self.send_response(200)
        self.send_header("Content-Length", "300")
        self.end_headers()
        try:
            for _ in range(300):
                self.wfile.write(b" ")
                self.wfile.flush()
                time.sleep(0.075)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            self.closed(start, event)


if __name__ == "__main__":
    secure = ThreadingHTTPServer(("0.0.0.0", 8443), Provider)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain("/fixture-cert.pem", "/fixture-key.pem")
    secure.socket = tls.wrap_socket(secure.socket, server_side=True)
    threading.Thread(target=secure.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Control).serve_forever()
