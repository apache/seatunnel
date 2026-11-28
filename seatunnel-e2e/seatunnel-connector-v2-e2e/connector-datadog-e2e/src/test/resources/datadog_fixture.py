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


"""Deterministic HTTPS provider and independent fixture control plane for engine tests."""

import email.utils
import gzip
import json
import math
import os
import select
import ssl
import threading
import time

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


ACCESS = os.environ["FIXTURE_ACCESS"]
SECRET = os.environ["FIXTURE_SECRET"]
lock = threading.Lock()
scenario = "normal"
requests = []
released = threading.Event()
cursor_one = "opaque +/%= &雪"
start = "2026-01-01T00:00:00.000001Z"
boundary = "2026-01-02T00:00:00.000001Z"
end = "2026-01-03T00:00:00.000001Z"


def as_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf8")


def event(row_id, stamp):
    return {"id": row_id, "attributes": {"timestamp": stamp, "value": {"empty": [], "n": None}}}


def event_response(q):
    current = q.get("page[cursor]")
    if q.get("filter[from]") == start:
        if current is None:
            return {"data": [event("first", start), event("same", start)],
                    "meta": {"page": {"after": cursor_one}, "status": "done"}}
        if current == cursor_one:
            return {"data": [], "meta": {"page": {"after": "last"}, "status": "done"}}
        if current == "last":
            return {"data": [event("previous", "2026-01-01T23:59:59.999999Z"),
                             event("window-boundary", boundary)]}
    if q.get("filter[from]") == boundary and current is None:
        return {"data": [event("window-boundary", boundary), event("end-exclusive", end)]}
    raise ValueError("Unexpected event query")


def catalog(path, q):
    """Each endpoint has its documented response shape and paging protocol."""
    if path == "/api/v1/dashboard":
        return {"dashboards": {0: [{"id": "d1"}], 1: [{"id": "d2"}, {"id": "d3"}], 3: []}[int(q["start"])]}
    if path == "/api/v1/downtime":
        return [{"id": 9223372036854775807, "active": True, "message": "暂停"}]
    if path == "/api/v2/team":
        return {0: {"data": [{"id": "t1", "type": "team", "attributes": {"name": "Operations", "handle": "ops"}}],
                    "meta": {"pagination": {"next_number": 1, "total": 3}}},
                1: {"data": [{"id": "t2", "type": "team", "attributes": {"name": "Engineering", "handle": "eng"}},
                             {"id": "t3", "type": "team", "attributes": {"name": "Support", "handle": "support"}}],
                    "meta": {"pagination": {"total": 3}}}}[int(q["page[number]"])]
    if path == "/api/v2/incidents":
        return {0: {"data": [{"id": "i1"}], "meta": {"pagination": {"next_offset": 2}}},
                2: {"data": [{"id": "i2"}], "meta": {"pagination": {"next_offset": 4}}},
                4: {"data": [], "meta": {"pagination": {"next_offset": 6}}}}[int(q["page[offset]"])]
    if path == "/api/v2/metrics":
        return {None: b'{"data":["metric.scalar",{"id":"metric.exact","attributes":{"i":9223372036854775807980,"d":0.123456789012345678900,"n":null}}],"meta":{"pagination":{"next_cursor":"metric+next"}}}',
                "metric+next": {"data": [], "links": {"next": "https://api.ap2.datadoghq.com/api/v2/metrics?page%5Bcursor%5D=tail%2Bnext"}},
                "tail+next": {"data": ["metric.tail"]}}[q.get("page[cursor]")]
    if path == "/api/v1/monitor":
        return {0: [{"id": 9007199254740993}], 9007199254740993: [{"id": 9007199254740995}],
                9007199254740995: []}[int(q["id_offset"])]
    if path == "/api/v1/slo":
        return {0: {"data": [{"id": "s1"}], "metadata": {"page": {"total_filtered_count": 3}}},
                1: {"data": [{"id": "s2"}, {"id": "s3"}], "metadata": {"page": {"total_filtered_count": 3}}}}[int(q["offset"])]
    if path == "/api/v1/synthetics/tests":
        return {"tests": {0: [{"public_id": "p1"}], 1: [{"public_id": "p2"}], 2: []}[int(q["page_number"])]}
    if path == "/api/v2/users":
        return {"data": {0: [{"id": "u1"}, {"id": "u2"}], 1: [{"id": "u3", "attributes": {"disabled": True}}]}[int(q["page[number]"])],
                "meta": {"page": {"total_filtered_count": 3}}}
    raise ValueError("Unexpected catalog route")


def parse_query(target):
    values = parse_qs(target.query, keep_blank_values=True, strict_parsing=True)
    if any(len(value) != 1 for value in values.values()):
        raise ValueError("Duplicated query value")
    return {key: value[0] for key, value in values.items()}


class QuietHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def write(self, status, value, headers=None, compressed=False):
        payload = value if isinstance(value, bytes) else as_json(value)
        if compressed:
            payload = gzip.compress(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Connection", "close")
        if compressed:
            self.send_header("Content-Encoding", "gzip")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)


class Api(QuietHandler):
    def do_GET(self):
        target = urlsplit(self.path)
        try:
            q = parse_query(target)
        except ValueError:
            self.write(400, {"errors": ["malformed query"]})
            return
        auth = self.headers.get("DD-API-KEY") == ACCESS and self.headers.get("DD-APPLICATION-KEY") == SECRET
        request = {"method": self.command, "path": target.path, "query": q,
                   "host": self.headers.get("Host"), "auth": auth, "time": time.monotonic()}
        with lock:
            active = scenario
            requests.append(request)
            number = len(requests)
        if not auth:
            self.write(401, {"errors": ["invalid credentials"]})
            return
        try:
            self.reply(active, request, number)
        except (ConnectionError, ssl.SSLError):
            request["closed_after"] = time.monotonic() - request["time"]
        except (KeyError, ValueError):
            self.write(400, {"errors": ["unexpected fixture request"]})

    def reply(self, active, request, number):
        path, q = request["path"], request["query"]
        if active == "normal":
            size_key = {
                "/api/v2/logs/events": "page[limit]", "/api/v2/audit/events": "page[limit]",
                "/api/v1/dashboard": "count", "/api/v2/team": "page[size]",
                "/api/v2/incidents": "page[size]", "/api/v2/metrics": "page[size]",
                "/api/v1/slo": "limit", "/api/v1/synthetics/tests": "page_size", "/api/v2/users": "page[size]",
            }.get(path)
            if size_key and q.get(size_key) != "2":
                raise ValueError("Expected bounded page")
            if path in ("/api/v2/logs/events", "/api/v2/audit/events"):
                self.write(200, event_response(q), compressed=number == 1)
            else:
                self.write(200, catalog(path, q))
            return
        if active == "cancel-backoff":
            self.write(429, {"errors": ["fixture-response-secret"]}, {"X-RateLimit-Reset": "12"})
            return
        if active == "rate" and number == 1:
            self.write(429, {"errors": ["fixture-response-secret"]}, {"Retry-After": "0", "X-RateLimit-Reset": "1"})
            return
        if active == "rate" and number == 2:
            self.write(503, {}, {"Retry-After": email.utils.formatdate(math.ceil(time.time()) + 1, usegmt=True)})
            return
        if active == "rate-numeric" and number == 1:
            self.write(429, {}, {"Retry-After": "1", "X-RateLimit-Reset": "0"})
            return
        if active == "rate-invalid" and number == 1:
            self.write(429, {}, {"Retry-After": "nonsense", "X-RateLimit-Reset": "-3"})
            return
        if active == "rate-excess":
            self.write(429, {}, {"X-RateLimit-Reset": "3600"})
            return
        if active == "retry-exhaust":
            self.write(502, {})
            return
        if active in ("rate", "rate-numeric", "rate-invalid"):
            self.write(200, {"data": ["metric.recovered"]})
            return
        if active in ("held", "cancel-io"):
            self.hold({"data": []}, request)
            return
        if active == "trickle":
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            for _ in range(100):
                self.wfile.write(b" ")
                self.wfile.flush()
                time.sleep(0.05)
            return
        if active == "restore-cursor":
            stamp = q["filter[from]"]
            if "page[cursor]" not in q:
                self.write(200, {"data": [event("before", stamp)], "meta": {"page": {"after": "held"}}})
            else:
                self.hold({"data": [event("after", stamp)]}, request)
            return
        if active == "restore-offset":
            offset = int(q["start"])
            if offset == 0:
                self.write(200, {"dashboards": [{"id": "before"}]})
            elif offset == 1:
                self.hold({"dashboards": [{"id": "after"}]}, request)
            else:
                self.write(200, {"dashboards": []})
            return
        if active == "cycle":
            next_token = "second" if q.get("page[cursor]") == "first" else "first"
            self.write(200, {"data": [], "meta": {"pagination": {"next_cursor": next_token}}})
            return
        if active == "oversize":
            self.write(200, {"data": [{"id": "oversized", "value": "x" * 4000}]}, compressed=True)
            return
        if active == "malformed":
            self.write(200, b'{"data":[{"id":"unused"}]')  # Deliberately incomplete JSON.
            return
        statuses = {
            "unauthorized": (401, {"errors": [ACCESS]}),
            "forbidden": (403, {"errors": [SECRET]}),
            "forbidden-200": (200, {"errors": ["fixture-response-secret"], "data": ["bad"]}),
            "partial": (200, {"data": ["bad"], "meta": {"status": "timeout", "warnings": [{"detail": "fixture-response-secret"}]}}),
            "foreign": (200, {"data": [], "links": {"next": "https://foreign-fixture/api/v2/metrics?page%5Bcursor%5D=next"}}),
            "bad-link": (200, {"data": [], "links": {"next": "/api/v2/metrics?page%5Bcursor%5D=a&page%5Bcursor%5D=b"}}),
        }
        status, body = statuses[active]
        self.write(status, body)

    def hold(self, value, request):
        payload = as_json(value)
        self.send_response(200)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        request["held"] = True
        while not released.is_set():
            ready, _, _ = select.select([self.connection], [], [], 0.1)
            if ready and not self.connection.recv(1):
                request["closed_after"] = time.monotonic() - request["time"]
                return
        request["released"] = True
        self.wfile.write(payload)


class Control(QuietHandler):
    def do_GET(self):
        global scenario
        path = urlsplit(self.path)
        if path.path == "/select":
            released.clear()
            with lock:
                scenario = parse_query(path)["scenario"]
                requests.clear()
        elif path.path == "/release":
            released.set()
        elif path.path == "/requests":
            with lock:
                self.write(200, [dict(request) for request in requests])
            return
        elif path.path != "/ready":
            self.write(404, {})
            return
        self.write(200, {"valid": True})


server = ThreadingHTTPServer(("", 443), Api)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain("/fixture-cert.pem", "/fixture-key.pem")
server.socket = tls.wrap_socket(server.socket, server_side=True)
threading.Thread(target=server.serve_forever, daemon=True).start()
ThreadingHTTPServer(("", 8080), Control).serve_forever()
