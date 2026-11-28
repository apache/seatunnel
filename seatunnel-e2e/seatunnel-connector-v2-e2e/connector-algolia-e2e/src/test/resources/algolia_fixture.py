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

import gzip
import json
import os
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlsplit

HOST = "tenant.algolia.test:8443"
APP = os.environ["FIXTURE_APP"]
KEY = os.environ["FIXTURE_TOKEN"]
INDEX = "shops é+"
INDICES = "/1/indexes"
LANGS = "/1/dictionaries/*/languages"
LOGS = "/1/logs"
START = "2026-11-27T00:00:00Z"
END = "2026-11-27T00:05:00Z"
EARLY = "2026-11-26T23:59:59Z"
MILLIS = "2026-11-27T00:00:00.125Z"
PRIVATE = "algolia-fixture-private-query-header"
PREFIX = '{"label":"café 雪","nested":{"nullable":null,"number":1234567890123456789012345678901234567890,"price":9876543210987654321.123456789012345678900}}'
lock = threading.Lock()


class Scenario:
    def __init__(self, mode):
        self.mode = mode
        self.wire = []
        self.gate = threading.Event()
        self.changed = False


active = Scenario("catalog")


def item(name, updated=START, changed=False):
    record = {"name": name, "updatedAt": updated, "entries": 73 if changed else 70}
    return encoded(record)


def log_record(key, at):
    return encoded({"sha1": key, "timestamp": at, "query_headers": PRIVATE, "answer": None})


def hit(number):
    return '{"objectID":"hit-' + str(number) + '","_rankingInfo":{"nbTypos":1},' \
        + '"_highlightResult":{"title":{"value":"<em>café</em>"}},"record":' + PREFIX + '}'


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def page(field, values, **metadata):
    return '{"' + field + '":[' + ",".join(values) + ']' + (',' + encoded(metadata)[1:] if metadata else '}')


class Wire(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def answer(self, status, value, headers=None, compress=False):
        data = value if isinstance(value, bytes) else (
            value.encode("utf-8") if isinstance(value, str)
            else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        if compress:
            data = gzip.compress(data)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        if compress:
            self.send_header("Content-Encoding", "gzip")
        for key, val in (headers or {}).items():
            self.send_header(key, val)
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def error(self, status, headers=None):
        self.answer(status, {"code": status, "message": PRIVATE}, headers)

    def observe(self, route, query, kind, allowed):
        with lock:
            state = active
            row = {
                "path": route,
                "query": query,
                "kind": kind,
                "auth": allowed,
                "start_ms": time.monotonic_ns() // 1000000,
            }
            state.wire.append(row)
            ordinal = sum(old["path"] == route for old in state.wire)
        return state, row, ordinal

    def wait_for_release(self, state, entry, trickle=False):
        start = time.monotonic()
        entry["held"] = True
        self.connection.settimeout(0.15)
        if trickle:
            self.send_response(200)
            self.send_header("Content-Length", "200000")
            self.end_headers()
        while not state.gate.is_set() and time.monotonic() - start < 110:
            try:
                if trickle:
                    self.wfile.write(b" ")
                    self.wfile.flush()
                if not self.connection.recv(1):
                    break
            except TimeoutError:
                continue
            except (OSError, ValueError):
                break
        else:
            self.connection.settimeout(None)
            entry["released"] = state.gate.is_set()
            return entry["released"]
        entry["closed_ms"] = int((time.monotonic() - start) * 1000)
        self.close_connection = True
        return False

    def credentials(self):
        return (self.headers.get("X-Algolia-Application-Id") == APP
                and self.headers.get("X-Algolia-API-Key") == KEY
                and self.headers.get("Host") == HOST)

    def common_error(self, state, entry, ordinal):
        mode = state.mode
        if not entry["auth"]:
            self.error(401)
        elif mode == "denied":
            self.error(403)
        elif mode == "redirect":
            self.error(302, {"Location": "https://redirect.algolia.test:8443/trap"})
        elif mode == "oversize":
            self.answer(200, {"large": "z" * 150000}, compress=True)
        elif mode == "bad-json":
            self.answer(200, b'{"items":')
        elif mode == "busy":
            self.error(503)
        elif mode == "long-wait":
            self.error(429, {"Retry-After": "120"})
        elif mode == "retry" and ordinal <= 2:
            self.error(429 if ordinal == 1 else 503, {"Retry-After": "1"})
        elif mode in {"stall", "trickle", "cancel", "hold-first"} and ordinal == 1:
            return not self.wait_for_release(state, entry, trickle=mode == "trickle")
        else:
            return False
        return True

    def do_GET(self):
        request = urlsplit(self.path)
        route = request.path
        query = parse_qs(request.query)
        state, entry, ordinal = self.observe(route, query, "GET", self.credentials())
        if self.common_error(state, entry, ordinal):
            return
        if self.headers.get("Accept") != "application/json":
            return self.error(406)
        mode = state.mode
        if route == LANGS and not query:
            langs = {} if mode == "empty-languages" else {
                "en": {"stopwords": ["the"], "enabled": True},
                "fr": {"stopwords": ["été"], "enabled": False},
            }
            return self.answer(200, langs, compress=True)
        if route == INDICES:
            try:
                number = int(query["page"][0])
                size = int(query["hitsPerPage"][0])
            except (ValueError, KeyError):
                return self.error(400)
            if set(query) != {"page", "hitsPerPage"} or not 1 <= size <= 1000 or number < 0:
                return self.error(400)
            if mode == "all":
                sets = [[item("archive", EARLY), item(INDEX)], [item("new", MILLIS)],
                        [item("cutoff", END), item("tie", START)]]
            elif mode == "empty-index-middle":
                sets = [[item(INDEX)], [], [item("new", MILLIS)]]
            elif mode == "repeat-indices":
                sets = [[item(INDEX),item("new", MILLIS)], [item("new", MILLIS),item(INDEX)]]
            elif mode == "hold-index":
                sets = [[item(INDEX)], [item("new", MILLIS)]]
                if number == 1 and ordinal == 2 and not self.wait_for_release(state, entry):
                    return
            elif mode == "long-index":
                sets = [[item("batch-%02d" % i, changed=state.changed and i == 0) for i in range(50)]]
            elif mode == "zero-indices":
                return self.answer(200, page("items", [], nbPages=0, page=0))
            else:
                sets = [[item(INDEX)]]
            if number >= len(sets):
                return self.error(416)
            return self.answer(200, page("items", sets[number], nbPages=len(sets), page=number), compress=True)
        if route == LOGS:
            try:
                offset = int(query["offset"][0])
                count = int(query["length"][0])
            except (ValueError, KeyError):
                return self.error(400)
            if set(query) != {"offset", "length"} or not 1 <= count <= 1000 or offset + count > 1000:
                return self.error(400)
            records = [
                log_record("log-end", END), log_record("log-millis", MILLIS),
                log_record("log-tie-a", START), log_record("log-tie-b", START),
                log_record("log-old", EARLY),
            ]
            if mode == "retention":
                records = [log_record("retained-%04d" % i, START) for i in range(1000)]
            if mode == "repeat-logs" and offset == 2:
                records[2] = records[1]
            if mode == "hold-logs" and offset == 2 and ordinal == 2 and not self.wait_for_release(state, entry):
                return
            return self.answer(200, page("logs", records[offset:offset + count]), compress=True)
        if route.startswith(INDICES + "/") and route.endswith("/settings") and not query:
            index = self.index_name(route)
            if index is None:
                return self.error(404)
            return self.answer(200, {"paginationLimitedTo": 1000, "customRanking": ["desc(score)"], "fixture": index})
        return self.error(404)

    @staticmethod
    def index_name(route):
        parts = route.split("/")
        if len(parts) != 5:
            return None
        raw = parts[3]
        decoded = unquote(raw)
        if decoded in {INDEX, "archive", "new", "tie", "cutoff"} and raw == quote(decoded, safe=""):
            return decoded
        return None

    def do_POST(self):
        route = self.path
        supplied = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(supplied)) if 0 < supplied <= 24000 else {}
        except ValueError:
            body = {}
        params = body.get("params")
        has_form = (isinstance(params, str) and set(body) == {"params"}
                    and self.headers.get("Content-Type") == "application/json")
        query = parse_qs(params, keep_blank_values=True) if has_form else {}
        state, entry, ordinal = self.observe(route, query, "POST", self.credentials())
        entry["params"] = params if has_form else None
        index = self.index_name(route)
        if self.common_error(state, entry, ordinal):
            return
        if index is None or not route.endswith("/query") or not has_form:
            return self.error(422)
        mode = state.mode
        try:
            size = int(query.get("hitsPerPage", ["2"])[0])
            number = int(query.get("page", ["0"])[0])
            if "offset" in query:
                offset = int(query["offset"][0])
                length = int(query.get("length", [size])[0])
                return self.answer(200, page("hits", [hit(90 + offset)], offset=offset, length=length))
        except ValueError:
            return self.error(400)
        if mode == "explicit-page":
            return self.answer(200, page("hits", [hit(40 + number)], nbPages=1, hitsPerPage=size, page=number, nbHits=34))
        if mode == "hold-query" and number == 1 and ordinal == 2 and not self.wait_for_release(state, entry):
            return
        if mode == "cap":
            return self.answer(200, page("hits", [hit(1), hit(2)], nbPages=2, hitsPerPage=size, page=number, nbHits=5))
        if mode == "final-clipped":
            data = [hit(2 * number + 1), hit(2 * number + 2)] if number < 2 else []
            return self.answer(200, page("hits", data, nbPages=3, hitsPerPage=size, page=number, nbHits=5))
        if mode == "approximate":
            return self.answer(200, page("hits", [hit(1)], nbPages=1, hitsPerPage=size, page=number,
                                         nbHits=1, exhaustive={"nbHits": False}))
        if mode == "search-repeat":
            data = [hit(1), hit(2)] if number == 0 else [hit(2)]
            return self.answer(200, page("hits", data, nbPages=2, hitsPerPage=size, page=number, nbHits=3))
        if mode == "wrong-page":
            return self.answer(200, page("hits", [hit(1)], nbPages=1, hitsPerPage=size, page=2, nbHits=1))
        if mode == "zero-hits":
            return self.answer(200, page("hits", [], nbPages=0, hitsPerPage=size, page=0, nbHits=0))
        if index == INDEX:
            data = [hit(1), hit(2)] if number == 0 else [hit(3)]
            pages, count = 2, 3
        else:
            data = [hit(10 + ["archive", "new", "cutoff", "tie"].index(index))]
            pages, count = 1, 1
        return self.answer(200, page("hits", data, nbPages=pages, hitsPerPage=size, page=number,
                                     nbHits=count, exhaustive={"nbHits": True}), compress=True)


class Control(Wire):
    def do_GET(self):
        global active
        parsed = urlsplit(self.path)
        q = parse_qs(parsed.query)
        if parsed.path == "/ready":
            return self.answer(200, {"ready": True})
        if parsed.path == "/select":
            with lock:
                active.gate.set()
                active = Scenario(q["mode"][0])
            return self.answer(200, None)
        if parsed.path == "/release":
            active.gate.set()
            return self.answer(200, None)
        if parsed.path == "/change":
            active.changed = q.get("value") == ["1"]
            return self.answer(200, None)
        if parsed.path == "/wire":
            with lock:
                snapshot = [dict(event) for event in active.wire]
            return self.answer(200, snapshot)
        return self.error(404)


def main():
    service = ThreadingHTTPServer(("0.0.0.0", 8443), Wire)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain("/algolia-cert.pem", "/algolia-key.pem")
    service.socket = tls.wrap_socket(service.socket, server_side=True)
    threading.Thread(target=service.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Control).serve_forever()


if __name__ == "__main__":
    main()
