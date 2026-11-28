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
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, unquote, urlencode, urlsplit

HOST = "tenant.akeneo.test:8443"
PREFIX = "/api/rest/v1"
TOKEN = "/api/oauth/v1/token"
FAMILY = "tailor-é+"
CURSOR = "opaque+/=akeneo"
PRIVATE = "private-akeneo-provider-payload"
USER = os.environ["FIXTURE_USER"]
PASSWORD = os.environ["FIXTURE_PASSWORD"]
BASIC = "Basic " + base64.b64encode(
    (os.environ["FIXTURE_CLIENT"] + ":" + os.environ["FIXTURE_SECRET"]).encode("utf-8")
).decode("ascii")
LOOKUPS = {
    "categories": "furniture",
    "attributes": "description",
    "attribute-groups": "marketing",
    "association-types": "replacements",
    "channels": "ecommerce",
    "locales": "en_US",
    "currencies": "EUR",
}

lock = threading.Lock()


class Scenario:
    def __init__(self, mode):
        self.mode = mode
        self.wire = []
        self.sessions = []
        self.gate = threading.Event()
        self.changed = False


active = Scenario("catalog")


def item(code):
    return {"code": code, "labels": {"fr_FR": "café 雪", "en_US": None}}


def uuid(number):
    return "7dc8455f-c25d-4114-a018-%012d" % number


def product(number, changed=False):
    raw = json.dumps({"uuid": uuid(number), "identifier": None if number == 1 else "sku-%d" % number,
                      "values": {"title": [{"locale": "fr_FR", "scope": "ecommerce", "data": "café 雪"},
                                            {"locale": "en_US", "scope": None, "data": None}]},
                      "version": 1234567890123456789012345678901234567890},
                     ensure_ascii=False, separators=(",", ":"))
    amount = "1.012300" if changed and number == 1 else "9876543210987654321.123456789012345678900"
    return raw[:-1] + ',"amount":' + amount + '}'


def hal(rows, link=None):
    links = {} if link is None else {"next": {"href": link}}
    return '{"_links":' + json.dumps(links, separators=(",", ":")) + ',"_embedded":{"items":[' + ",".join(
        value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        for value in rows) + ']}}'


def url(route, **query):
    return "https://" + HOST + route + "?" + urlencode(query)


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

    def do_POST(self):
        if self.path != TOKEN:
            self.observe(self.path, {}, "unexpected-post", False)
            return self.error(404)
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length)) if 0 < length <= 16384 else {}
        except ValueError:
            body = {}
        grant = body.get("grant_type")
        basic = (self.headers.get("Authorization") == BASIC and
                 self.headers.get("Host") == HOST and self.headers.get("Content-Type") == "application/json")
        with lock:
            state = active
            last = state.sessions[-1] if state.sessions else None
            valid = basic and ((grant == "password" and body == {
                "grant_type": "password", "username": USER, "password": PASSWORD
            }) or (grant == "refresh_token" and last and body == {
                "grant_type": "refresh_token", "refresh_token": last["refresh_token"]
            }))
        state, event, ordinal = self.observe(TOKEN, {}, grant, bool(valid))
        if not valid:
            return self.error(401)
        if state.mode == "auth-redirect":
            return self.error(302, {"Location": "https://redirect.akeneo.test:8443/trap"})
        if state.mode == "auth-bad-refresh" and grant == "refresh_token":
            return self.error(400)
        if state.mode == "auth-oversize":
            return self.answer(200, {"access_token": "x" * 70000}, compress=True)
        if state.mode == "auth-rate" and ordinal == 1:
            return self.error(429, {"Retry-After": "1"})
        if state.mode == "auth-drop":
            self.close_connection = True
            return
        if state.mode == "auth-invalid":
            return self.answer(200, {"access_token": "invalid\nBearer", "expires_in": 3600})
        if state.mode == "hold-auth" and ordinal == 1 and not self.wait_for_release(state, event):
            return
        with lock:
            serial = len(state.sessions) + 1
            session = {"access_token": "akeneo-private-access-" + str(serial),
                       "refresh_token": "akeneo-private-refresh-" + str(serial),
                       "expires_in": 1 if state.mode == "expires" and serial == 1 else 3600,
                       "token_type": "bearer"}
            state.sessions.append(session)
        self.answer(200, session)

    def do_GET(self):
        parsed = urlsplit(self.path)
        route, args = parsed.path, parse_qs(parsed.query, keep_blank_values=True)
        with lock:
            state = active
            last = state.sessions[-1] if state.sessions else None
            allowed = (last is not None and self.headers.get("Authorization") == "Bearer " + last["access_token"]
                       and self.headers.get("Host") == HOST and self.headers.get("Accept") == "application/json")
        state, event, ordinal = self.observe(route, args, "get", allowed)
        if not allowed:
            return self.error(401)
        if route == "/trap":
            return self.error(418)
        if not route.startswith(PREFIX + "/"):
            return self.error(404)
        name = route[len(PREFIX) + 1:]
        product_route = name == "products-uuid"
        measured = name == "measurement-families"
        if measured:
            if args:
                return self.error(422)
            return self.answer(200, [item("Area") | {"standard_unit_code": "SQUARE_METER"}], compress=True)
        if any(len(values) != 1 for values in args.values()):
            return self.error(422)
        try:
            limit = int(args["limit"][0])
            page = int(args.get("page", ["1"])[0])
        except (KeyError, ValueError):
            return self.error(422)
        cursor = args.get("search_after", [None])[0]
        if not 1 <= limit <= 100 or page < 1:
            return self.error(422)
        if product_route:
            if (args.get("pagination_type") != ["search_after"] or "page" in args
                    or (cursor is not None and cursor != CURSOR)):
                return self.error(422)
        elif not set(args) <= {"limit", "page"}:
            return self.error(422)

        if product_route or name == "categories":
            if state.mode in {"auth-bad-refresh", "auth-repeated-401"} and (
                ordinal == 1 or state.mode == "auth-repeated-401"
            ):
                return self.error(401)
            if state.mode == "forbidden":
                return self.error(403)
            if state.mode == "missing":
                return self.error(404)
            if state.mode == "redirect":
                return self.error(302, {"Location": "https://redirect.akeneo.test:8443/trap"})
            if state.mode == "busy":
                return self.error(503)
            if state.mode == "long-retry":
                return self.error(429, {"Retry-After": "120"})
            if state.mode == "retry" and ordinal <= 2:
                return self.error(429 if ordinal == 1 else 503, {"Retry-After": "1"})
            if state.mode == "oversize":
                return self.answer(200, {"value": "a" * 150000}, compress=True)
            if state.mode == "bad-json":
                return self.answer(200, b'{"_embedded": {"items":[')
            if state.mode == "invalid-envelope":
                return self.answer(200, {"code": 200, "message": PRIVATE})
            if state.mode in {"stall", "trickle", "cancel"} and not self.wait_for_release(
                    state, event, trickle=state.mode == "trickle"):
                return

        if product_route:
            if state.mode == "long-page":
                return self.answer(200, hal([product(i, state.changed) for i in range(1, 51)]), compress=True)
            next_link = url(route, pagination_type="search_after", limit=limit, search_after=CURSOR)
            if state.mode == "foreign":
                next_link = next_link.replace(HOST, "redirect.akeneo.test:8443")
            if state.mode == "with-userinfo":
                next_link = next_link.replace("https://", "https://unexpected@")
            if state.mode == "query-change":
                next_link += "&search=%7B%7D"
            if state.mode == "expires" and cursor is None:
                time.sleep(1.2)
            if state.mode == "refresh-on-401" and cursor is not None and ordinal == 2:
                return self.error(401)
            if state.mode == "hold-cursor" and cursor is not None and ordinal == 2:
                if not self.wait_for_release(state, event):
                    return
            if cursor is None:
                initial = [] if state.mode == "empty-next" else [product(1)]
                return self.answer(200, hal(initial, next_link), compress=True)
            last_link = next_link if state.mode == "cursor-loop" else None
            return self.answer(200, hal([product(2)], last_link), compress=True)

        if name == "families":
            families = [item(FAMILY)] if page == 1 else [item("shirts")]
            return self.answer(200, hal(families, url(route, page=2, limit=limit) if page == 1 else None))
        if name.startswith("families/") and name.endswith("/variants"):
            raw_parent = name.split("/")[1]
            family = unquote(raw_parent)
            if family not in {FAMILY, "shirts"} or raw_parent != quote(family, safe=""):
                return self.error(422)
            if state.mode == "hold-child" and family == FAMILY and page == 2 and ordinal == 2:
                if not self.wait_for_release(state, event):
                    return
            next_link = url(route, page=2, limit=limit) if family == FAMILY and page == 1 else None
            if next_link and state.mode == "wrong-parent":
                next_link = next_link.replace(quote(FAMILY, safe=""), "shirts")
            return self.answer(200, hal([item("variant-" + ("left" if family == FAMILY else "right")
                                               + "-" + str(page))], next_link))
        if name == "measure-families":
            return self.answer(200, hal([item("legacy-" + str(page)) | {"standard": "PIECE"}],
                                       url(route, page=2, limit=limit) if page == 1 else None))
        if name in LOOKUPS:
            if name == "categories" and state.mode == "hold-offset":
                if page == 2 and ordinal == 2 and not self.wait_for_release(state, event):
                    return
                return self.answer(200, hal([item("category-" + str(page))],
                                            url(route, page=2, limit=limit) if page == 1 else None))
            if name == "categories" and state.mode in {"page-skip", "limit-change"}:
                return self.answer(200, hal([item("furniture")],
                                            url(route, page=3 if state.mode == "page-skip" else 2,
                                                limit=limit + 1 if state.mode == "limit-change" else limit)))
            return self.answer(200, hal([item(LOOKUPS[name])]), compress=True)
        return self.error(404)


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
    tls.load_cert_chain("/akeneo-cert.pem", "/akeneo-key.pem")
    service.socket = tls.wrap_socket(service.socket, server_side=True)
    threading.Thread(target=service.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Control).serve_forever()


if __name__ == "__main__":
    main()
