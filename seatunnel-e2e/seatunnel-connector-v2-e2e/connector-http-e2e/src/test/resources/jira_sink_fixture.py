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

import base64
import gzip
import json
import socket
import threading
import time
import zlib
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ISSUE = "/rest/api/3/issue"
AUTH = "Basic " + base64.b64encode("fixture-王@example.test:fixture-token-δ:791".encode()).decode()
LOCK = threading.Lock()
STATE = {}


def reset(steps):
    STATE.clear()
    STATE.update(
        steps=steps,
        requests=[],
        issues={},
        pending=[],
        next_id=100,
        redirects=0,
        disconnect_ms=None,
        hold=threading.Event(),
    )


def apply(method, target, fields):
    if method == "POST":
        identifier = str(STATE["next_id"])
        key = f"DEMO-{STATE['next_id']}"
        STATE["next_id"] += 1
        STATE["issues"][key] = {"id": identifier, "fields": fields}
        return key, identifier
    if target.isdigit():
        target = next((key for key, issue in STATE["issues"].items() if issue["id"] == target), target)
    issue = STATE["issues"].get(target)
    if issue is None:
        return None, None
    if method == "PUT":
        issue["fields"].update(fields)
    else:
        del STATE["issues"][target]
    return target, issue["id"]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def respond(self, status, body=b"", **headers):
        self.send_response(status)
        self.send_header("Connection", "close")
        if status != 204 and "Content-Length" not in headers:
            self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        if body:
            self.wfile.write(body)
        self.close_connection = True

    def read_input(self):
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def do_GET(self):
        if self.path == "/_health":
            self.respond(200)
        elif self.path == "/_state":
            with LOCK:
                body = json.dumps({key: STATE[key] for key in
                                   ("issues", "requests", "redirects", "disconnect_ms")}, default=str).encode()
            self.respond(200, body, **{"Content-Type": "application/json"})
        else:
            self.respond(404)

    def do_POST(self):
        if self.path == "/_reset":
            steps = json.loads(self.read_input())
            with LOCK:
                reset(steps)
            self.respond(204)
        elif self.path == "/_release":
            with LOCK:
                for method, target, fields in STATE["pending"]:
                    apply(method, target, fields)
                STATE["pending"].clear()
                STATE["hold"].set()
            self.respond(204)
        else:
            self.mutate("POST")

    def do_PUT(self):
        self.mutate("PUT")

    def do_DELETE(self):
        self.mutate("DELETE")

    def mutate(self, method):
        raw = self.read_input().decode("utf-8")
        start = time.monotonic_ns()
        authorized = self.headers.get("Authorization") == AUTH
        with LOCK:
            if self.path == "/_redirect":
                STATE["redirects"] += 1
                self.respond(500)
                return
            index = len(STATE["requests"])
            step = STATE["steps"][index] if index < len(STATE["steps"]) else {}
            STATE["requests"].append({
                "path": self.path, "method": method, "body": raw,
                "authorized": authorized, "time_ns": start,
            })
            state = STATE.copy()
        if not authorized:
            self.respond(401, b'{"errorMessages":["fixture-error-body-912"]}')
            return

        path, _, query = self.path.partition("?")
        if method == "POST":
            valid_path = path == ISSUE and not query
        else:
            valid_path = path.startswith(f"{ISSUE}/") and bool(path[len(ISSUE) + 1:])
            valid_path = valid_path and query == ("deleteSubtasks=false" if method == "DELETE" else "")
        if not valid_path:
            self.respond(400)
            return

        fields = None
        if method != "DELETE":
            try:
                body = json.loads(raw, parse_float=Decimal)
                if set(body) != {"fields"} or not isinstance(body["fields"], dict):
                    raise ValueError("invalid fields envelope")
                fields = body["fields"]
            except ValueError:
                self.respond(400)
                return
        elif raw:
            self.respond(400)
            return

        kind = step.get("kind", "ok")
        if kind == "hold":
            # An out-of-band release permits the acknowledgement. A client abort is measured
            # from the time this request arrived without relying on engine startup or shutdown.
            self.connection.settimeout(0.1)
            while not state["hold"].is_set():
                try:
                    if not self.connection.recv(1):
                        with LOCK:
                            STATE["disconnect_ms"] = (time.monotonic_ns() - start) // 1_000_000
                        self.close_connection = True
                        return
                except socket.timeout:
                    continue
                except (ConnectionResetError, BrokenPipeError):
                    with LOCK:
                        STATE["disconnect_ms"] = (time.monotonic_ns() - start) // 1_000_000
                    self.close_connection = True
                    return
            self.connection.settimeout(5)
        if kind == "redirect":
            self.respond(307, **{"Location": "/_redirect"})
            return

        target = path[len(ISSUE) + 1:]
        if kind == "pending_503":
            with LOCK:
                STATE["pending"].append((method, target, fields))
            self.respond(503, **{"Retry-After": "0"})
            return
        if "status" in step:
            headers = {"Retry-After": step["retry"]} if "retry" in step else {}
            self.respond(step["status"], b'{"errorMessages":["fixture-error-body-912"]}', **headers)
            return

        with LOCK:
            key, identifier = apply(method, target, fields)
        if key is None:
            self.respond(404)
        elif kind == "disconnect":
            self.close_connection = True
        elif method != "POST":
            if kind == "contradictory_204":
                self.respond(204, **{"Content-Length": "20"})
            else:
                self.respond(204)
        else:
            body = json.dumps({"key": key, "id": identifier}).encode()
            headers = {"Content-Type": "application/json"}
            if kind == "gzip":
                body = gzip.compress(body)
                headers["Content-Encoding"] = "gzip"
            elif kind == "deflate":
                body = zlib.compress(body)
                headers["Content-Encoding"] = "deflate"
            elif kind == "ack_override":
                body = step["body"].encode()
            self.respond(201, body, **headers)


reset([])
ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
