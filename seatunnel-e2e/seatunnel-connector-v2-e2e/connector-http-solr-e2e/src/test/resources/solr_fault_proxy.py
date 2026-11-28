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
import ssl
import threading
import time
import urllib.error
import urllib.request
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SOLR = "http://solr-fixture:8983"
PREFIX = "/solr/records/"
AUTH = "Basic " + base64.b64encode(b"fixture-solr-admin:fixture-password:912").decode()
MARKER = "private-solr-response-533"
LOCK = threading.Lock()
STATE = {}

def reset(options):
    STATE.clear()
    STATE.update(
        options=options,
        calls=[],
        redirect_hits=0,
        disconnect_ms=None,
        release=threading.Event(),
    )

def provider(method, path, body):
    request = urllib.request.Request(
        SOLR + path, data=body, method=method,
        headers={"Accept": "application/json", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as result:
            return result.status, result.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def read_bytes(self, limit):
        length = int(self.headers.get("Content-Length", "0"))
        if not 0 <= length <= limit:
            raise ValueError("invalid request size")
        return self.rfile.read(length)

    def respond(self, code, body=b"", **headers):
        self.send_response(code)
        self.send_header("Connection", "close")
        self.send_header("Content-Type", "application/json")
        if code != 204:
            self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        if body:
            self.wfile.write(body)
        self.close_connection = True

    def do_GET(self):
        if self.path == "/health":
            self.respond(200)
        elif self.path == "/state":
            with LOCK:
                response = json.dumps({key: STATE[key] for key in
                            ("calls", "redirect_hits", "disconnect_ms")}).encode()
            self.respond(200, response)
        elif self.path == "/redirect":
            self.redirect()
        elif self.path.startswith(PREFIX + "schema/uniquekey?"):
            self.forward("GET", None)
        else:
            self.respond(404)

    def do_POST(self):
        if self.path == "/reset":
            config = json.loads(self.read_bytes(65536))
            with LOCK:
                reset(config)
            self.respond(204)
        elif self.path == "/release":
            with LOCK:
                STATE["release"].set()
            self.respond(204)
        elif self.path == "/redirect":
            self.redirect()
        elif self.path.startswith(PREFIX + "update?"):
            self.forward("POST", self.read_bytes(4 * 1024 * 1024))
        else:
            self.respond(404)

    def redirect(self):
        with LOCK:
            STATE["redirect_hits"] += 1
        self.respond(500)

    def forward(self, method, body):
        start = time.monotonic_ns()
        verified = self.headers.get("Authorization") == AUTH
        with LOCK:
            config = STATE["options"]
            calls = STATE["calls"]
            index = sum(call["method"] == "POST" for call in calls)
            calls.append({"method": method, "verified": verified,
                          "body": None if body is None else body.decode(),
                          "path": self.path})
            release = STATE["release"]
        if config.get("auth", False) and not verified:
            self.respond(401, json.dumps({"error": {"msg": MARKER}}).encode())
            return
        if method == "GET":
            if "key" in config:
                self.respond(200, json.dumps({
                    "uniqueKey": config["key"], "responseHeader": {"status": 0}
                }).encode())
            else:
                self.respond(*provider(method, self.path, body))
            return
        steps = config.get("steps", [])
        step = steps[index] if index < len(steps) else {"kind": "forward"}
        kind = step.get("kind", "forward")
        if kind == "status":
            self.respond(step["status"], json.dumps({"error": {"msg": MARKER}}).encode())
            return
        if kind == "redirect":
            self.respond(307, **{"Location": "/redirect"})
            return
        if kind == "hold":
            self.connection.settimeout(0.1)
            while not release.is_set():
                try:
                    if not self.connection.recv(1):
                        self.disconnected(start)
                        return
                except socket.timeout:
                    continue
                except ConnectionResetError:
                    self.disconnected(start)
                    return
            self.connection.settimeout(5)
        if kind == "trickle":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "8000")
            self.end_headers()
            for _ in range(120):
                try:
                    self.connection.sendall(b" ")
                    time.sleep(0.05)
                except (BrokenPipeError, ConnectionResetError):
                    self.disconnected(start)
                    return
            self.close_connection = True
            return
        if kind == "invalid_json":
            self.respond(200, b'{"responseHeader":')
            return
        if kind == "oversize":
            self.respond(200, gzip.compress(b" " * (256 * 1024 + 1)),
                         **{"Content-Encoding": "gzip"})
            return
        if kind == "partial":
            batch = json.loads(body)
            status, _ = provider(method, self.path, json.dumps(batch[:1]).encode())
            if status != 200:
                self.respond(502)
                return
            self.respond(200, json.dumps({
                "responseHeader": {"status": 0, "errors": [{"message": MARKER}]},
            }).encode())
            return
        status, response = provider(method, self.path, body)
        if kind == "applied_disconnect" and status == 200:
            self.close_connection = True
        elif kind == "gzip":
            self.respond(status, gzip.compress(response), **{"Content-Encoding": "gzip"})
        elif kind == "deflate":
            self.respond(status, zlib.compress(response), **{"Content-Encoding": "deflate"})
        else:
            self.respond(status, response)

    def disconnected(self, start):
        with LOCK:
            STATE["disconnect_ms"] = (time.monotonic_ns() - start) // 1_000_000
        self.close_connection = True

reset({})
secure = ThreadingHTTPServer(("0.0.0.0", 8443), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/fixture-cert.pem", "/fixture-key.pem")
secure.socket = context.wrap_socket(secure.socket, server_side=True)
threading.Thread(target=secure.serve_forever, daemon=True).start()
ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
