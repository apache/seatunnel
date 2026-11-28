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
import hashlib
import hmac
import json
import re
import socket
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ACCESS = "AKIA000000000000TEST"
SECRET = b"syntheticFirehoseSecret73+test"
TOKEN = "syntheticFirehoseSessionToken-5981"
TARGET = "Firehose_20150804.PutRecordBatch"
STREAM = "seatunnel-test-delivery"
MAX_BODY = 4 * 1024 * 1024
LOCK = threading.Lock()
STATE = {}

def reset(steps):
    STATE.clear()
    STATE.update(steps=steps, requests=[], accepted=[], redirects=0,
                 disconnect_ms=None, release=threading.Event())

def signature_is_valid(headers, payload):
    value = headers.get("Authorization", "")
    match = re.fullmatch(
        r"AWS4-HMAC-SHA256 Credential=([^/]+)/(\d{8})/us-east-1/firehose/aws4_request, "
        r"SignedHeaders=([a-z0-9;-]+), Signature=([a-f0-9]{64})", value)
    if not match or match[1] != ACCESS:
        return False
    date = headers.get("X-Amz-Date", "")
    sent_token = headers.get("X-Amz-Security-Token")
    names = match[3].split(";")
    required = {"host", "content-type", "x-amz-date", "x-amz-target"}
    if sent_token is not None:
        required.add("x-amz-security-token")
    if (sent_token not in (None, TOKEN) or not re.fullmatch(match[2] + r"T\d{6}Z", date)
            or not required.issubset(names) or names != sorted(set(names))):
        return False
    rendered = []
    for key in names:
        parts = headers.get_all(key)
        if not parts:
            return False
        rendered.append(f"{key}:{','.join(' '.join(v.split()) for v in parts)}\n")
    canonical = "POST\n/\n\n" + "".join(rendered) + "\n" + match[3] + "\n" + hashlib.sha256(payload).hexdigest()
    scope = f"{match[2]}/us-east-1/firehose/aws4_request"
    message = f"AWS4-HMAC-SHA256\n{date}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = b"AWS4" + SECRET
    for item in scope.split("/"):
        key = hmac.digest(key, item.encode(), "sha256")
    return hmac.compare_digest(hmac.digest(key, message.encode(), "sha256").hex(), match[4])

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def respond(self, status, body=b"", **headers):
        self.send_response(status)
        self.send_header("Connection", "close")
        self.send_header("Content-Type", "application/x-amz-json-1.1")
        self.send_header("Content-Length", str(len(body)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        if body:
            self.wfile.write(body)
        self.close_connection = True

    def read_input(self, limit):
        length = int(self.headers.get("Content-Length", "0"))
        if length < 0 or length > limit:
            raise ValueError("invalid content length")
        return self.rfile.read(length)

    def do_GET(self):
        if self.path == "/health":
            self.respond(200)
        elif self.path == "/state":
            with LOCK:
                result = {key: STATE[key] for key in ("requests", "accepted", "redirects", "disconnect_ms")}
                body = json.dumps(result).encode()
            self.respond(200, body)
        else:
            self.respond(404)

    def do_POST(self):
        if self.path == "/reset":
            with LOCK:
                reset(json.loads(self.read_input(65536)))
            self.respond(204)
        elif self.path == "/release":
            with LOCK:
                STATE["release"].set()
            self.respond(204)
        elif self.path == "/redirect-catcher":
            with LOCK:
                STATE["redirects"] += 1
            self.respond(500)
        elif self.path == "/":
            self.put_record_batch()
        else:
            self.respond(404)

    def put_record_batch(self):
        start = time.monotonic_ns()
        request = self.read_input(MAX_BODY)
        verified = signature_is_valid(self.headers, request)
        payload = json.loads(request)
        records = payload["Records"]
        decoded = [base64.b64decode(item["Data"], validate=True) for item in records]
        target = self.headers.get("X-Amz-Target")
        wire_type = self.headers.get("Content-Type")
        with LOCK:
            index = len(STATE["requests"])
            STATE["requests"].append({
                "target": target, "content_type": wire_type, "size": len(request),
                "stream": payload["DeliveryStreamName"], "verified": verified,
                "session": self.headers.get("X-Amz-Security-Token") is not None,
                "records": [base64.b64encode(value).decode() for value in decoded], "time_ns": start,
            })
            step = STATE["steps"][index] if index < len(STATE["steps"]) else {}
            release = STATE["release"]
        if not verified:
            self.respond(403, b'{"__type":"AccessDeniedException","message":"provider-private-marker-327"}')
            return
        if target != TARGET or wire_type != "application/x-amz-json-1.1" or payload["DeliveryStreamName"] != STREAM:
            self.respond(400)
            return
        if not 1 <= len(records) <= 500 or any(not 1 <= len(item) <= 1000 * 1024 for item in decoded):
            self.respond(400)
            return
        kind = step.get("kind", "ok")
        if kind == "status":
            code = {400: "ResourceNotFoundException", 403: "AccessDeniedException",
                    500: "ServiceUnavailableException"}[step["status"]]
            self.respond(step["status"], json.dumps({"__type": code,
                         "message": "provider-private-marker-327"}).encode(),
                         **{"x-amzn-ErrorType": code})
            return
        if kind == "redirect":
            self.respond(307, **{"Location": "/redirect-catcher"})
            return
        if kind == "hold":
            self.connection.settimeout(0.1)
            while not release.is_set():
                try:
                    if not self.connection.recv(1):
                        self.record_disconnect(start)
                        return
                except socket.timeout:
                    continue
                except ConnectionResetError:
                    self.record_disconnect(start)
                    return
            self.connection.settimeout(5)
        if kind == "trickle":
            self.send_response(200)
            self.send_header("Content-Type", "application/x-amz-json-1.1")
            self.send_header("Content-Length", "9999")
            self.end_headers()
            for _ in range(120):
                try:
                    self.connection.sendall(b" ")
                    time.sleep(0.05)
                except (BrokenPipeError, ConnectionResetError):
                    self.record_disconnect(start)
                    return
            self.close_connection = True
            return
        if kind == "oversize":
            self.respond(200, gzip.compress(b" " * (1024 * 1024 + 1)),
                         **{"Content-Encoding": "gzip"})
            return

        responses = []
        failures = step.get("failures", {})
        with LOCK:
            for i, item in enumerate(decoded):
                if str(i) in failures:
                    responses.append({"ErrorCode": failures[str(i)], "ErrorMessage": "provider-private-marker-327"})
                else:
                    STATE["accepted"].append(base64.b64encode(item).decode())
                    responses.append({"RecordId": f"record-{len(STATE['accepted'])}"})
        if kind == "disconnect":
            self.close_connection = True
            return
        body = json.dumps({"FailedPutCount": len(failures), "RequestResponses": responses}).encode()
        headers = {}
        if kind == "override":
            body = step["body"].encode()
        if kind == "gzip":
            headers["Content-Encoding"] = "gzip"
            body = gzip.compress(body)
        if kind == "deflate":
            headers["Content-Encoding"] = "deflate"
            body = zlib.compress(body)
        self.respond(200, body, **headers)

    def record_disconnect(self, start):
        with LOCK:
            STATE["disconnect_ms"] = (time.monotonic_ns() - start) // 1_000_000
        self.close_connection = True

reset([])
ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
