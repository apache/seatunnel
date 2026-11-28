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


"""Independent AWS CBOR and SigV4 fixture for the packaged Kinesis client."""

import base64
import gzip
import hashlib
import hmac
import io
import json
import os
import re
import select
import ssl
import struct
import threading
import time

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


ACCESS = os.environ["FIXTURE_ACCESS"]
SECRET = os.environ["FIXTURE_SECRET"]
TOKEN = os.environ["FIXTURE_TOKEN"]
ARN = "arn:aws:kinesis:us-east-1:111122223333:stream/in"
FIRST = "90071992547409931234567890"
SECOND = "90071992547409931234567891"
SHARD_A = "shardId-000000000000"
SHARD_B = "shardId-000000000001"
CHILD = "shardId-000000000002"
CREATED = 1700000000000
WHEN = 1700000000123
MAX = 16 * 1024 * 1024
scenario = "read"
calls = []
accepted = []
seen = {}
released = threading.Event()
lock = threading.Lock()


def cbor_read(raw):
    stream = io.BytesIO(raw)

    def read(count):
        value = stream.read(count)
        if len(value) != count:
            raise ValueError("truncated CBOR")
        return value

    def token():
        first = read(1)[0]
        kind, info = first >> 5, first & 31
        if kind == 7:
            if info == 20:
                return False
            if info == 21:
                return True
            if info == 22:
                return None
            if info in (25, 26, 27):
                return struct.unpack({25: ">e", 26: ">f", 27: ">d"}[info], read(1 << (info - 24)))[0]
            raise ValueError("unsupported CBOR simple value")
        length = (None if info == 31 else info if info < 24
                  else int.from_bytes(read(1 << (info - 24)), "big"))
        if kind in (0, 1):
            return length if kind == 0 else -length - 1
        if kind in (2, 3):
            value = read(length)
            return value if kind == 2 else value.decode()
        if kind in (4, 5):
            items = []
            while length is None or len(items) < (length if kind == 4 else length * 2):
                if length is None and stream.getbuffer()[stream.tell()] == 255:
                    read(1)
                    break
                items.append(token())
            return items if kind == 4 else dict(zip(items[::2], items[1::2]))
        if kind == 6:
            return token()
        raise ValueError("unsupported CBOR major type")

    value = token()
    if stream.read(1):
        raise ValueError("trailing CBOR")
    return value


def cbor_encode(value):
    def header(kind, size):
        if size < 24:
            return bytes([kind * 32 + size])
        width = 1 if size < 256 else 2 if size < 65536 else 4 if size < 2**32 else 8
        return bytes([kind * 32 + 24 + (width.bit_length() - 1)]) + size.to_bytes(width, "big")

    if value is None:
        return b"\xf6"
    if value is True:
        return b"\xf5"
    if value is False:
        return b"\xf4"
    if isinstance(value, int):
        return header(0, value) if value >= 0 else header(1, -1 - value)
    if isinstance(value, bytes):
        return header(2, len(value)) + value
    if isinstance(value, str):
        raw = value.encode()
        return header(3, len(raw)) + raw
    if isinstance(value, list):
        return header(4, len(value)) + b"".join(cbor_encode(x) for x in value)
    if isinstance(value, dict):
        return header(5, len(value)) + b"".join(cbor_encode(k) + cbor_encode(v) for k, v in value.items())
    raise ValueError("unsupported fixture value")


def authenticate(headers, body):
    authorization = headers.get("Authorization", "")
    match = re.fullmatch(
        r"AWS4-HMAC-SHA256 Credential=([^/]+)/([0-9]{8})/([^/]+)/kinesis/aws4_request, "
        r"SignedHeaders=([^,]+), Signature=([0-9a-f]{64})", authorization)
    if not match:
        return False
    access, day, region, signed, signature = match.groups()
    timestamp = headers.get("X-Amz-Date", "")
    names = signed.split(";")
    if (access != ACCESS or region != "us-east-1" or not timestamp.startswith(day)
            or headers.get("X-Amz-Security-Token") != TOKEN
            or sorted(set(names)) != names
            or not {"host", "content-type", "x-amz-date", "x-amz-target", "x-amz-security-token"}.issubset(names)):
        return False
    payload_hash = hashlib.sha256(body).hexdigest()
    if headers.get("X-Amz-Content-Sha256") != payload_hash:
        return False
    def combine(key):
        return ",".join(" ".join(v.split()) for v in headers.get_all(key, []))
    canonical_headers = "".join(key + ":" + combine(key) + "\n" for key in names)
    canonical = "POST\n/\n\n" + canonical_headers + "\n" + signed + "\n" + payload_hash
    scope = day + "/" + region + "/kinesis/aws4_request"
    message = "AWS4-HMAC-SHA256\n" + timestamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
    key = ("AWS4" + SECRET).encode()
    for part in (day, region, "kinesis", "aws4_request", message):
        key = hmac.digest(key, part.encode(), "sha256")
    return hmac.compare_digest(key.hex(), signature)


def json_safe(value):
    if isinstance(value, bytes):
        return {"bytes": base64.b64encode(value).decode()}
    if isinstance(value, list):
        return [json_safe(x) for x in value]
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    return value


def shard(shard_id, closed=False, parents=()):
    result = {"ShardId": shard_id,
              "HashKeyRange": {"StartingHashKey": "0", "EndingHashKey": "340282366920938463463374607431768211455"},
              "SequenceNumberRange": {"StartingSequenceNumber": "1"}}
    if closed:
        result["SequenceNumberRange"]["EndingSequenceNumber"] = SECOND
    if parents:
        result["ParentShardId"] = parents[0]
    if len(parents) == 2:
        result["AdjacentParentShardId"] = parents[1]
    return result


def record(sequence, data, key=None, timestamp=WHEN):
    result = {"SequenceNumber": sequence, "Data": data}
    if key is not None:
        result["PartitionKey"] = key
    if timestamp is not None:
        result["ApproximateArrivalTimestamp"] = timestamp
    return result


def get_records(records, token=None, lag=0, children=None):
    value = {"Records": records, "MillisBehindLatest": lag}
    if token is not None:
        value["NextShardIterator"] = token
    if children is not None:
        value["ChildShards"] = children
    return value


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def reply(self, status, raw, media, headers=None):
        self.send_response(status)
        self.send_header("Content-Type", media)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.send_header("x-amzn-RequestId", "fixture-request")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(raw)


class Provider(Handler):
    def result(self, value, compressed=False):
        payload = cbor_encode(value)
        self.reply(200, gzip.compress(payload) if compressed else payload,
                   "application/x-amz-cbor-1.1", {"Content-Encoding": "gzip"} if compressed else None)

    def error(self, code, status=400):
        self.reply(status, cbor_encode({"__type": code, "message": "sensitive-provider-message"}),
                   "application/x-amz-cbor-1.1", {"x-amzn-errortype": code})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        media = self.headers.get("Content-Type", "")
        operation = self.headers.get("X-Amz-Target", "").rsplit(".", 1)[-1]
        try:
            if self.path != "/" or not 0 < length <= MAX or media != "application/x-amz-cbor-1.1":
                raise ValueError("expected signed default CBOR")
            wire = self.rfile.read(length)
            valid = authenticate(self.headers, wire)
            body = cbor_read(wire)
        except (ValueError, UnicodeError, IndexError, OverflowError, struct.error):
            self.error("InvalidArgumentException")
            return
        call = {"op": operation, "body": json_safe(body), "valid": valid, "media": media,
                "time": time.monotonic()}
        with lock:
            action = scenario
            calls.append(call)
            seen[operation] = seen.get(operation, 0) + 1
            nth = seen[operation]
        if not valid:
            self.error("AccessDeniedException")
            return
        try:
            self.action(action, operation, body, nth, call)
        except (BrokenPipeError, ConnectionError, ssl.SSLError):
            call["closed_after"] = time.monotonic() - call["time"]
        except (KeyError, ValueError, IndexError):
            self.error("InvalidArgumentException")

    def action(self, mode, op, body, nth, call):
        if op == "DescribeStreamSummary":
            if body.get("StreamName") != "in":
                raise ValueError("wrong stream identity")
            self.result({"StreamDescriptionSummary": {"StreamARN": ARN, "StreamName": "in",
                         "StreamCreationTimestamp": CREATED, "StreamStatus": "ACTIVE"}})
        elif op == "ListShards":
            self.list_shards(mode, body, nth)
        elif op == "GetShardIterator":
            identity = body["ShardId"]
            if body.get("StreamARN") != ARN or body.get("StreamName") is not None:
                raise ValueError("wrong shard iterator scope")
            if body.get("ShardIteratorType") == "AFTER_SEQUENCE_NUMBER":
                if body.get("StartingSequenceNumber") != FIRST:
                    raise ValueError("wrong restored sequence")
                token = "restored"
            elif mode == "read" and identity == SHARD_A and not any(
                    old["op"] == op and old is not call and old["body"].get("ShardId") == SHARD_A for old in calls):
                token = "expire-initial"
            else:
                token = identity
            self.result({"ShardIterator": token})
        elif op == "GetRecords":
            if body.get("Limit") != 10:
                raise ValueError("wrong page limit")
            self.records(mode, body["ShardIterator"], call)
        elif op == "PutRecords":
            if body.get("StreamName") != "out" or body.get("StreamARN") is not None:
                raise ValueError("wrong sink destination")
            self.put(mode, body["Records"], nth, call)
        elif op == "PutRecord":
            if body.get("StreamName") != "out" or not isinstance(body.get("Data"), bytes):
                raise ValueError("invalid ordered request")
            key = body["PartitionKey"]
            predecessor = "1" if key == "甲" and nth == 3 else None
            if body.get("SequenceNumberForOrdering") != predecessor:
                raise ValueError("incorrect ordered predecessor")
            with lock:
                accepted.append(json_safe(body))
            self.result({"SequenceNumber": str(nth), "ShardId": SHARD_A})
        else:
            raise ValueError("unrecognized operation")

    def list_shards(self, mode, body, nth):
        token = body.get("NextToken")
        if "StreamName" in body or ((token is None) != (body.get("StreamARN") == ARN)):
            raise ValueError("invalid page request scope")
        if mode == "bad-list":
            self.result({"NextToken": "forgot-shards"})
        elif mode == "token-loop":
            self.result({"Shards": [], "NextToken": "loop"})
        elif mode == "read":
            if nth == 1:
                self.result({"Shards": [shard(SHARD_A, True)], "NextToken": "expired"})
            elif token == "expired":
                self.error("ExpiredNextTokenException")
            elif nth == 3:
                self.result({"Shards": [], "NextToken": "last"})
            else:
                self.result({"Shards": [shard(CHILD, parents=[SHARD_A, SHARD_B]), shard(SHARD_A, True), shard(SHARD_B, True)]})
        elif mode in ("dynamic", "split"):
            if mode == "split":
                items = [shard(SHARD_A)] if nth == 1 else [
                    shard(SHARD_A, True), shard(SHARD_B, parents=[SHARD_A]), shard(CHILD, parents=[SHARD_A])]
            else:
                items = [shard(SHARD_A), shard(SHARD_B)] if nth == 1 else [
                    shard(CHILD, parents=[SHARD_A, SHARD_B]), shard(SHARD_A, True), shard(SHARD_B, True)]
            self.result({"Shards": items})
        else:
            self.result({"Shards": [shard(SHARD_A)]})

    def records(self, mode, token, call):
        hints = [{"ShardId": CHILD, "ParentShards": [SHARD_A, SHARD_B]}]
        if mode == "split":
            hints = [{"ShardId": child, "ParentShards": [SHARD_A]} for child in (SHARD_B, CHILD)]
        if mode in ("read", "dynamic", "split"):
            if token in ("expire-initial", "expire-after"):
                self.error("ExpiredIteratorException")
            elif token == SHARD_A:
                self.result(get_records([record(FIRST, b"\x00\xff", "雪")], "expire-after"), compressed=True)
            elif token == SHARD_B:
                if mode == "split":
                    self.result(get_records([record("42", b"", timestamp=None)], "split-b-tip"))
                else:
                    self.result(get_records([record("42", b"", timestamp=None)], children=hints))
            elif token == "restored":
                self.result(get_records([], "terminal-a", lag=8))
            elif token == "terminal-a":
                self.result(get_records([record(SECOND, b"tail", "next")], children=hints))
            elif token == CHILD:
                self.result(get_records([record("9", b"merged", "key")], "child-tip"))
            elif token in ("child-tip", "split-b-tip"):
                self.result(get_records([], token))
            else:
                raise ValueError("unexpected record iterator")
        elif mode in ("held-source", "checkpoint-sink"):
            if token == SHARD_A:
                self.result(get_records([record(FIRST, b"first", "k")], "hold"))
            elif token in ("hold", "restored"):
                if not released.is_set():
                    self.hold(call, get_records([record(SECOND, b"second", "k")], "tip"))
                else:
                    self.result(get_records([record(SECOND, b"second", "k")], "tip"))
            else:
                self.result(get_records([], "tip"))
        elif mode == "idle":
            self.result(get_records([], "idle", lag=20))
        elif mode == "held-deadline":
            self.hold(call, get_records([], "idle"))
        elif mode == "source-denied":
            self.error("AccessDeniedException")
        else:
            raise ValueError("unexpected read scenario")

    def put(self, mode, records, nth, call):
        if mode == "ambiguous":
            self.error("InternalFailure", 503)
            return
        if mode == "bad-count":
            self.result({"Records": [{"SequenceNumber": "1", "ShardId": SHARD_A}], "FailedRecordCount": 0})
            return
        if mode == "failure-count":
            self.result({"Records": [{"SequenceNumber": str(i + 1), "ShardId": SHARD_A}
                                     for i in range(len(records))], "FailedRecordCount": 1})
            return
        if mode == "held-put":
            self.hold(call, {"Records": [], "FailedRecordCount": 0})
            return
        failures = {1: {1: "ProvisionedThroughputExceededException", 2: "InternalFailure"},
                    2: {1: "ProvisionedThroughputExceededException"}}.get(nth, {}) if mode == "partial" else {}
        if mode == "permanent":
            failures = {0: "ValidationException"}
        output = []
        for index, item in enumerate(records):
            if index in failures:
                output.append({"ErrorCode": failures[index], "ErrorMessage": "sensitive-provider-message"})
            else:
                with lock:
                    accepted.append(json_safe(item))
                    sequence = str(len(accepted))
                output.append({"SequenceNumber": sequence, "ShardId": SHARD_A})
        self.result({"Records": output, "FailedRecordCount": len(failures)})

    def hold(self, call, value):
        payload = cbor_encode(value)
        self.send_response(200)
        self.send_header("Content-Type", "application/x-amz-cbor-1.1")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        call["held"] = True
        while not released.is_set():
            ready, _, _ = select.select([self.connection], [], [], 0.1)
            if ready and not self.connection.recv(1):
                call["closed_after"] = time.monotonic() - call["time"]
                return
        call["released"] = True
        self.wfile.write(payload)


class Control(Handler):
    def do_GET(self):
        global scenario
        url = urlsplit(self.path)
        if url.path == "/select":
            with lock:
                scenario = parse_qs(url.query)["value"][0]
                calls.clear()
                accepted.clear()
                seen.clear()
                released.clear()
        elif url.path == "/release":
            released.set()
        elif url.path == "/capture":
            with lock:
                data = json.dumps({"calls": calls, "accepted": accepted}, ensure_ascii=False).encode()
            self.reply(200, data, "application/json")
            return
        elif url.path != "/ready":
            self.reply(404, b"{}", "application/json")
            return
        self.reply(200, b"{}", "application/json")


service = ThreadingHTTPServer(("", 443), Provider)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain("/cert.pem", "/key.pem")
service.socket = tls.wrap_socket(service.socket, server_side=True)
threading.Thread(target=service.serve_forever, daemon=True).start()
ThreadingHTTPServer(("", 8080), Control).serve_forever()
