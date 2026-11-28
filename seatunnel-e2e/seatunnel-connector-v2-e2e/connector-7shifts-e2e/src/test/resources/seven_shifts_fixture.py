# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import email.utils
import gzip
import json
import select
import socket
import ssl
import threading
import time
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


C1 = 9007199254740993
C2 = 9007199254740995
U1 = 9007199254742017
U2 = 9007199254742019
CURSOR = 'page +中/=?&'
LAST = 'last+batch/?='
VERSION = '2026-01-01'
TOKEN = 'fixture-seven-shifts-read-9674'
PRIVATE_ERROR = 'private-seven-shifts-body-5162'
BOUNDARY = '2026-04-05T23:00:00.123456789Z'
DECIMAL = '12345678901234567890.12345678901234567890'

lock = threading.Lock()
cases = {}
calls = []
release = threading.Event()


def millis():
    return int(time.monotonic() * 1000)


def page(data, next_cursor=None):
    return {'data': data, 'meta': {'cursor': {'next': next_cursor}}}


def company(company_id, modified=BOUNDARY):
    return {'id': company_id, 'marker': f'company-{company_id}',
            'modified': modified, 'amount': 'DECIMAL_SENTINEL', 'nullable': None,
            'name': 'Équipe 中文', 'nested': [{'flag': True, 'number': 9007199254740997}]}


def user(company_id, user_id, active):
    return {'id': user_id, 'company_id': company_id, 'active': active,
            'modified': '2025-01-01T10:00:00Z',
            'marker': f'employee-{company_id}-{user_id}'}


def body(data):
    return (json.dumps(data, ensure_ascii=False, separators=(',', ':'))
            .replace('"DECIMAL_SENTINEL"', DECIMAL).encode('utf-8'))


def resource(path, query):
    segments = path.split('/')
    cursor = query.get('cursor', [None])[0]
    filtered = 'modified_since' in query
    if segments == ['', 'v2', 'companies']:
        if cursor is None:
            rows = [company(C1, BOUNDARY if filtered else "2025-01-01T10:00:00Z")]
            if filtered:
                rows.insert(0, company(101, '2026-04-05T23:00:00.123456788Z'))
            return page(rows, CURSOR)
        if cursor == CURSOR:
            return page([], LAST)
        if cursor == LAST:
            rows = [company(C2)]
            if not filtered:
                rows.append(company(C1))
            return page(rows)
    if len(segments) == 5 and segments[:3] == ['', 'v2', 'company']:
        company_id = int(segments[3])
        name = segments[4]
        if company_id not in (C1, C2):
            return None
        if name == 'users':
            if filtered:
                return page([dict(user(company_id, U1, True), modified=BOUNDARY)])
            if company_id == C1 and cursor is None:
                return page([user(company_id, U1, True)], CURSOR)
            if company_id == C1 and cursor == CURSOR:
                return page([user(company_id, U1, True), user(company_id, U2, False)])
            return page([user(company_id, U1, True)])
        record = {'id': company_id + 200, 'company_id': company_id,
                  'marker': f'{name}-{company_id}', 'modified': BOUNDARY}
        if name in ('shifts', 'time_punches'):
            record.update(user_id=0 if name == 'shifts' else U1)
        if name == 'roles':
            record.update(department_id=0)
        if name == 'locations' and company_id == C1:
            if cursor is None:
                record['modified'] = '2026-04-06T01:00:00.123456789+02:00'
                return page([record], CURSOR)
            if cursor == CURSOR:
                return page([], LAST)
            if cursor == LAST:
                return page([{'id': company_id + 202, 'company_id': company_id,
                              'marker': 'location-after-cursor', 'modified': None}])
        return page([record])
    if len(segments) == 7 and segments[:3] == ['', 'v2', 'company'] and segments[4] == 'users':
        company_id, user_id = int(segments[3]), int(segments[5])
        name = segments[6]
        marker = f'{name}-{company_id}-{user_id}'
        if name == 'wages':
            return {'data': {'current_wages': [{'id': user_id + 8, 'wage_cents': 2001}],
                             'upcoming_wages': [{'id': user_id + 10, 'wage_cents': 2500}],
                             'marker': marker}}
        if name == 'assignments':
            return {'data': {'locations': [{'id': 15}], 'departments': [], 'roles': [{'id': 45}],
                             'marker': marker}}
        if name == 'location_assignments':
            return {'data': [{'location_id': 15, 'name': 'La Cocina', 'marker': marker}]}
        if name == 'department_assignments':
            return {'data': [{'department_id': 30, 'location_id': 15, 'marker': marker}]}
        if name == 'role_assignments':
            return {'data': [{'role_id': 45, 'company_id': company_id, 'department_id': 0,
                             'is_primary': True, 'marker': marker}]}
    return None


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def send(self, status, payload=b'', **headers):
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name.replace('_', '-'), value)
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        self.wfile.write(payload)

    def do_POST(self):
        if self.path == '/reset':
            settings = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with lock:
                cases.clear()
                cases.update(settings)
                calls.clear()
                release.clear()
            self.send(204)
        elif self.path == '/release':
            release.set()
            self.send(204)
        else:
            self.send(404)

    def do_GET(self):
        if self.path == '/health':
            self.send(200, b'{}')
            return
        if self.path == '/state':
            with lock:
                payload = body({'calls': calls})
            self.send(200, payload)
            return
        url = urlsplit(self.path)
        query = parse_qs(url.query, keep_blank_values=True)
        at = millis()
        event = {'path': url.path, 'query': query, 'received_ms': at,
                 'auth': self.headers.get('Authorization') == 'Bearer ' + TOKEN,
                 'version': self.headers.get('x-api-version') == VERSION,
                 'customer': self.headers.get('x-company-guid') is None}
        with lock:
            calls.append(event)
            index = len(calls) - 1
            settings = dict(cases)

        if not event['auth']:
            self.send(401, body({'message': PRIVATE_ERROR}))
            return
        if not event['version'] or not event['customer'] or url.path == '/v2/whoami':
            self.send(400, body({'message': PRIVATE_ERROR}))
            return
        step = settings.get('steps', [])
        fault = step[index] if index < len(step) else settings.get('mode')
        cursor = query.get('cursor', [None])[0]

        if fault in ('401', '403', '404', '500', '503', '429'):
            retry = {'Retry_After': '1'} if fault == '429' else {}
            self.send(int(fault), body({'message': PRIVATE_ERROR}), **retry)
            return
        if fault == 'long_retry':
            self.send(429, body({'message': PRIVATE_ERROR}), Retry_After='3600')
            return
        if fault == 'dated_retry':
            after = int(time.time()) + 3
            remaining = max(0, after * 1000 - int(time.time() * 1000))
            with lock:
                event['retry_after_monotonic_ms'] = millis() + remaining
            self.send(503, body({'message': PRIVATE_ERROR}),
                      Retry_After=email.utils.formatdate(after, usegmt=True))
            return
        if fault == 'redirect':
            self.send(307, body({'message': PRIVATE_ERROR}),
                      Location='/trap?opaque=' + PRIVATE_ERROR)
            return
        if fault == 'empty':
            self.send(200, body(page([])))
            return
        if fault == 'invalid_json':
            self.send(200, b'{"data":[' + PRIVATE_ERROR.encode())
            return
        if fault == 'invalid_shape':
            self.send(200, body({'data': {'summary': PRIVATE_ERROR}}))
            return
        if fault == 'invalid_cursor':
            self.send(200, body(page([company(C1)], True)))
            return
        if fault == 'repeat':
            self.send(200, body(page([company(C1 if cursor is None else C2)], CURSOR)))
            return
        if fault == 'wrong_parent' and url.path.startswith('/v2/company/'):
            self.send(200, body(page([{'company_id': 1234, 'id': 1}])))
            return
        if fault == 'zip_bomb':
            compressed = gzip.compress(b' ' * (8 * 1024 * 1024 + 1))
            self.send(200, compressed, Content_Encoding='gzip')
            return

        data = resource(url.path, query)
        if data is None:
            self.send(404, body({'message': PRIVATE_ERROR}))
            return
        payload = body(data)
        if fault == 'hold' or (settings.get('hold_cursor') == cursor and cursor is not None
                               and settings.get('hold_path') == url.path):
            if not release.is_set():
                with lock:
                    event['held'] = True
            while not release.wait(0.03):
                ready, _, _ = select.select([self.connection], [], [], 0)
                if ready:
                    try:
                        disconnected = self.connection.recv(1, socket.MSG_PEEK) == b''
                    except ConnectionResetError:
                        disconnected = True
                    if disconnected:
                        with lock:
                            event['disconnect_ms'] = millis() - at
                        return
        if fault == 'trickle':
            self.send_response(200)
            self.send_header('Content-Length', str(len(payload)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            for octet in payload:
                try:
                    self.wfile.write(bytes([octet]))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    with lock:
                        event['disconnect_ms'] = millis() - at
                    return
                time.sleep(0.06)
            return
        if fault == 'gzip':
            self.send(200, gzip.compress(payload), Content_Encoding='gzip')
        elif fault == 'deflate':
            self.send(200, zlib.compress(payload), Content_Encoding='deflate')
        else:
            self.send(200, payload)


if __name__ == '__main__':
    http = ThreadingHTTPServer(('', 8080), Handler)
    https = ThreadingHTTPServer(('', 8443), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain('/fixture-cert.pem', '/fixture-key.pem')
    https.socket = tls.wrap_socket(https.socket, server_side=True)
    threading.Thread(target=https.serve_forever, daemon=True).start()
    http.serve_forever()
