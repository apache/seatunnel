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

import base64
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


ACCOUNT = 'fixture-user@example.invalid'
KEY = 'fixture-agilecrm-key-527'
PRIVATE = 'agilecrm-fixture-private-server-data-493'
CONTACT_A = 9007199254740993
CONTACT_B = 9007199254740995
FILTER_A = 9007199254742001
FILTER_B = 9007199254742003
NEXT = 'another page+中/?='
LAST = 'final&cursor/=?'
ORIGIN = '/dev/api'
control = threading.Lock()
allow_response = threading.Event()
calls = []
settings = {}


def milliseconds():
    return time.monotonic_ns() // 1000000


def json_bytes(value):
    data = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    return data.replace('"EXACT_NUMBER"', '12345678901234567890.123456789012345678901').encode()


def owned_record(identity, tag):
    return {'id': identity, 'tag': tag, 'title': 'Café 中文',
            'price': 'EXACT_NUMBER', 'optional': None,
            'attributes': {'flag': False, 'order': [1, 'x']}}


def list_resource(route, parameters):
    cursor = parameters.get('cursor', [None])[0]
    roots = {
        '/contacts': ('contacts', CONTACT_A, CONTACT_B),
        '/contacts/companies/list': ('companies', 71001, 71002),
        '/opportunity': ('deals', 72001, 72002),
        '/workflows': ('campaigns', 73001, 73002),
    }
    if route in roots:
        name, first, second = roots[route]
        if cursor is None:
            page = [dict(owned_record(first, name + '-first'), cursor=NEXT, count=2)]
            return page[0] if name == 'campaigns' else page
        if cursor == NEXT:
            return [dict(owned_record(second, name + '-second'), cursor=LAST if name == 'contacts' else '')]
        if cursor == LAST and name == 'contacts':
            return []
        return None
    if route == '/tasks':
        return [owned_record(74001, 'tasks-one')]
    if route == '/events':
        if not parameters.get('start') or not parameters.get('end'):
            return None
        return [dict(owned_record(75001, 'events-one'), start=int(parameters['start'][0]))]
    if route == '/milestone/pipelines':
        return [dict(owned_record(76001, 'milestone-one'), milestones=['A', 'B'])]
    if route == '/tickets/filters':
        return [owned_record(FILTER_A, 'filter-a'), owned_record(str(FILTER_B), 'filter-b')]
    if route == '/tickets/filter':
        parent = parameters.get('filter_id', [None])[0]
        if parent not in (str(FILTER_A), str(FILTER_B)):
            return None
        return [dict(owned_record(77, 'ticket-' + parent), filter_marker=parent)]
    for parent in (CONTACT_A, CONTACT_B):
        if route == f'/contacts/{parent}/notes':
            return [owned_record(801, 'note-' + str(parent))]
        if route == f'/documents/contact/{parent}/docs':
            return [dict(owned_record(802, 'document-' + str(parent)), url='https://files.invalid/sample')]
    return None


class AgileFixture(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def answer(self, code, body=b'', extra=None):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.close_connection = True
        if body:
            self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get('Content-Length', '0'))
        body = self.rfile.read(length)
        if self.path == '/control/reset':
            options = json.loads(body)
            with control:
                calls.clear()
                settings.clear()
                settings.update(options)
                allow_response.clear()
            self.answer(204)
        elif self.path == '/control/release':
            allow_response.set()
            self.answer(204)
        else:
            self.serve_resource(parse_qs(body.decode(), keep_blank_values=True))

    def do_GET(self):
        if self.path == '/health':
            self.answer(200, b'{}')
        elif self.path == '/control/state':
            with control:
                state = json_bytes(calls)
            self.answer(200, state)
        else:
            self.serve_resource({})

    def serve_resource(self, form):
        url = urlsplit(self.path)
        started = milliseconds()
        query = parse_qs(url.query, keep_blank_values=True)
        signature = base64.b64encode(f'{ACCOUNT}:{KEY}'.encode()).decode()
        event = {'path': url.path, 'method': self.command, 'query': query,
                 'form': form, 'time_ms': started,
                 'authenticated': self.headers.get('Authorization') == 'Basic ' + signature,
                 'json': self.headers.get('Accept') == 'application/json',
                 'form_header': self.headers.get('Content-Type') == 'application/x-www-form-urlencoded'}
        arguments = form if self.command == 'POST' else query
        current = arguments.get('cursor', [None])[0]
        with control:
            calls.append(event)
            sequence = settings.get('sequence', [])
            index = len(calls) - 1
            mode = sequence[index] if index < len(sequence) else settings.get('mode')
            if settings.get('hold_path') == url.path and settings.get('hold_cursor') == current:
                mode = 'hold'
        if not event['authenticated']:
            self.answer(401, json_bytes({'message': PRIVATE}))
            return
        if url.path == ORIGIN + '/contacts/companies/list':
            if self.command != 'POST' or query or not event['form_header']:
                self.answer(405, json_bytes({'message': PRIVATE}))
                return
        elif self.command != 'GET':
            self.answer(405, json_bytes({'message': PRIVATE}))
            return
        if not event['json'] or not url.path.startswith(ORIGIN + '/'):
            self.answer(400, json_bytes({'message': PRIVATE}))
            return
        if mode in ('401', '403', '404', '429', '500', '503'):
            extra = {'Retry-After': '1'} if mode == '429' else {}
            self.answer(int(mode), json_bytes({'message': PRIVATE}), extra)
            return
        if mode == 'retry_date':
            due = int(time.time()) + 3
            with control:
                event['due_ms'] = milliseconds() + due * 1000 - int(time.time() * 1000)
            self.answer(503, json_bytes({'message': PRIVATE}),
                        {'Retry-After': email.utils.formatdate(due, usegmt=True)})
            return
        if mode == 'wait_forbidden':
            self.answer(429, json_bytes({'message': PRIVATE}), {'Retry-After': '900'})
            return
        if mode == 'redirect':
            self.answer(307, json_bytes({'message': PRIVATE}),
                        {'Location': '/trap?value=' + PRIVATE})
            return
        if mode == 'invalid':
            self.answer(200, b'{"data":[' + PRIVATE.encode())
            return
        if mode == 'empty':
            self.answer(200, b'[]')
            return
        if mode == 'no_content':
            self.answer(204)
            return
        if mode == 'huge':
            self.answer(200, gzip.compress(b' ' * (8 * 1024 * 1024 + 1)),
                        {'Content-Encoding': 'gzip'})
            return

        route = url.path[len(ORIGIN):]
        rows = list_resource(route, arguments)
        if rows is None:
            self.answer(400, json_bytes({'message': PRIVATE}))
            return
        if mode == 'cycle':
            rows = [dict(owned_record(CONTACT_A if current is None else CONTACT_B, 'cycle'),
                         cursor=NEXT)]
        elif mode == 'duplicate_parent' and route == '/contacts' and current == NEXT:
            rows[0].pop('cursor')
            rows.append(dict(owned_record(CONTACT_A, 'repeated-parent'), cursor=LAST))
        elif mode == 'bad_cursor':
            rows = [dict(owned_record(CONTACT_A, 'bad'), cursor=42)]
        elif mode == 'bad_parent':
            rows = [dict(owned_record('900719925474099390007', 'bad-parent'))]
        elif mode == 'shape':
            rows = {'message': PRIVATE}
        elif mode == 'ticket_full' and route == '/tickets/filter':
            rows.append(owned_record(78, 'unknown-tail'))
        elif mode == 'ticket_cursor' and route == '/tickets/filter':
            rows[0]['cursor'] = NEXT

        wire = json_bytes(rows)
        if mode == 'hold' and not allow_response.is_set():
            with control:
                event['held'] = True
            while not allow_response.wait(.03):
                ready, _, _ = select.select([self.connection], [], [], 0)
                if ready:
                    try:
                        stopped = not self.connection.recv(1, socket.MSG_PEEK)
                    except ConnectionResetError:
                        stopped = True
                    if stopped:
                        self.closed(event, started)
                        return
        if mode == 'drip':
            self.send_response(200)
            self.send_header('Content-Length', str(len(wire)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            for byte in wire:
                try:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    self.closed(event, started)
                    return
                time.sleep(.06)
        elif mode == 'gzip':
            self.answer(200, gzip.compress(wire), {'Content-Encoding': 'gzip'})
        elif mode == 'deflate':
            self.answer(200, zlib.compress(wire), {'Content-Encoding': 'deflate'})
        else:
            self.answer(200, wire)

    @staticmethod
    def closed(event, started):
        with control:
            event['disconnect_ms'] = milliseconds() - started


if __name__ == '__main__':
    api = ThreadingHTTPServer(('', 8080), AgileFixture)
    secure_api = ThreadingHTTPServer(('', 8443), AgileFixture)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain('/fixture.crt', '/fixture.key')
    secure_api.socket = tls.wrap_socket(secure_api.socket, server_side=True)
    threading.Thread(target=secure_api.serve_forever, daemon=True).start()
    api.serve_forever()
