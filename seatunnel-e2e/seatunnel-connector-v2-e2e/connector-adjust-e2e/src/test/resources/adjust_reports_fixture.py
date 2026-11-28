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
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


ACCESS = 'adjust-fixture-report-access-597'
PRIVATE = 'adjust-fixture-private-error-258'
REPORT = '/reports-service/report'
guard = threading.Lock()
requests = []
settings = {}
allow = threading.Event()


def clock_ms():
    return time.monotonic_ns() // 1_000_000


def encode(payload):
    wire = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
    return wire.replace('"FIXTURE_DECIMAL"', '21.000000000000000137').encode('utf-8')


def sample(day, dimensions, metrics):
    standard = {'app': 'Café 東京', 'country': 'DE', 'campaign_id_network': 9007199254740993}
    data = {'installs': '9223372036854775808', 'network_cost': 'FIXTURE_DECIMAL',
            'purchase_renewals': '-3.141592653589793238', 'extra_count': '320'}
    row = {name: standard.get(name, 'fixture-value') for name in dimensions}
    row['day'] = day
    row.update({name: data.get(name, '10') for name in metrics})
    row['attr_dependency'] = {'internal_marker': 'not-a-selected-field'}
    extra = {name: ('JP' if name == 'country' else None) for name in dimensions}
    extra.update(day=day, installs=None, purchase_renewals='0')
    records = [row, extra] if 'country' in dimensions else [row]
    return {'rows': records, 'totals': {'installs': 1}, 'warnings': [], 'pagination': None}


class Reports(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def reply(self, code, content=b'', extra=None):
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(content)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        if content:
            self.wfile.write(content)

    def do_POST(self):
        if self.path == '/control/reset':
            value = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            with guard:
                requests.clear()
                settings.clear()
                settings.update(value)
                allow.clear()
            self.reply(204)
        elif self.path == '/control/release':
            allow.set()
            self.reply(204)
        else:
            self.reply(404)

    def do_GET(self):
        parts = urlsplit(self.path)
        if parts.path == '/health':
            self.reply(200, b'{}')
            return
        if parts.path == '/control/state':
            with guard:
                output = encode(requests)
            self.reply(200, output)
            return
        query = parse_qs(parts.query, keep_blank_values=True)
        period = query.get('date_period', [''])[0]
        started = clock_ms()
        entry = {'route': parts.path, 'query': query, 'started': started,
                 'bearer': self.headers.get('Authorization', '') == 'Bearer ' + ACCESS}
        with guard:
            requests.append(entry)
            sequence = settings.get('sequence', [])
            offset = len(requests) - 1
            mode = sequence[offset] if offset < len(sequence) else settings.get('mode')
            mode = settings.get('days', {}).get(period.split(':')[0], mode)
        if not entry['bearer']:
            self.reply(401, encode({'message': PRIVATE}))
            return
        if parts.path != REPORT:
            self.reply(404, encode({'message': PRIVATE}))
            return
        days = period.split(':')
        dimensions = query.get('dimensions', [''])[0].split(',')
        metrics = query.get('metrics', [''])[0].split(',')
        if len(days) != 2 or days[0] != days[1] or 'day' not in dimensions:
            self.reply(400, encode({'message': PRIVATE}))
            return
        day = date.fromisoformat(days[0])
        if mode in ('401', '403', '429', '500', '503'):
            extras = {'Retry-After': '1'} if mode == '429' else {}
            self.reply(int(mode), encode({'message': PRIVATE}), extras)
            return
        if mode == 'excessive_retry':
            self.reply(429, encode({'message': PRIVATE}), {'Retry-After': '180'})
            return
        if mode == 'date_retry':
            due = int(time.time()) + 3
            with guard:
                entry['due'] = clock_ms() + (due * 1000 - int(time.time() * 1000))
            self.reply(503, encode({'message': PRIVATE}),
                       {'Retry-After': email.utils.formatdate(due, usegmt=True)})
            return
        if mode == 'redirect':
            self.reply(307, encode({'message': PRIVATE}), {'Location': '/private?value=' + PRIVATE})
            return
        if mode == '204':
            self.reply(204)
            return

        payload = sample(day.isoformat(), dimensions, metrics)
        if mode == 'empty':
            payload['rows'] = []
            payload['pagination'] = {}
        elif mode == 'pagination':
            payload['pagination'] = {'next': '/private?value=' + PRIVATE}
        elif mode == 'warning':
            payload['warnings'] = [{'message': PRIVATE, 'code': 'results_truncated'}]
        elif mode == 'malformed':
            self.reply(200, b'{"rows":[' + PRIVATE.encode())
            return
        elif mode == 'shape':
            payload['rows'] = {'not': 'an array'}
        elif mode == 'stale':
            payload['rows'].append(dict(payload['rows'][0], day=(day - timedelta(days=1)).isoformat()))
        elif mode == 'repeated':
            payload['rows'].append(dict(payload['rows'][0], installs='5'))
        elif mode == 'precision':
            payload['rows'][0]['installs'] = '1.2'
        elif mode == 'exponent':
            payload['rows'][0]['installs'] = '1E+1000000000'
        elif mode == 'huge':
            self.reply(200, gzip.compress(b' ' * (8 * 1024 * 1024 + 1)),
                       {'Content-Encoding': 'gzip'})
            return

        content = encode(payload)
        if mode == 'hold' and not allow.is_set():
            with guard:
                entry['held'] = True
            while not allow.wait(.03):
                readable, _, _ = select.select([self.connection], [], [], 0)
                if readable:
                    try:
                        ended = self.connection.recv(1, socket.MSG_PEEK) == b''
                    except ConnectionResetError:
                        ended = True
                    if ended:
                        with guard:
                            entry['closed_after'] = clock_ms() - started
                        return
        if mode == 'drip':
            self.send_response(200)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            for byte in content:
                try:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    with guard:
                        entry['closed_after'] = clock_ms() - started
                    return
                time.sleep(.06)
            return
        if mode == 'gzip':
            self.reply(200, gzip.compress(content), {'Content-Encoding': 'gzip'})
        elif mode == 'deflate':
            self.reply(200, zlib.compress(content), {'Content-Encoding': 'deflate'})
        else:
            self.reply(200, content)


if __name__ == '__main__':
    primary = ThreadingHTTPServer(('', 8080), Reports)
    secure = ThreadingHTTPServer(('', 8443), Reports)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain('/fixture.pem', '/fixture.key')
    secure.socket = context.wrap_socket(secure.socket, server_side=True)
    threading.Thread(target=secure.serve_forever, daemon=True).start()
    primary.serve_forever()
