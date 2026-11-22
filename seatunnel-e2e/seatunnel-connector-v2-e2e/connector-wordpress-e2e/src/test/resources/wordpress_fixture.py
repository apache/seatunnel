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

import json
import os
import ssl
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit


POSTS = [
    {"ID": 7, "slug": "earliest", "title": "First &amp; exact", "URL": "https://fixture.wordpress.com/earliest/",
     "date": "2005-08-31T17:00:00-07:00", "modified": "2005-09-02T08:34:56+02:00"},
    {"ID": 8, "slug": "latest", "title": "Second", "URL": "https://fixture.wordpress.com/latest/",
     "date": "2005-10-01T01:59:59+02:00", "modified": "2005-10-01T02:00:01+02:00"},
]
PARAMETERS = {
    "number": ["1"],
    "after": ["2005-09-01T00:00:00Z"],
    "before": ["2005-09-30T23:59:59Z"],
    "order": ["ASC"],
    "order_by": ["ID"],
    "status": ["publish"],
    "type": ["post"],
    "fields": ["ID,slug,title,URL,date,modified"],
}


class Fixture(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/health":
            self.send_response(200)
            self.end_headers()
            return

        params = parse_qs(url.query)
        offset = len(self.server.offsets)
        if (url.path != "/rest/v1/sites/fixture.wordpress.com/posts/"
                or offset >= len(POSTS)
                or params != dict(PARAMETERS, offset=[str(offset)])
                or self.headers.get("Host") != "public-api.wordpress.com"
                or self.headers.get("Accept") != "application/json"
                or self.headers.get("Authorization") is not None):
            self.send_error(400, "unexpected WordPress.com request")
            return

        self.server.offsets.append(offset)
        print("verified WordPress fixture offset=" + str(offset), flush=True)
        if os.environ.get("WORDPRESS_HOLD_PAGE") == str(offset):
            # Send neither headers nor body. Only a real client disconnect completes this fixture.
            self.connection.settimeout(60)
            print("WordPress fixture holds offset=" + str(offset), flush=True)
            try:
                peer_closed = not self.connection.recv(1)
            except (ConnectionResetError, ssl.SSLEOFError):
                peer_closed = True
            if peer_closed:
                print("WordPress fixture saw offset=" + str(offset) + " disconnect", flush=True)
            return

        body = json.dumps({"found": len(POSTS), "posts": [POSTS[offset]]}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 443), Fixture)
    server.offsets = []
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain("/tmp/wordpress-fixture.pem")
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    print("WordPress HTTPS fixture ready", flush=True)
    server.serve_forever()
