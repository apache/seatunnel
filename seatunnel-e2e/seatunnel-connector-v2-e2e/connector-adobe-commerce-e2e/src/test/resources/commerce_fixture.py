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
import select
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

PREFIX = "/shop/rest/en_us/V1/"
TOKEN = os.environ["FIXTURE_TOKEN"]
PRIVATE = "provider-private-commerce-note"
LOCK = threading.Lock()
RELEASED = threading.Event()
CALLS = []
MODE = "catalog"
# Contract data is independently populated for each route.
ROUTES = {
    "carts/search": ("carts", "items", "id", "entity_id", "updated_at"),
    "orders": ("orders", "items", "entity_id", "entity_id", "updated_at"),
    "coupons/search": ("coupons", "items", "coupon_id", "coupon_id", "created_at"),
    "invoices": ("invoices", "items", "entity_id", "entity_id", "updated_at"),
    "products": ("products", "items", "id", "entity_id", "updated_at"),
    "customers/search": ("customers", "items", "id", "entity_id", "updated_at"),
    "shipments": ("shipments", "items", "entity_id", "entity_id", "updated_at"),
    "taxRates/search": ("tax_rates", "items", "id", "tax_calculation_rate_id", None),
    "creditmemos": ("creditmemos", "items", "entity_id", "entity_id", "updated_at"),
    "salesRules/search": ("sales_rules", "items", "rule_id", "rule_id", "from_date"),
    "store/storeViews": ("store_views", "array", "id", None, None),
    "taxClasses/search": ("tax_classes", "items", "class_id", "class_id", None),
    "store/storeGroups": ("store_groups", "array", "id", None, None),
    "transactions": ("transactions", "items", "transaction_id", "transaction_id", "created_at"),
    "store/websites": ("store_websites", "array", "id", None, None),
    "customerGroups/search": ("customer_groups", "items", "id", "customer_group_id", None),
    "inventory/stocks": ("inventory_stocks", "items", "stock_id", "stock_id", None),
    "inventory/sources": ("inventory_sources", "items", "source_code", "source_code", None),
    "directory/currency": ("directory_currency", "object", "base_currency_code", None, None),
    "directory/countries": ("directory_countries", "array", "id", None, None),
    "inventory/source-items": ("inventory_source_items", "items", "source_code", "source_code", None),
}

DATE_VALUES = ("2023-07-01", "2023-07-02", "2023-07-04", "2023-07-05")
TIME_VALUES = ("2023-07-01 11:59:59", "2023-07-01 12:00:00",
               "2023-07-04 11:59:59", "2023-07-04 12:00:00")


def data(resource, id_name, timestamp, index, window=False):
    identifier = index + 1
    if resource == "products" and index == 3:
        identifier = 1234567890123456789012345678901234567890
    if id_name == "base_currency_code":
        identifier = "USD"
    elif id_name == "source_code":
        identifier = "north" if resource == "inventory_source_items" else f"store-{index:03}"
    elif resource == "directory_countries":
        identifier = ("US", "JP")[index]
    row = {id_name: identifier, "description": "Café 雪"}
    if resource == "inventory_source_items":
        row["sku"] = f"SKU+雪-{index:03}"
    if resource == "directory_currency":
        row["available_currency_codes"] = ["USD", "JPY"]
        row["exchange_rates"] = [{"currency_to": "JPY", "rate": "FIXTURE_DECIMAL"}]
    if timestamp:
        if timestamp == "from_date":
            row[timestamp] = DATE_VALUES[index] if window else None
        else:
            row[timestamp] = TIME_VALUES[index] if window else "2023-07-02 01:00:00"
    row["extension_attributes"] = {"empty": None, "values": [True, "雪", "FIXTURE_DECIMAL"]}
    return row


def encode(payload):
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace(
        '"FIXTURE_DECIMAL"', "9876543210987654321.123456789012345678900"
    ).encode("utf-8")


class Respond(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        try:
            self.route()
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            pass

    def send(self, status, body, headers=None):
        content = body if isinstance(body, bytes) else encode(body)
        self.send_response(status)
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Content-Type", "application/json")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(content)


class Control(Respond):
    def route(self):
        global MODE
        path = urlsplit(self.path)
        if path.path == "/ready":
            self.send(200, {"ready": True})
        elif path.path == "/select":
            RELEASED.set()
            with LOCK:
                CALLS.clear()
                MODE = parse_qs(path.query)["mode"][0]
                RELEASED.clear()
            self.send(200, {"selected": MODE})
        elif path.path == "/release":
            RELEASED.set()
            self.send(200, {"released": True})
        elif path.path in ("/changed", "/unchanged"):
            with LOCK:
                MODE = "changed" if path.path == "/changed" else "midpage"
            self.send(200, {"changed": True})
        elif path.path == "/calls":
            with LOCK:
                snapshot = [event.copy() for event in CALLS]
            self.send(200, snapshot)
        else:
            self.send(404, {})


class Commerce(Respond):
    protocol_version = "HTTP/1.1"

    def route(self):
        self.close_connection = True
        url = urlsplit(self.path)
        start = time.monotonic()
        query = parse_qs(url.query, strict_parsing=True)
        authorized = self.headers.get_all("Authorization") == ["Bearer " + TOKEN]
        details = ROUTES.get(url.path.removeprefix(PREFIX)) if url.path.startswith(PREFIX) else None
        event = {"path": url.path, "query": query, "start_ms": int(start * 1000),
                 "host": self.headers.get("Host"), "authorized": authorized,
                 "gzip": self.headers.get("Accept-Encoding") == "gzip"}
        with LOCK:
            CALLS.append(event)
            count = len(CALLS)
            mode = MODE
        if url.path == "/trap":
            self.send(200, {"followed": True})
            return
        if details is None:
            self.send(404, {"message": PRIVATE})
            return
        if not authorized or mode == "forbidden":
            self.send(403, {"message": PRIVATE})
            return
        resource, shape, identity, sorting, timestamp = details
        window = mode == "window" and timestamp is not None
        bounded = mode in ("window", "boundary", "bad-window") and timestamp is not None
        size, page, bounds = self.criteria(query, shape, sorting, timestamp, bounded, resource)
        if page is None:
            self.send(400, {"message": PRIVATE})
            return
        if mode == "unauthorized":
            self.send(401, {"message": PRIVATE})
        elif mode == "redirect":
            self.send(302, {"message": PRIVATE}, {"Location": "https://redirect.commerce.test:8443/trap"})
        elif mode == "bad-json":
            self.send(200, b'{"' + PRIVATE.encode() + b'":')
        elif mode == "huge":
            self.send(200, gzip.compress(encode({"items": [{"entity_id": 1, "x": "x" * 6000}],
                                                  "total_count": 1})), {"Content-Encoding": "gzip"})
        elif mode == "provider-error":
            self.send(200, {"errors": {"message": PRIVATE}})
        elif mode in ("stall", "cancel") or mode == "boundary" and page == 2 and count == 2:
            if self.hold(start, event) and mode == "boundary":
                self.write_page(details, size, page, 3)
        elif mode == "trickle":
            self.trickle(start, event)
        elif mode in ("busy", "long-retry") or mode == "retries" and count <= 2:
            status = 429 if mode == "long-retry" or mode == "retries" and count == 1 else 500
            wait = "3600" if mode == "long-retry" else "1" if status == 429 else "0"
            self.send(status, {"message": PRIVATE}, {"Retry-After": wait})
        elif mode == "bad-total":
            self.send(200, {"items": [], "total_count": "bad"})
        elif mode == "bad-echo":
            self.write_page(details, size, page, 2, echo=page + 1)
        elif mode == "short":
            self.write_page(details, size, page, 9, indices=[0])
        elif mode == "empty-more":
            self.write_page(details, size, page, 9, indices=[])
        elif mode == "repeat":
            indices = range(size) if page == 1 else reversed(range(size))
            self.write_page(details, size, page, 5, indices=indices)
        elif mode == "bad-window":
            self.write_page(details, size, page, 1, indices=[3], window=True)
        elif mode == "window":
            if shape != "items":
                self.write_list(details)
            elif bounded:
                values = DATE_VALUES if timestamp == "from_date" else TIME_VALUES
                eligible = [i for i, value in enumerate(values) if bounds[0] <= value < bounds[1]]
                self.write_page(details, size, page, len(eligible), indices=eligible, window=True)
            else:
                self.write_page(details, size, page, 1)
        elif mode == "catalog":
            if shape == "items":
                self.write_page(details, size, page, 3 if resource == "orders" else 4)
            else:
                self.write_list(details)
        elif mode == "shrink":
            self.write_page(details, size, page, 4 if page == 1 else 2)
        elif mode in ("midpage", "changed"):
            self.write_page(details, size, page, 100, modified=mode == "changed")
        else:
            self.write_page(details, size, page, 3 if mode == "boundary" else 0 if mode == "empty" else 1)

    def criteria(self, query, shape, sorting, timestamp, window, resource):
        if shape != "items":
            return (0, 1, None) if not query else (None, None, None)
        prefix = "searchCriteria"
        keys = {"limit": prefix + "[pageSize]", "page": prefix + "[currentPage]"}
        expected = {
            prefix + "[sortOrders][0][field]": sorting,
            prefix + "[sortOrders][0][direction]": "ASC"
        }
        if resource == "inventory_source_items":
            expected[prefix + "[sortOrders][1][field]"] = "sku"
            expected[prefix + "[sortOrders][1][direction]"] = "ASC"
        bounds = None
        if window:
            values = []
            for position, condition in enumerate(("gteq", "lt")):
                group = f"{prefix}[filter_groups][{position}][filters][0]"
                expected[group + "[field]"] = timestamp
                expected[group + "[condition_type]"] = condition
                val = group + "[value]"
                keys[f"window{position}"] = val
                values.extend(query.get(val, []))
            bounds = values
        if set(query) != set(expected) | set(keys.values()) or any(len(v) != 1 for v in query.values()):
            return None, None, None
        if any(query[k] != [v] for k, v in expected.items()):
            return None, None, None
        if not query[keys["limit"]][0].isdigit() or not query[keys["page"]][0].isdigit():
            return None, None, None
        limit, page = int(query[keys["limit"]][0]), int(query[keys["page"]][0])
        if not 1 <= limit <= 1000 or page < 1:
            return None, None, None
        return limit, page, bounds

    def write_list(self, details):
        resource, shape, identity, _, timestamp = details
        rows = [data(resource, identity, timestamp, i) for i in range(1 if shape == "object" else 2)]
        self.send(200, rows[0] if shape == "object" else rows)

    def write_page(self, details, size, page, total, indices=None, echo=None, window=False, modified=False):
        resource, _, identity, _, timestamp = details
        if indices is None:
            indices = range((page - 1) * size, min(page * size, total))
        rows = [data(resource, identity, timestamp, i, window) for i in indices]
        if modified:
            rows[-1]["description"] = "Changed after checkpoint"
        content = {"items": rows, "total_count": total,
                   "search_criteria": {"current_page": echo or page, "page_size": size}}
        self.send(200, gzip.compress(encode(content)), {"Content-Encoding": "gzip"})

    def observed_close(self, start, event):
        with LOCK:
            event["closed_ms"] = int((time.monotonic() - start) * 1000)

    def hold(self, start, event):
        with LOCK:
            event["held"] = True
        until = time.monotonic() + 80
        while time.monotonic() < until:
            if RELEASED.wait(0.05):
                with LOCK:
                    event["released"] = True
                return True
            if select.select([self.connection], [], [], 0)[0]:
                try:
                    self.connection.settimeout(0.05)
                    if not self.connection.recv(1):
                        self.observed_close(start, event)
                        return False
                except (ConnectionResetError, ssl.SSLError):
                    self.observed_close(start, event)
                    return False
                except TimeoutError:
                    continue
        return False

    def trickle(self, start, event):
        with LOCK:
            event["held"] = True
        self.send_response(200)
        self.send_header("Content-Length", "320")
        self.end_headers()
        try:
            for _ in range(320):
                self.wfile.write(b" ")
                self.wfile.flush()
                time.sleep(0.08)
        except (BrokenPipeError, ConnectionResetError, ssl.SSLError):
            self.observed_close(start, event)


if __name__ == "__main__":
    ingress = ThreadingHTTPServer(("0.0.0.0", 8443), Commerce)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain("/case.crt", "/case.key")
    ingress.socket = tls.wrap_socket(ingress.socket, server_side=True)
    threading.Thread(target=ingress.serve_forever, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", 8080), Control).serve_forever()
