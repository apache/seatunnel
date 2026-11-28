/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.opendatadc;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serves ArcGIS-shaped crash responses locally and checks query predicates, projections and retry
 * offsets. Each test selects an explicit mode and resets the observed request sequence.
 */
final class ArcGisCrashesFixture implements AutoCloseable {
    private static final String PROJECTION =
            "OBJECTID,CCN,REPORTDATE,WARD,TOTAL_VEHICLES,LATITUDE,LONGITUDE";
    private static final String WHERE_FIRST = "OBJECTID > 0 AND OBJECTID <= 19";
    private static final String WHERE_SECOND = "OBJECTID > 7 AND OBJECTID <= 19";
    private static final String WHERE_THIRD = "OBJECTID > 12 AND OBJECTID <= 19";

    private final HttpServer server;
    private final List<String> queries = new ArrayList<>();

    /**
     * Selects one deterministic response sequence for a test, including recovery and retry
     * exhaustion. Failure modes exercise ArcGIS errors, redirects and bounded response rejection.
     */
    enum Mode {
        RECOVER,
        ARCGIS_ERROR,
        THROTTLE_EXHAUST,
        EMPTY_EXHAUST,
        REDIRECT,
        OVERSIZED_RESPONSE,
        CHECKPOINT_ROWS
    }

    private Mode mode;
    private int throttleAttempts;
    private final List<String> checkpointQueries = new ArrayList<>();

    ArcGisCrashesFixture() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/query", this::handle);
    }

    int getPort() {
        return server.getAddress().getPort();
    }

    void start() {
        server.start();
    }

    synchronized void reset(Mode mode) {
        this.mode = mode;
        throttleAttempts = 0;
        queries.clear();
        checkpointQueries.clear();
    }

    synchronized List<String> getQueries() {
        return new ArrayList<>(queries);
    }

    synchronized List<String> getCheckpointQueries() {
        return new ArrayList<>(checkpointQueries);
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private synchronized void handle(HttpExchange exchange) throws IOException {
        try {
            Map<String, String> params = queryParams(exchange);
            String where = params.get("where");
            String offset = params.get("resultOffset");
            queries.add(where + " @" + offset);
            if (!"GET".equals(exchange.getRequestMethod())
                    || !"/query".equals(exchange.getRequestURI().getPath())
                    || exchange.getRequestHeaders().containsKey("Authorization")
                    || !"json".equals(params.get("f"))
                    || !"false".equals(params.get("returnGeometry"))) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            if (mode == Mode.CHECKPOINT_ROWS) {
                checkpointPage(exchange, params);
                return;
            }
            String body;
            if ("1=1".equals(where) && queryMatches(params, "OBJECTID", "OBJECTID DESC", "1")) {
                if (mode == Mode.OVERSIZED_RESPONSE) {
                    // Valid JSON over 4 MiB, chunked so rejection must enforce the streamed bound.
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, 0);
                    byte[] block = new byte[8192];
                    Arrays.fill(block, (byte) 'x');
                    try {
                        exchange.getResponseBody()
                                .write(
                                        "{\"features\":[],\"padding\":\""
                                                .getBytes(StandardCharsets.UTF_8));
                        for (int i = 0; i < 513; i++) {
                            exchange.getResponseBody().write(block);
                        }
                        exchange.getResponseBody().write("\"}".getBytes(StandardCharsets.UTF_8));
                    } catch (IOException expected) {
                        // The bounded reader closes this oversized response before it finishes.
                    }
                    return;
                }
                if (mode == Mode.REDIRECT) {
                    // Redirect back to the same query: following it would make extra requests.
                    exchange.getResponseHeaders()
                            .set("Location", exchange.getRequestURI().toString());
                    exchange.sendResponseHeaders(302, -1);
                    return;
                }
                if (mode == Mode.RECOVER && "0".equals(offset)) {
                    body = emptyWindow();
                } else if (mode == Mode.RECOVER ? "1".equals(offset) : "0".equals(offset)) {
                    body = "{\"features\":[{\"attributes\":{\"OBJECTID\":19}}]}";
                } else {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }
            } else if (WHERE_FIRST.equals(where)
                    && "0".equals(offset)
                    && queryMatches(params, PROJECTION, "OBJECTID ASC", "2")) {
                body = page(7);
            } else if (WHERE_SECOND.equals(where)
                    && queryMatches(params, PROJECTION, "OBJECTID ASC", "2")) {
                if (mode == Mode.ARCGIS_ERROR && "0".equals(offset)) {
                    body = "{\"error\":{\"code\":400}}";
                } else if (mode == Mode.EMPTY_EXHAUST || "0".equals(offset)) {
                    body = emptyWindow();
                } else if ("2".equals(offset)) {
                    if (mode == Mode.THROTTLE_EXHAUST || throttleAttempts++ == 0) {
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        exchange.sendResponseHeaders(429, -1);
                        return;
                    }
                    body = page(12);
                } else {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }
            } else if (WHERE_THIRD.equals(where)
                    && "0".equals(offset)
                    && queryMatches(params, PROJECTION, "OBJECTID ASC", "1")) {
                body = page(17);
            } else {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            byte[] data = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, data.length);
            exchange.getResponseBody().write(data);
        } finally {
            exchange.close();
        }
    }

    /**
     * Serves one normal-sized page slowly collected by Zeta, then validates the restored key, bound
     * and remaining row cap through the actual next HTTP query rather than engine-private state.
     */
    private void checkpointPage(HttpExchange exchange, Map<String, String> params)
            throws IOException {
        String where = params.get("where");
        String size = params.get("resultRecordCount");
        checkpointQueries.add(where + " @" + params.get("resultOffset") + " #" + size);
        String body;
        if ("1=1".equals(where) && queryMatches(params, "OBJECTID", "OBJECTID DESC", "1")) {
            body = "{\"features\":[{\"attributes\":{\"OBJECTID\":1000}}]}";
        } else {
            if (!where.matches("OBJECTID > [0-9]+ AND OBJECTID <= 1000")
                    || !"0".equals(params.get("resultOffset"))) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            long last = Long.parseLong(where.split(" ")[2]);
            int count = Integer.parseInt(size);
            if (last >= 400
                    || count != 400 - last
                    || !queryMatches(params, PROJECTION, "OBJECTID ASC", size)) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            StringBuilder rows = new StringBuilder("{\"features\":[");
            for (long id = last + 1; id <= 400; id++) {
                if (id > last + 1) {
                    rows.append(',');
                }
                rows.append("{\"attributes\":{\"OBJECTID\":")
                        .append(id)
                        .append(
                                ",\"CCN\":\"00123456\",\"REPORTDATE\":1000,\"WARD\":\"Ward 1\","
                                        + "\"TOTAL_VEHICLES\":2,\"LATITUDE\":38.9,\"LONGITUDE\":null}}");
            }
            body = rows.append("]}").toString();
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    private static Map<String, String> queryParams(HttpExchange exchange) throws IOException {
        Map<String, String> result = new LinkedHashMap<>();
        String query = exchange.getRequestURI().getRawQuery();
        if (query != null) {
            for (String pair : query.split("&")) {
                String[] parts = pair.split("=", 2);
                if (parts.length == 2) {
                    result.put(
                            URLDecoder.decode(parts[0], StandardCharsets.UTF_8.name()),
                            URLDecoder.decode(parts[1], StandardCharsets.UTF_8.name()));
                }
            }
        }
        return result;
    }

    private static boolean queryMatches(
            Map<String, String> params, String fields, String order, String count) {
        return params.size() == 7
                && fields.equals(params.get("outFields"))
                && order.equals(params.get("orderByFields"))
                && count.equals(params.get("resultRecordCount"));
    }

    private static String emptyWindow() {
        return "{\"features\":[],\"exceededTransferLimit\":true}";
    }

    private static String page(long id) {
        return "{\"features\":[{\"attributes\":{\"OBJECTID\":"
                + id
                + ",\"CCN\":\"00123456\",\"REPORTDATE\":1000,\"WARD\":\"Ward 1\","
                + "\"TOTAL_VEHICLES\":2,\"LATITUDE\":38.9,\"LONGITUDE\":null}}]}";
    }
}
