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

package org.apache.seatunnel.trace.collector.http;

import com.sun.net.httpserver.HttpExchange;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

final class HttpUtils {
    private HttpUtils() {}

    static void addCorsHeaders(HttpExchange exchange) {
        String origin = exchange.getRequestHeaders().getFirst("Origin");
        if (origin == null || origin.isEmpty()) {
            return;
        }
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", origin);
        exchange.getResponseHeaders().set("Access-Control-Allow-Credentials", "true");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
        exchange.getResponseHeaders()
                .set("Access-Control-Allow-Headers", "Content-Type,X-Seatunnel-Token");
        exchange.getResponseHeaders().set("Vary", "Origin");
    }

    static void handlePreflightIfNeeded(HttpExchange exchange) throws IOException {
        if (!"OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            return;
        }
        addCorsHeaders(exchange);
        exchange.sendResponseHeaders(204, -1);
        exchange.close();
    }

    static byte[] readAllBytesWithLimit(InputStream in, int maxBytes) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(Math.min(maxBytes, 16 * 1024));
        byte[] buf = new byte[8192];
        int total = 0;
        for (; ; ) {
            int n = in.read(buf);
            if (n < 0) {
                break;
            }
            total += n;
            if (total > maxBytes) {
                throw new IllegalArgumentException("request body too large");
            }
            bos.write(buf, 0, n);
        }
        return bos.toByteArray();
    }

    static void writeText(HttpExchange exchange, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        addCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    static void writeJson(HttpExchange exchange, int code, byte[] json) throws IOException {
        addCorsHeaders(exchange);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, json.length);
        exchange.getResponseBody().write(json);
        exchange.close();
    }

    static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> m = new HashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return m;
        }
        String[] parts = rawQuery.split("&");
        for (String p : parts) {
            int idx = p.indexOf('=');
            if (idx <= 0) {
                m.put(p, "");
            } else {
                m.put(urlDecode(p.substring(0, idx)), urlDecode(p.substring(idx + 1)));
            }
        }
        return m;
    }

    private static String urlDecode(String s) {
        try {
            return java.net.URLDecoder.decode(s, "UTF-8");
        } catch (Exception e) {
            return s;
        }
    }
}
