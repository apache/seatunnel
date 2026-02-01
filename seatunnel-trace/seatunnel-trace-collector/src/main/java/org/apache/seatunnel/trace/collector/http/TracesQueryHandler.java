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

import org.apache.seatunnel.trace.collector.db.TraceQuery;
import org.apache.seatunnel.trace.collector.db.TraceRepository;
import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;
import org.apache.seatunnel.trace.collector.model.TraceSummary;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
final class TracesQueryHandler implements HttpHandler {
    private final TraceRepository repository;
    private final TraceCollectorMetrics metrics;
    private final TraceAuth auth;

    TracesQueryHandler(TraceRepository repository, TraceCollectorMetrics metrics, TraceAuth auth) {
        this.repository = repository;
        this.metrics = metrics;
        this.auth = auth;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        long startNanos = System.nanoTime();
        String method = exchange.getRequestMethod();
        int code = 200;
        try {
            HttpUtils.handlePreflightIfNeeded(exchange);
            if ("OPTIONS".equalsIgnoreCase(method)) {
                return;
            }
            if (!"GET".equalsIgnoreCase(method)) {
                code = 405;
                HttpUtils.writeText(exchange, 405, "method not allowed\n");
                return;
            }
            if (!auth.isAuthorized(exchange)) {
                code = 401;
                HttpUtils.writeText(exchange, 401, "unauthorized\n");
                return;
            }

            Map<String, String> q = HttpUtils.parseQuery(exchange.getRequestURI().getRawQuery());
            TraceQuery query =
                    TraceQuery.builder()
                            .jobId(q.get("jobId"))
                            .tableId(q.get("tableId"))
                            .fromMs(parseLongOrNull(q.get("fromMs")))
                            .toMs(parseLongOrNull(q.get("toMs")))
                            .limit(parseIntOrDefault(q.get("limit"), 100))
                            .offset(parseIntOrDefault(q.get("offset"), 0))
                            .build();

            List<TraceSummary> items = repository.queryTraces(query);
            byte[] resp =
                    TraceHttpServer.MAPPER
                            .createObjectNode()
                            .put("count", items.size())
                            .set("items", TraceHttpServer.MAPPER.valueToTree(items))
                            .toString()
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpUtils.writeJson(exchange, 200, resp);
        } catch (Throwable t) {
            code = 500;
            log.warn("Failed to query traces", t);
            HttpUtils.writeText(exchange, 500, "internal error\n");
        } finally {
            metrics.httpRequestsTotal.labels("/api/v1/traces", method, String.valueOf(code)).inc();
            metrics.httpRequestSeconds
                    .labels("/api/v1/traces", method)
                    .observe((System.nanoTime() - startNanos) / 1e9);
        }
    }

    private static Long parseLongOrNull(String s) {
        if (s == null || s.trim().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int parseIntOrDefault(String s, int def) {
        if (s == null || s.trim().isEmpty()) {
            return def;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
