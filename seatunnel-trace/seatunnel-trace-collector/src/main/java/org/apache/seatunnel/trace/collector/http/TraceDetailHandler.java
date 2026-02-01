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

import org.apache.seatunnel.trace.collector.db.TraceRepository;
import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;
import org.apache.seatunnel.trace.collector.model.TraceDetail;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@Slf4j
final class TraceDetailHandler implements HttpHandler {
    private static final String PREFIX = "/api/v1/traces/";

    private final TraceRepository repository;
    private final TraceCollectorMetrics metrics;
    private final TraceAuth auth;

    TraceDetailHandler(TraceRepository repository, TraceCollectorMetrics metrics, TraceAuth auth) {
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

            String path = exchange.getRequestURI().getPath();
            if (path == null || !path.startsWith(PREFIX) || path.length() <= PREFIX.length()) {
                code = 400;
                HttpUtils.writeText(exchange, 400, "missing traceId\n");
                return;
            }
            String traceIdStr = path.substring(PREFIX.length());
            long traceId = Long.parseLong(traceIdStr);

            Map<String, String> q = HttpUtils.parseQuery(exchange.getRequestURI().getRawQuery());
            Long sinkTaskId = parseLongOrNull(q.get("sinkTaskId"));

            TraceDetail detail = repository.getTrace(traceId, sinkTaskId);
            byte[] resp =
                    TraceHttpServer.MAPPER
                            .valueToTree(detail)
                            .toString()
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpUtils.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            code = 400;
            HttpUtils.writeText(exchange, 400, "bad request\n");
        } catch (Throwable t) {
            code = 500;
            log.warn("Failed to query trace detail", t);
            HttpUtils.writeText(exchange, 500, "internal error\n");
        } finally {
            metrics.httpRequestsTotal
                    .labels("/api/v1/traces/{id}", method, String.valueOf(code))
                    .inc();
            metrics.httpRequestSeconds
                    .labels("/api/v1/traces/{id}", method)
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
}
