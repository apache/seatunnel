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

import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

final class MetricsHandler implements HttpHandler {
    private final TraceCollectorMetrics metrics;

    MetricsHandler(TraceCollectorMetrics metrics) {
        this.metrics = metrics;
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
            HttpUtils.addCorsHeaders(exchange);
            exchange.getResponseHeaders().set("Content-Type", TextFormat.CONTENT_TYPE_004);
            exchange.sendResponseHeaders(200, 0);
            try (Writer writer =
                    new OutputStreamWriter(exchange.getResponseBody(), StandardCharsets.UTF_8)) {
                TextFormat.write004(
                        writer, CollectorRegistry.defaultRegistry.metricFamilySamples());
            }
        } catch (Throwable t) {
            code = 500;
            HttpUtils.writeText(exchange, 500, "error\n");
        } finally {
            metrics.httpRequestsTotal.labels("/metrics", method, String.valueOf(code)).inc();
            metrics.httpRequestSeconds
                    .labels("/metrics", method)
                    .observe((System.nanoTime() - startNanos) / 1e9);
        }
    }
}
