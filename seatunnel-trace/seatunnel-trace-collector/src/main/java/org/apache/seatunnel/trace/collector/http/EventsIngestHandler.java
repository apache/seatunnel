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

import org.apache.seatunnel.trace.collector.config.TraceCollectorConfig;
import org.apache.seatunnel.trace.collector.db.IngestedEvent;
import org.apache.seatunnel.trace.collector.db.TraceRepository;
import org.apache.seatunnel.trace.collector.enrich.TaskMappingCache;
import org.apache.seatunnel.trace.collector.metrics.TraceCollectorMetrics;
import org.apache.seatunnel.trace.collector.model.TraceEntry;
import org.apache.seatunnel.trace.collector.payload.SttrPayloadDecoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Slf4j
final class EventsIngestHandler implements HttpHandler {

    private final TraceCollectorConfig config;
    private final TraceRepository repository;
    private final TraceCollectorMetrics metrics;
    private final TraceAuth auth;
    private final TaskMappingCache mappingCache;

    private static final int MAX_STAIN_TRACE_PAYLOAD_BYTES = 8 * 1024;

    EventsIngestHandler(
            TraceCollectorConfig config,
            TraceRepository repository,
            TraceCollectorMetrics metrics,
            TraceAuth auth,
            TaskMappingCache mappingCache) {
        this.config = config;
        this.repository = repository;
        this.metrics = metrics;
        this.auth = auth;
        this.mappingCache = mappingCache;
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
            if (!"POST".equalsIgnoreCase(method)) {
                code = 405;
                HttpUtils.writeText(exchange, 405, "method not allowed\n");
                return;
            }
            if (!auth.isAuthorized(exchange)) {
                code = 401;
                HttpUtils.writeText(exchange, 401, "unauthorized\n");
                return;
            }

            byte[] body =
                    HttpUtils.readAllBytesWithLimit(
                            exchange.getRequestBody(), config.getMaxRequestBytes());
            JsonNode root = TraceHttpServer.MAPPER.readTree(body);
            if (root == null || !root.isArray()) {
                code = 400;
                HttpUtils.writeText(exchange, 400, "expect json array\n");
                return;
            }

            long receivedMs = System.currentTimeMillis();
            List<IngestedEvent> events = new ArrayList<>();
            for (JsonNode node : root) {
                String eventType = asTextOrNull(node.get("eventType"));
                String jobId = asTextOrNull(node.get("jobId"));
                if (eventType == null) {
                    eventType = "UNKNOWN";
                }
                metrics.eventsReceivedTotal.labels(eventType).inc();

                IngestedEvent.IngestedEventBuilder builder =
                        IngestedEvent.builder()
                                .receivedTimeMs(receivedMs)
                                .jobId(jobId)
                                .eventType(eventType)
                                .rawJson(node.toString());

                if ("STAIN_TRACE".equalsIgnoreCase(eventType)) {
                    metrics.stainTraceReceivedTotal.inc();
                    Long createdTimeMs = asLongOrNull(node.get("createdTime"));
                    Long traceId = asLongOrNull(node.get("traceId"));
                    Long sinkTaskId = asLongOrNull(node.get("sinkTaskId"));
                    String tableId = asTextOrNull(node.get("tableId"));
                    byte[] payload = decodePayload(node.get("payload"));

                    if (traceId == null || traceId <= 0 || sinkTaskId == null || sinkTaskId <= 0) {
                        metrics.invalidEventsTotal.labels("stain_trace_missing_ids").inc();
                    } else {
                        if (payload != null && payload.length > MAX_STAIN_TRACE_PAYLOAD_BYTES) {
                            metrics.invalidEventsTotal.labels("stain_trace_payload_oversize").inc();
                            payload = null;
                        }
                        builder.createdTimeMs(createdTimeMs)
                                .traceId(traceId)
                                .sinkTaskId(sinkTaskId)
                                .tableId(tableId)
                                .payloadBytes(payload);

                        if (config.isParsePayloadEnabled()
                                && payload != null
                                && SttrPayloadDecoder.isValid(payload)) {
                            SttrPayloadDecoder.DecodedPayload decoded =
                                    SttrPayloadDecoder.decode(payload);
                            maybeEnrich(jobId, decoded.getEntries());
                            builder.startTsMs(decoded.getStartTsMs())
                                    .entryCount(decoded.getEntryCount())
                                    .entries(decoded.getEntries());
                        } else if (payload != null) {
                            metrics.invalidEventsTotal.labels("stain_trace_payload_invalid").inc();
                        }
                    }
                }
                events.add(builder.build());
            }

            repository.ingest(events);

            byte[] resp =
                    TraceHttpServer.MAPPER
                            .createObjectNode()
                            .put("received", events.size())
                            .toString()
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            HttpUtils.writeJson(exchange, 200, resp);
        } catch (IllegalArgumentException e) {
            code = 400;
            HttpUtils.writeText(exchange, 400, e.getMessage() + "\n");
        } catch (Throwable t) {
            code = 500;
            log.warn("Failed to ingest events", t);
            HttpUtils.writeText(exchange, 500, "internal error\n");
        } finally {
            metrics.httpRequestsTotal
                    .labels("/api/v1/seatunnel/events", method, String.valueOf(code))
                    .inc();
            metrics.httpRequestSeconds
                    .labels("/api/v1/seatunnel/events", method)
                    .observe((System.nanoTime() - startNanos) / 1e9);
        }
    }

    private void maybeEnrich(String jobId, List<TraceEntry> entries) {
        if (entries == null
                || entries.isEmpty()
                || mappingCache == null
                || !mappingCache.isEnabled()) {
            return;
        }
        java.util.Map<Long, TaskMappingCache.Mapping> mapping = mappingCache.getMapping(jobId);
        if (mapping.isEmpty()) {
            return;
        }
        for (TraceEntry e : entries) {
            TaskMappingCache.Mapping m = mapping.get(e.getTaskId());
            if (m == null) {
                continue;
            }
            e.setWorkerAddress(m.getWorker());
            e.setTaskGroupName(m.getTaskGroupName());
            e.setTaskClass(m.getTaskClass());
        }
    }

    private static String asTextOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        return node.toString();
    }

    private static Long asLongOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            String t = node.asText();
            if (t == null || t.isEmpty()) {
                return null;
            }
            try {
                return Long.parseLong(t);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static byte[] decodePayload(JsonNode payloadNode) {
        if (payloadNode == null || payloadNode.isNull()) {
            return null;
        }
        if (payloadNode.isBinary()) {
            try {
                return payloadNode.binaryValue();
            } catch (IOException e) {
                return null;
            }
        }
        if (payloadNode.isTextual()) {
            String base64 = payloadNode.asText();
            if (base64 == null || base64.isEmpty()) {
                return null;
            }
            try {
                return Base64.getDecoder().decode(base64);
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        if (payloadNode.isArray()) {
            byte[] out = new byte[payloadNode.size()];
            for (int i = 0; i < payloadNode.size(); i++) {
                out[i] = (byte) payloadNode.get(i).asInt();
            }
            return out;
        }
        return null;
    }
}
