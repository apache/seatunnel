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

package org.apache.seatunnel.trace.analyzer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.trace.analyzer.model.TraceEntry;
import org.apache.seatunnel.trace.analyzer.model.TraceRecord;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Reads StainTrace OTLP JSONL files produced by {@code JobEventLocalFileHandler}.
 *
 * <p>Each line is an {@code ExportTraceServiceRequest} in OTLP JSON format:
 *
 * <pre>
 * {
 *   "resourceSpans": [{
 *     "resource": {"attributes": [{"key":"seatunnel.job_id","value":{"stringValue":"..."}}]},
 *     "scopeSpans": [{
 *       "spans": [{
 *         "traceId": "<32-hex>",
 *         "spanId": "<16-hex>",
 *         "startTimeUnixNano": "<ns-string>",
 *         "endTimeUnixNano": "<ns-string>",
 *         "attributes": [...],
 *         "events": [{"name":"STAGE","timeUnixNano":"<ns>","attributes":[...]}],
 *         "status": {"code":1}
 *       }]
 *     }]
 *   }]
 * }
 * </pre>
 */
@Slf4j
public class TraceFileReader {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Loads OTLP JSONL trace files under the selected directory, job, and date filters. */
    public List<TraceRecord> readTraces(String baseDir, String jobId, String date)
            throws IOException {
        Path searchPath = buildSearchPath(baseDir, jobId, date);

        if (!Files.exists(searchPath)) {
            log.warn("Search path does not exist: {}", searchPath);
            return new ArrayList<>();
        }

        List<TraceRecord> records = new ArrayList<>();

        try (Stream<Path> paths = Files.walk(searchPath)) {
            paths.filter(p -> p.toString().endsWith(".jsonl"))
                    .forEach(
                            p -> {
                                try {
                                    readFile(p, records);
                                } catch (Exception e) {
                                    log.error("Failed to read file: " + p, e);
                                }
                            });
        }

        log.info("Read {} trace records from {}", records.size(), searchPath);
        return records;
    }

    private void readFile(Path filePath, List<TraceRecord> records) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                try {
                    JsonNode root = MAPPER.readTree(line);
                    parseAllSpans(root, records);
                } catch (Exception e) {
                    log.warn(
                            "Skip invalid JSON at {}:{} - {}",
                            filePath.getFileName(),
                            lineNumber,
                            e.getMessage());
                }
            }
        }
    }

    /**
     * Parses one OTLP ExportTraceServiceRequest JSON line, iterating all resourceSpans → scopeSpans
     * → spans and producing one {@link TraceRecord} per span.
     */
    private void parseAllSpans(JsonNode root, List<TraceRecord> records) {
        JsonNode resourceSpans = root.path("resourceSpans");
        if (!resourceSpans.isArray()) {
            return;
        }
        for (JsonNode resourceSpan : resourceSpans) {
            String jobId =
                    extractStringAttr(
                            resourceSpan.path("resource").path("attributes"), "seatunnel.job_id");

            JsonNode scopeSpans = resourceSpan.path("scopeSpans");
            if (!scopeSpans.isArray()) {
                continue;
            }
            for (JsonNode scopeSpan : scopeSpans) {
                JsonNode spans = scopeSpan.path("spans");
                if (!spans.isArray()) {
                    continue;
                }
                for (JsonNode span : spans) {
                    TraceRecord record = parseSpanToRecord(span, jobId);
                    if (record != null) {
                        records.add(record);
                    }
                }
            }
        }
    }

    /** Converts a single OTLP span node into a {@link TraceRecord}. */
    private TraceRecord parseSpanToRecord(JsonNode span, String jobId) {
        // Parse traceId: 32-hex string, keep lower 64 bits (last 16 hex chars)
        long traceId = parseTraceIdHex(span.path("traceId").asText(""));

        // Parse spanId as sinkTaskId: 16-hex string
        long sinkTaskId = parseHexLong(span.path("spanId").asText(""));

        // Parse startTimeUnixNano → ms
        long startNs = parseLongString(span.path("startTimeUnixNano").asText("0"));
        long createdTime = startNs / 1_000_000L;

        // Extract tableId from span attributes
        String tableId = extractStringAttr(span.path("attributes"), "seatunnel.table_id");

        // Parse events
        JsonNode eventsNode = span.path("events");
        if (!eventsNode.isArray() || eventsNode.size() == 0) {
            return null;
        }

        List<TraceEntry> entries = new ArrayList<>();
        for (JsonNode eventNode : eventsNode) {
            String stageName = eventNode.path("name").asText("");
            long timeNs = parseLongString(eventNode.path("timeUnixNano").asText("0"));
            long timestampMs = timeNs / 1_000_000L;

            JsonNode eventAttrs = eventNode.path("attributes");
            int stageCode = (int) extractLongAttr(eventAttrs, "seatunnel.stage_code");
            long taskId = extractLongAttr(eventAttrs, "seatunnel.task_id");

            entries.add(new TraceEntry(stageCode, taskId, timestampMs, stageName));
        }

        if (entries.isEmpty()) {
            return null;
        }

        entries.sort(Comparator.comparingLong(TraceEntry::getTimestampMs));

        return new TraceRecord(traceId, sinkTaskId, jobId, tableId, createdTime, entries);
    }

    /** Extracts a stringValue from an OTLP attribute array by key. */
    private String extractStringAttr(JsonNode attrs, String key) {
        if (!attrs.isArray()) {
            return "";
        }
        for (JsonNode attr : attrs) {
            if (key.equals(attr.path("key").asText())) {
                return attr.path("value").path("stringValue").asText("");
            }
        }
        return "";
    }

    /** Extracts an intValue (stored as string) from an OTLP attribute array by key. */
    private long extractLongAttr(JsonNode attrs, String key) {
        if (!attrs.isArray()) {
            return 0L;
        }
        for (JsonNode attr : attrs) {
            if (key.equals(attr.path("key").asText())) {
                return parseLongString(attr.path("value").path("intValue").asText("0"));
            }
        }
        return 0L;
    }

    /** Parses a 32-char hex traceId, returning the lower 64 bits as a long. */
    private long parseTraceIdHex(String hex) {
        if (hex == null || hex.length() < 16) {
            return 0L;
        }
        // Take the last 16 hex characters (lower 64 bits)
        String lower16 = hex.substring(hex.length() - 16);
        return parseHexLong(lower16);
    }

    private long parseHexLong(String hex) {
        if (hex == null || hex.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseUnsignedLong(hex, 16);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private long parseLongString(String value) {
        if (value == null || value.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseUnsignedLong(value);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private Path buildSearchPath(String baseDir, String jobId, String date) {
        Path path = Paths.get(baseDir, "traces");
        if (jobId != null && !jobId.isEmpty()) {
            path = path.resolve(jobId);
        }
        if (date != null && !date.isEmpty()) {
            path = path.resolve(date);
        }
        return path;
    }
}
