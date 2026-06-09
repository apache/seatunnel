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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.trace.analyzer.model.BottleneckPoint;
import org.apache.seatunnel.trace.analyzer.model.TraceRecord;
import org.apache.seatunnel.trace.analyzer.model.TraceStatistics;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Renders aggregated trace statistics into the standalone HTML report. */
@Slf4j
public class HtmlReportGenerator {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Fills the HTML template with summary statistics and the trace timeline JSON payload. */
    public void generate(
            TraceStatistics stats,
            List<BottleneckPoint> bottlenecks,
            List<TraceRecord> allTraces,
            String outputFile)
            throws IOException {
        String template = loadTemplate();

        String traceDataJson = generateTraceDataJson(allTraces);
        // Escape < > & to their JSON Unicode equivalents so the HTML parser can never
        // mistake embedded JSON content for a closing </script> tag (covers all case /
        // whitespace variants such as </SCRIPT> or </script >).
        // \u003c / \u003e / \u0026 are valid JSON Unicode escapes; JSON.parse() will
        // transparently decode them back to the original characters.
        String safeTraceDataJson =
                traceDataJson
                        .replace("&", "\\u0026")
                        .replace("<", "\\u003c")
                        .replace(">", "\\u003e");

        String html =
                template.replace(
                                "{{TIMESTAMP}}",
                                LocalDateTime.now()
                                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                        .replace("{{TOTAL_COUNT}}", String.valueOf(stats.getTotalCount()))
                        .replace("{{AVG_LATENCY}}", String.format("%.2f", stats.getAvgLatencyMs()))
                        .replace("{{MAX_LATENCY}}", String.valueOf(stats.getMaxLatencyMs()))
                        .replace("{{MIN_LATENCY}}", String.valueOf(stats.getMinLatencyMs()))
                        .replace("{{P95_LATENCY}}", String.valueOf(stats.getP95LatencyMs()))
                        .replace("{{P99_LATENCY}}", String.valueOf(stats.getP99LatencyMs()))
                        .replace("__TRACE_DATA_JSON__", safeTraceDataJson);

        Files.write(Paths.get(outputFile), html.getBytes(StandardCharsets.UTF_8));
        log.info("Generated HTML report: {}", outputFile);
    }

    private String loadTemplate() throws IOException {
        try (InputStream is =
                getClass().getClassLoader().getResourceAsStream("template/report.html")) {
            if (is == null) {
                throw new IOException("Template file not found: template/report.html");
            }
            // Java 8 compatible: use byte array buffer instead of readAllBytes()
            StringBuilder sb = new StringBuilder();
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                sb.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
            }
            return sb.toString();
        }
    }

    private String generateTraceDataJson(List<TraceRecord> allTraces) throws IOException {
        List<Map<String, Object>> traceDataList =
                allTraces.stream()
                        .map(
                                trace -> {
                                    Map<String, Object> traceMap = new HashMap<>();
                                    traceMap.put("traceId", String.valueOf(trace.getTraceId()));
                                    traceMap.put(
                                            "sinkTaskId", String.valueOf(trace.getSinkTaskId()));
                                    traceMap.put("jobId", trace.getJobId());
                                    traceMap.put("tableId", trace.getTableId());

                                    List<Map<String, Object>> entries =
                                            trace.getEntries().stream()
                                                    .map(
                                                            entry -> {
                                                                Map<String, Object> entryMap =
                                                                        new HashMap<>();
                                                                entryMap.put(
                                                                        "stage", entry.getStage());
                                                                entryMap.put(
                                                                        "stageName",
                                                                        entry.getStageName());
                                                                entryMap.put(
                                                                        "taskId",
                                                                        String.valueOf(
                                                                                entry.getTaskId()));
                                                                entryMap.put(
                                                                        "timestampMs",
                                                                        entry.getTimestampMs());
                                                                return entryMap;
                                                            })
                                                    .collect(Collectors.toList());
                                    traceMap.put("entries", entries);

                                    return traceMap;
                                })
                        .collect(Collectors.toList());

        return MAPPER.writeValueAsString(traceDataList);
    }
}
