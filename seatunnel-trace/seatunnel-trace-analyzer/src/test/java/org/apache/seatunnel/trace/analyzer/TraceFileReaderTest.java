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

import org.apache.seatunnel.trace.analyzer.model.TraceRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link TraceFileReader} OTLP JSONL parsing. */
class TraceFileReaderTest {

    @TempDir Path tempDir;

    private final TraceFileReader reader = new TraceFileReader();

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    /** Writes content to {tempDir}/traces/{jobId}/{date}/traces-test.jsonl */
    private Path writeTraceFile(String jobId, String date, String... lines) throws IOException {
        Path traceDir = tempDir.resolve("traces").resolve(jobId).resolve(date);
        Files.createDirectories(traceDir);
        Path file = traceDir.resolve("traces-test.jsonl");
        Files.write(file, String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
        return file;
    }

    private String otlpLine(
            String jobId, String traceId, String spanId, String tableId, String... events) {
        StringBuilder eventsJson = new StringBuilder("[");
        for (int i = 0; i < events.length; i++) {
            if (i > 0) eventsJson.append(",");
            eventsJson.append(events[i]);
        }
        eventsJson.append("]");

        return "{"
                + "\"resourceSpans\":[{"
                + "  \"resource\":{\"attributes\":[{\"key\":\"seatunnel.job_id\","
                + "    \"value\":{\"stringValue\":\""
                + jobId
                + "\"}}]},"
                + "  \"scopeSpans\":[{"
                + "    \"spans\":[{"
                + "      \"traceId\":\""
                + traceId
                + "\","
                + "      \"spanId\":\""
                + spanId
                + "\","
                + "      \"startTimeUnixNano\":\"1000000000\","
                + "      \"endTimeUnixNano\":\"2000000000\","
                + "      \"attributes\":[{\"key\":\"seatunnel.table_id\","
                + "        \"value\":{\"stringValue\":\""
                + tableId
                + "\"}}],"
                + "      \"events\":"
                + eventsJson
                + "    }]"
                + "  }]"
                + "}]}";
    }

    private String eventJson(String stageName, long timeNs, int stageCode, long taskId) {
        return "{"
                + "\"name\":\""
                + stageName
                + "\","
                + "\"timeUnixNano\":\""
                + timeNs
                + "\","
                + "\"attributes\":["
                + "  {\"key\":\"seatunnel.stage_code\",\"value\":{\"intValue\":\""
                + stageCode
                + "\"}},"
                + "  {\"key\":\"seatunnel.task_id\",\"value\":{\"intValue\":\""
                + taskId
                + "\"}}"
                + "]}";
    }

    // -----------------------------------------------------------------------
    // tests
    // -----------------------------------------------------------------------

    @Test
    void testSingleSpanParsedCorrectly() throws IOException {
        String event = eventJson("STAGE_A", 1_500_000_000L, 1, 42L);
        String line =
                otlpLine(
                        "job1",
                        "00000000000000000000000000000001",
                        "0000000000000002",
                        "myTable",
                        event);
        writeTraceFile("job1", "20260101", line);

        List<TraceRecord> records = reader.readTraces(tempDir.toString(), "job1", "20260101");

        assertEquals(1, records.size());
        TraceRecord r = records.get(0);
        assertEquals("job1", r.getJobId());
        assertEquals("myTable", r.getTableId());
        assertEquals(1, r.getEntries().size());
        assertEquals("STAGE_A", r.getEntries().get(0).getStageName());
        assertEquals(1, r.getEntries().get(0).getStage());
        assertEquals(42L, r.getEntries().get(0).getTaskId());
        // 1_500_000_000 ns / 1_000_000 = 1500 ms
        assertEquals(1500L, r.getEntries().get(0).getTimestampMs());
    }

    @Test
    void testMultipleSpansInSingleOtlpLine() throws IOException {
        // Two spans inside a single OTLP JSON line (multiple resourceSpans)
        String event1 = eventJson("STAGE_A", 1_000_000_000L, 1, 10L);
        String event2 = eventJson("STAGE_B", 2_000_000_000L, 2, 20L);
        String line =
                "{"
                        + "\"resourceSpans\":["
                        + "  {"
                        + "    \"resource\":{\"attributes\":[{\"key\":\"seatunnel.job_id\","
                        + "      \"value\":{\"stringValue\":\"jobX\"}}]},"
                        + "    \"scopeSpans\":[{"
                        + "      \"spans\":[{"
                        + "        \"traceId\":\"00000000000000000000000000000010\","
                        + "        \"spanId\":\"0000000000000011\","
                        + "        \"startTimeUnixNano\":\"1000000000\","
                        + "        \"endTimeUnixNano\":\"2000000000\","
                        + "        \"attributes\":[{\"key\":\"seatunnel.table_id\","
                        + "          \"value\":{\"stringValue\":\"tableA\"}}],"
                        + "        \"events\":["
                        + event1
                        + "]"
                        + "      }]"
                        + "    }]"
                        + "  },"
                        + "  {"
                        + "    \"resource\":{\"attributes\":[{\"key\":\"seatunnel.job_id\","
                        + "      \"value\":{\"stringValue\":\"jobX\"}}]},"
                        + "    \"scopeSpans\":[{"
                        + "      \"spans\":[{"
                        + "        \"traceId\":\"00000000000000000000000000000020\","
                        + "        \"spanId\":\"0000000000000022\","
                        + "        \"startTimeUnixNano\":\"1000000000\","
                        + "        \"endTimeUnixNano\":\"2000000000\","
                        + "        \"attributes\":[{\"key\":\"seatunnel.table_id\","
                        + "          \"value\":{\"stringValue\":\"tableB\"}}],"
                        + "        \"events\":["
                        + event2
                        + "]"
                        + "      }]"
                        + "    }]"
                        + "  }"
                        + "]}";

        writeTraceFile("jobX", "20260101", line);

        List<TraceRecord> records = reader.readTraces(tempDir.toString(), "jobX", "20260101");

        assertEquals(2, records.size(), "Should parse both resourceSpans as separate TraceRecords");

        boolean foundTableA = records.stream().anyMatch(r -> "tableA".equals(r.getTableId()));
        boolean foundTableB = records.stream().anyMatch(r -> "tableB".equals(r.getTableId()));
        assertTrue(foundTableA, "Should have record for tableA");
        assertTrue(foundTableB, "Should have record for tableB");
    }

    @Test
    void testMultipleSpansInsideSameScopeSpans() throws IOException {
        // Two spans inside the same scopeSpan → two TraceRecords
        String event1 = eventJson("STAGE_A", 1_000_000_000L, 1, 10L);
        String event2 = eventJson("STAGE_B", 2_000_000_000L, 2, 20L);
        String line =
                "{"
                        + "\"resourceSpans\":[{"
                        + "  \"resource\":{\"attributes\":[{\"key\":\"seatunnel.job_id\","
                        + "    \"value\":{\"stringValue\":\"jobY\"}}]},"
                        + "  \"scopeSpans\":[{"
                        + "    \"spans\":["
                        + "      {"
                        + "        \"traceId\":\"00000000000000000000000000000030\","
                        + "        \"spanId\":\"0000000000000031\","
                        + "        \"startTimeUnixNano\":\"1000000000\","
                        + "        \"endTimeUnixNano\":\"2000000000\","
                        + "        \"attributes\":[{\"key\":\"seatunnel.table_id\","
                        + "          \"value\":{\"stringValue\":\"tableC\"}}],"
                        + "        \"events\":["
                        + event1
                        + "]"
                        + "      },"
                        + "      {"
                        + "        \"traceId\":\"00000000000000000000000000000040\","
                        + "        \"spanId\":\"0000000000000042\","
                        + "        \"startTimeUnixNano\":\"1000000000\","
                        + "        \"endTimeUnixNano\":\"2000000000\","
                        + "        \"attributes\":[{\"key\":\"seatunnel.table_id\","
                        + "          \"value\":{\"stringValue\":\"tableD\"}}],"
                        + "        \"events\":["
                        + event2
                        + "]"
                        + "      }"
                        + "    ]"
                        + "  }]"
                        + "}]}";

        writeTraceFile("jobY", "20260101", line);

        List<TraceRecord> records = reader.readTraces(tempDir.toString(), "jobY", "20260101");

        assertEquals(2, records.size(), "Should parse both spans from same scopeSpan");
        assertTrue(records.stream().anyMatch(r -> "tableC".equals(r.getTableId())));
        assertTrue(records.stream().anyMatch(r -> "tableD".equals(r.getTableId())));
    }

    @Test
    void testSpanWithNoEventsIsSkipped() throws IOException {
        // Span without events → should produce no TraceRecord
        String line =
                "{"
                        + "\"resourceSpans\":[{"
                        + "  \"resource\":{\"attributes\":[{\"key\":\"seatunnel.job_id\","
                        + "    \"value\":{\"stringValue\":\"jobZ\"}}]},"
                        + "  \"scopeSpans\":[{"
                        + "    \"spans\":[{"
                        + "      \"traceId\":\"00000000000000000000000000000050\","
                        + "      \"spanId\":\"0000000000000051\","
                        + "      \"startTimeUnixNano\":\"1000000000\","
                        + "      \"endTimeUnixNano\":\"2000000000\","
                        + "      \"attributes\":[],"
                        + "      \"events\":[]"
                        + "    }]"
                        + "  }]"
                        + "}]}";

        writeTraceFile("jobZ", "20260101", line);

        List<TraceRecord> records = reader.readTraces(tempDir.toString(), "jobZ", "20260101");

        assertTrue(records.isEmpty(), "Span with empty events should be skipped");
    }

    @Test
    void testInvalidJsonLineIsSkipped() throws IOException {
        String event = eventJson("STAGE_A", 1_500_000_000L, 1, 42L);
        String validLine =
                otlpLine(
                        "job2",
                        "00000000000000000000000000000060",
                        "0000000000000061",
                        "myTable2",
                        event);

        // Put an invalid JSON line before and after the valid one
        writeTraceFile("job2", "20260101", "NOT_JSON", validLine, "{broken");

        List<TraceRecord> records = reader.readTraces(tempDir.toString(), "job2", "20260101");

        assertEquals(1, records.size(), "Only the valid line should produce a record");
    }

    @Test
    void testNonExistentPathReturnsEmptyList() throws IOException {
        List<TraceRecord> records =
                reader.readTraces(tempDir.toString(), "nonexistent-job", "99999999");

        assertTrue(records.isEmpty());
    }

    @Test
    void testMultipleFiles() throws IOException {
        // Two separate .jsonl files in the same directory
        String event1 = eventJson("STAGE_A", 1_000_000_000L, 1, 10L);
        String event2 = eventJson("STAGE_B", 2_000_000_000L, 2, 20L);
        String line1 =
                otlpLine(
                        "job3",
                        "00000000000000000000000000000070",
                        "0000000000000071",
                        "tableE",
                        event1);
        String line2 =
                otlpLine(
                        "job3",
                        "00000000000000000000000000000080",
                        "0000000000000081",
                        "tableF",
                        event2);

        Path traceDir = tempDir.resolve("traces").resolve("job3").resolve("20260101");
        Files.createDirectories(traceDir);
        Files.write(traceDir.resolve("traces-file1.jsonl"), line1.getBytes(StandardCharsets.UTF_8));
        Files.write(traceDir.resolve("traces-file2.jsonl"), line2.getBytes(StandardCharsets.UTF_8));

        List<TraceRecord> records = reader.readTraces(tempDir.toString(), "job3", "20260101");

        assertEquals(2, records.size());
        assertTrue(records.stream().anyMatch(r -> "tableE".equals(r.getTableId())));
        assertTrue(records.stream().anyMatch(r -> "tableF".equals(r.getTableId())));
    }
}
