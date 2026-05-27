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

package org.apache.seatunnel.edge.agent.connector.file;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectConfig;
import org.apache.seatunnel.edge.agent.connector.config.FileCollectOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileCollectReaderMultilineTest {

    @TempDir Path tempDir;

    /**
     * When a multiline event spans across poll boundaries (no next boundary arrived yet), the
     * buffer must NOT be flushed prematurely. The incomplete event stays buffered until a boundary
     * or timeout triggers flush.
     */
    @Test
    void crossPollMultilineEventNotSplitPrematurely() throws Exception {
        Path logFile = tempDir.resolve("app.log");
        // Write first part of a Java stacktrace (no next boundary yet)
        StringBuilder sb = new StringBuilder();
        sb.append("2024-01-01 ERROR NullPointerException\n");
        sb.append("\tat com.foo.Bar.method(Bar.java:42)\n");
        sb.append("\tat com.foo.Baz.run(Baz.java:10)\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^\\d{4}-\\d{2}-\\d{2}", "after");
        // Set a long timeout so it won't trigger during test
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 60000L);

        FileCollectReader reader =
                new FileCollectReader(FileCollectConfig.from(ReadonlyConfig.fromMap(map)), null);
        reader.open();
        try {
            // First poll: no boundary seen yet, buffer holds 3 lines but nothing emitted
            List<EdgeEvent> events = reader.poll(128);
            Assertions.assertTrue(
                    events.isEmpty(),
                    "Incomplete multiline event should not be emitted without boundary");

            // Second poll still empty (no new data)
            events = reader.poll(128);
            Assertions.assertTrue(events.isEmpty());

            // Now write the next boundary line
            Files.write(
                    logFile,
                    "2024-01-02 INFO Application started\n".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);

            // Third poll: boundary triggers flush of the first event (3 lines)
            events = reader.poll(128);
            Assertions.assertEquals(1, events.size());
            String payload = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload.contains("NullPointerException"));
            Assertions.assertTrue(payload.contains("com.foo.Bar"));
            Assertions.assertTrue(payload.contains("com.foo.Baz"));
        } finally {
            reader.close();
        }
    }

    /**
     * When the multiline buffer has been idle longer than flush-idle-timeout-ms, the buffer is
     * flushed as a complete event even without a boundary.
     */
    @Test
    void timeoutFlushEmitsBufferedEvent() throws Exception {
        Path logFile = tempDir.resolve("timeout.log");
        StringBuilder sb = new StringBuilder();
        sb.append("2024-01-01 ERROR Timeout exception\n");
        sb.append("\tat com.foo.Service.call(Service.java:99)\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^\\d{4}-\\d{2}-\\d{2}", "after");
        // Set a very short timeout to trigger flush quickly
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 50L);

        FileCollectReader reader =
                new FileCollectReader(FileCollectConfig.from(ReadonlyConfig.fromMap(map)), null);
        reader.open();
        try {
            // First poll buffers the lines, may or may not flush depending on timing
            reader.poll(128);

            // Wait for the timeout to elapse
            Thread.sleep(100L);

            // Next poll should flush due to timeout
            List<EdgeEvent> events = reader.poll(128);
            Assertions.assertEquals(1, events.size());
            String payload = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload.contains("Timeout exception"));
            Assertions.assertTrue(payload.contains("com.foo.Service"));
        } finally {
            reader.close();
        }
    }

    /**
     * When records reaches maxRecords, the multiline buffer must NOT be flushed and cleared. The
     * buffer remains intact for the next poll.
     */
    @Test
    void recordsFullDoesNotDiscardBuffer() throws Exception {
        Path logFile = tempDir.resolve("full.log");
        StringBuilder sb = new StringBuilder();
        // Write 3 complete single-line events followed by a partial multiline event
        sb.append("2024-01-01 INFO event1\n");
        sb.append("2024-01-02 INFO event2\n");
        sb.append("2024-01-03 ERROR partial\n");
        sb.append("\tat com.foo.X.y(X.java:1)\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^\\d{4}-\\d{2}-\\d{2}", "after");
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 60000L);

        FileCollectReader reader =
                new FileCollectReader(FileCollectConfig.from(ReadonlyConfig.fromMap(map)), null);
        reader.open();
        try {
            // Poll with maxRecords=2 — only 2 events can be emitted
            List<EdgeEvent> events = reader.poll(2);
            Assertions.assertEquals(2, events.size());

            // The partial event (lines 3+4) must still be in the buffer
            // Now write a boundary to trigger flush
            Files.write(
                    logFile,
                    "2024-01-04 INFO next\n".getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);

            events = reader.poll(128);
            // Should emit the previously buffered multiline event (lines 3+4)
            Assertions.assertFalse(events.isEmpty());
            boolean foundPartial = false;
            for (EdgeEvent e : events) {
                String p = new String(e.getPayload(), StandardCharsets.UTF_8);
                if (p.contains("partial") && p.contains("com.foo.X")) {
                    foundPartial = true;
                    break;
                }
            }
            Assertions.assertTrue(
                    foundPartial, "Multiline buffer should not be discarded when records is full");
        } finally {
            reader.close();
        }
    }

    /** BEFORE mode end-to-end: matching line terminates the current event. */
    @Test
    void beforeModeEndToEnd() throws Exception {
        Path logFile = tempDir.resolve("before.log");
        StringBuilder sb = new StringBuilder();
        sb.append("line A\n");
        sb.append("line B\n");
        sb.append("END\n");
        sb.append("line C\n");
        sb.append("END\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^END$", "before");
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 60000L);

        FileCollectReader reader =
                new FileCollectReader(FileCollectConfig.from(ReadonlyConfig.fromMap(map)), null);
        reader.open();
        try {
            List<EdgeEvent> events = reader.poll(128);
            Assertions.assertEquals(2, events.size());

            String payload1 = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload1.contains("line A"));
            Assertions.assertTrue(payload1.contains("line B"));
            Assertions.assertTrue(payload1.contains("END"));

            String payload2 = new String(events.get(1).getPayload(), StandardCharsets.UTF_8);
            Assertions.assertTrue(payload2.contains("line C"));
            Assertions.assertTrue(payload2.contains("END"));
        } finally {
            reader.close();
        }
    }

    /**
     * After close(), remaining buffered lines are cleared. No data loss occurs because the saved
     * position has not advanced past these lines — they will be re-read on next restart.
     */
    @Test
    void closeFlushesRemainingBuffer() throws Exception {
        Path logFile = tempDir.resolve("close.log");
        StringBuilder sb = new StringBuilder();
        sb.append("2024-01-01 ERROR final event\n");
        sb.append("\tat last.stack.Frame(F.java:1)\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^\\d{4}-\\d{2}-\\d{2}", "after");
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 60000L);

        FileCollectReader reader =
                new FileCollectReader(FileCollectConfig.from(ReadonlyConfig.fromMap(map)), null);
        reader.open();

        // Poll without triggering flush (no boundary, no timeout)
        List<EdgeEvent> events = reader.poll(128);
        Assertions.assertTrue(events.isEmpty());

        // Close should flush the remaining buffer internally without exception
        reader.close();
    }

    private static Map<String, Object> multilineConfigMap(
            Path logFile, String pattern, String match) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-multiline");
        map.put(FileCollectOptions.PATHS.key(), Collections.singletonList(logFile.toString()));
        map.put(FileCollectOptions.READ_FROM_BEGINNING.key(), true);
        map.put(FileCollectOptions.MULTILINE_PATTERN.key(), pattern);
        map.put(FileCollectOptions.MULTILINE_MATCH.key(), match);
        map.put(FileCollectOptions.GLOB_SCAN_INTERVAL_MS.key(), Long.MAX_VALUE);
        return map;
    }
}
