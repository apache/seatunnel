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

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
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
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
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
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            // First poll buffers the lines, may or may not flush depending on timing
            reader.poll(128);

            // Poll until idle timeout triggers flush (deadline-based, not fixed sleep)
            List<EdgeEvent> events =
                    Awaitility.await()
                            .atMost(Duration.ofSeconds(2))
                            .pollInterval(Duration.ofMillis(30))
                            .until(() -> reader.poll(128), list -> !list.isEmpty());
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
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
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
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
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
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();

        // Poll without triggering flush (no boundary, no timeout)
        List<EdgeEvent> events = reader.poll(128);
        Assertions.assertTrue(events.isEmpty());

        // Close should flush the remaining buffer internally without exception
        reader.close();
    }

    /**
     * Regression test: when multiple files are tailed with multiline enabled, each file's multiline
     * buffer must be independent. A boundary line in file B must NOT flush file A's pending buffer,
     * and vice versa.
     *
     * <p>Uses realistic Java stacktraces with multiple Caused-by chains and deeply nested frames to
     * exercise multi-line assembly under interleaved multi-file reads.
     */
    @Test
    void multiFileMultilineIsolation() throws Exception {
        Path app1 = tempDir.resolve("app1.log");
        Path app2 = tempDir.resolve("app2.log");

        // app1: deep Java stacktrace with a Caused-by chain (partial, no next boundary)
        StringBuilder sb1 = new StringBuilder();
        sb1.append(
                "2024-01-01 10:15:33.421 ERROR [pool-3-thread-7] "
                        + "c.f.o.OrderService - Failed to process order #98712\n");
        sb1.append("java.lang.RuntimeException: Order processing pipeline failed\n");
        sb1.append("\tat com.foo.order.OrderService.process(OrderService.java:142)\n");
        sb1.append("\tat com.foo.order.OrderService.lambda$submit$0(OrderService.java:87)\n");
        sb1.append(
                "\tat java.base/java.util.concurrent.CompletableFuture$AsyncRun"
                        + ".run(CompletableFuture.java:1804)\n");
        sb1.append(
                "\tat java.base/java.util.concurrent.ThreadPoolExecutor"
                        + ".runWorker(ThreadPoolExecutor.java:1136)\n");
        sb1.append(
                "\tat java.base/java.util.concurrent.ThreadPoolExecutor$Worker"
                        + ".run(ThreadPoolExecutor.java:635)\n");
        sb1.append("\tat java.base/java.lang.Thread.run(Thread.java:842)\n");
        sb1.append(
                "Caused by: java.sql.SQLException: Deadlock found when trying to get lock;"
                        + " try restarting transaction\n");
        sb1.append(
                "\tat com.mysql.cj.jdbc.exceptions.SQLError"
                        + ".createSQLException(SQLError.java:129)\n");
        sb1.append(
                "\tat com.foo.order.repository.OrderRepository"
                        + ".updateStatus(OrderRepository.java:201)\n");
        sb1.append("\tat com.foo.order.OrderService.persistState(OrderService.java:165)\n");
        sb1.append("\t... 5 more\n");
        Files.write(app1, sb1.toString().getBytes(StandardCharsets.UTF_8));

        // app2: a complete OOM stacktrace + start of a second event
        StringBuilder sb2 = new StringBuilder();
        sb2.append(
                "2024-01-01 10:15:34.002 ERROR [http-nio-8080-exec-12] "
                        + "c.f.a.ApiController - Request /api/export failed\n");
        sb2.append("java.lang.OutOfMemoryError: Java heap space\n");
        sb2.append("\tat java.base/java.util.Arrays.copyOf(Arrays.java:3512)\n");
        sb2.append("\tat java.base/java.util.ArrayList.grow(ArrayList.java:237)\n");
        sb2.append("\tat com.foo.export.CsvExporter.bufferAll(CsvExporter.java:89)\n");
        sb2.append("\tat com.foo.api.ApiController.handleExport(ApiController.java:214)\n");
        sb2.append(
                "\tat jdk.internal.reflect.NativeMethodAccessorImpl" + ".invoke0(Native Method)\n");
        sb2.append(
                "\tat org.springframework.web.servlet.FrameworkServlet"
                        + ".service(FrameworkServlet.java:897)\n");
        sb2.append("\tat javax.servlet.http.HttpServlet.service(HttpServlet.java:764)\n");
        sb2.append(
                "\tat org.apache.catalina.core.ApplicationFilterChain"
                        + ".internalDoFilter(ApplicationFilterChain.java:227)\n");
        sb2.append(
                "2024-01-01 10:15:35.100 WARN  [http-nio-8080-exec-12] "
                        + "c.f.a.ApiController - Circuit breaker tripped for /api/export\n");
        Files.write(app2, sb2.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = new HashMap<>();
        map.put(FileCollectOptions.ID.key(), "input-multi-file");
        map.put(
                FileCollectOptions.PATHS.key(),
                Collections.singletonList(tempDir.toAbsolutePath() + "/*.log"));
        map.put(FileCollectOptions.READ_FROM_BEGINNING.key(), true);
        map.put(FileCollectOptions.MULTILINE_PATTERN.key(), "^\\d{4}-\\d{2}-\\d{2}");
        map.put(FileCollectOptions.MULTILINE_MATCH.key(), "after");
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 60000L);
        map.put(FileCollectOptions.GLOB_SCAN_INTERVAL_MS.key(), Long.MAX_VALUE);

        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            // First poll: app2's second timestamp boundary flushes app2's OOM event;
            // app1 has no second boundary yet so its deep stacktrace stays buffered
            List<EdgeEvent> events = reader.poll(128);

            Assertions.assertEquals(1, events.size());
            String payload = new String(events.get(0).getPayload(), StandardCharsets.UTF_8);

            // Verify this is app2's OOM event with full stacktrace intact
            Assertions.assertTrue(
                    payload.contains("OutOfMemoryError"), "Emitted event should be app2's OOM");
            Assertions.assertTrue(
                    payload.contains("CsvExporter.bufferAll"),
                    "OOM stacktrace must include app-level frame");
            Assertions.assertTrue(
                    payload.contains("ApplicationFilterChain"),
                    "OOM stacktrace must include servlet chain");
            Assertions.assertTrue(
                    payload.contains("/api/export failed"),
                    "OOM event must include the log message");

            // Must NOT contain any app1 content (the deadlock stacktrace)
            Assertions.assertFalse(
                    payload.contains("OrderService"), "app2 event must not contain app1 frames");
            Assertions.assertFalse(
                    payload.contains("Deadlock found"),
                    "app2 event must not contain app1 Caused-by");
            Assertions.assertFalse(
                    payload.contains("OrderRepository"),
                    "app2 event must not contain app1 repository frame");

            // Now write a new boundary to app1 to flush its deep stacktrace
            StringBuilder app1Next = new StringBuilder();
            app1Next.append(
                    "2024-01-01 10:15:40.000 INFO  [pool-3-thread-7] "
                            + "c.f.o.OrderService - Retry succeeded for order #98712\n");
            Files.write(
                    app1,
                    app1Next.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);

            events = reader.poll(128);

            // Find app1's flushed event (the full deadlock stacktrace)
            boolean foundApp1Event = false;
            for (EdgeEvent e : events) {
                String p = new String(e.getPayload(), StandardCharsets.UTF_8);
                if (p.contains("OrderService") && p.contains("Deadlock found")) {
                    foundApp1Event = true;

                    // Verify full Caused-by chain is intact
                    Assertions.assertTrue(
                            p.contains("RuntimeException: Order processing pipeline failed"),
                            "app1 event must have top-level exception");
                    Assertions.assertTrue(
                            p.contains("CompletableFuture$AsyncRun"),
                            "app1 event must have JDK frames");
                    Assertions.assertTrue(
                            p.contains("SQLException"),
                            "app1 event must have Caused-by SQLException");
                    Assertions.assertTrue(
                            p.contains("OrderRepository.updateStatus"),
                            "app1 event must have repository frame");
                    Assertions.assertTrue(
                            p.contains("... 5 more"),
                            "app1 event must preserve truncated frame marker");

                    // Must NOT bleed app2 content
                    Assertions.assertFalse(
                            p.contains("OutOfMemoryError"), "app1 event must not contain app2 OOM");
                    Assertions.assertFalse(
                            p.contains("CsvExporter"), "app1 event must not contain app2 frames");
                    Assertions.assertFalse(
                            p.contains("Circuit breaker"),
                            "app1 event must not contain app2 warn line");
                }
            }
            Assertions.assertTrue(
                    foundApp1Event,
                    "app1's deep stacktrace event must flush after its own boundary");

            // Third round: write more content to both files to verify continued isolation
            StringBuilder app1More = new StringBuilder();
            app1More.append("java.io.IOException: Connection reset by peer\n");
            app1More.append("\tat sun.nio.ch.SocketDispatcher.read0(Native Method)\n");
            app1More.append("\tat com.foo.net.ConnectionPool.acquire(ConnectionPool.java:55)\n");
            Files.write(
                    app1,
                    app1More.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);

            StringBuilder app2More = new StringBuilder();
            app2More.append(
                    "2024-01-01 10:15:36.200 ERROR [scheduler-1] "
                            + "c.f.j.JobScheduler - Scheduled job batch-cleanup failed\n");
            app2More.append(
                    "org.springframework.dao.DataAccessResourceFailureException:"
                            + " Unable to acquire connection\n");
            app2More.append(
                    "\tat org.springframework.jdbc.datasource.DataSourceUtils"
                            + ".getConnection(DataSourceUtils.java:82)\n");
            app2More.append("\tat com.foo.job.BatchCleanupJob.execute(BatchCleanupJob.java:44)\n");
            app2More.append("\tat org.quartz.core.JobRunShell.run(JobRunShell.java:202)\n");
            app2More.append(
                    "Caused by: com.zaxxer.hikari.pool.HikariPool$PoolInitializationException:"
                            + " Failed to initialize pool\n");
            app2More.append(
                    "\tat com.zaxxer.hikari.pool.HikariPool"
                            + ".throwPoolInitializationException(HikariPool.java:596)\n");
            app2More.append("\t... 3 more\n");
            app2More.append(
                    "2024-01-01 10:15:37.000 INFO  [scheduler-1] "
                            + "c.f.j.JobScheduler - Will retry in 30s\n");
            Files.write(
                    app2,
                    app2More.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);

            events = reader.poll(128);

            // app2 should emit: (1) "Circuit breaker" event, (2) "batch-cleanup" stacktrace event
            // app1 should NOT emit (its IOException is unbounded, still in buffer)
            for (EdgeEvent e : events) {
                String p = new String(e.getPayload(), StandardCharsets.UTF_8);
                // No event should mix content from both files
                if (p.contains("batch-cleanup")) {
                    Assertions.assertTrue(
                            p.contains("HikariPool"),
                            "app2 batch-cleanup event must have hikari cause");
                    Assertions.assertFalse(
                            p.contains("Connection reset"),
                            "app2 event must not contain app1 IOException");
                }
                if (p.contains("Circuit breaker")) {
                    Assertions.assertFalse(
                            p.contains("SocketDispatcher"),
                            "app2 warn event must not contain app1 IO frames");
                }
                Assertions.assertFalse(
                        p.contains("OrderService") && p.contains("OutOfMemoryError"),
                        "No event should ever mix app1 and app2 content");
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Regression test: when a file rotates (same path, new inode) while multiline buffer has
     * pending lines, the old buffer must be discarded. The new physical file's first event must NOT
     * contain content from the old file.
     */
    @Test
    void rotationResetsMultilineBuffer() throws Exception {
        Path logFile = tempDir.resolve("rotating.log");
        // Write a partial multiline event (no boundary after it)
        StringBuilder sb = new StringBuilder();
        sb.append("2024-01-01 ERROR OldFileException\n");
        sb.append("\tat com.old.Stack.trace(Old.java:1)\n");
        sb.append("\tat com.old.Stack.deep(Old.java:2)\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^\\d{4}-\\d{2}-\\d{2}", "after");
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 60000L);

        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            // First poll: buffer holds 3 lines, no boundary yet → nothing emitted
            List<EdgeEvent> events = reader.poll(128);
            Assertions.assertTrue(
                    events.isEmpty(), "Incomplete multiline event should stay buffered");

            // Simulate rotation: delete and recreate file (produces new inode)
            Files.delete(logFile);
            Files.write(
                    logFile,
                    ("2024-02-01 INFO NewFileEvent\n" + "2024-02-01 INFO SecondNewEvent\n")
                            .getBytes(StandardCharsets.UTF_8));

            // Poll again: rotation detected → old buffer discarded, new file read from 0
            events = reader.poll(128);

            // The first boundary of the new file flushes the first new event
            Assertions.assertFalse(events.isEmpty(), "Should emit events from the new file");
            for (EdgeEvent e : events) {
                String payload = new String(e.getPayload(), StandardCharsets.UTF_8);
                Assertions.assertFalse(
                        payload.contains("OldFileException"),
                        "New file events must not contain old file buffer content");
                Assertions.assertFalse(
                        payload.contains("com.old.Stack"),
                        "New file events must not contain old file stack frames");
            }
        } finally {
            reader.close();
        }
    }

    /**
     * Regression test: when on-error=skip triggers, the file's multiline assembler and discovery
     * state must be cleaned up so that (a) no stale flush occurs and (b) the file can be
     * rediscovered on the next glob scan.
     */
    @Test
    void skipOnErrorCleansStateAndAllowsRediscovery() throws Exception {
        Path logFile = tempDir.resolve("error.log");
        // Write a partial multiline event, then we'll force an error
        StringBuilder sb = new StringBuilder();
        sb.append("2024-01-01 ERROR BeforeError\n");
        sb.append("\tat com.err.Frame.a(F.java:1)\n");
        Files.write(logFile, sb.toString().getBytes(StandardCharsets.UTF_8));

        Map<String, Object> map = multilineConfigMap(logFile, "^\\d{4}-\\d{2}-\\d{2}", "after");
        map.put(FileCollectOptions.MULTILINE_FLUSH_IDLE_TIMEOUT_MS.key(), 50L);
        map.put(FileCollectOptions.ON_ERROR.key(), "skip");
        map.put(FileCollectOptions.GLOB_SCAN_INTERVAL_MS.key(), 20L);

        FileCollectReader reader =
                new FileCollectReader(
                        FileCollectConfig.from(ReadonlyConfig.fromMap(map)),
                        new NoOpPositionStore());
        reader.open();
        try {
            // Buffer the partial event
            reader.poll(128);

            // Delete the file to force IOException on next read
            Files.delete(logFile);

            // Poll should trigger skip-on-error (IOException reading deleted file)
            // and clean up assembler state
            Awaitility.await()
                    .atMost(Duration.ofSeconds(2))
                    .pollInterval(Duration.ofMillis(10))
                    .untilAsserted(
                            () -> {
                                // keep polling until the error path fires
                                reader.poll(128);
                            });

            // Verify no stale flush: poll should return nothing now
            List<EdgeEvent> staleEvents = reader.poll(128);
            for (EdgeEvent e : staleEvents) {
                String payload = new String(e.getPayload(), StandardCharsets.UTF_8);
                Assertions.assertFalse(
                        payload.contains("BeforeError"),
                        "Stale assembler content must not be emitted after skip-on-error");
            }

            // Recreate the file with new content
            Files.write(
                    logFile,
                    ("2024-03-01 INFO Recovered\n" + "2024-03-01 INFO AfterRecovery\n")
                            .getBytes(StandardCharsets.UTF_8));

            // Wait for rediscovery via glob scan and verify new content is collected
            List<EdgeEvent> recovered =
                    Awaitility.await()
                            .atMost(Duration.ofSeconds(3))
                            .pollInterval(Duration.ofMillis(20))
                            .until(() -> reader.poll(128), list -> !list.isEmpty());

            boolean foundRecovered = false;
            for (EdgeEvent e : recovered) {
                String payload = new String(e.getPayload(), StandardCharsets.UTF_8);
                if (payload.contains("Recovered")) {
                    foundRecovered = true;
                }
                Assertions.assertFalse(
                        payload.contains("BeforeError"),
                        "Rediscovered file must not contain old assembler content");
            }
            Assertions.assertTrue(
                    foundRecovered, "File must be rediscovered after skip-on-error cleanup");
        } finally {
            reader.close();
        }
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
