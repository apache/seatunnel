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

package org.apache.seatunnel.connectors.seatunnel.python.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/** Verifies the Python subprocess protocol, bounded completion, and cancellation boundaries. */
class PythonSourceTest {

    @TempDir Path tempDir;

    @AfterEach
    void clearPythonSourcePolicy() {
        System.clearProperty(PythonSourceExecutionPolicy.PYTHON_SOURCE_ENABLED_PROPERTY);
        System.clearProperty(PythonSourceExecutionPolicy.PYTHON_ALLOWED_EXECUTABLES_PROPERTY);
    }

    @Test
    void testSourceMetadataAndBoundedness() {
        PythonSource source =
                new PythonSource(ReadonlyConfig.fromMap(baseConfig("python3", "/tmp/fake.py")));

        Assertions.assertEquals(PythonSourceOptions.CONNECTOR_IDENTITY, source.getPluginName());
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
    }

    @Test
    void testProducedCatalogTables() {
        PythonSource source =
                new PythonSource(ReadonlyConfig.fromMap(baseConfig("python3", "/tmp/fake.py")));

        List<CatalogTable> catalogTables = source.getProducedCatalogTables();

        Assertions.assertEquals(1, catalogTables.size());
        Assertions.assertArrayEquals(
                new String[] {"id", "name"}, catalogTables.get(0).getTableSchema().getFieldNames());
    }

    @Test
    void testCreateReader() throws Exception {
        PythonSource source =
                new PythonSource(ReadonlyConfig.fromMap(baseConfig("python3", "/tmp/fake.py")));

        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        AbstractSingleSplitReader<?> reader = source.createReader(readerContext);

        Assertions.assertInstanceOf(PythonSourceReader.class, reader);
    }

    @Test
    void testReaderRejectsPythonExecutionWhenServerPolicyIsDisabled() throws Exception {
        Path scriptPath = copyResource("python/emit_rows.py");
        String javaExecutable = javaExecutablePath();
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(javaExecutable, scriptPath.toString()))));

        IllegalStateException exception =
                Assertions.assertThrows(IllegalStateException.class, reader::open);

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(PythonSourceExecutionPolicy.PYTHON_SOURCE_ENABLED_PROPERTY));
    }

    @Test
    void testReaderRejectsExecutableOutsideServerAllowlist() throws Exception {
        Path scriptPath = copyResource("python/emit_rows.py");
        String javaExecutable = javaExecutablePath();
        System.setProperty(PythonSourceExecutionPolicy.PYTHON_SOURCE_ENABLED_PROPERTY, "true");
        System.setProperty(
                PythonSourceExecutionPolicy.PYTHON_ALLOWED_EXECUTABLES_PROPERTY,
                tempDir.resolve("not-allowed-python").toAbsolutePath().toString());
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(javaExecutable, scriptPath.toString()))));

        IllegalStateException exception =
                Assertions.assertThrows(IllegalStateException.class, reader::open);

        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(PythonSourceExecutionPolicy.PYTHON_ALLOWED_EXECUTABLES_PROPERTY));
    }

    @Test
    void testReaderCollectsRowsFromPythonScript() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_rows.py");

        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        Map<String, Object> scriptConfig = new HashMap<>();
        scriptConfig.put("prefix", "seatunnel");
        scriptConfig.put("count", 2);
        config.put(PythonSourceOptions.PYTHON_SCRIPT_CONFIG.key(), scriptConfig);

        PythonSource source = new PythonSource(ReadonlyConfig.fromMap(config));
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader = createReader(source, readerContext);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            pollUntilRows(reader, collector, 2);
            pollUntilBoundedCompletion(reader, collector, readerContext);
        } finally {
            reader.close();
        }

        Assertions.assertEquals(2, collector.rows.size());
        Assertions.assertEquals(1, collector.rows.get(0).getField(0));
        Assertions.assertEquals("seatunnel_1", collector.rows.get(0).getField(1));
        Assertions.assertEquals(2, collector.rows.get(1).getField(0));
        Assertions.assertEquals("seatunnel_2", collector.rows.get(1).getField(1));
        Mockito.verify(readerContext).signalNoMoreElement();
    }

    /**
     * Ensures output larger than the bounded queue is fully drained after process exit.
     *
     * <p>The row count exceeds {@code STDOUT_QUEUE_CAPACITY} so completion must yield to later poll
     * calls instead of waiting for a blocked stdout pump.
     */
    @Test
    void testReaderDrainsBufferedRowsAfterProcessExit() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_rows.py");

        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        Map<String, Object> scriptConfig = new HashMap<>();
        scriptConfig.put("prefix", "buffered");
        scriptConfig.put("count", 300);
        config.put(PythonSourceOptions.PYTHON_SCRIPT_CONFIG.key(), scriptConfig);

        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader =
                createReader(new PythonSource(ReadonlyConfig.fromMap(config)), readerContext);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            waitUntilStdoutQueueIsFull(reader);
            reader.pollNext(collector);
            pollUntilRows(reader, collector, 300);
            pollUntilBoundedCompletion(reader, collector, readerContext);
        } finally {
            reader.close();
        }

        Assertions.assertEquals(300, collector.rows.size());
        for (int index = 0; index < collector.rows.size(); index++) {
            Assertions.assertEquals(index + 1, collector.rows.get(index).getField(0));
            Assertions.assertEquals(
                    "buffered_" + (index + 1), collector.rows.get(index).getField(1));
        }
        Mockito.verify(readerContext).signalNoMoreElement();
    }

    /**
     * Ensures a final stdout row without a newline is emitted before bounded completion.
     *
     * <p>This covers the line reader returning its trailing data only at EOF.
     */
    @Test
    void testReaderCollectsTrailingRowWithoutNewline() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_without_newline.py");

        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(pythonExecutable, scriptPath.toString()))),
                        readerContext);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            pollUntilRows(reader, collector, 1);
            pollUntilBoundedCompletion(reader, collector, readerContext);
        } finally {
            reader.close();
        }

        Assertions.assertEquals(1, collector.rows.size());
        Assertions.assertEquals("python_1", collector.rows.get(0).getField(1));
        Mockito.verify(readerContext).signalNoMoreElement();
    }

    @Test
    void testReaderFailsWhenPythonProcessExitsNonZero() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/exit_with_error.py");

        PythonSource source =
                new PythonSource(
                        ReadonlyConfig.fromMap(
                                baseConfig(pythonExecutable, scriptPath.toString())));
        PythonSourceReader reader = createReader(source);

        reader.open();
        try {
            IllegalStateException exception = pollUntilFailure(reader, new RecordingCollector());
            Assertions.assertTrue(exception.getMessage().contains("exited with code"));
            Assertions.assertTrue(exception.getMessage().contains("boom from python"));
        } finally {
            reader.close();
        }
    }

    @Test
    void testReaderFailsFastWhenScriptPathDoesNotExist() {
        PythonSource source =
                new PythonSource(
                        ReadonlyConfig.fromMap(
                                baseConfig("python3", tempDir.resolve("missing.py").toString())));
        PythonSourceReader reader = createReader(source);

        Assertions.assertThrows(IllegalArgumentException.class, reader::open);
    }

    @Test
    void testReaderReturnsBeforePythonProcessExit() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_then_sleep.py");

        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        Map<String, Object> scriptConfig = new HashMap<>();
        scriptConfig.put("prefix", "seatunnel");
        // Sleep comfortably longer than the poll budget below so a slow interpreter cold start
        // cannot make the process exit before the assertion window closes and mask a real block.
        scriptConfig.put("sleep_seconds", 10);
        config.put(PythonSourceOptions.PYTHON_SCRIPT_CONFIG.key(), scriptConfig);

        PythonSource source = new PythonSource(ReadonlyConfig.fromMap(config));
        PythonSourceReader reader = createReader(source);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
            while (collector.rows.isEmpty() && System.nanoTime() < deadlineNanos) {
                reader.pollNext(collector);
            }

            Assertions.assertFalse(
                    collector.rows.isEmpty(),
                    "pollNext should return rows before the Python process exits");
            Assertions.assertEquals(1, collector.rows.size());
            Assertions.assertEquals(1, collector.rows.get(0).getField(0));
            Assertions.assertEquals("seatunnel_1", collector.rows.get(0).getField(1));
        } finally {
            reader.close();
        }
    }

    @Test
    void testReaderCloseStopsLongRunningPythonProcess() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_then_wait_forever.py");

        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        PythonSource source = new PythonSource(ReadonlyConfig.fromMap(config));
        PythonSourceReader reader = createReader(source);
        RecordingCollector collector = new RecordingCollector();
        boolean closed = false;

        reader.open();
        try {
            pollUntilRows(reader, collector, 1);
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), reader::close);
            closed = true;
        } finally {
            if (!closed) {
                reader.close();
            }
        }
    }

    @Test
    void testReaderOpenTimesOutWhenPythonDoesNotReadLargeConfig() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/ignore_stdin_wait_forever.py");

        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        Map<String, Object> scriptConfig = new HashMap<>();
        scriptConfig.put("payload", new String(new char[2 * 1024 * 1024]).replace('\0', 'x'));
        config.put(PythonSourceOptions.PYTHON_SCRIPT_CONFIG.key(), scriptConfig);

        PythonSourceReader reader = createReader(new PythonSource(ReadonlyConfig.fromMap(config)));
        try {
            IOException exception =
                    Assertions.assertTimeoutPreemptively(
                            Duration.ofSeconds(10),
                            () -> Assertions.assertThrows(IOException.class, reader::open));
            Assertions.assertTrue(exception.getMessage().contains("Timed out"));
        } finally {
            reader.close();
        }
    }

    @Test
    void testReaderCannotOpenAfterCloseReturns() throws Exception {
        Path scriptPath = copyResource("python/emit_rows.py");
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(
                                                "executable-must-not-run",
                                                scriptPath.toString()))));

        reader.close();
        IOException exception = Assertions.assertThrows(IOException.class, reader::open);

        Assertions.assertTrue(exception.getMessage().contains("already been closed"));
    }

    @Test
    void testConcurrentCloseDoesNotFailOrEmitAfterBlockedPoll() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_then_wait_forever.py");

        PythonSource source =
                new PythonSource(
                        ReadonlyConfig.fromMap(
                                baseConfig(pythonExecutable, scriptPath.toString())));
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader = createReader(source, readerContext);
        RecordingCollector collector = new RecordingCollector();
        AtomicReference<Throwable> pollFailure = new AtomicReference<>();

        reader.open();
        pollUntilRows(reader, collector, 1);
        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                reader.pollNext(collector);
                            } catch (Throwable e) {
                                pollFailure.set(e);
                            }
                        },
                        "python-source-concurrent-poll-test");
        pollThread.start();
        Thread.sleep(50L);
        reader.close();
        pollThread.join(TimeUnit.SECONDS.toMillis(5));

        Assertions.assertFalse(pollThread.isAlive());
        Assertions.assertNull(pollFailure.get());
        Assertions.assertEquals(1, collector.rows.size());
        Mockito.verify(readerContext, Mockito.never()).signalNoMoreElement();
    }

    /**
     * Ensures cancellation finishes within the reader's bounded shutdown window while inherited
     * stdout is awaiting explicit failure.
     */
    @Test
    void testCloseInterruptsInheritedStdoutWait() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/spawn_stdout_child_then_exit.py");
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(pythonExecutable, scriptPath.toString()))),
                        readerContext);
        RecordingCollector collector = new RecordingCollector();
        AtomicBoolean stopPolling = new AtomicBoolean();
        AtomicReference<Throwable> pollFailure = new AtomicReference<>();

        reader.open();
        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                while (!stopPolling.get()) {
                                    reader.pollNext(collector);
                                }
                            } catch (Throwable e) {
                                pollFailure.set(e);
                            }
                        },
                        "python-source-inherited-stdout-close-test");
        pollThread.start();
        waitUntilStdoutCloseDeadlineIsInitialized(reader);
        try {
            Assertions.assertTimeoutPreemptively(Duration.ofSeconds(6), reader::close);
        } finally {
            stopPolling.set(true);
        }
        pollThread.join(TimeUnit.SECONDS.toMillis(2));

        Assertions.assertFalse(pollThread.isAlive());
        Assertions.assertNull(pollFailure.get());
        Mockito.verify(readerContext, Mockito.never()).signalNoMoreElement();
    }

    @Test
    void testConcurrentCloseStopsBufferedBatchEmission() throws Exception {
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(baseConfig("python", "/tmp/fake.py"))));
        enqueueStdoutLines(reader, "1,python_1", "2,python_2", "3,python_3");
        BlockingCollector collector = new BlockingCollector();
        AtomicReference<Throwable> pollFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        CountDownLatch closeStarted = new CountDownLatch(1);

        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                reader.pollNext(collector);
                            } catch (Throwable e) {
                                pollFailure.set(e);
                            }
                        },
                        "python-source-buffered-poll-test");
        pollThread.start();
        Assertions.assertTrue(collector.collectEntered.await(5, TimeUnit.SECONDS));

        Thread closeThread =
                new Thread(
                        () -> {
                            closeStarted.countDown();
                            try {
                                reader.close();
                            } catch (Throwable e) {
                                closeFailure.set(e);
                            }
                        },
                        "python-source-buffered-close-test");
        closeThread.start();
        Assertions.assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
        Thread.sleep(50L);
        collector.releaseCollect.countDown();

        pollThread.join(TimeUnit.SECONDS.toMillis(5));
        closeThread.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertFalse(pollThread.isAlive());
        Assertions.assertFalse(closeThread.isAlive());
        Assertions.assertNull(pollFailure.get());
        Assertions.assertNull(closeFailure.get());
        Assertions.assertEquals(1, collector.rows.size());
    }

    @Test
    void testCloseWaitsForBoundedCompletionSignal() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_rows.py");
        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        Map<String, Object> scriptConfig = new HashMap<>();
        scriptConfig.put("count", 0);
        config.put(PythonSourceOptions.PYTHON_SCRIPT_CONFIG.key(), scriptConfig);

        CountDownLatch signalEntered = new CountDownLatch(1);
        CountDownLatch releaseSignal = new CountDownLatch(1);
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        Mockito.doAnswer(
                        invocation -> {
                            signalEntered.countDown();
                            if (!releaseSignal.await(5, TimeUnit.SECONDS)) {
                                throw new IllegalStateException(
                                        "Timed out waiting to release signal");
                            }
                            return null;
                        })
                .when(readerContext)
                .signalNoMoreElement();
        PythonSourceReader reader =
                createReader(new PythonSource(ReadonlyConfig.fromMap(config)), readerContext);
        AtomicReference<Throwable> pollFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();

        reader.open();
        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                while (signalEntered.getCount() > 0) {
                                    reader.pollNext(new RecordingCollector());
                                }
                            } catch (Throwable e) {
                                pollFailure.set(e);
                            }
                        },
                        "python-source-completion-poll-test");
        pollThread.start();
        Assertions.assertTrue(signalEntered.await(5, TimeUnit.SECONDS));

        Thread closeThread =
                new Thread(
                        () -> {
                            try {
                                reader.close();
                            } catch (Throwable e) {
                                closeFailure.set(e);
                            }
                        },
                        "python-source-completion-close-test");
        closeThread.start();
        Thread.sleep(100L);
        Assertions.assertTrue(
                closeThread.isAlive(), "close must wait for the in-flight completion signal");
        releaseSignal.countDown();

        pollThread.join(TimeUnit.SECONDS.toMillis(5));
        closeThread.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertFalse(pollThread.isAlive());
        Assertions.assertFalse(closeThread.isAlive());
        Assertions.assertNull(pollFailure.get());
        Assertions.assertNull(closeFailure.get());
        Mockito.verify(readerContext).signalNoMoreElement();
    }

    @Test
    void testNormalExitIgnoresIntentionalStderrPipeShutdown() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/spawn_stderr_child_then_exit.py");
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(pythonExecutable, scriptPath.toString()))),
                        readerContext);

        reader.open();
        try {
            pollUntilBoundedCompletion(reader, new RecordingCollector(), readerContext);
        } finally {
            reader.close();
        }

        Mockito.verify(readerContext).signalNoMoreElement();
    }

    /** Ensures a child inheriting stdout fails explicitly instead of hanging or succeeding. */
    @Test
    void testReaderFailsWhenChildKeepsStdoutOpen() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/spawn_stdout_child_then_exit.py");
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(pythonExecutable, scriptPath.toString()))),
                        readerContext);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            IOException exception =
                    Assertions.assertTimeoutPreemptively(
                            Duration.ofSeconds(25), () -> pollUntilIOException(reader, collector));
            Assertions.assertTrue(exception.getMessage().contains("child processes"));
        } finally {
            reader.close();
        }

        Mockito.verify(readerContext, Mockito.never()).signalNoMoreElement();
    }

    /**
     * A full stdout queue during the post-exit grace period must not renew the drain deadline.
     *
     * <p>This is the scenario a chatty inherited-stdout descendant creates: {@code
     * stdoutLines.remainingCapacity() == 0} when {@link PythonSourceReader#pollNext} starts. It
     * cannot be exercised with a real grandchild process that writes fast enough to saturate the
     * queue: verified independently of SeaTunnel (bare {@code java.lang.ProcessBuilder}, no
     * connector code involved) that on this JDK a grandchild's write to stdout it inherited from an
     * already-exited direct child reliably fails with {@code BrokenPipeError}/{@code SIGPIPE}
     * roughly 100-150ms after the direct child exits — not enough time to reliably fill a 256-entry
     * queue on a loaded CI runner. That failure window is a JDK/OS pipe-lifetime characteristic,
     * not something either the reader or a test script can control, and racing every write inside
     * it would trade this flake for a narrower and less reproducible one. Filling the reader's
     * internal queue directly to capacity exercises the exact branch deterministically and without
     * that dependency. Eventual rejection of a child that never releases stdout is already covered
     * by {@link #testReaderFailsWhenChildKeepsStdoutOpen()} using the same fixture script, so this
     * test only needs to isolate the deadline-renewal defect itself.
     */
    @Test
    void testFullStdoutQueueDuringGracePeriodCannotRenewDeadline() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/spawn_stdout_child_then_exit.py");
        SingleSplitReaderContext readerContext = Mockito.mock(SingleSplitReaderContext.class);
        PythonSourceReader reader =
                createReader(
                        new PythonSource(
                                ReadonlyConfig.fromMap(
                                        baseConfig(pythonExecutable, scriptPath.toString()))),
                        readerContext);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            // Drive polling directly rather than via waitUntilStdoutCloseDeadlineIsInitialized,
            // which assumes a separate thread is already calling pollNext concurrently.
            long armDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (getStdoutCloseDeadline(reader) == 0L && System.nanoTime() < armDeadlineNanos) {
                reader.pollNext(collector);
            }
            long deadlineAfterArm = getStdoutCloseDeadline(reader);
            Assertions.assertNotEquals(
                    0L, deadlineAfterArm, "poll did not enter inherited stdout wait");

            fillStdoutQueueToCapacity(reader);
            reader.pollNext(collector);
            Assertions.assertFalse(
                    collector.rows.isEmpty(), "buffered filler rows should have been emitted");
            Assertions.assertEquals(
                    deadlineAfterArm,
                    getStdoutCloseDeadline(reader),
                    "a full stdout queue during the grace period must not renew the deadline");
        } finally {
            reader.close();
        }

        Mockito.verify(readerContext, Mockito.never()).signalNoMoreElement();
    }

    private PythonSourceReader createReader(PythonSource source) {
        return createReader(source, Mockito.mock(SingleSplitReaderContext.class));
    }

    private PythonSourceReader createReader(
            PythonSource source, SingleSplitReaderContext readerContext) {
        return (PythonSourceReader) source.createReader(readerContext);
    }

    private Map<String, Object> baseConfig(String pythonExecutable, String scriptPath) {
        Map<String, Object> config = new HashMap<>();
        config.put(PythonSourceOptions.PYTHON_EXECUTABLE.key(), pythonExecutable);
        config.put(PythonSourceOptions.PYTHON_SCRIPT_PATH.key(), scriptPath);
        config.put(PythonSourceOptions.FIELD_DELIMITER.key(), ",");
        config.put("schema", schemaConfig());
        return config;
    }

    private Map<String, Object> schemaConfig() {
        Map<String, Object> schema = new HashMap<>();
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "int");
        fields.put("name", "string");
        schema.put("fields", fields);
        return schema;
    }

    private String requirePythonExecutable() {
        String executable = findPythonExecutable();
        Assumptions.assumeTrue(
                executable != null, "python interpreter not available in test environment");
        System.setProperty(PythonSourceExecutionPolicy.PYTHON_SOURCE_ENABLED_PROPERTY, "true");
        System.setProperty(
                PythonSourceExecutionPolicy.PYTHON_ALLOWED_EXECUTABLES_PROPERTY, executable);
        return executable;
    }

    private String findPythonExecutable() {
        String pathValue = System.getenv("PATH");
        if (pathValue == null || pathValue.trim().isEmpty()) {
            return null;
        }
        String[] commandNames =
                System.getProperty("os.name", "").toLowerCase().contains("windows")
                        ? new String[] {"python3.exe", "python.exe", "python3", "python"}
                        : new String[] {"python3", "python"};
        for (String commandName : commandNames) {
            for (String directory : pathValue.split(Pattern.quote(File.pathSeparator))) {
                Path candidate = Paths.get(directory, commandName);
                if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) {
                    return candidate.toAbsolutePath().normalize().toString();
                }
            }
        }
        return null;
    }

    private String javaExecutablePath() {
        Path javaExecutable = Paths.get(System.getProperty("java.home"), "bin", "java");
        if (!Files.isRegularFile(javaExecutable)) {
            javaExecutable = Paths.get(System.getProperty("java.home"), "bin", "java.exe");
        }
        return javaExecutable.toAbsolutePath().normalize().toString();
    }

    private Path copyResource(String resourceName) throws IOException {
        Path resourcePath = tempDir.resolve(Paths.get(resourceName).getFileName().toString());
        try (InputStream inputStream =
                PythonSourceTest.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }
            Files.copy(inputStream, resourcePath, StandardCopyOption.REPLACE_EXISTING);
        }
        return resourcePath;
    }

    private void pollUntilRows(
            PythonSourceReader reader, RecordingCollector collector, int expectedRows)
            throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (collector.rows.size() < expectedRows && System.nanoTime() < deadlineNanos) {
            reader.pollNext(collector);
        }

        Assertions.assertEquals(expectedRows, collector.rows.size());
    }

    private IllegalStateException pollUntilFailure(
            PythonSourceReader reader, RecordingCollector collector) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            try {
                reader.pollNext(collector);
            } catch (IllegalStateException e) {
                return e;
            }
        }

        Assertions.fail("Expected python source reader to fail");
        return null;
    }

    /**
     * Polls until inherited stdout is reported as a bounded-source protocol violation.
     *
     * <p>The reader can only arm its inherited-stdout grace period once the Python parent has
     * started, spawned its child and exited. That startup cost is unbounded on a loaded CI runner,
     * so it is drained in a separate phase first. The failure budget then measures only the grace
     * period itself, which keeps the assertion strict while removing interpreter startup jitter
     * from the measurement.
     */
    private IOException pollUntilIOException(
            PythonSourceReader reader, RecordingCollector collector) throws Exception {
        long startupDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (getStdoutCloseDeadline(reader) == 0L && System.nanoTime() < startupDeadlineNanos) {
            try {
                reader.pollNext(collector);
            } catch (IOException e) {
                return e;
            }
        }
        Assertions.assertNotEquals(
                0L, getStdoutCloseDeadline(reader), "poll did not enter inherited stdout wait");

        // The grace period is PROCESS_DESTROY_TIMEOUT_SECONDS (5s) from the moment it is armed.
        // 7s leaves room for poll granularity while staying far below the 20s of continuous child
        // output, so a deadline that is wrongly renewed by that output still fails this helper.
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(7);
        while (System.nanoTime() < deadlineNanos) {
            try {
                reader.pollNext(collector);
            } catch (IOException e) {
                return e;
            }
        }

        Assertions.fail("Expected python source reader to reject inherited stdout");
        return null;
    }

    /**
     * Fills the reader's internal stdout queue to capacity, bypassing the process, so the next poll
     * deterministically observes {@code stdoutLines.remainingCapacity() == 0} without depending on
     * a real process's output rate.
     */
    @SuppressWarnings("unchecked")
    private void fillStdoutQueueToCapacity(PythonSourceReader reader) throws Exception {
        Field stdoutLinesField = PythonSourceReader.class.getDeclaredField("stdoutLines");
        stdoutLinesField.setAccessible(true);
        BlockingQueue<String> stdoutQueue = (BlockingQueue<String>) stdoutLinesField.get(reader);
        while (stdoutQueue.offer("999,filler")) {
            // Keep offering synthetic rows until the bounded queue reports no remaining capacity.
        }
        Assertions.assertEquals(0, stdoutQueue.remainingCapacity(), "stdout queue did not fill");
    }

    /**
     * Seeds rows directly into the reader queue so concurrent-close coverage is independent of
     * Python interpreter startup time on CI runners.
     */
    @SuppressWarnings("unchecked")
    private void enqueueStdoutLines(PythonSourceReader reader, String... lines) throws Exception {
        Field stdoutLinesField = PythonSourceReader.class.getDeclaredField("stdoutLines");
        stdoutLinesField.setAccessible(true);
        BlockingQueue<String> stdoutQueue = (BlockingQueue<String>) stdoutLinesField.get(reader);
        for (String line : lines) {
            Assertions.assertTrue(stdoutQueue.offer(line), "stdout queue unexpectedly filled");
        }
    }

    /** Waits until the producer is deterministically backpressured by the bounded stdout queue. */
    private void waitUntilStdoutQueueIsFull(PythonSourceReader reader) throws Exception {
        Field stdoutLinesField = PythonSourceReader.class.getDeclaredField("stdoutLines");
        stdoutLinesField.setAccessible(true);
        BlockingQueue<?> stdoutQueue = (BlockingQueue<?>) stdoutLinesField.get(reader);
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (stdoutQueue.remainingCapacity() > 0 && System.nanoTime() < deadlineNanos) {
            Thread.sleep(10L);
        }
        Assertions.assertEquals(0, stdoutQueue.remainingCapacity(), "stdout queue did not fill");
    }

    /**
     * Waits until polling has entered the inherited-stdout grace period.
     *
     * <p>Arming requires a full real-process cycle (interpreter cold start, child spawn, exit, exit
     * detection), so this uses the same 15s startup budget as {@code pollUntilIOException}; 5s has
     * been observed losing that race on loaded windows-latest runners (fork run 32041891318). The
     * wait returns as soon as the deadline is armed, so the budget only costs time on runners that
     * actually need it.
     */
    private void waitUntilStdoutCloseDeadlineIsInitialized(PythonSourceReader reader)
            throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (getStdoutCloseDeadline(reader) == 0L && System.nanoTime() < deadlineNanos) {
            Thread.sleep(10L);
        }
        Assertions.assertNotEquals(
                0L, getStdoutCloseDeadline(reader), "poll did not enter inherited stdout wait");
    }

    private long getStdoutCloseDeadline(PythonSourceReader reader) throws Exception {
        Field deadlineField = PythonSourceReader.class.getDeclaredField("stdoutCloseDeadlineNanos");
        deadlineField.setAccessible(true);
        return deadlineField.getLong(reader);
    }

    private void pollUntilBoundedCompletion(
            PythonSourceReader reader,
            RecordingCollector collector,
            SingleSplitReaderContext readerContext)
            throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            try {
                Mockito.verify(readerContext).signalNoMoreElement();
                return;
            } catch (AssertionError ignored) {
                reader.pollNext(collector);
            }
        }

        Mockito.verify(readerContext).signalNoMoreElement();
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    /** Holds the first collect call so the test can race engine close with buffered records. */
    private static class BlockingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final CountDownLatch collectEntered = new CountDownLatch(1);
        private final CountDownLatch releaseCollect = new CountDownLatch(1);

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
            collectEntered.countDown();
            try {
                releaseCollect.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while holding test collector", e);
            }
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
