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

package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Covers the main runtime contracts of the Python transform implementation. */
class PythonTransformTest {

    /** Opened transforms that need explicit teardown because they own subprocesses. */
    private final List<PythonTransform> openedTransforms = new ArrayList<>();

    /** Opened wrappers that may hold subprocess-backed inner transforms. */
    private final List<PythonMultiCatalogTransform> openedWrappers = new ArrayList<>();

    /** Temporary directory used for path-based script tests. */
    @TempDir Path tempDir;

    /** Absolute python path allowlisted for runtime-backed test cases. */
    private String availablePythonExecutable;

    /** Closes subprocess-backed transforms created by the test cases. */
    @AfterEach
    void tearDown() {
        openedWrappers.forEach(PythonMultiCatalogTransform::close);
        openedWrappers.clear();
        openedTransforms.forEach(PythonTransform::close);
        openedTransforms.clear();
        System.clearProperty(PythonProcessWorker.PYTHON_TRANSFORM_ENABLED_PROPERTY);
        System.clearProperty(PythonProcessWorker.PYTHON_ALLOWED_EXECUTABLES_PROPERTY);
    }

    /** Verifies inline source execution, schema expansion, and script_config propagation. */
    @Test
    void testInlineSourceCodeTransform() {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n"
                        + "    return {\n"
                        + "        'normalized_name': context['config']['prefix'] + row['name'].strip().lower(),\n"
                        + "        'age_plus_one': row['age'] + 1,\n"
                        + "    }\n");
        config.put(PythonTransformConfig.SCRIPT_CONFIG.key(), createScriptConfig());

        PythonTransform transform = createOpenedTransform(config);
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {1, " Alice ", 20}));

        Assertions.assertNotNull(outputRow);
        Assertions.assertEquals("user:alice", outputRow.getField(3));
        Assertions.assertEquals(21, outputRow.getField(4));

        TableSchema outputSchema = transform.getProducedCatalogTable().getTableSchema();
        Assertions.assertArrayEquals(
                new String[] {"id", "name", "age", "normalized_name", "age_plus_one"},
                outputSchema.getFieldNames());
    }

    /** Verifies path-based scripts can be loaded from the runtime filesystem. */
    @Test
    void testSourceCodePathTransform() throws IOException {
        assumePythonAvailable();

        Path scriptPath = tempDir.resolve("python_transform_path.py");
        Files.write(
                scriptPath,
                ("def process(row, context):\n"
                                + "    return [row['name'].upper(), row['age'] * 2]\n")
                        .getBytes(StandardCharsets.UTF_8));

        Map<String, Object> config = baseConfig();
        config.put(PythonTransformConfig.SOURCE_CODE_PATH.key(), scriptPath.toString());

        PythonTransform transform = createOpenedTransform(config);
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20}));

        Assertions.assertNotNull(outputRow);
        Assertions.assertEquals("ALICE", outputRow.getField(3));
        Assertions.assertEquals(40, outputRow.getField(4));
    }

    /** Verifies row_error_handle_way = SKIP drops the whole row after a Python execution error. */
    @Test
    void testSkipRowOnPythonExecutionError() {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n" + "    raise ValueError('boom')\n");
        config.put(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(), "SKIP");

        PythonTransform transform = createOpenedTransform(config);
        SeaTunnelRow outputRow = transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20}));

        Assertions.assertNull(outputRow);
    }

    /** Verifies closing the transform also terminates the background stderr collector thread. */
    @Test
    void testCloseStopsStderrCollectorThread() throws Exception {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n"
                        + "    print('transform-started')\n"
                        + "    return [row['name'], row['age'] + 1]\n");

        PythonTransform transform = createOpenedTransform(config);
        transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20}));

        Object processWorker = readFieldValue(transform, "processWorker");
        Thread stderrCollectorThread =
                (Thread) readFieldValue(processWorker, "stderrCollectorThread");
        Assertions.assertNotNull(stderrCollectorThread);

        transform.close();

        long waitDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (stderrCollectorThread.isAlive() && System.nanoTime() < waitDeadlineNanos) {
            stderrCollectorThread.join(100L);
        }

        Assertions.assertFalse(
                stderrCollectorThread.isAlive(),
                "stderr collector thread should stop after the transform is closed");
    }

    /** Verifies normal shutdown gives the Python script a chance to run its close hook. */
    @Test
    void testCloseInvokesPythonCloseHook() throws Exception {
        assumePythonAvailable();

        Path markerPath = tempDir.resolve("python-close-hook-called");
        String escapedMarkerPath = markerPath.toString().replace("\\", "\\\\").replace("'", "\\'");
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import pathlib\n"
                        + "def process(row, context):\n"
                        + "    return [row['name'], row['age']]\n"
                        + "def close():\n"
                        + "    pathlib.Path('"
                        + escapedMarkerPath
                        + "').touch()\n");

        PythonTransform transform = createOpenedTransform(config);
        transform.close();

        Assertions.assertTrue(Files.exists(markerPath), "Python close hook should run before exit");
    }

    /** Verifies close hook failures are surfaced from transform cleanup for runtime logging. */
    @Test
    void testClosePropagatesPythonCloseHookFailure() {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n"
                        + "    return [row['name'], row['age']]\n"
                        + "def close():\n"
                        + "    raise RuntimeError('close-boom')\n");

        PythonTransform transform = createOpenedTransform(config);
        TransformException exception =
                Assertions.assertThrows(TransformException.class, transform::close);

        Assertions.assertTrue(exception.getMessage().contains("close hook failed"));
        Assertions.assertTrue(exception.getMessage().contains("close-boom"));
        openedTransforms.remove(transform);
    }

    /** Verifies concurrent close callers wait for and replay one shared teardown failure. */
    @Test
    void testConcurrentCloseCallersShareCompletionAndFailure() throws Exception {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import time\n"
                        + "def process(row, context):\n"
                        + "    return [row['name'], row['age']]\n"
                        + "def close():\n"
                        + "    time.sleep(1)\n"
                        + "    raise RuntimeError('shared-close-boom')\n");
        PythonTransform transform = createOpenedTransform(config);
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        2,
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "python-transform-concurrent-close-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<?> first =
                    executor.submit(
                            () -> {
                                start.await();
                                transform.close();
                                return null;
                            });
            Future<?> second =
                    executor.submit(
                            () -> {
                                start.await();
                                transform.close();
                                return null;
                            });
            start.countDown();

            ExecutionException firstFailure =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS));
            ExecutionException secondFailure =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> second.get(10, TimeUnit.SECONDS));
            Assertions.assertTrue(firstFailure.getCause() instanceof TransformException);
            Assertions.assertTrue(secondFailure.getCause() instanceof TransformException);
            Assertions.assertTrue(
                    firstFailure.getCause().getMessage().contains("shared-close-boom"));
            Assertions.assertTrue(
                    secondFailure.getCause().getMessage().contains("shared-close-boom"));
            openedTransforms.remove(transform);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies inherited child pipes cannot block an active row or concurrent shutdown. */
    @Test
    void testCloseIsBoundedWhenChildInheritsWorkerPipes() throws Exception {
        assumePythonAvailable();

        Path markerPath = tempDir.resolve("python-transform-child-pipe-inherited");
        String escapedMarkerPath = markerPath.toString().replace("\\", "\\\\").replace("'", "\\'");
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import pathlib\n"
                        + "import subprocess\n"
                        + "import sys\n"
                        + "import time\n"
                        + "def process(row, context):\n"
                        + "    subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(9)'])\n"
                        + "    pathlib.Path('"
                        + escapedMarkerPath
                        + "').touch()\n"
                        + "    while True:\n"
                        + "        time.sleep(1)\n");

        PythonTransform transform = createOpenedTransform(config);
        ExecutorService executor =
                Executors.newFixedThreadPool(
                        2,
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "python-transform-bounded-close-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<SeaTunnelRow> rowFuture =
                    executor.submit(
                            () -> transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20})));
            long markerDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(markerPath) && System.nanoTime() < markerDeadlineNanos) {
                Thread.sleep(50L);
            }
            Assertions.assertTrue(
                    Files.exists(markerPath), "Python child should inherit pipes before close");

            Object processWorker = readFieldValue(transform, "processWorker");
            Process process = (Process) readFieldValue(processWorker, "process");
            Thread stdoutCollectorThread =
                    (Thread) readFieldValue(processWorker, "stdoutCollectorThread");
            Thread stderrCollectorThread =
                    (Thread) readFieldValue(processWorker, "stderrCollectorThread");
            Future<?> closeFuture = executor.submit(transform::close);

            ExecutionException rowFailure =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> rowFuture.get(5, TimeUnit.SECONDS));
            ExecutionException closeFailure =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> closeFuture.get(7, TimeUnit.SECONDS));
            Assertions.assertTrue(rowFailure.getCause() instanceof TransformException);
            Assertions.assertTrue(closeFailure.getCause() instanceof TransformException);
            Assertions.assertFalse(process.isAlive());
            waitForThreadToStop(stdoutCollectorThread);
            waitForThreadToStop(stderrCollectorThread);
            Assertions.assertFalse(stdoutCollectorThread.isAlive());
            Assertions.assertFalse(stderrCollectorThread.isAlive());
            openedTransforms.remove(transform);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies a closed transform never creates its first Python worker. */
    @Test
    void testCloseBeforeOpenPreventsWorkerCreation() {
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return [row['name'], row['age']]\n");
        PythonTransform transform =
                new PythonTransform(
                        createCatalogTable(),
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));

        transform.close();

        Assertions.assertThrows(TransformException.class, transform::open);
        Assertions.assertThrows(
                TransformException.class,
                () -> transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20})));
    }

    /** Verifies operators must opt in before the Python transform may start external code. */
    @Test
    void testOpenRejectedWhenPythonTransformDisabled() {
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return [row['name'], row['age']]\n");

        PythonTransform transform =
                new PythonTransform(
                        createCatalogTable(),
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));
        transform.getProducedCatalogTable();

        TransformException exception =
                Assertions.assertThrows(TransformException.class, transform::open);
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(PythonProcessWorker.PYTHON_TRANSFORM_ENABLED_PROPERTY));
    }

    /** Verifies the server-side allowlist rejects non-approved executables before launch. */
    @Test
    void testOpenRejectedWhenPythonExecutableNotAllowlisted() {
        System.setProperty(PythonProcessWorker.PYTHON_TRANSFORM_ENABLED_PROPERTY, "true");
        System.setProperty(
                PythonProcessWorker.PYTHON_ALLOWED_EXECUTABLES_PROPERTY,
                createUnusedAbsoluteAllowlistPath());

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return [row['name'], row['age']]\n");
        config.put(PythonTransformConfig.PYTHON_EXECUTABLE.key(), createJavaExecutablePath());

        PythonTransform transform =
                new PythonTransform(
                        createCatalogTable(),
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));
        transform.getProducedCatalogTable();

        TransformException exception =
                Assertions.assertThrows(TransformException.class, transform::open);
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains(
                                PythonTransformErrorCode.PYTHON_EXECUTABLE_NOT_ALLOWED
                                        .getDescription()),
                exception.getMessage());
    }

    /** Verifies stdout protocol pollution poisons the worker instead of shifting later rows. */
    @Test
    void testMismatchedResponseIdPoisonsWorker() {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import json\n"
                        + "import sys\n"
                        + "def process(row, context):\n"
                        + "    sys.stdout.write(json.dumps({'id': 999, 'result': ['bad', 0]}) + '\\n')\n"
                        + "    sys.stdout.flush()\n"
                        + "    return [row['name'], row['age']]\n");

        PythonTransform transform = createOpenedTransform(config);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 20});
        TransformException first =
                Assertions.assertThrows(TransformException.class, () -> transform.map(row));
        TransformException second =
                Assertions.assertThrows(TransformException.class, () -> transform.map(row));

        Assertions.assertTrue(first.getMessage().contains("expected response id 1"));
        Assertions.assertTrue(second.getMessage().contains("closed"));
    }

    /** Verifies a dead established worker is cleaned up and never mixed with a new generation. */
    @Test
    void testDeadWorkerBetweenRowsIsPermanentlyPoisoned() throws Exception {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return [row['name'], row['age']]\n");
        PythonTransform transform = createOpenedTransform(config);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 20});
        Assertions.assertNotNull(transform.map(row));

        Object processWorker = readFieldValue(transform, "processWorker");
        Process process = (Process) readFieldValue(processWorker, "process");
        Thread stdoutCollectorThread =
                (Thread) readFieldValue(processWorker, "stdoutCollectorThread");
        Thread stderrCollectorThread =
                (Thread) readFieldValue(processWorker, "stderrCollectorThread");
        Path workerScriptPath = (Path) readFieldValue(processWorker, "workerScriptPath");
        Path inlineSourceCodePath = (Path) readFieldValue(processWorker, "inlineSourceCodePath");

        process.destroyForcibly();
        Assertions.assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        TransformException failure =
                Assertions.assertThrows(TransformException.class, () -> transform.map(row));

        Assertions.assertTrue(failure.getMessage().contains("cannot be restarted safely"));
        waitForThreadToStop(stdoutCollectorThread);
        waitForThreadToStop(stderrCollectorThread);
        Assertions.assertFalse(stdoutCollectorThread.isAlive());
        Assertions.assertFalse(stderrCollectorThread.isAlive());
        Assertions.assertFalse(Files.exists(workerScriptPath));
        Assertions.assertFalse(Files.exists(inlineSourceCodePath));
        Assertions.assertNull(readFieldValue(processWorker, "process"));
    }

    /** Verifies a request arriving after the terminal stdin gate fails instead of orphaning. */
    @Test
    void testLateStdinRequestAfterTerminalGateFailsWithoutBlocking() throws Exception {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return [row['name'], row['age']]\n");
        PythonProcessWorker worker =
                new PythonProcessWorker(
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)),
                        createCatalogTable());
        worker.open();
        Object stdinLifecycleLock = readFieldValue(worker, "stdinLifecycleLock");
        synchronized (stdinLifecycleLock) {
            writeFieldValue(worker, "stdinAccepting", false);
        }

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> request =
                    executor.submit(
                            () ->
                                    worker.processRow(
                                            new SeaTunnelRowAccessor(
                                                    new SeaTunnelRow(
                                                            new Object[] {1, "Alice", 20}))));
            ExecutionException failure =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> request.get(10, TimeUnit.SECONDS));
            Assertions.assertTrue(failure.getCause() instanceof TransformException);
            Assertions.assertTrue(failure.getCause().getMessage().contains("not available"));
            Assertions.assertNull(readFieldValue(worker, "process"));
        } finally {
            executor.shutdownNow();
            worker.close();
        }
    }

    /**
     * Verifies object results must include every declared field, while explicit null remains valid.
     */
    @Test
    void testObjectResultRejectsMissingDeclaredField() {
        assumePythonAvailable();

        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n" + "    return {'normalized_name': row['name']}\n");

        PythonTransform transform = createOpenedTransform(config);
        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20})));

        Assertions.assertTrue(exception.getMessage().contains("age_plus_one"));
    }

    /** Verifies task-thread interruption can terminate a Python function that never responds. */
    @Test
    void testCloseInterruptsBlockedPythonFunction() throws Exception {
        assumePythonAvailable();

        Path markerPath = tempDir.resolve("blocked-python-function-started");
        String escapedMarkerPath = markerPath.toString().replace("\\", "\\\\").replace("'", "\\'");
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import time\n"
                        + "def process(row, context):\n"
                        + "    open('"
                        + escapedMarkerPath
                        + "', 'w').close()\n"
                        + "    while True:\n"
                        + "        time.sleep(1)\n");

        PythonTransform transform = createOpenedTransform(config);
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "python-transform-close-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        CountDownLatch taskStopped = new CountDownLatch(1);
        try {
            Future<SeaTunnelRow> rowFuture =
                    executor.submit(
                            () -> {
                                try {
                                    return transform.map(
                                            new SeaTunnelRow(new Object[] {1, "Alice", 20}));
                                } finally {
                                    taskStopped.countDown();
                                }
                            });
            long markerDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(markerPath) && System.nanoTime() < markerDeadlineNanos) {
                Thread.sleep(50L);
            }
            Assertions.assertTrue(
                    Files.exists(markerPath), "Python function should start before cancellation");

            Object processWorker = readFieldValue(transform, "processWorker");
            Process process = (Process) readFieldValue(processWorker, "process");
            Thread stdoutCollectorThread =
                    (Thread) readFieldValue(processWorker, "stdoutCollectorThread");
            Thread stderrCollectorThread =
                    (Thread) readFieldValue(processWorker, "stderrCollectorThread");
            Path workerScriptPath = (Path) readFieldValue(processWorker, "workerScriptPath");
            Path inlineSourceCodePath =
                    (Path) readFieldValue(processWorker, "inlineSourceCodePath");
            Assertions.assertTrue(rowFuture.cancel(true), "running row should accept cancellation");
            Assertions.assertTrue(
                    taskStopped.await(10, TimeUnit.SECONDS),
                    "interrupted task should leave the Python response wait");
            Assertions.assertFalse(process.isAlive(), "Python process should stop after close");
            waitForThreadToStop(stdoutCollectorThread);
            waitForThreadToStop(stderrCollectorThread);
            Assertions.assertFalse(
                    stdoutCollectorThread.isAlive(), "stdout collector should stop after cancel");
            Assertions.assertFalse(
                    stderrCollectorThread.isAlive(), "stderr collector should stop after cancel");
            Assertions.assertFalse(
                    Files.exists(workerScriptPath), "worker script should be deleted after cancel");
            Assertions.assertFalse(
                    Files.exists(inlineSourceCodePath),
                    "inline source script should be deleted after cancel");
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies task-thread interruption can terminate a Python open hook that never returns. */
    @Test
    void testCloseInterruptsBlockedPythonOpenHook() throws Exception {
        assumePythonAvailable();

        Path markerPath = tempDir.resolve("blocked-python-open-started");
        String escapedMarkerPath = markerPath.toString().replace("\\", "\\\\").replace("'", "\\'");
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import pathlib\n"
                        + "import time\n"
                        + "def open(context):\n"
                        + "    pathlib.Path('"
                        + escapedMarkerPath
                        + "').touch()\n"
                        + "    while True:\n"
                        + "        time.sleep(1)\n"
                        + "def process(row, context):\n"
                        + "    return [row['name'], row['age']]\n");

        PythonTransform transform =
                new PythonTransform(
                        createCatalogTable(),
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));
        transform.getProducedCatalogTable();
        openedTransforms.add(transform);
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "python-transform-open-close-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        CountDownLatch taskStopped = new CountDownLatch(1);
        try {
            Future<?> openFuture =
                    executor.submit(
                            () -> {
                                try {
                                    transform.open();
                                } finally {
                                    taskStopped.countDown();
                                }
                            });
            long markerDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(markerPath) && System.nanoTime() < markerDeadlineNanos) {
                Thread.sleep(50L);
            }
            Assertions.assertTrue(
                    Files.exists(markerPath), "Python open hook should start before cancellation");

            Object processWorker = readFieldValue(transform, "processWorker");
            Process process = (Process) readFieldValue(processWorker, "process");
            Path workerScriptPath = (Path) readFieldValue(processWorker, "workerScriptPath");
            Path inlineSourceCodePath =
                    (Path) readFieldValue(processWorker, "inlineSourceCodePath");
            Thread stdoutCollectorThread =
                    (Thread) readFieldValue(processWorker, "stdoutCollectorThread");
            Thread stderrCollectorThread =
                    (Thread) readFieldValue(processWorker, "stderrCollectorThread");
            TransformException closeFailure =
                    Assertions.assertThrows(TransformException.class, transform::close);
            Assertions.assertTrue(
                    closeFailure.getMessage().contains("did not stop cleanly during close"));
            Assertions.assertTrue(
                    taskStopped.await(10, TimeUnit.SECONDS),
                    "close should release the Python initialization wait");
            Assertions.assertFalse(process.isAlive(), "Python process should stop after close");
            waitForThreadToStop(stdoutCollectorThread);
            waitForThreadToStop(stderrCollectorThread);
            Assertions.assertFalse(
                    stdoutCollectorThread.isAlive(), "stdout collector should stop after cancel");
            Assertions.assertFalse(
                    stderrCollectorThread.isAlive(), "stderr collector should stop after cancel");
            Assertions.assertFalse(
                    Files.exists(workerScriptPath), "worker script should be deleted after close");
            Assertions.assertFalse(
                    Files.exists(inlineSourceCodePath),
                    "inline source script should be deleted after close");
            openedTransforms.remove(transform);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies close can cancel an init write blocked before Python starts reading stdin. */
    @Test
    void testCloseInterruptsBlockedLargeInitWrite() throws Exception {
        assumePythonAvailable();

        Path markerPath = tempDir.resolve("blocked-python-module-load-started");
        String escapedMarkerPath = markerPath.toString().replace("\\", "\\\\").replace("'", "\\'");
        char[] payloadChars = new char[2 * 1024 * 1024];
        java.util.Arrays.fill(payloadChars, 'x');
        Map<String, Object> scriptConfig = new LinkedHashMap<>();
        scriptConfig.put("payload", new String(payloadChars));
        Map<String, Object> config = baseConfig();
        config.put(PythonTransformConfig.SCRIPT_CONFIG.key(), scriptConfig);
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import pathlib\n"
                        + "import time\n"
                        + "pathlib.Path('"
                        + escapedMarkerPath
                        + "').touch()\n"
                        + "while True:\n"
                        + "    time.sleep(1)\n"
                        + "def process(row, context):\n"
                        + "    return [row['name'], row['age']]\n");

        PythonTransform transform =
                new PythonTransform(
                        createCatalogTable(),
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));
        transform.getProducedCatalogTable();
        openedTransforms.add(transform);
        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "python-transform-init-write-close-test");
                            thread.setDaemon(true);
                            return thread;
                        });
        CountDownLatch taskStopped = new CountDownLatch(1);
        try {
            executor.submit(
                    () -> {
                        try {
                            transform.open();
                        } finally {
                            taskStopped.countDown();
                        }
                    });
            long markerDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(markerPath) && System.nanoTime() < markerDeadlineNanos) {
                Thread.sleep(50L);
            }
            Assertions.assertTrue(Files.exists(markerPath));
            Thread.sleep(200L);

            Object processWorker = readFieldValue(transform, "processWorker");
            Process process = (Process) readFieldValue(processWorker, "process");
            Thread stdinWriterThread = (Thread) readFieldValue(processWorker, "stdinWriterThread");
            Path workerScriptPath = (Path) readFieldValue(processWorker, "workerScriptPath");
            Path inlineSourceCodePath =
                    (Path) readFieldValue(processWorker, "inlineSourceCodePath");
            Assertions.assertThrows(TransformException.class, transform::close);
            Assertions.assertTrue(
                    taskStopped.await(10, TimeUnit.SECONDS),
                    "forced process shutdown should release the blocked init writer");
            waitForThreadToStop(stdinWriterThread);
            Assertions.assertFalse(process.isAlive());
            Assertions.assertFalse(stdinWriterThread.isAlive());
            Assertions.assertFalse(Files.exists(workerScriptPath));
            Assertions.assertFalse(Files.exists(inlineSourceCodePath));
            openedTransforms.remove(transform);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies schema worker replacement participates in the terminal close barrier. */
    @Test
    void testSchemaInvalidationFailureIsSharedWithConcurrentClose() throws Exception {
        assumePythonAvailable();

        Path markerPath = tempDir.resolve("python-schema-invalidation-close-started");
        String escapedMarkerPath = markerPath.toString().replace("\\", "\\\\").replace("'", "\\'");
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "import pathlib\n"
                        + "import time\n"
                        + "def process(row, context):\n"
                        + "    return [row['name'], row['age']]\n"
                        + "def close():\n"
                        + "    pathlib.Path('"
                        + escapedMarkerPath
                        + "').touch()\n"
                        + "    time.sleep(1)\n"
                        + "    raise RuntimeError('schema-close-boom')\n");
        PythonTransform transform = createOpenedTransform(config);
        Assertions.assertNotNull(transform.map(new SeaTunnelRow(new Object[] {1, "Alice", 20})));
        SchemaChangeEvent event =
                AlterTableAddColumnEvent.add(
                        createCatalogTable().getTableId(),
                        PhysicalColumn.of(
                                "vip_level", BasicType.STRING_TYPE, (Long) null, true, null, null));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> schemaChange = executor.submit(() -> transform.mapSchemaChangeEvent(event));
            long markerDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(markerPath) && System.nanoTime() < markerDeadlineNanos) {
                Thread.sleep(50L);
            }
            Assertions.assertTrue(Files.exists(markerPath));

            TransformException closeFailure =
                    Assertions.assertThrows(TransformException.class, transform::close);
            ExecutionException schemaFailure =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> schemaChange.get(10, TimeUnit.SECONDS));

            Assertions.assertSame(schemaFailure.getCause(), closeFailure);
            Assertions.assertTrue(closeFailure.getMessage().contains("schema-close-boom"));
            openedTransforms.remove(transform);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Verifies the factory-returned wrapper rebuilds the Python worker after schema changes. */
    @Test
    void testMultiCatalogWrapperSchemaChangeRebuildsWorkerContext() {
        assumePythonAvailable();

        CatalogTable inputTable = createCatalogTable();
        PythonMultiCatalogTransform wrapper = createWrapper(inputTable, schemaChangeConfig());

        SeaTunnelRow preOutput = wrapper.map(new SeaTunnelRow(new Object[] {1, "Alice", 20}));
        Assertions.assertNotNull(preOutput);
        Assertions.assertEquals(5, preOutput.getArity());
        Assertions.assertNull(preOutput.getField(3));
        Assertions.assertEquals(3, preOutput.getField(4));

        wrapper.mapSchemaChangeEvent(
                AlterTableAddColumnEvent.add(
                        inputTable.getTableId(),
                        PhysicalColumn.of(
                                "vip_level",
                                BasicType.STRING_TYPE,
                                (Long) null,
                                true,
                                null,
                                null)));

        SeaTunnelRow postOutput =
                wrapper.map(new SeaTunnelRow(new Object[] {2, "Bob", 30, "gold"}));
        Assertions.assertNotNull(postOutput);
        Assertions.assertEquals(6, postOutput.getArity());
        Assertions.assertEquals("gold", postOutput.getField(3));
        Assertions.assertEquals("gold", postOutput.getField(4));
        Assertions.assertEquals(4, postOutput.getField(5));
    }

    /** Verifies wrapper close delegates to the inner Python transform and releases its worker. */
    @Test
    void testMultiCatalogWrapperCloseStopsInnerPythonWorker() throws Exception {
        assumePythonAvailable();

        CatalogTable inputTable = createCatalogTable();
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n" + "    return [row['name'], row['age'] + 1]\n");

        PythonMultiCatalogTransform wrapper = createWrapper(inputTable, config);
        SeaTunnelRow outputRow = wrapper.map(new SeaTunnelRow(new Object[] {1, "Alice", 20}));
        Assertions.assertNotNull(outputRow);

        PythonTransform innerTransform = getOnlyInnerPythonTransform(wrapper);
        Object processWorker = readFieldValue(innerTransform, "processWorker");
        Thread stderrCollectorThread =
                (Thread) readFieldValue(processWorker, "stderrCollectorThread");
        Assertions.assertNotNull(stderrCollectorThread);

        wrapper.close();

        long waitDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (stderrCollectorThread.isAlive() && System.nanoTime() < waitDeadlineNanos) {
            stderrCollectorThread.join(100L);
        }

        Assertions.assertFalse(
                stderrCollectorThread.isAlive(),
                "wrapper close should stop the inner stderr collector thread");
    }

    /** Verifies exactly one script source is configured for each transform instance. */
    @Test
    void testConfigRequiresExactlyOneScriptSource() {
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return []\n");
        config.put(PythonTransformConfig.SOURCE_CODE_PATH.key(), "/tmp/script.py");

        Assertions.assertThrows(
                TransformException.class,
                () -> PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));
    }

    /** Verifies unsupported error-table routing is rejected before a Python worker starts. */
    @Test
    void testConfigRejectsRouteToTableErrorHandling() {
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return []\n");
        config.put(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION.key(), "ROUTE_TO_TABLE");

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));

        Assertions.assertTrue(exception.getMessage().contains("FAIL and SKIP"));
    }

    /** Verifies duplicate output names cannot collapse onto one produced-schema field. */
    @Test
    @SuppressWarnings("unchecked")
    void testConfigRejectsDuplicateDestinationFields() {
        Map<String, Object> config = baseConfig();
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n    return []\n");
        List<Map<String, String>> columns =
                (List<Map<String, String>>) config.get(PythonTransformConfig.COLUMNS.key());
        columns.get(1).put(PythonTransformConfig.DEST_FIELD.key(), "normalized_name");

        TransformException exception =
                Assertions.assertThrows(
                        TransformException.class,
                        () -> PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));

        Assertions.assertTrue(exception.getMessage().contains("must be unique"));
    }

    /**
     * Creates and opens a transform so the caller can run row-level assertions.
     *
     * @param config raw transform config
     * @return opened transform instance
     */
    private PythonTransform createOpenedTransform(Map<String, Object> config) {
        PythonTransform transform =
                new PythonTransform(
                        createCatalogTable(),
                        PythonTransformConfig.of(ReadonlyConfig.fromMap(config)));
        transform.getProducedCatalogTable();
        transform.open();
        openedTransforms.add(transform);
        return transform;
    }

    /**
     * Creates the factory-returned wrapper so tests exercise the same runtime shape used by the
     * engine.
     *
     * @param inputTable input table for the wrapped transform
     * @param config raw transform config
     * @return wrapper containing one Python transform
     */
    private PythonMultiCatalogTransform createWrapper(
            CatalogTable inputTable, Map<String, Object> config) {
        PythonMultiCatalogTransform wrapper =
                new PythonMultiCatalogTransform(
                        Collections.singletonList(inputTable), ReadonlyConfig.fromMap(config));
        wrapper.getProducedCatalogTable();
        openedWrappers.add(wrapper);
        return wrapper;
    }

    /**
     * Builds the baseline config shared by all test cases.
     *
     * @return mutable config map for one transform instance
     */
    private Map<String, Object> baseConfig() {
        List<Map<String, String>> columns = new ArrayList<>();

        Map<String, String> firstColumn = new LinkedHashMap<>();
        firstColumn.put(PythonTransformConfig.DEST_FIELD.key(), "normalized_name");
        firstColumn.put(PythonTransformConfig.DEST_TYPE.key(), "string");
        columns.add(firstColumn);

        Map<String, String> secondColumn = new LinkedHashMap<>();
        secondColumn.put(PythonTransformConfig.DEST_FIELD.key(), "age_plus_one");
        secondColumn.put(PythonTransformConfig.DEST_TYPE.key(), "int");
        columns.add(secondColumn);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put(PythonTransformConfig.COLUMNS.key(), columns);
        return config;
    }

    /**
     * Builds a minimal input table used by the transform tests.
     *
     * @return catalog table with three input fields
     */
    private CatalogTable createCatalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        return CatalogTableUtil.getCatalogTable(
                "test", "default", "default", "python_input", rowType);
    }

    /**
     * Creates a simple script_config payload for runtime context verification.
     *
     * @return script config map
     */
    private Map<String, Object> createScriptConfig() {
        Map<String, Object> scriptConfig = new LinkedHashMap<>();
        scriptConfig.put("prefix", "user:");
        return scriptConfig;
    }

    /**
     * Builds a config whose script reads a newly added upstream column and reports the Python
     * worker's cached input-field count.
     *
     * @return config used to prove schema changes rebuild the worker context
     */
    private Map<String, Object> schemaChangeConfig() {
        List<Map<String, String>> columns = new ArrayList<>();

        Map<String, String> firstColumn = new LinkedHashMap<>();
        firstColumn.put(PythonTransformConfig.DEST_FIELD.key(), "vip_level_seen");
        firstColumn.put(PythonTransformConfig.DEST_TYPE.key(), "string");
        columns.add(firstColumn);

        Map<String, String> secondColumn = new LinkedHashMap<>();
        secondColumn.put(PythonTransformConfig.DEST_FIELD.key(), "input_field_count");
        secondColumn.put(PythonTransformConfig.DEST_TYPE.key(), "int");
        columns.add(secondColumn);

        Map<String, Object> config = new LinkedHashMap<>();
        config.put(PythonTransformConfig.COLUMNS.key(), columns);
        config.put(
                PythonTransformConfig.SOURCE_CODE.key(),
                "def process(row, context):\n"
                        + "    return {\n"
                        + "        'vip_level_seen': row.get('vip_level'),\n"
                        + "        'input_field_count': len(context['input_fields']),\n"
                        + "    }\n");
        return config;
    }

    /** Skips runtime-dependent tests when no python executable is available locally. */
    private void assumePythonAvailable() {
        if (availablePythonExecutable == null) {
            availablePythonExecutable = queryPythonExecutable("python3");
            if (availablePythonExecutable == null) {
                availablePythonExecutable = queryPythonExecutable("python");
            }
        }
        Assumptions.assumeTrue(
                availablePythonExecutable != null,
                "python runtime is required for PythonTransform tests");
        System.setProperty(PythonProcessWorker.PYTHON_TRANSFORM_ENABLED_PROPERTY, "true");
        System.setProperty(
                PythonProcessWorker.PYTHON_ALLOWED_EXECUTABLES_PROPERTY, availablePythonExecutable);
    }

    /**
     * Resolves one python executable into an absolute path that matches the worker allowlist.
     *
     * @param executable python binary candidate
     * @return absolute runtime path, or null when the executable is unavailable
     */
    private String queryPythonExecutable(String executable) {
        try {
            Process process =
                    new ProcessBuilder(
                                    executable,
                                    "-c",
                                    "import pathlib, sys; print(pathlib.Path(sys.executable).resolve())")
                            .start();
            if (process.waitFor() != 0) {
                return null;
            }
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    process.getInputStream(), StandardCharsets.UTF_8))) {
                return reader.readLine();
            }
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
    }

    /**
     * Resolves the current JVM executable with the platform-specific suffix for allowlist tests.
     *
     * @return absolute Java executable path
     */
    private String createJavaExecutablePath() {
        String executableName =
                System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("windows")
                        ? "java.exe"
                        : "java";
        return Paths.get(System.getProperty("java.home"), "bin", executableName)
                .toAbsolutePath()
                .normalize()
                .toString();
    }

    /**
     * Creates a valid absolute allowlist entry that intentionally differs from the Java executable.
     *
     * @return absolute non-matching allowlist path
     */
    private String createUnusedAbsoluteAllowlistPath() {
        return Paths.get(System.getProperty("java.home"), "bin", "not-python")
                .toAbsolutePath()
                .normalize()
                .toString();
    }

    /**
     * Reads one private field value so the regression test can verify worker lifecycle details.
     *
     * @param target object that owns the field
     * @param fieldName private field name
     * @return current field value
     * @throws ReflectiveOperationException when the field cannot be accessed
     */
    private Object readFieldValue(Object target, String fieldName)
            throws ReflectiveOperationException {
        Class<?> currentClass = target.getClass();
        while (currentClass != null) {
            try {
                Field field = currentClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException ignored) {
                currentClass = currentClass.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    /** Writes one private field for deterministic lifecycle gate testing. */
    private void writeFieldValue(Object target, String fieldName, Object value)
            throws ReflectiveOperationException {
        Class<?> currentClass = target.getClass();
        while (currentClass != null) {
            try {
                Field field = currentClass.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException ignored) {
                currentClass = currentClass.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    /** Waits briefly for one daemon collector to observe process termination. */
    private void waitForThreadToStop(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && System.nanoTime() < deadlineNanos) {
            thread.join(100L);
        }
    }

    /**
     * Extracts the only inner Python transform from the multi-table wrapper for lifecycle checks.
     *
     * @param wrapper wrapper under test
     * @return the only inner Python transform
     * @throws ReflectiveOperationException when internal fields are inaccessible
     */
    @SuppressWarnings("unchecked")
    private PythonTransform getOnlyInnerPythonTransform(PythonMultiCatalogTransform wrapper)
            throws ReflectiveOperationException {
        Map<String, Object> transformMap =
                (Map<String, Object>) readFieldValue(wrapper, "transformMap");
        return (PythonTransform) transformMap.values().iterator().next();
    }
}
