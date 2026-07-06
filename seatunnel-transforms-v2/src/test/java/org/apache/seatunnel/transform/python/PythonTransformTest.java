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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.exception.TransformException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Covers the main runtime contracts of the Python transform implementation. */
class PythonTransformTest {

    /** Opened transforms that need explicit teardown because they own subprocesses. */
    private final List<PythonTransform> openedTransforms = new ArrayList<>();

    /** Temporary directory used for path-based script tests. */
    @TempDir Path tempDir;

    /** Closes subprocess-backed transforms created by the test cases. */
    @AfterEach
    void tearDown() {
        openedTransforms.forEach(PythonTransform::close);
        openedTransforms.clear();
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

    /** Skips runtime-dependent tests when no python executable is available locally. */
    private void assumePythonAvailable() {
        Assumptions.assumeTrue(
                isCommandAvailable("python3") || isCommandAvailable("python"),
                "python runtime is required for PythonTransform tests");
    }

    /**
     * Checks whether one python executable can be launched on the current host.
     *
     * @param executable python binary candidate
     * @return true when the executable starts successfully
     */
    private boolean isCommandAvailable(String executable) {
        try {
            Process process = new ProcessBuilder(executable, "--version").start();
            return process.waitFor() == 0;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
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
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
