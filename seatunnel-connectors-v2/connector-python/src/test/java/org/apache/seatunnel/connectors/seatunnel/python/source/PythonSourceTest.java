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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class PythonSourceTest {

    @TempDir Path tempDir;

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
    void testReaderCollectsRowsFromPythonScript() throws Exception {
        String pythonExecutable = requirePythonExecutable();
        Path scriptPath = copyResource("python/emit_rows.py");

        Map<String, Object> config = baseConfig(pythonExecutable, scriptPath.toString());
        Map<String, Object> scriptConfig = new HashMap<>();
        scriptConfig.put("prefix", "seatunnel");
        scriptConfig.put("count", 2);
        config.put(PythonSourceOptions.PYTHON_SCRIPT_CONFIG.key(), scriptConfig);

        PythonSource source = new PythonSource(ReadonlyConfig.fromMap(config));
        PythonSourceReader reader = createReader(source);
        RecordingCollector collector = new RecordingCollector();

        reader.open();
        try {
            reader.pollNext(collector);
        } finally {
            reader.close();
        }

        Assertions.assertEquals(2, collector.rows.size());
        Assertions.assertEquals(1, collector.rows.get(0).getField(0));
        Assertions.assertEquals("seatunnel_1", collector.rows.get(0).getField(1));
        Assertions.assertEquals(2, collector.rows.get(1).getField(0));
        Assertions.assertEquals("seatunnel_2", collector.rows.get(1).getField(1));
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
            IllegalStateException exception =
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () -> reader.pollNext(new RecordingCollector()));
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

    private PythonSourceReader createReader(PythonSource source) {
        return (PythonSourceReader)
                source.createReader(Mockito.mock(SingleSplitReaderContext.class));
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
        return executable;
    }

    private String findPythonExecutable() {
        String[] candidates = new String[] {"python3", "python"};
        for (String candidate : candidates) {
            try {
                Process process = new ProcessBuilder(candidate, "--version").start();
                int exitCode = process.waitFor();
                if (exitCode == 0) {
                    return candidate;
                }
            } catch (Exception ignored) {
                // Try the next common interpreter name.
            }
        }
        return null;
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
}
