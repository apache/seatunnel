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

package org.apache.seatunnel.connectors.seatunnel.file.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.BinaryReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.Getter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class BinaryReadStrategyTest {

    @TempDir Path tempDir;

    private BinaryReadStrategy binaryReadStrategy;
    private LocalConf localConf;

    @BeforeEach
    public void setUp() {
        binaryReadStrategy = new BinaryReadStrategy();
        localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
    }

    @Test
    public void testBinaryReadWithDefaultChunkSize() throws IOException {
        // Create a test file with 2048 bytes (2 chunks of 1024 bytes each)
        File testFile = createTestFile("test_binary_default.bin", 2048);

        Config config = createConfig(testFile.getParent(), null, null);
        binaryReadStrategy.setPluginConfig(config);
        binaryReadStrategy.init(localConf);

        TestCollector collector = new TestCollector();
        binaryReadStrategy.read(testFile.getAbsolutePath(), "test_table", collector);

        List<SeaTunnelRow> rows = collector.getRows();
        Assertions.assertEquals(
                2, rows.size(), "Should have 2 chunks for 2048 bytes with default 1024 chunk size");

        // Verify first chunk
        SeaTunnelRow firstRow = rows.get(0);
        Assertions.assertEquals(3, firstRow.getArity());
        byte[] firstChunkData = (byte[]) firstRow.getField(0);
        Assertions.assertEquals(1024, firstChunkData.length);
        Assertions.assertEquals("test_binary_default.bin", firstRow.getField(1));
        Assertions.assertEquals(0L, firstRow.getField(2));

        // Verify second chunk
        SeaTunnelRow secondRow = rows.get(1);
        byte[] secondChunkData = (byte[]) secondRow.getField(0);
        Assertions.assertEquals(1024, secondChunkData.length);
        Assertions.assertEquals("test_binary_default.bin", secondRow.getField(1));
        Assertions.assertEquals(1L, secondRow.getField(2));
    }

    @Test
    public void testBinaryReadWithCustomChunkSize() throws IOException {
        // Create a test file with 1500 bytes
        File testFile = createTestFile("test_binary_custom.bin", 1500);

        Config config = createConfig(testFile.getParent(), 512, null);
        binaryReadStrategy.setPluginConfig(config);
        binaryReadStrategy.init(localConf);

        TestCollector collector = new TestCollector();
        binaryReadStrategy.read(testFile.getAbsolutePath(), "test_table", collector);

        List<SeaTunnelRow> rows = collector.getRows();
        Assertions.assertEquals(
                3, rows.size(), "Should have 3 chunks for 1500 bytes with 512 chunk size");

        // Verify chunk sizes: 512, 512, 476
        Assertions.assertEquals(512, ((byte[]) rows.get(0).getField(0)).length);
        Assertions.assertEquals(512, ((byte[]) rows.get(1).getField(0)).length);
        Assertions.assertEquals(476, ((byte[]) rows.get(2).getField(0)).length);

        // Verify part indices
        Assertions.assertEquals(0L, rows.get(0).getField(2));
        Assertions.assertEquals(1L, rows.get(1).getField(2));
        Assertions.assertEquals(2L, rows.get(2).getField(2));
    }

    @Test
    public void testBinaryReadCompleteFileMode() throws IOException {
        // Create a test file with 2048 bytes
        File testFile = createTestFile("test_binary_complete.bin", 2048);

        Config config = createConfig(testFile.getParent(), null, true);
        binaryReadStrategy.setPluginConfig(config);
        binaryReadStrategy.init(localConf);

        TestCollector collector = new TestCollector();
        binaryReadStrategy.read(testFile.getAbsolutePath(), "test_table", collector);

        List<SeaTunnelRow> rows = collector.getRows();
        Assertions.assertEquals(1, rows.size(), "Should have 1 row in complete file mode");

        SeaTunnelRow row = rows.get(0);
        byte[] fileData = (byte[]) row.getField(0);
        Assertions.assertEquals(2048, fileData.length, "Should read entire file content");
        Assertions.assertEquals("test_binary_complete.bin", row.getField(1));
        Assertions.assertEquals(0L, row.getField(2));
    }

    private File createTestFile(String fileName, int sizeInBytes) throws IOException {
        File testFile = tempDir.resolve(fileName).toFile();

        if (sizeInBytes > 0) {
            try (FileOutputStream fos = new FileOutputStream(testFile)) {
                // Create test data with a pattern for verification
                byte[] pattern = "SEATUNNEL_TEST_DATA_".getBytes();
                int written = 0;
                while (written < sizeInBytes) {
                    int toWrite = Math.min(pattern.length, sizeInBytes - written);
                    fos.write(pattern, 0, toWrite);
                    written += toWrite;
                }
            }
        } else {
            // Create empty file
            testFile.createNewFile();
        }

        return testFile;
    }

    private Config createConfig(String filePath, Integer chunkSize, Boolean completeFileMode) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("path", filePath); // Fixed: use "path" instead of "file_path"
        configMap.put("file_format_type", "binary");

        if (chunkSize != null) {
            configMap.put("binary_chunk_size", chunkSize);
        }
        if (completeFileMode != null) {
            configMap.put("binary_complete_file_mode", completeFileMode);
        }

        return ConfigFactory.parseMap(configMap);
    }

    @Getter
    public static class TestCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }
    }

    public static class LocalConf extends HadoopConf {
        private static final String HDFS_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
        private static final String SCHEMA = "file";

        public LocalConf(String hdfsNameKey) {
            super(hdfsNameKey);
        }

        @Override
        public String getFsHdfsImpl() {
            return HDFS_IMPL;
        }

        @Override
        public String getSchema() {
            return SCHEMA;
        }
    }
}
