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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.OrcWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class OrcWriteStrategyTest {
    private static final String TMP_PATH = "file:///tmp/seatunnel/orc/batch/test";
    private static final int ORC_WRITE_NUMBER = 2000;

    @Test
    public void testZstdCompressionMapping() {
        Assertions.assertEquals("ZSTD", CompressFormat.ZSTD.getOrcCompression().name());
    }

    @ParameterizedTest
    @ValueSource(strings = {"ZSTD", "SNAPPY", "NONE", "ZLIB", "LZ4"})
    public void testOrcCompressionRoundTrip(String codec, @TempDir Path directory)
            throws Exception {
        String tmpPath = directory.resolve("pending").toUri().toString();
        Map<String, Object> config = new HashMap<>();
        config.put("tmp_path", tmpPath);
        config.put("path", directory.resolve("output").toUri().toString());
        config.put("file_format_type", FileFormat.ORC.name());
        config.put("compress_codec", codec);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "amount", "created_at"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            new DecimalType(10, 2),
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        LocalDateTime timestamp = LocalDateTime.of(2026, 10, 21, 12, 34, 56, 123456789);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT) {
                    @Override
                    public String getFsHdfsImpl() {
                        return JavaLocalFileSystem.class.getName();
                    }
                };
        OrcWriteStrategy writer =
                new OrcWriteStrategy(new FileSinkConfig(ReadonlyConfig.fromMap(config), rowType));
        try {
            writer.setCatalogTable(
                    CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
            writer.init(hadoopConf, "compression", "compression", 0);
            writer.beginTransaction(1L);
            writer.write(
                    new SeaTunnelRow(
                            new Object[] {1, "alpha", new BigDecimal("123.45"), timestamp}));
            writer.write(new SeaTunnelRow(new Object[] {2, "\u4e2d\u6587", null, null}));
            writer.write(
                    new SeaTunnelRow(new Object[] {3, null, new BigDecimal("-0.01"), timestamp}));
            writer.finishAndCloseFile();
        } finally {
            writer.close();
        }

        OrcReadStrategy reader = new OrcReadStrategy();
        try {
            reader.init(hadoopConf);
            List<String> files;
            try (Stream<Path> paths = Files.walk(directory)) {
                files =
                        paths.filter(path -> path.toString().endsWith(".orc"))
                                .map(path -> path.toUri().toString())
                                .collect(Collectors.toList());
            }
            Assertions.assertEquals(1, files.size());
            String file = files.get(0);
            // A round trip alone would also pass when ZSTD is silently mapped to NONE.
            try (Reader footer =
                    OrcFile.createReader(
                            new org.apache.hadoop.fs.Path(file),
                            OrcFile.readerOptions(new Configuration()))) {
                Assertions.assertEquals(codec, footer.getCompressionKind().name());
                Assertions.assertEquals(3, footer.getNumberOfRows());
            }
            Assertions.assertEquals(rowType, reader.getSeaTunnelRowTypeInfo(file));
            List<SeaTunnelRow> rows = new ArrayList<>();
            reader.read(
                    file,
                    "test",
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow record) {
                            rows.add(record);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return null;
                        }
                    });
            Assertions.assertEquals(3, rows.size());
            Assertions.assertArrayEquals(
                    new Object[] {1, "alpha", new BigDecimal("123.45"), timestamp},
                    rows.get(0).getFields());
            Assertions.assertArrayEquals(
                    new Object[] {2, "\u4e2d\u6587", null, null}, rows.get(1).getFields());
            Assertions.assertArrayEquals(
                    new Object[] {3, null, new BigDecimal("-0.01"), timestamp},
                    rows.get(2).getFields());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testOrcStructSchemaPreservesFieldCase() {
        SeaTunnelRowType structType =
                new SeaTunnelRowType(
                        new String[] {"MD5"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});

        org.apache.orc.TypeDescription structSchema =
                OrcWriteStrategy.buildFieldWithRowType(structType);

        Assertions.assertEquals("MD5", structSchema.getFieldNames().get(0));
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testOrcNestedFieldCaseRoundTrip() throws Exception {
        String tmpPath = "file:///tmp/seatunnel/orc/nested/test";
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", "file:///tmp/seatunnel/orc/nested");
        writeConfig.put("file_format_type", FileFormat.ORC.name());

        SeaTunnelRowType nestedFieldType =
                new SeaTunnelRowType(
                        new String[] {"MD5"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"payload"}, new SeaTunnelDataType[] {nestedFieldType});
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        OrcWriteStrategy writeStrategy = new OrcWriteStrategy(writeSinkConfig);

        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);

        SeaTunnelRow nestedValue = new SeaTunnelRow(new Object[] {"abc123"});
        writeStrategy.write(new SeaTunnelRow(new Object[] {nestedValue}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        OrcReadStrategy readStrategy = new OrcReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(tmpPath);
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);

        SeaTunnelRowType readRowType = readStrategy.getSeaTunnelRowTypeInfo(readFilePath);
        Assertions.assertEquals(1, readRowType.getTotalFields());
        SeaTunnelDataType<?> nestedReadType = readRowType.getFieldType(0);
        Assertions.assertTrue(nestedReadType instanceof SeaTunnelRowType);
        SeaTunnelRowType nestedReadRowType = (SeaTunnelRowType) nestedReadType;
        Assertions.assertArrayEquals(new String[] {"MD5"}, nestedReadRowType.getFieldNames());

        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(readFilePath, "test", readCollector);
        Assertions.assertEquals(1, readRows.size());
        SeaTunnelRow readRow = readRows.get(0);
        Object nestedObj = readRow.getField(0);
        Assertions.assertTrue(nestedObj instanceof SeaTunnelRow);
        SeaTunnelRow nestedRead = (SeaTunnelRow) nestedObj;
        Assertions.assertEquals("abc123", nestedRead.getField(0));
        readStrategy.close();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testCloseWriterWhenRollingFile() throws Exception {
        String tmpPath = "file:///tmp/seatunnel/orc/rolling/test-" + System.nanoTime();
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", "file:///tmp/seatunnel/orc/rolling");
        writeConfig.put("file_format_type", FileFormat.ORC.name());
        writeConfig.put("batch_size", 1);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        OrcWriteStrategy writeStrategy =
                new OrcWriteStrategy(
                        new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), rowType));
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
        writeStrategy.init(hadoopConf, "rolling-test", "rolling-test", 0);
        writeStrategy.beginTransaction(1L);

        writeStrategy.write(new SeaTunnelRow(new Object[] {"first"}));
        writeStrategy.write(new SeaTunnelRow(new Object[] {"second"}));

        OrcReadStrategy readStrategy = new OrcReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(tmpPath);
        Assertions.assertEquals(1, readFiles.size());
        String rolledFile = readFiles.get(0);
        Assertions.assertTrue(rolledFile.endsWith("_0.orc"));
        readStrategy.getSeaTunnelRowTypeInfo(rolledFile);
        List<String> values = new ArrayList<>();
        readStrategy.read(rolledFile, "test", collectorForFirstField(values));
        Assertions.assertEquals(java.util.Arrays.asList("first"), values);

        writeStrategy.finishAndCloseFile();
        writeStrategy.close();
        Assertions.assertEquals(2, readStrategy.getFileNamesByPath(tmpPath).size());
        readStrategy.close();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testOrcWriteWithBatch() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/orc/batch");
        writeConfig.put("file_format_type", FileFormat.ORC.name());

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        OrcWriteStrategy writeStrategy = new OrcWriteStrategy(writeSinkConfig);

        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        for (int i = 0; i < ORC_WRITE_NUMBER; i++) {
            writeStrategy.write(new SeaTunnelRow(new Object[] {"test_" + i}));
        }
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        OrcReadStrategy readStrategy = new OrcReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);

        SeaTunnelRowType readRowType = readStrategy.getSeaTunnelRowTypeInfo(readFilePath);
        Assertions.assertEquals(
                BasicType.STRING_TYPE.getSqlType(), readRowType.getFieldType(0).getSqlType());
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertTrue(record.getField(0) instanceof String);
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(readFilePath, "test", readCollector);
        Assertions.assertEquals(ORC_WRITE_NUMBER, readRows.size());
        readStrategy.close();
    }

    private static Collector<SeaTunnelRow> collectorForFirstField(List<String> values) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow record) {
                values.add((String) record.getField(0));
            }

            @Override
            public Object getCheckpointLock() {
                return null;
            }
        };
    }

    /** Uses real local files without requiring Hadoop's native Windows permission utilities. */
    public static class JavaLocalFileSystem extends RawLocalFileSystem {
        private final List<FSDataInputStream> inputs = new ArrayList<>();

        @Override
        public FSDataInputStream open(org.apache.hadoop.fs.Path path, int bufferSize)
                throws IOException {
            FSDataInputStream input = super.open(path, bufferSize);
            inputs.add(input);
            return input;
        }

        @Override
        public void close() throws IOException {
            try {
                for (FSDataInputStream input : inputs) {
                    input.close();
                }
            } finally {
                super.close();
            }
        }

        @Override
        public boolean mkdirs(org.apache.hadoop.fs.Path path) throws IOException {
            Files.createDirectories(pathToFile(path).toPath());
            return true;
        }

        @Override
        public boolean mkdirs(org.apache.hadoop.fs.Path path, FsPermission permission)
                throws IOException {
            Files.createDirectories(pathToFile(path).toPath());
            return true;
        }

        @Override
        protected OutputStream createOutputStreamWithMode(
                org.apache.hadoop.fs.Path path, boolean overwrite, FsPermission permission)
                throws IOException {
            if (overwrite) {
                return Files.newOutputStream(pathToFile(path).toPath());
            }
            return Files.newOutputStream(
                    pathToFile(path).toPath(),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        }
    }
}
