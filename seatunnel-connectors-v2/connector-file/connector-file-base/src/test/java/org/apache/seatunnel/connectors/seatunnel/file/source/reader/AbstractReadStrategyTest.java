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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class AbstractReadStrategyTest {

    @Test
    void testSafeSliceUsesSeekForSeekableStream() throws Exception {
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        TrackingSeekableInputStream in = new TrackingSeekableInputStream(data);

        try (InputStream sliced = AbstractReadStrategy.safeSlice(in, 5, 3)) {
            byte[] buffer = new byte[10];
            int n = sliced.read(buffer);
            Assertions.assertEquals(3, n);
            Assertions.assertEquals("567", new String(buffer, 0, n, StandardCharsets.UTF_8));
            Assertions.assertTrue(in.seekCalled);
        }
    }

    @Test
    void testSafeSliceReadsToEndWhenLengthIsNegative() throws Exception {
        byte[] data = "0123456789".getBytes(StandardCharsets.UTF_8);
        TrackingSeekableInputStream in = new TrackingSeekableInputStream(data);

        try (InputStream sliced = AbstractReadStrategy.safeSlice(in, 5, -1)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[4];
            int n;
            while ((n = sliced.read(buffer)) != -1) {
                out.write(buffer, 0, n);
            }
            Assertions.assertEquals("56789", new String(out.toByteArray(), StandardCharsets.UTF_8));
            Assertions.assertTrue(in.seekCalled);
        }
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testReadDirectorySkipHiddenDirectories() throws Exception {
        AutoGenerateParquetData.generateTestData();
        try (ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy(); ) {
            LocalFileSystemConf.LocalConf localConf =
                    new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
            parquetReadStrategy.init(localConf);
            List<String> list =
                    parquetReadStrategy.getFileNamesByPath(AutoGenerateParquetData.DATA_FILE_PATH);
            Assertions.assertEquals(1, list.size());
            Assertions.assertTrue(
                    list.get(0).endsWith(AutoGenerateParquetData.DATA_FILE_PATH_KEEP));
        } finally {
            AutoGenerateParquetData.deleteFile(AutoGenerateParquetData.DATA_FILE_PATH);
        }
    }

    public static class AutoGenerateParquetData {

        public static final String DATA_FILE_PATH = "/tmp/tmp_1";
        public static final String DATA_FILE_PATH_KEEP = "/tmp/tmp_1/dt=20241230/00000";
        public static final String DATA_FILE_PATH_IGNORE = "/tmp/tmp_1/.hive-stage/00000";

        public static void generateTestData() throws IOException {
            deleteFile(DATA_FILE_PATH);
            createFile(DATA_FILE_PATH_KEEP);
            createFile(DATA_FILE_PATH_IGNORE);
        }

        public static void write(String filePath) throws IOException {
            String schemaString =
                    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\": \"array\", \"items\": {\"type\": \"array\", \"items\": \"bytes\"}}},{\"name\":\"id2\",\"type\":{\"type\": \"array\", \"items\": {\"type\": \"array\", \"items\": \"bytes\"}}},{\"name\":\"long\",\"type\":\"long\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            Configuration conf = new Configuration();

            Path file = new Path(filePath);

            ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(file)
                            .withSchema(schema)
                            .withConf(conf)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build();

            GenericRecord record1 = new GenericData.Record(schema);
            GenericArray<GenericData.Array<Utf8>> id =
                    new GenericData.Array<>(2, schema.getField("id").schema());
            id.add(new GenericData.Array<>(2, schema.getField("id").schema().getElementType()));
            id.add(new GenericData.Array<>(2, schema.getField("id").schema().getElementType()));
            record1.put("id", id);
            record1.put("id2", id);
            record1.put("long", Long.MAX_VALUE);
            writer.write(record1);
            writer.close();
        }

        public static void createFile(String dir) throws IOException {
            File f2 = new File(dir);
            if (!f2.exists()) {
                if (!f2.getParentFile().exists()) {
                    boolean b = f2.getParentFile().mkdirs();
                    Assertions.assertTrue(b);
                }
                write(f2.getPath());
            }
        }

        public static void deleteFile(String file) {
            File parquetFile = new File(file);
            if (parquetFile.exists()) {
                if (parquetFile.isDirectory()) {
                    File[] l = parquetFile.listFiles();
                    if (l != null) {
                        for (File s : l) {
                            deleteFile(s.getPath());
                        }
                    }
                    boolean b = parquetFile.delete();
                    Assertions.assertTrue(b);
                } else {
                    boolean b = parquetFile.delete();
                    Assertions.assertTrue(b);
                }
            }
        }
    }

    private static class TrackingSeekableInputStream extends InputStream implements Seekable {
        private final byte[] data;
        private int pos;
        private boolean seekCalled;

        private TrackingSeekableInputStream(byte[] data) {
            this.data = data;
            this.pos = 0;
        }

        @Override
        public int read() {
            if (pos >= data.length) {
                return -1;
            }
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void seek(long newPos) {
            this.seekCalled = true;
            this.pos = (int) newPos;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public boolean seekToNewSource(long targetPos) {
            return false;
        }
    }

    @Test
    void testBothStartAndEndWithinRange() throws Exception {
        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date startDateStr = dateFormat.parse("2024-01-01 00:00:00");
            Date endDateStr = dateFormat.parse("2024-12-31 00:00:00");

            long modificationTime =
                    new SimpleDateFormat("yyyy-MM-dd").parse("2024-06-01").getTime();

            strategy.fileModifiedStartDate = startDateStr;
            strategy.fileModifiedEndDate = endDateStr;

            FileStatus fileStatus =
                    new FileStatus(0L, false, 0, 0, modificationTime, 0, null, null, null, null);
            boolean result = strategy.filterFileByModificationDate(fileStatus);
            Assertions.assertTrue(result);
        }
    }

    @Test
    void testOnlyEndDateOutOfRange() throws Exception {

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date endDateStr = dateFormat.parse("2024-07-01 00:00:00");

            strategy.fileModifiedStartDate = null;
            strategy.fileModifiedEndDate = endDateStr;

            long modificationTime =
                    new SimpleDateFormat("yyyy-MM-dd").parse("2024-06-01").getTime();

            FileStatus fileStatus =
                    new FileStatus(0L, false, 0, 0, modificationTime, 0, null, null, null, null);
            boolean result = strategy.filterFileByModificationDate(fileStatus);
            Assertions.assertTrue(result);
        }
    }

    @Test
    void testOnlyEndDateOutOfRangeWithHour() throws Exception {

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date endDateStr = dateFormat.parse("2024-07-01 14:00:00");

            strategy.fileModifiedStartDate = null;
            strategy.fileModifiedEndDate = endDateStr;

            long modificationTime = dateFormat.parse("2024-07-01 13:00:00").getTime();

            FileStatus fileStatus =
                    new FileStatus(0L, false, 0, 0, modificationTime, 0, null, null, null, null);
            boolean result = strategy.filterFileByModificationDate(fileStatus);
            Assertions.assertTrue(result);
        }
    }

    @Test
    void testNoDateSet() throws Exception {

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            strategy.fileModifiedStartDate = null;
            strategy.fileModifiedEndDate = null;
            FileStatus fileStatus =
                    new FileStatus(
                            0L, false, 0, 0, System.currentTimeMillis(), 0, null, null, null, null);
            boolean result = strategy.filterFileByModificationDate(fileStatus);
            Assertions.assertTrue(result);
        }
    }

    @Test
    void testOnlyStartDateOutOfRange() throws Exception {

        try (CsvReadStrategy strategy = new CsvReadStrategy()) {
            Date startDateStr =
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2024-04-01 00:00:00");

            strategy.fileModifiedStartDate = startDateStr;
            strategy.fileModifiedEndDate = null;

            long modificationTime =
                    new SimpleDateFormat("yyyy-MM-dd").parse("2024-06-01").getTime();

            FileStatus fileStatus =
                    new FileStatus(0L, false, 0, 0, modificationTime, 0, null, null, null, null);
            boolean result = strategy.filterFileByModificationDate(fileStatus);
            Assertions.assertTrue(result);
        }
    }

    @Test
    public void testSetCatalogTableShouldNotThrowWhenFileListIsEmpty() {
        Config pluginConfig = ConfigFactory.parseMap(buildBasePluginConfigWithPartitions());
        CatalogTable catalogTable = buildCatalogTable();

        Assertions.assertAll(
                () -> {
                    try (ReadStrategy strategy = new TextReadStrategy()) {
                        assertSetCatalogTableWithEmptyFileNames(
                                strategy, pluginConfig, catalogTable);
                    }
                },
                () -> {
                    try (ReadStrategy strategy = new CsvReadStrategy()) {
                        assertSetCatalogTableWithEmptyFileNames(
                                strategy, pluginConfig, catalogTable);
                    }
                },
                () -> {
                    try (ReadStrategy strategy = new ExcelReadStrategy()) {
                        assertSetCatalogTableWithEmptyFileNames(
                                strategy, pluginConfig, catalogTable);
                    }
                },
                () -> {
                    try (ReadStrategy strategy = new XmlReadStrategy()) {
                        assertSetCatalogTableWithEmptyFileNames(
                                strategy, pluginConfig, catalogTable);
                    }
                },
                () -> {
                    try (ReadStrategy strategy = new JsonReadStrategy()) {
                        assertSetCatalogTableWithEmptyFileNames(
                                strategy, pluginConfig, catalogTable);
                    }
                });
    }

    @Test
    public void testGetSeaTunnelRowTypeInfoShouldNotThrowWhenFileListIsEmpty() throws Exception {
        Config pluginConfig = ConfigFactory.parseMap(buildBasePluginConfigWithPartitions());

        try (TextReadStrategy textReadStrategy = new TextReadStrategy()) {
            textReadStrategy.setPluginConfig(pluginConfig);
            SeaTunnelRowType textRowType =
                    Assertions.assertDoesNotThrow(
                            () -> textReadStrategy.getSeaTunnelRowTypeInfo("/tmp/dt=2024-01-01"));
            Assertions.assertEquals(
                    "dt", textRowType.getFieldNames()[textRowType.getTotalFields() - 1]);
        }

        try (CsvReadStrategy csvReadStrategy = new CsvReadStrategy()) {
            csvReadStrategy.setPluginConfig(pluginConfig);
            SeaTunnelRowType csvRowType =
                    Assertions.assertDoesNotThrow(
                            () -> csvReadStrategy.getSeaTunnelRowTypeInfo("/tmp/dt=2024-01-01"));
            Assertions.assertEquals(
                    "dt", csvRowType.getFieldNames()[csvRowType.getTotalFields() - 1]);
        }
    }

    @Test
    void testTextReadStrategyShouldSkipUtf8Bom() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);
        TempCollector collector = new TempCollector();

        try (TextReadStrategy textReadStrategy = new TextReadStrategy()) {
            textReadStrategy.setPluginConfig(ConfigFactory.empty());
            textReadStrategy.setCatalogTable(catalogTable);
            textReadStrategy.readProcess(
                    new FileSourceSplit("test", "/tmp/bom.txt"),
                    collector,
                    new ByteArrayInputStream(
                            ("\uFEFF" + "alice\n").getBytes(StandardCharsets.UTF_8)),
                    new HashMap<>(),
                    "bom.txt");
        }

        Assertions.assertEquals(1, collector.getRows().size());
        Assertions.assertEquals("alice", collector.getRows().get(0).getField(0));
    }

    @Test
    void testJsonReadStrategyShouldSkipUtf8Bom() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);
        TempCollector collector = new TempCollector();

        try (JsonReadStrategy jsonReadStrategy = new JsonReadStrategy()) {
            jsonReadStrategy.setPluginConfig(ConfigFactory.empty());
            jsonReadStrategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));
            jsonReadStrategy.setCatalogTable(catalogTable);
            jsonReadStrategy.readProcess(
                    new FileSourceSplit("test", "/tmp/bom.json"),
                    collector,
                    new ByteArrayInputStream(
                            ("\uFEFF" + "{\"name\":\"alice\"}\n").getBytes(StandardCharsets.UTF_8)),
                    new HashMap<>(),
                    "bom.json");
        }

        Assertions.assertEquals(1, collector.getRows().size());
        Assertions.assertEquals("alice", collector.getRows().get(0).getField(0));
    }

    @Test
    void testXmlReadStrategyShouldSkipUtf8Bom() throws Exception {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        CatalogTable catalogTable = CatalogTableUtil.getCatalogTable("test", rowType);
        Map<String, Object> pluginConfig = new HashMap<>();
        pluginConfig.put(FileBaseSourceOptions.XML_ROW_TAG.key(), "row");
        pluginConfig.put(FileBaseSourceOptions.XML_USE_ATTR_FORMAT.key(), false);
        TempCollector collector = new TempCollector();

        try (XmlReadStrategy xmlReadStrategy = new XmlReadStrategy()) {
            xmlReadStrategy.setPluginConfig(ConfigFactory.parseMap(pluginConfig));
            xmlReadStrategy.init(new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT));
            xmlReadStrategy.setCatalogTable(catalogTable);
            xmlReadStrategy.readProcess(
                    new FileSourceSplit("test", "/tmp/bom.xml"),
                    collector,
                    new ByteArrayInputStream(
                            ("\uFEFF" + "<rows><row><name>alice</name></row></rows>")
                                    .getBytes(StandardCharsets.UTF_8)),
                    new HashMap<>(),
                    "bom.xml");
        }

        Assertions.assertEquals(1, collector.getRows().size());
        Assertions.assertEquals("alice", collector.getRows().get(0).getField(0));
    }

    @Test
    void testResolveRelativePathWithSftpUri() {
        String basePath = "sftp://server:22/path";
        String fullFilePath = "sftp://server:22/path/sub/file.txt";
        Assertions.assertEquals(
                "sub/file.txt", AbstractReadStrategy.resolveRelativePath(basePath, fullFilePath));
    }

    @Test
    void testResolveRelativePathWithFtpUri() {
        String basePath = "ftp://server:21/tmp/seatunnel/read";
        String fullFilePath = "ftp://server:21/tmp/seatunnel/read/file.txt";
        Assertions.assertEquals(
                "file.txt", AbstractReadStrategy.resolveRelativePath(basePath, fullFilePath));
    }

    @Test
    void testResolveRelativePathWithCustomSchemeUri() {
        String basePath = "default.default_sftp://sftp:22/tmp/seatunnel/update/src";
        String fullFilePath = "default.default_sftp://sftp:22/tmp/seatunnel/update/src/test.bin_0";
        Assertions.assertEquals(
                "test.bin_0", AbstractReadStrategy.resolveRelativePath(basePath, fullFilePath));
    }

    private static Map<String, Object> buildBasePluginConfigWithPartitions() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSourceOptions.FILE_PATH.key(), "/tmp/dt=2024-01-01");
        return config;
    }

    private static CatalogTable buildCatalogTable() {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});
        return CatalogTableUtil.getCatalogTable("test", rowType);
    }

    private static void assertSetCatalogTableWithEmptyFileNames(
            ReadStrategy readStrategy, Config pluginConfig, CatalogTable catalogTable) {
        readStrategy.setPluginConfig(pluginConfig);
        Assertions.assertDoesNotThrow(() -> readStrategy.setCatalogTable(catalogTable));
        SeaTunnelRowType actualRowType = readStrategy.getActualSeaTunnelRowTypeInfo();
        Assertions.assertArrayEquals(new String[] {"id", "dt"}, actualRowType.getFieldNames());
    }
}
