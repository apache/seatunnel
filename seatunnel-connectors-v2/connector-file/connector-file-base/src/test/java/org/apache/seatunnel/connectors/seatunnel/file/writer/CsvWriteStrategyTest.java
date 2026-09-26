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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.CsvWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.CsvReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.Mockito;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class CsvWriteStrategyTest {
    private static final String TMP_PATH = "file:///tmp/seatunnel/csv/test";

    @Test
    public void testHeaderUsesDefaultFieldDelimiter() throws Exception {
        List<String> lines = writeCsvWithHeader("default", null, "UTF-8", "name", "a");

        Assertions.assertEquals("id,name", lines.get(0));
        Assertions.assertEquals("1,a", lines.get(1));
    }

    @Test
    public void testHeaderUsesMultiCharacterFieldDelimiter() throws Exception {
        List<String> lines = writeCsvWithHeader("multi", "||", "UTF-8", "name", "a");

        Assertions.assertEquals("id||name", lines.get(0));
        Assertions.assertEquals("1||a", lines.get(1));
    }

    @Test
    public void testHeaderUsesConfiguredEncoding() throws Exception {
        List<String> lines = writeCsvWithHeader("encoding", "|", "UTF-16LE", "名称", "测试");

        Assertions.assertEquals("id|名称", lines.get(0));
        Assertions.assertEquals("1|测试", lines.get(1));
    }

    /**
     * Runs one real CsvWriteStrategy lifecycle (beginTransaction, write, finishAndCloseFile, close)
     * against an in-memory Hadoop output stream and returns the produced file split into lines. The
     * mocked HadoopFileSystemProxy hands every requested path the same ByteArrayOutputStream, so
     * the returned bytes are exactly what the writer emitted; they are decoded with the SAME
     * charset passed as {@code encoding}, which is what lets the UTF-16LE case detect a header
     * written with the platform default charset instead of the configured one.
     *
     * @param directory unique per-test output directory suffix, keeping tmp paths distinct
     * @param fieldDelimiter field_delimiter to configure, or null to exercise the default comma
     * @param encoding charset name used both for the writer config and for decoding the output
     * @param fieldName second column name (non-ASCII in the encoding case on purpose)
     * @param fieldValue second column value written in the single data row
     * @return the decoded output split on line breaks: header first, then the data row
     */
    private List<String> writeCsvWithHeader(
            String directory,
            String fieldDelimiter,
            String encoding,
            String fieldName,
            String fieldValue)
            throws Exception {
        String outputPath = "file:///tmp/seatunnel/csv/" + directory;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(fileSystemProxy.getOutputStream(Mockito.anyString()))
                .thenReturn(new FSDataOutputStream(output, null));

        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", outputPath);
        writeConfig.put("path", outputPath);
        writeConfig.put("file_format_type", FileFormat.CSV.name());
        writeConfig.put("enable_header_write", true);
        writeConfig.put("encoding", encoding);
        if (fieldDelimiter != null) {
            writeConfig.put("field_delimiter", fieldDelimiter);
        }

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"id", fieldName},
                        new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        TestCsvWriteStrategy writeStrategy = new TestCsvWriteStrategy(writeSinkConfig);
        writeStrategy.setFileSystemProxy(fileSystemProxy);
        writeStrategy.setTransactionContext("test1", "test1", 0);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(new SeaTunnelRow(new Object[] {1, fieldValue}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();
        return Arrays.asList(
                new String(output.toByteArray(), Charset.forName(encoding)).split("\\R"));
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteInt96() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/csv/int96");
        writeConfig.put("file_format_type", FileFormat.CSV.name());

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        CsvWriteStrategy writeStrategy = new CsvWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(new SeaTunnelRow(new Object[] {1, "a", 20}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        CsvReadStrategy readStrategy = new CsvReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        readStrategy.setPluginConfig(ConfigFactory.empty());
        readStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertEquals(1, record.getField(0));
                        Assertions.assertEquals("a", record.getField(1));
                        Assertions.assertEquals(20, record.getField(2));
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(readFilePath, "test", readCollector);
        Assertions.assertEquals(1, readRows.size());
        readStrategy.close();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testCsv2() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/csv/int96");
        writeConfig.put("file_format_type", FileFormat.CSV.name());
        writeConfig.put("field_delimiter", ",");

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age"},
                        new SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        CsvWriteStrategy writeStrategy = new CsvWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(new SeaTunnelRow(new Object[] {1, "a", 20}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        CsvReadStrategy readStrategy = new CsvReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        readStrategy.setPluginConfig(ConfigFactory.empty());
        readStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertEquals(1, record.getField(0));
                        Assertions.assertEquals("a", record.getField(1));
                        Assertions.assertEquals(20, record.getField(2));
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(readFilePath, "test", readCollector);
        Assertions.assertEquals(1, readRows.size());
        readStrategy.close();
    }

    /**
     * Test subclass whose only job is to inject the mocked filesystem proxy and the transaction
     * identifiers that AbstractWriteStrategy normally receives through init()/beginTransaction()
     * wiring. It writes through the real production code paths; nothing is overridden, so the bytes
     * asserted by the tests come from the genuine writer logic.
     */
    private static class TestCsvWriteStrategy extends CsvWriteStrategy {

        private TestCsvWriteStrategy(FileSinkConfig fileSinkConfig) {
            super(fileSinkConfig);
        }

        private void setFileSystemProxy(HadoopFileSystemProxy proxy) {
            this.hadoopFileSystemProxy = proxy;
        }

        private void setTransactionContext(String jobId, String uuidPrefix, int subTaskIndex) {
            this.jobId = jobId;
            this.uuidPrefix = uuidPrefix;
            this.subTaskIndex = subTaskIndex;
        }
    }
}
