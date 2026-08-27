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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkFactory;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.HdfsFileSink;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.HdfsFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.sink.SinkFlowTestUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DisabledOnOs(value = OS.WINDOWS)
public class HdfsFileSinkTest {
    private static final String ROW_NAME = "name";
    private static final String ROW_AGE = "age";
    private static final String FS_TARGET_PATH = "file:///tmp/seatunnel/hdfs_file_sink_test";
    private static final String FS_MULTI_TABLE_SINK_PATH =
            "file:///tmp/seatunnel/hdfs_multi_table_sink_test";
    private static final String DEFAULT_FS = "file:///";

    CatalogTable catalogTable =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            ROW_NAME, BasicType.STRING_TYPE, 1L, true, null, ""))
                            .column(
                                    PhysicalColumn.of(
                                            ROW_AGE, BasicType.INT_TYPE, 1L, true, null, ""))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    public void testHdfsFileSinkWithTextFormat() throws Exception {
        Map<String, Object> config = createBasicConfig();
        config.put(FileBaseSinkOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT.toString());
        config.put(FileBaseSinkOptions.FIELD_DELIMITER.key(), ",");

        List<SeaTunnelRow> rows = createTestRows();

        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable, ReadonlyConfig.fromMap(config), new HdfsFileSinkFactory(), rows);

        Path resultPath = new Path(FS_TARGET_PATH);
        FileSystem fs = resultPath.getFileSystem(new Configuration());

        FileStatus[] fileStatuses =
                fs.listStatus(resultPath, path -> path.getName().endsWith(".txt"));

        Assertions.assertTrue(fileStatuses.length > 0);

        List<String> readData = readFileContent(fileStatuses[0].getPath(), fs);

        Assertions.assertEquals("Alice,18", readData.get(0));
        Assertions.assertEquals("Bob,20", readData.get(1));

        fs.delete(new Path(FS_TARGET_PATH), true);
    }

    @Test
    public void testHdfsFileSinkWithParquetFormat() throws Exception {
        Map<String, Object> config = createBasicConfig();
        config.put(FileBaseSinkOptions.FILE_FORMAT_TYPE.key(), FileFormat.PARQUET.toString());

        List<SeaTunnelRow> rows = createTestRows();

        FileUtils.deleteDirectory(new File(FS_TARGET_PATH));
        SinkFlowTestUtils.runBatchWithCheckpointDisabled(
                catalogTable, ReadonlyConfig.fromMap(config), new HdfsFileSinkFactory(), rows);

        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FileSystem.get(hadoopConf);

        Path outputPath = new Path(FS_TARGET_PATH);
        FileStatus[] fileStatuses = fileSystem.listStatus(outputPath);

        Path parquetFile = null;
        for (FileStatus status : fileStatuses) {
            if (!status.isDirectory() && status.getPath().getName().endsWith(".parquet")) {
                parquetFile = status.getPath();
                break;
            }
        }

        Assertions.assertNotNull(parquetFile);

        ParquetReader<GenericRecord> reader =
                AvroParquetReader.<GenericRecord>builder(parquetFile).withConf(hadoopConf).build();

        GenericRecord record;
        int recordCount = 0;
        while ((record = reader.read()) != null) {
            recordCount++;
            if (recordCount == 1) {
                Assertions.assertEquals("Alice", record.get(ROW_NAME).toString());
                Assertions.assertEquals(18, record.get(ROW_AGE));
            } else if (recordCount == 2) {
                Assertions.assertEquals("Bob", record.get(ROW_NAME).toString());
                Assertions.assertEquals(20, record.get(ROW_AGE));
            }
        }

        Assertions.assertEquals(2, recordCount);
        reader.close();

        fileSystem.delete(new Path(FS_TARGET_PATH), true);
    }

    @Test
    public void testTextFormatWithMultiTableSink() throws Exception {
        String table1Path = FS_MULTI_TABLE_SINK_PATH + "/table1";
        String table2Path = FS_MULTI_TABLE_SINK_PATH + "/table2";

        Map<String, Object> basicConfig = createBasicConfig();
        basicConfig.put(FileBaseSinkOptions.FILE_FORMAT_TYPE.key(), FileFormat.TEXT.toString());
        basicConfig.put(FileBaseSinkOptions.FIELD_DELIMITER.key(), ",");

        Map<String, Object> table1Options = new HashMap<>(basicConfig);
        table1Options.put(FileBaseSinkOptions.FILE_PATH.key(), table1Path);

        Map<String, Object> table2Options = new HashMap<>(basicConfig);
        table2Options.put(FileBaseSinkOptions.FILE_PATH.key(), table2Path);

        TablePath tablePath1 = TablePath.of("test.table1");
        TablePath tablePath2 = TablePath.of("test.table2");

        HadoopConf hadoopConf = new HadoopConf(DEFAULT_FS);

        // create multi sink
        HdfsFileSink sink1 =
                new HdfsFileSink(hadoopConf, ReadonlyConfig.fromMap(table1Options), catalogTable);
        HdfsFileSink sink2 =
                new HdfsFileSink(hadoopConf, ReadonlyConfig.fromMap(table2Options), catalogTable);

        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(tablePath1, sink1);
        sinks.put(tablePath2, sink2);

        // create multi table factory context
        basicConfig.put(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(), 1);
        MultiTableFactoryContext multiTableContext =
                new MultiTableFactoryContext(
                        ReadonlyConfig.fromMap(basicConfig), getClass().getClassLoader(), sinks);

        // create test rows
        List<SeaTunnelRow> rows = createTestRows();

        // run multi table sink
        SinkFlowTestUtils.runBatchWithMultiTableSink(
                new MultiTableSinkFactory(), multiTableContext, rows, false, 1);

        FileSystem fs = FileSystem.get(new Configuration());

        FileStatus[] fileStatuses1 = fs.listStatus(new Path(table1Path));
        FileStatus[] fileStatuses2 = fs.listStatus(new Path(table2Path));

        Assertions.assertTrue(fileStatuses1.length > 0);
        Assertions.assertTrue(fileStatuses2.length > 0);

        List<String> readDataTable1 = readFileContent(fileStatuses1[0].getPath(), fs);
        List<String> readDataTable2 = readFileContent(fileStatuses2[0].getPath(), fs);

        Assertions.assertEquals("Alice,18", readDataTable1.get(0));
        Assertions.assertEquals("Bob,20", readDataTable2.get(0));

        fs.delete(new Path(FS_MULTI_TABLE_SINK_PATH), true);
    }

    private Map<String, Object> createBasicConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSinkOptions.DEFAULT_FS.key(), DEFAULT_FS);
        config.put(FileBaseSinkOptions.FILE_PATH.key(), FS_TARGET_PATH);
        config.put(FileBaseSinkOptions.IS_ENABLE_TRANSACTION.key(), false);
        config.put(FileBaseSinkOptions.HAVE_PARTITION.key(), false);
        config.put(FileBaseSinkOptions.ENCODING.key(), "UTF-8");
        return config;
    }

    private List<String> readFileContent(Path path, FileSystem fs) throws Exception {
        List<String> data = new ArrayList<>();
        try (FSDataInputStream inputStream = fs.open(path);
                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                data.add(line);
            }
        }

        return data;
    }

    private List<SeaTunnelRow> createTestRows() {
        List<SeaTunnelRow> rows = new ArrayList<>();

        // create first record
        SeaTunnelRow row1 = new SeaTunnelRow(new Object[] {"Alice", 18});
        row1.setTableId("test.table1");
        rows.add(row1);

        // create second record
        SeaTunnelRow row2 = new SeaTunnelRow(new Object[] {"Bob", 20});
        row2.setTableId("test.table2");
        rows.add(row2);

        return rows;
    }
}
