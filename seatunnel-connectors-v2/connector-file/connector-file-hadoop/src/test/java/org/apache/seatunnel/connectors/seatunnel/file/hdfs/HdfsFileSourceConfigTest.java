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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.HdfsFileSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.source.SourceFlowTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@DisabledOnOs(value = OS.WINDOWS)
class HdfsFileSourceConfigTest {

    public static final String DATA_FILE_PATH1 = "/tmp/seatunnel/data1.parquet";
    public static final String DATA_FILE_PATH2 = "/tmp/seatunnel/data2.parquet";

    private static final String DEFAULT_FS = "file:///";

    @BeforeEach
    public void init() throws IOException {
        createParquetFile();
    }

    /** Test whether the Hadoop configuration and Catalog are generated correctly */
    @Test
    void testHadoopConfigAndCatalogTable() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HdfsSourceConfigOptions.FILE_PATH.key(), DATA_FILE_PATH1);
        configMap.put(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key(), "parquet");
        configMap.put(HdfsSourceConfigOptions.DEFAULT_FS.key(), DEFAULT_FS);

        Map<String, Object> schemaMap = new HashMap<>();
        Map<String, Object> filedMap = new HashMap<>();
        filedMap.put("id", "int");
        filedMap.put("name", "string");
        schemaMap.put("fields", filedMap);
        configMap.put(HdfsSourceConfigOptions.SCHEMA.key(), schemaMap);

        Config config = ConfigFactory.parseMap(configMap);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);

        HdfsFileSourceConfig sourceConfig = new HdfsFileSourceConfig(readonlyConfig);
        ReadStrategy readStrategy = sourceConfig.getReadStrategy();
        CatalogTable catalogTable = sourceConfig.getCatalogTable();
        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        HadoopConf hadoopConf = sourceConfig.getHadoopConfig();

        Assertions.assertNotNull(hadoopConf);
        Assertions.assertNotNull(catalogTable);
        Assertions.assertNotNull(seaTunnelRowType);

        // verify field names in seaTunnelRowType
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        assertEquals("id", fieldNames[0]);
        assertEquals("name", fieldNames[1]);

        // verify field types in seaTunnelRowType
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        assertEquals(BasicType.INT_TYPE, fieldTypes[0]);
        assertEquals(BasicType.STRING_TYPE, fieldTypes[1]);

        Assertions.assertInstanceOf(ParquetReadStrategy.class, readStrategy);
    }

    /** Test multi-file reading based on the parquet file format */
    @Test
    public void parquetFileMultiSourceRead() throws Exception {
        List<Map<String, Object>> tableConfigList = new ArrayList<>();

        Map<String, Object> tableConfig1 = new HashMap<>();
        // schema1
        Map<String, Object> schema1 = new HashMap<>();
        schema1.put("table", "db1.table1");

        tableConfig1.put(HdfsSourceConfigOptions.SCHEMA.key(), schema1);
        tableConfig1.put(HdfsSourceConfigOptions.FILE_PATH.key(), DATA_FILE_PATH1);
        tableConfig1.put(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key(), "parquet");
        tableConfig1.put(HdfsSourceConfigOptions.DEFAULT_FS.key(), DEFAULT_FS);

        Map<String, Object> tableConfig2 = new HashMap<>();
        // schema2
        Map<String, Object> schema2 = new HashMap<>();
        schema2.put("table", "db2.table2");
        tableConfig2.put(HdfsSourceConfigOptions.SCHEMA.key(), schema2);
        tableConfig2.put(HdfsSourceConfigOptions.FILE_PATH.key(), DATA_FILE_PATH2);
        tableConfig2.put(HdfsSourceConfigOptions.FILE_FORMAT_TYPE.key(), "parquet");
        tableConfig2.put(HdfsSourceConfigOptions.DEFAULT_FS.key(), DEFAULT_FS);

        tableConfigList.add(tableConfig1);
        tableConfigList.add(tableConfig2);

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HdfsSourceConfigOptions.TABLE_CONFIGS.key(), tableConfigList);

        // create parquet file
        createParquetFile();

        List<SeaTunnelRow> seaTunnelRows =
                SourceFlowTestUtils.runBatchWithCheckpointDisabled(
                        ReadonlyConfig.fromMap(configMap), new HdfsFileSourceFactory());

        Assertions.assertEquals(4, seaTunnelRows.size());

        Assertions.assertEquals("db1.table1", seaTunnelRows.get(0).getTableId());
        Assertions.assertEquals("db1.table1", seaTunnelRows.get(1).getTableId());
        Assertions.assertEquals("db2.table2", seaTunnelRows.get(2).getTableId());
        Assertions.assertEquals("db2.table2", seaTunnelRows.get(3).getTableId());

        Assertions.assertEquals(1, seaTunnelRows.get(0).getField(0));
        Assertions.assertEquals("hdfs_multi_source_read1", seaTunnelRows.get(0).getField(1));
        Assertions.assertEquals(2, seaTunnelRows.get(1).getField(0));
        Assertions.assertEquals("hdfs_multi_source_read2", seaTunnelRows.get(1).getField(1));
        Assertions.assertEquals(3, seaTunnelRows.get(2).getField(0));
        Assertions.assertEquals("hdfs_multi_source_read3", seaTunnelRows.get(2).getField(1));
        Assertions.assertEquals(4, seaTunnelRows.get(3).getField(0));
        Assertions.assertEquals("hdfs_multi_source_read4", seaTunnelRows.get(3).getField(1));
    }

    @AfterEach
    public void clear() throws IOException {
        deleteFile(DATA_FILE_PATH1);
        deleteFile(DATA_FILE_PATH2);
    }

    /** Create two parquet files for test */
    private void createParquetFile() throws IOException {

        // create avro schema
        String schemaJson =
                "{\"type\":\"record\",\"name\":\"test\",\"fields\":["
                        + "{\"name\":\"id\",\"type\":\"int\"},"
                        + "{\"name\":\"name\",\"type\":\"string\"}"
                        + "]}";
        Schema avroSchema = new Schema.Parser().parse(schemaJson);

        // create first parquet file
        Configuration conf1 = new Configuration();
        Path path1 = new Path(DATA_FILE_PATH1);

        try (ParquetWriter<GenericData.Record> writer =
                AvroParquetWriter.<GenericData.Record>builder(path1)
                        .withSchema(avroSchema)
                        .withConf(conf1)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build()) {

            // write first data
            GenericData.Record record1 = new GenericData.Record(avroSchema);
            record1.put("id", 1);
            record1.put("name", "hdfs_multi_source_read1");
            writer.write(record1);

            // write second data
            GenericData.Record record2 = new GenericData.Record(avroSchema);
            record2.put("id", 2);
            record2.put("name", "hdfs_multi_source_read2");
            writer.write(record2);
        }

        // create second file
        Configuration conf2 = new Configuration();
        Path path2 = new Path(DATA_FILE_PATH2);

        try (ParquetWriter<GenericData.Record> writer =
                AvroParquetWriter.<GenericData.Record>builder(path2)
                        .withSchema(avroSchema)
                        .withConf(conf2)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build()) {

            // write first data
            GenericData.Record record1 = new GenericData.Record(avroSchema);
            record1.put("id", 3);
            record1.put("name", "hdfs_multi_source_read3");
            writer.write(record1);

            // write second data
            GenericData.Record record2 = new GenericData.Record(avroSchema);
            record2.put("id", 4);
            record2.put("name", "hdfs_multi_source_read4");
            writer.write(record2);
        }
    }

    private void deleteFile(String path) throws IOException {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        FileSystem fileSystem = FileSystem.get(hadoopConf);

        fileSystem.delete(new Path(path), true);
    }
}
