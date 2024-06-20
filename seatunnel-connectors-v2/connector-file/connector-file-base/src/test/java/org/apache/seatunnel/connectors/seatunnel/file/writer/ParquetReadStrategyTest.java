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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class ParquetReadStrategyTest {
    @Test
    public void testParquetRead1() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/timestamp_as_int64.parquet");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo = parquetReadStrategy.getSeaTunnelRowTypeInfo(path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, "", testCollector);
    }

    @Test
    public void testParquetRead2() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/hive.parquet");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo = parquetReadStrategy.getSeaTunnelRowTypeInfo(path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, "", testCollector);
    }

    @Test
    public void testParquetReadUseSystemDefaultTimeZone() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/timestamp_as_int64.parquet");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo = parquetReadStrategy.getSeaTunnelRowTypeInfo(path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        int index = seaTunnelRowTypeInfo.indexOf("c_timestamp");
        TimeZone tz1 = TimeZone.getTimeZone("Asia/Shanghai");
        TimeZone.setDefault(tz1);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, "", testCollector);
        LocalDateTime time1 = (LocalDateTime) testCollector.getRows().get(0).getField(index);

        TimeZone tz2 = TimeZone.getTimeZone("UTC");
        TimeZone.setDefault(tz2);
        TestCollector testCollector2 = new TestCollector();
        parquetReadStrategy.read(path, "", testCollector2);
        LocalDateTime time2 = (LocalDateTime) testCollector2.getRows().get(0).getField(index);

        Assertions.assertTrue(time1.isAfter(time2));
        Assertions.assertEquals(
                time1.atZone(tz1.toZoneId()).withZoneSameInstant(tz2.toZoneId()).toLocalDateTime(),
                time2);
    }

    @Test
    public void testParquetReadProjection1() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/timestamp_as_int96.parquet");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_parquet.conf");
        Assertions.assertNotNull(resource);
        Assertions.assertNotNull(conf);
        String path = Paths.get(resource.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        parquetReadStrategy.setPluginConfig(pluginConfig);
        SeaTunnelRowType seaTunnelRowTypeInfo = parquetReadStrategy.getSeaTunnelRowTypeInfo(path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, "", testCollector);
        List<SeaTunnelRow> rows = testCollector.getRows();
        for (SeaTunnelRow row : rows) {
            Assertions.assertEquals(row.getField(0).getClass(), Long.class);
            Assertions.assertEquals(row.getField(1).getClass(), Byte.class);
            Assertions.assertEquals(row.getField(2).getClass(), Short.class);
            Assertions.assertEquals(row.getField(0), 40000000000L);
            Assertions.assertEquals(row.getField(1), (byte) 1);
            Assertions.assertEquals(row.getField(2), (short) 1);
        }
    }

    @Test
    public void testParquetReadProjection2() throws Exception {
        URL resource = ParquetReadStrategyTest.class.getResource("/hive.parquet");
        URL conf = OrcReadStrategyTest.class.getResource("/test_read_parquet2.conf");
        Assertions.assertNotNull(resource);
        Assertions.assertNotNull(conf);
        String path = Paths.get(resource.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        parquetReadStrategy.setPluginConfig(pluginConfig);
        SeaTunnelRowType seaTunnelRowTypeInfo = parquetReadStrategy.getSeaTunnelRowTypeInfo(path);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        log.info(seaTunnelRowTypeInfo.toString());
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, "", testCollector);
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetReadArray() throws Exception {
        AutoGenerateParquetData.generateTestData();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(AutoGenerateParquetData.DATA_FILE_PATH);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        Assertions.assertEquals(seaTunnelRowTypeInfo.getFieldType(3).getClass(), ArrayType.class);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(AutoGenerateParquetData.DATA_FILE_PATH, "1", testCollector);
        List<SeaTunnelRow> rows = testCollector.getRows();
        SeaTunnelRow seaTunnelRow = rows.get(0);
        Assertions.assertEquals(seaTunnelRow.getField(1).toString(), "Alice");
        String[] arrayData = (String[]) seaTunnelRow.getField(3);
        Assertions.assertEquals(arrayData.length, 2);
        Assertions.assertEquals(arrayData[0], "Java");
        AutoGenerateParquetData.deleteFile();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetReadUnsupportedType() throws Exception {
        AutoGenerateParquetDataWithUnsupportedType.generateTestData();
        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRuntimeException exception =
                Assertions.assertThrows(
                        SeaTunnelRuntimeException.class,
                        () ->
                                parquetReadStrategy.getSeaTunnelRowTypeInfo(
                                        AutoGenerateParquetDataWithUnsupportedType.DATA_FILE_PATH));
        Assertions.assertEquals(
                "ErrorCode:[COMMON-20], ErrorDescription:['Parquet' table 'default.default.default' unsupported get catalog table with field data types"
                        + " '{\"id\":\"required group id (LIST) {\\n  repeated group array (LIST) {\\n    repeated binary array;\\n  }\\n}\",\"id2\":\"required group id2 (LIST) {\\n  repeated group array (LIST)"
                        + " {\\n    repeated binary array;\\n  }\\n}\"}']",
                exception.getMessage());
        AutoGenerateParquetData.deleteFile();
    }

    public static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        public List<SeaTunnelRow> getRows() {
            return rows;
        }

        @Override
        public void collect(SeaTunnelRow record) {
            log.info(record.toString());
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

    public static class AutoGenerateParquetData {

        public static final String DATA_FILE_PATH = "/tmp/data.parquet";

        public static void generateTestData() throws IOException {
            deleteFile();
            String schemaString =
                    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"salary\",\"type\":\"double\"},{\"name\":\"skills\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            Configuration conf = new Configuration();

            Path file = new Path(DATA_FILE_PATH);

            ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(file)
                            .withSchema(schema)
                            .withConf(conf)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build();

            GenericRecord record1 = new GenericData.Record(schema);
            record1.put("id", 1);
            record1.put("name", "Alice");
            record1.put("salary", 50000.0);
            GenericArray<Utf8> skills1 =
                    new GenericData.Array<>(2, schema.getField("skills").schema());
            skills1.add(new Utf8("Java"));
            skills1.add(new Utf8("Python"));
            record1.put("skills", skills1);
            writer.write(record1);

            GenericRecord record2 = new GenericData.Record(schema);
            record2.put("id", 2);
            record2.put("name", "Bob");
            record2.put("salary", 60000.0);
            GenericArray<Utf8> skills2 =
                    new GenericData.Array<>(2, schema.getField("skills").schema());
            skills2.add(new Utf8("C++"));
            skills2.add(new Utf8("Go"));
            record2.put("skills", skills2);
            writer.write(record2);

            writer.close();
        }

        public static void deleteFile() {
            File parquetFile = new File(DATA_FILE_PATH);
            if (parquetFile.exists()) {
                parquetFile.delete();
            }
        }
    }

    public static class AutoGenerateParquetDataWithUnsupportedType {

        public static final String DATA_FILE_PATH = "/tmp/data_unsupported.parquet";

        public static void generateTestData() throws IOException {
            deleteFile();
            String schemaString =
                    "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":{\"type\": \"array\", \"items\": {\"type\": \"array\", \"items\": \"bytes\"}}},{\"name\":\"id2\",\"type\":{\"type\": \"array\", \"items\": {\"type\": \"array\", \"items\": \"bytes\"}}},{\"name\":\"long\",\"type\":\"long\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            Configuration conf = new Configuration();

            Path file = new Path(DATA_FILE_PATH);

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

        public static void deleteFile() {
            File parquetFile = new File(DATA_FILE_PATH);
            if (parquetFile.exists()) {
                parquetFile.delete();
            }
        }
    }
}
