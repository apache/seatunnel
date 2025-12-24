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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

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

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetTypeInt32WithLogicalTypeAnnotation() throws IOException {

        NativeParquetWriter.generateTestData();

        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(
                                new Path(NativeParquetWriter.DATA_FILE_PATH),
                                new Configuration()))) {

            MessageType schema = reader.getFileMetaData().getSchema();
            LogicalTypeAnnotation type = schema.getType("id").getLogicalTypeAnnotation();
            Assertions.assertTrue(type instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation);
        }

        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);
        SeaTunnelRowType seaTunnelRowTypeInfo =
                parquetReadStrategy.getSeaTunnelRowTypeInfo(NativeParquetWriter.DATA_FILE_PATH);
        Assertions.assertNotNull(seaTunnelRowTypeInfo);
        Assertions.assertEquals(seaTunnelRowTypeInfo.getFieldType(0).getTypeClass(), Integer.class);
        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(NativeParquetWriter.DATA_FILE_PATH, "", testCollector);
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWithUserConfigRowType() throws Exception {
        AutoGenerateParquetData.generateTestData();
        String path = AutoGenerateParquetData.DATA_FILE_PATH;

        URL conf = ParquetReadStrategyTest.class.getResource("/test_user_config_read_parquet.conf");
        Assertions.assertNotNull(conf);
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);

        ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        parquetReadStrategy.init(localConf);

        SeaTunnelRowType configRowType = catalogTable.getSeaTunnelRowType();
        parquetReadStrategy.getSeaTunnelRowTypeInfoWithUserConfigRowType(path, configRowType);

        TestCollector testCollector = new TestCollector();
        parquetReadStrategy.read(path, "default", testCollector);
        List<SeaTunnelRow> rows = testCollector.getRows();
        SeaTunnelRow row = rows.get(0);

        // Verify whether the data type and type conversion are correct
        // id convert to String
        Assertions.assertEquals(String.class, row.getField(0).getClass());
        Assertions.assertEquals(String.class, row.getField(1).getClass());
        // salary convert to Double
        Assertions.assertEquals(Double.class, row.getField(2).getClass());
        Assertions.assertTrue(row.getField(3) instanceof String[]);
        // age convert to Long
        Assertions.assertEquals(Long.class, row.getField(4).getClass());
        Assertions.assertEquals(Boolean.class, row.getField(5).getClass());
        // score convert to Decimal
        Assertions.assertEquals(BigDecimal.class, row.getField(6).getClass());
        Assertions.assertEquals(BigDecimal.class, row.getField(7).getClass());
        Assertions.assertEquals(LocalDate.class, row.getField(8).getClass());
        Assertions.assertEquals(LocalDateTime.class, row.getField(9).getClass());
        Assertions.assertEquals(HashMap.class, row.getField(10).getClass());
        Assertions.assertEquals(byte[].class, row.getField(11).getClass());
        // binary_as_string convert to String
        Assertions.assertEquals(String.class, row.getField(12).getClass());

        Assertions.assertEquals("1", row.getField(0));
        Assertions.assertEquals("Alice", row.getField(1));
        Assertions.assertEquals(50000.0, row.getField(2));
        String[] skills = (String[]) row.getField(3);
        Assertions.assertEquals(2, skills.length);
        Assertions.assertEquals("Java", skills[0]);
        Assertions.assertEquals("Python", skills[1]);
        Assertions.assertEquals(30L, row.getField(4));
        Assertions.assertEquals(true, row.getField(5));
        Assertions.assertEquals(new BigDecimal("98.50"), row.getField(6));
        Assertions.assertEquals(new BigDecimal("1198.02"), row.getField(7));
        Assertions.assertNotNull(row.getField(8));
        Assertions.assertNotNull(row.getField(9));
        Assertions.assertTrue(((HashMap<?, ?>) row.getField(10)).containsKey("department"));
        Assertions.assertArrayEquals(
                "binary data example".getBytes(StandardCharsets.UTF_8), (byte[]) row.getField(11));
        Assertions.assertEquals("binary_as_string", row.getField(12));

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

            // create schema, which includes various data types
            String schemaString =
                    "{\"type\":\"record\",\"name\":\"User\",\"fields\":["
                            + "{\"name\":\"id\",\"type\":\"int\"},"
                            + "{\"name\":\"name\",\"type\":\"string\"},"
                            + "{\"name\":\"salary\",\"type\":\"float\"},"
                            + "{\"name\":\"skills\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                            + "{\"name\":\"age\",\"type\":\"int\"},"
                            + "{\"name\":\"active\",\"type\":\"boolean\"},"
                            + "{\"name\":\"score\",\"type\":\"double\"},"
                            + "{\"name\":\"budget\",\"type\":{\"type\":\"fixed\",\"name\":\"BudgetDecimal\",\"size\":8,\"logicalType\":\"decimal\",\"precision\":8,\"scale\":2}},"
                            + "{\"name\":\"join_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                            + "{\"name\":\"created_at\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                            + "{\"name\":\"properties\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
                            + "{\"name\":\"binary_data\",\"type\":\"bytes\"},"
                            + "{\"name\":\"binary_as_string\",\"type\":\"bytes\"}"
                            + "]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            Configuration conf = new Configuration();
            Path file = new Path(DATA_FILE_PATH);

            ParquetWriter<GenericRecord> writer =
                    AvroParquetWriter.<GenericRecord>builder(file)
                            .withSchema(schema)
                            .withConf(conf)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build();

            // create first record
            GenericRecord record1 = new GenericData.Record(schema);
            record1.put("id", 1);
            record1.put("name", "Alice");
            record1.put("salary", 50000.0);
            record1.put("age", 30);
            record1.put("active", true);
            record1.put("score", 98.5f);
            record1.put("created_at", System.currentTimeMillis());

            // Date type
            record1.put("join_date", 20289);

            // Decimal type
            BigDecimal budget = new BigDecimal("1198.02");
            Schema.Field budgetField = schema.getField("budget");
            Schema budgetSchema = budgetField.schema();
            Conversions.DecimalConversion decimalConversion = new Conversions.DecimalConversion();
            GenericFixed budgetFixed =
                    decimalConversion.toFixed(budget, budgetSchema, budgetSchema.getLogicalType());
            record1.put("budget", budgetFixed);

            // Array type
            GenericArray<Utf8> skills1 =
                    new GenericData.Array<>(2, schema.getField("skills").schema());
            skills1.add(new Utf8("Java"));
            skills1.add(new Utf8("Python"));
            record1.put("skills", skills1);

            // Map type
            Map<Utf8, Utf8> properties1 = new HashMap<>();
            properties1.put(new Utf8("department"), new Utf8("Engineering"));
            properties1.put(new Utf8("location"), new Utf8("Beijing"));
            record1.put("properties", properties1);

            // Binary type
            record1.put(
                    "binary_data",
                    ByteBuffer.wrap("binary data example".getBytes(StandardCharsets.UTF_8)));
            record1.put(
                    "binary_as_string",
                    ByteBuffer.wrap("binary_as_string".getBytes(StandardCharsets.UTF_8)));

            writer.write(record1);

            // create second record
            GenericRecord record2 = new GenericData.Record(schema);
            record2.put("id", 2);
            record2.put("name", "Bob");
            record2.put("salary", 60000.0);
            record2.put("age", 35);
            record2.put("active", false);
            record2.put("score", 89.2f);
            record2.put("created_at", System.currentTimeMillis() - 86400000);

            // Date type
            record2.put("join_date", 20288);

            // Decimal type
            BigDecimal budget2 = new BigDecimal("2394.13");
            Schema.Field budgetField2 = schema.getField("budget");
            Schema budgetSchema2 = budgetField2.schema();
            GenericFixed budgetFixed2 =
                    decimalConversion.toFixed(
                            budget2, budgetSchema2, budgetSchema2.getLogicalType());
            record2.put("budget", budgetFixed2);

            GenericArray<Utf8> skills2 =
                    new GenericData.Array<>(2, schema.getField("skills").schema());
            skills2.add(new Utf8("C++"));
            skills2.add(new Utf8("Go"));
            record2.put("skills", skills2);

            Map<Utf8, Utf8> properties2 = new HashMap<>();
            properties2.put(new Utf8("department"), new Utf8("Marketing"));
            properties2.put(new Utf8("location"), new Utf8("Shanghai"));
            record2.put("properties", properties2);

            record2.put(
                    "binary_data",
                    ByteBuffer.wrap("another binary example".getBytes(StandardCharsets.UTF_8)));
            record2.put(
                    "binary_as_string",
                    ByteBuffer.wrap("another binary_as_string".getBytes(StandardCharsets.UTF_8)));

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

    /** Write data based on the Parquet native api */
    public static class NativeParquetWriter {

        public static final String DATA_FILE_PATH = "/tmp/data_native.parquet";

        // 1. Define Parquet Native Schema (MessageType)
        public static MessageType createSchema() {
            return Types.buildMessage()
                    .required(INT32)
                    .as(LogicalTypeAnnotation.intType(32, true))
                    .named("id")
                    .named("User");
        }

        // 2. write data
        public static void generateTestData() throws IOException {
            deleteFile();
            MessageType schema = createSchema();
            Configuration conf = new Configuration();

            GroupWriteSupport.setSchema(schema, conf);

            Path file = new Path(DATA_FILE_PATH);
            try (ParquetWriter<Group> writer =
                    ExampleParquetWriter.builder(file)
                            .withConf(conf)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .build()) {

                Group record1 = new SimpleGroup(schema);
                record1.add("id", 1);

                writer.write(record1);
            }
        }

        private static void deleteFile() {
            File parquetFile = new File(DATA_FILE_PATH);
            if (parquetFile.exists()) {
                parquetFile.delete();
            }
        }
    }
}
