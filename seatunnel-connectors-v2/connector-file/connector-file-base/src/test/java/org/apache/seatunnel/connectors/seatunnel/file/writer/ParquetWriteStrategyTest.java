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
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.ParquetWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ParquetReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class ParquetWriteStrategyTest {
    private static final String TMP_PATH = "file:///tmp/seatunnel/parquet/int96/test";

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testCloseWriterWhenRollingFile() throws Exception {
        String tmpPath = "file:///tmp/seatunnel/parquet/rolling/test-" + System.nanoTime();
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", tmpPath);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/rolling");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("batch_size", 1);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        ParquetWriteStrategy writeStrategy =
                new ParquetWriteStrategy(
                        new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), rowType));
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", rowType));
        writeStrategy.init(hadoopConf, "rolling-test", "rolling-test", 0);
        writeStrategy.beginTransaction(1L);

        writeStrategy.write(new SeaTunnelRow(new Object[] {"first"}));
        writeStrategy.write(new SeaTunnelRow(new Object[] {"second"}));

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(tmpPath);
        Assertions.assertEquals(1, readFiles.size());
        String rolledFile = readFiles.get(0);
        Assertions.assertTrue(rolledFile.endsWith("_0.parquet"));
        readStrategy.getSeaTunnelRowTypeInfo(rolledFile);
        List<String> values = new ArrayList<>();
        readStrategy.read(rolledFile, "test", collectorForFirstField(values));
        Assertions.assertEquals(Arrays.asList("first"), values);

        writeStrategy.finishAndCloseFile();
        writeStrategy.close();
        Assertions.assertEquals(2, readStrategy.getFileNamesByPath(tmpPath).size());
        readStrategy.close();
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testParquetWriteInt96() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("parquet_avro_write_timestamp_as_int96", "true");
        writeConfig.put("parquet_avro_write_fixed_as_int96", Arrays.asList("F3_Bytes"));

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text", "f2_timestamp", "F3_Bytes", "createTime"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(
                new SeaTunnelRow(
                        new Object[] {
                            "test", LocalDateTime.now(), new byte[12], LocalDateTime.now()
                        }));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(
                                new org.apache.hadoop.fs.Path(readFilePath),
                                new Configuration()))) {
            FileMetaData metadata = reader.getFooter().getFileMetaData();
            Type f1Type = metadata.getSchema().getType("f1_text");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    f1Type.asPrimitiveType().getPrimitiveTypeName());
            Assertions.assertEquals(
                    LogicalTypeAnnotation.stringType(), f1Type.getLogicalTypeAnnotation());

            Type f2Type = metadata.getSchema().getType("f2_timestamp");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    f2Type.asPrimitiveType().getPrimitiveTypeName());
            Type f3Type = metadata.getSchema().getType("f3_bytes");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    f3Type.asPrimitiveType().getPrimitiveTypeName());
            Type createTimeType = metadata.getSchema().getType("createtime");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    createTimeType.asPrimitiveType().getPrimitiveTypeName());
        }

        SeaTunnelRowType readRowType = readStrategy.getSeaTunnelRowTypeInfo(readFilePath);
        Assertions.assertEquals(
                BasicType.STRING_TYPE.getSqlType(), readRowType.getFieldType(0).getSqlType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE.getSqlType(),
                readRowType.getFieldType(1).getSqlType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE.getSqlType(),
                readRowType.getFieldType(2).getSqlType());
        Assertions.assertEquals(
                LocalTimeType.LOCAL_DATE_TIME_TYPE.getSqlType(),
                readRowType.getFieldType(3).getSqlType());
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertTrue(record.getField(0) instanceof String);
                        Assertions.assertTrue(record.getField(1) instanceof LocalDateTime);
                        Assertions.assertTrue(record.getField(2) instanceof LocalDateTime);
                        Assertions.assertTrue(record.getField(3) instanceof LocalDateTime);
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
    public void testParquetWriteInt96WithMixedCaseTimestampColumn() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH + "-mixed-case");
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96-mixed-case");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("parquet_avro_write_timestamp_as_int96", "true");

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"createTime"},
                        new SeaTunnelDataType[] {LocalTimeType.LOCAL_DATE_TIME_TYPE});
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(writeSinkConfig);
        LocalFileSystemConf.LocalConf hadoopConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(new SeaTunnelRow(new Object[] {LocalDateTime.now()}));
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        ParquetReadStrategy readStrategy = new ParquetReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH + "-mixed-case");
        Assertions.assertEquals(1, readFiles.size());
        try (ParquetFileReader reader =
                ParquetFileReader.open(
                        HadoopInputFile.fromPath(
                                new org.apache.hadoop.fs.Path(readFiles.get(0)),
                                new Configuration()))) {
            FileMetaData metadata = reader.getFooter().getFileMetaData();
            Type createTimeType = metadata.getSchema().getType("createtime");
            Assertions.assertEquals(
                    PrimitiveType.PrimitiveTypeName.INT96,
                    createTimeType.asPrimitiveType().getPrimitiveTypeName());
        }
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
}
