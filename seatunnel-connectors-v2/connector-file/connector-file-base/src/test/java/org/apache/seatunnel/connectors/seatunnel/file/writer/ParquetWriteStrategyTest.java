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
    public void testParquetWriteInt96() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/parquet/int96");
        writeConfig.put("file_format_type", FileFormat.PARQUET.name());
        writeConfig.put("parquet_avro_write_timestamp_as_int96", "true");
        writeConfig.put("parquet_avro_write_fixed_as_int96", Arrays.asList("f3_bytes"));

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text", "f2_timestamp", "f3_bytes"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ConfigFactory.parseMap(writeConfig), writeRowType);
        ParquetWriteStrategy writeStrategy = new ParquetWriteStrategy(writeSinkConfig);
        ParquetReadStrategyTest.LocalConf hadoopConf =
                new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        writeStrategy.write(
                new SeaTunnelRow(new Object[] {"test", LocalDateTime.now(), new byte[12]}));
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
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertTrue(record.getField(0) instanceof String);
                        Assertions.assertTrue(record.getField(1) instanceof LocalDateTime);
                        Assertions.assertTrue(record.getField(2) instanceof LocalDateTime);
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
}
