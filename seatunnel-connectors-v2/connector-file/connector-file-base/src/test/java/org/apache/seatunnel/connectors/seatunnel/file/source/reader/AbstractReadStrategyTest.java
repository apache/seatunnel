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

import org.apache.seatunnel.connectors.seatunnel.file.writer.ParquetReadStrategyTest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class AbstractReadStrategyTest {

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testReadDirectorySkipHiddenDirectories() throws Exception {
        AutoGenerateParquetData.generateTestData();
        try (ParquetReadStrategy parquetReadStrategy = new ParquetReadStrategy(); ) {
            ParquetReadStrategyTest.LocalConf localConf =
                    new ParquetReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
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
}
