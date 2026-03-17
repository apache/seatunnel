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
package org.apache.seatunnel.connectors.seatunnel.file.local;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.local.config.LocalFileHadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.local.source.split.LocalFileAccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.CsvReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.AccordingToSplitSizeSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.Getter;
import lombok.SneakyThrows;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class SplitFileStrategyTest {

    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "In the Windows environment, the newline character of the text file is '\\r\\n', and the byte length and newline character are inconsistent, which will cause the test case to fail.")
    @SneakyThrows
    @Test
    public void testSplitNoSkipHeader() {
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        try (AccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new AccordingToSplitSizeSplitStrategy(
                        new LocalFileHadoopConf(), "\n", 0L, "utf-8", 100L)) {
            final List<FileSourceSplit> splits =
                    localFileSplitStrategy.split("test.table", realPath);
            Assertions.assertEquals(2, splits.size());
            // check split-1
            Assertions.assertEquals(0, splits.get(0).getStart());
            Assertions.assertEquals(105, splits.get(0).getLength());
            // check split-2
            Assertions.assertEquals(105, splits.get(1).getStart());
            Assertions.assertEquals(85, splits.get(1).getLength());
        }
    }

    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "In the Windows environment, the newline character of the text file is '\\r\\n', and the byte length and newline character are inconsistent, which will cause the test case to fail.")
    @SneakyThrows
    @Test
    public void testSplitSkipHeader() {
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        try (AccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new AccordingToSplitSizeSplitStrategy(
                        new LocalFileHadoopConf(), "\n", 1L, "utf-8", 30L)) {
            final List<FileSourceSplit> splits =
                    localFileSplitStrategy.split("test.table", realPath);
            Assertions.assertEquals(4, splits.size());
            // check split-1
            Assertions.assertEquals(21, splits.get(0).getStart());
            Assertions.assertEquals(41, splits.get(0).getLength());
            // check split-2
            Assertions.assertEquals(62, splits.get(1).getStart());
            Assertions.assertEquals(43, splits.get(1).getLength());
            // check split-3
            Assertions.assertEquals(105, splits.get(2).getStart());
            Assertions.assertEquals(43, splits.get(2).getLength());
            // check split-4
            Assertions.assertEquals(148, splits.get(3).getStart());
            Assertions.assertEquals(42, splits.get(3).getLength());
        }
    }

    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "In the Windows environment, the newline character of the text file is '\\r\\n', and the byte length and newline character are inconsistent, which will cause the test case to fail.")
    @SneakyThrows
    @Test
    public void testSplitSkipHeaderLargeSize() {
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        try (AccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new AccordingToSplitSizeSplitStrategy(
                        new LocalFileHadoopConf(), "\n", 1L, "utf-8", 300L)) {
            final List<FileSourceSplit> splits =
                    localFileSplitStrategy.split("test.table", realPath);
            Assertions.assertEquals(1, splits.size());
            // check split-1
            Assertions.assertEquals(21, splits.get(0).getStart());
            Assertions.assertEquals(169, splits.get(0).getLength());
        }
    }

    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason =
                    "In the Windows environment, the newline character of the text file is '\\r\\n', and the byte length and newline character are inconsistent, which will cause the test case to fail.")
    @SneakyThrows
    @Test
    public void testSplitSkipHeaderSmallSize() {
        URL url = getClass().getClassLoader().getResource("test_split_csv_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        try (AccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new AccordingToSplitSizeSplitStrategy(
                        new LocalFileHadoopConf(), "\n", 1L, "utf-8", 3L)) {
            final List<FileSourceSplit> splits =
                    localFileSplitStrategy.split("test.table", realPath);
            Assertions.assertEquals(8, splits.size());
            // check split
            Assertions.assertEquals(21, splits.get(0).getStart());
            Assertions.assertEquals(42, splits.get(1).getStart());
            Assertions.assertEquals(62, splits.get(2).getStart());
            Assertions.assertEquals(82, splits.get(3).getStart());
            Assertions.assertEquals(105, splits.get(4).getStart());
            Assertions.assertEquals(126, splits.get(5).getStart());
            Assertions.assertEquals(148, splits.get(6).getStart());
            Assertions.assertEquals(169, splits.get(7).getStart());
        }
    }

    @SneakyThrows
    @Test
    public void testSplitSkipHeaderSpecialRowDelimiter() {
        URL url =
                getClass()
                        .getClassLoader()
                        .getResource("test_split_special_row_delimiter_data.txt");
        String realPath = Paths.get(url.toURI()).toString();
        try (AccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new AccordingToSplitSizeSplitStrategy(
                        new LocalFileHadoopConf(), "|^|", 1L, "utf-8", 80L)) {
            final List<FileSourceSplit> splits =
                    localFileSplitStrategy.split("test.table", realPath);
            Assertions.assertEquals(2, splits.size());
            // check split-1
            Assertions.assertEquals(23, splits.get(0).getStart());
            Assertions.assertEquals(92, splits.get(0).getLength());
            // check split-2
            Assertions.assertEquals(115, splits.get(1).getStart());
            Assertions.assertEquals(91, splits.get(1).getLength());
        }
    }

    @SneakyThrows
    @Test
    public void testSplitEmpty() {
        URL url = getClass().getClassLoader().getResource("test_split_empty_data.csv");
        String realPath = Paths.get(url.toURI()).toString();
        try (AccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new AccordingToSplitSizeSplitStrategy(
                        new LocalFileHadoopConf(), "\n", 1L, "utf-8", 300L)) {
            final List<FileSourceSplit> splits =
                    localFileSplitStrategy.split("test.table", realPath);
            Assertions.assertEquals(0, splits.size());
        }
    }

    @Test
    public void testUtf8BomCsvSplitRead() throws Exception {
        String realPath;
        final List<FileSourceSplit> splits;
        try (LocalFileAccordingToSplitSizeSplitStrategy localFileSplitStrategy =
                new LocalFileAccordingToSplitSizeSplitStrategy("\n", 0L, "utf-8", 1024 * 5L)) {
            URL url = getClass().getClassLoader().getResource("utf8_bom_split.csv");
            realPath = Paths.get(url.toURI()).toString();
            splits = localFileSplitStrategy.split("test.table", realPath);
        }
        Assertions.assertEquals(3, splits.size());

        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {
                                    "id",
                                    "username",
                                    "email",
                                    "phone",
                                    "address",
                                    "city",
                                    "province",
                                    "country",
                                    "zip_code",
                                    "register_date",
                                    "login_time",
                                    "total_score",
                                    "avg_score",
                                    "is_active"
                                },
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.STRING_TYPE,
                                    LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.BOOLEAN_TYPE
                                }));

        TestCollector testCollector;
        try (CsvReadStrategy csvReadStrategy = new CsvReadStrategy()) {
            LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
            csvReadStrategy.init(localConf);
            csvReadStrategy.getFileNamesByPath(realPath);
            csvReadStrategy.setPluginConfig(ConfigFactory.parseMap(getCsvBomOptions()));
            csvReadStrategy.setCatalogTable(catalogTable);
            testCollector = new TestCollector();
            for (FileSourceSplit split : splits) {
                csvReadStrategy.read(split, testCollector);
            }
        }
        List<SeaTunnelRow> rows = testCollector.getRows();
        Assertions.assertEquals(100, rows.size());

        for (int rowIdx = 0; rowIdx < rows.size(); rowIdx++) {
            SeaTunnelRow currentRow = rows.get(rowIdx);
            int columnCount = currentRow.getFields().length;
            for (int colIdx = 0; colIdx < columnCount; colIdx++) {
                Object fieldValue = currentRow.getField(colIdx);
                Assertions.assertNotNull(
                        fieldValue,
                        String.format(
                                "Field value at row %d, column %d is null",
                                rowIdx + 1, colIdx + 1));
            }
        }
    }

    private Map<String, Object> getCsvBomOptions() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key(), true);
        map.put(FileBaseSourceOptions.ENABLE_FILE_SPLIT.key(), true);
        map.put(FileBaseSourceOptions.FILE_SPLIT_SIZE.key(), 1024 * 5L);
        return map;
    }

    @Getter
    public static class TestCollector implements Collector<SeaTunnelRow> {

        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
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
}
