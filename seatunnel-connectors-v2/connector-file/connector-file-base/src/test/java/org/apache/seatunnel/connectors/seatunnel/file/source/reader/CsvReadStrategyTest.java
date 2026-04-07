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

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class CsvReadStrategyTest {

    @Test
    public void testReadCsv() throws Exception {
        URL resource = CsvReadStrategyTest.class.getResource("/test.csv");
        String path = Paths.get(resource.toURI()).toString();
        CsvReadStrategy csvReadStrategy = new CsvReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        csvReadStrategy.init(localConf);
        csvReadStrategy.getFileNamesByPath(path);
        csvReadStrategy.setPluginConfig(ConfigFactory.empty());
        csvReadStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        TestCollector testCollector = new TestCollector();
        csvReadStrategy.read(path, "", testCollector);

        Assertions.assertEquals(2, testCollector.getRows().size());
        Assertions.assertEquals(1, testCollector.getRows().get(0).getField(0));
        Assertions.assertEquals("a", testCollector.getRows().get(0).getField(1));
        Assertions.assertEquals(10, testCollector.getRows().get(0).getField(2));
        Assertions.assertEquals(2, testCollector.getRows().get(1).getField(0));
        Assertions.assertEquals("b", testCollector.getRows().get(1).getField(1));
        Assertions.assertEquals(100, testCollector.getRows().get(1).getField(2));
    }

    @Test
    public void testReadComplexCsv() throws Exception {
        URL resource = CsvReadStrategyTest.class.getResource("/test-csv.csv");
        String path = Paths.get(resource.toURI()).toString();
        CsvReadStrategy csvReadStrategy = new CsvReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        csvReadStrategy.init(localConf);
        csvReadStrategy.getFileNamesByPath(path);
        System.setProperty("field_delimiter", ";");
        csvReadStrategy.setPluginConfig(ConfigFactory.systemProperties());
        csvReadStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        TestCollector testCollector = new TestCollector();
        csvReadStrategy.read(path, "", testCollector);

        Assertions.assertEquals(2, testCollector.getRows().size());
        Assertions.assertEquals(1, testCollector.getRows().get(0).getField(0));
        Assertions.assertEquals(
                "b" + System.lineSeparator() + "a", testCollector.getRows().get(0).getField(1));
        Assertions.assertEquals(10, testCollector.getRows().get(0).getField(2));
        Assertions.assertEquals(2, testCollector.getRows().get(1).getField(0));
        Assertions.assertEquals("b", testCollector.getRows().get(1).getField(1));
        Assertions.assertEquals(100, testCollector.getRows().get(1).getField(2));
    }

    @Test
    public void testSpecialQuoteCharForCsvRead() throws Exception {
        URL resource =
                CsvReadStrategyTest.class.getResource("/csv/special_quote_char_break_line.csv");
        String path = Paths.get(resource.toURI()).toString();
        CsvReadStrategy csvReadStrategy = new CsvReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        csvReadStrategy.init(localConf);
        csvReadStrategy.getFileNamesByPath(path);
        csvReadStrategy.setPluginConfig(ConfigFactory.parseMap(getOptionsForSpecialQuoteChar()));
        csvReadStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        TestCollector testCollector = new TestCollector();
        csvReadStrategy.read(path, "", testCollector);
        final List<SeaTunnelRow> rows = testCollector.getRows();
        Assertions.assertEquals(4, rows.size());
        if (isWindows()) {
            Assertions.assertEquals("harry\r\n potter", rows.get(0).getField(1));
        } else {
            Assertions.assertEquals("harry\n potter", rows.get(0).getField(1));
        }
        Assertions.assertEquals("tom", rows.get(1).getField(1));
        Assertions.assertEquals("Rose`Wang", rows.get(2).getField(1));
        if (isWindows()) {
            Assertions.assertEquals("Jock\r\nLi`Li", rows.get(3).getField(1));
        } else {
            Assertions.assertEquals("Jock\nLi`Li", rows.get(3).getField(1));
        }
    }

    @Test
    public void testUtf8BomCsvRead() throws Exception {
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age", "gender"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE,
                                    BasicType.INT_TYPE,
                                    BasicType.STRING_TYPE
                                }));
        URL resource = CsvReadStrategyTest.class.getResource("/csv/utf8_bom_with_header.csv");
        Map<String, Object> csvBomOptions = getCsvBomOptions(true);
        checkCsvBomRead(resource, csvBomOptions, catalogTable);

        URL resource1 = CsvReadStrategyTest.class.getResource("/csv/utf8_bom_without_header.csv");
        Map<String, Object> csvBomOptions1 = getCsvBomOptions(false);
        checkCsvBomRead(resource1, csvBomOptions1, catalogTable);
    }

    private void checkCsvBomRead(
            URL resource, Map<String, Object> csvBomOptions, CatalogTable catalogTable)
            throws Exception {
        String path = Paths.get(resource.toURI()).toString();
        TestCollector testCollector;
        try (CsvReadStrategy csvReadStrategy = new CsvReadStrategy()) {
            LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
            csvReadStrategy.init(localConf);
            csvReadStrategy.getFileNamesByPath(path);
            csvReadStrategy.setPluginConfig(ConfigFactory.parseMap(csvBomOptions));
            csvReadStrategy.setCatalogTable(catalogTable);
            testCollector = new TestCollector();
            csvReadStrategy.read(path, "", testCollector);
        }
        final List<SeaTunnelRow> rows = testCollector.getRows();
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(9821, rows.get(0).getField(0));
        Assertions.assertEquals("hawk", rows.get(0).getField(1));
        Assertions.assertEquals(37, rows.get(0).getField(2));
        Assertions.assertEquals("M", rows.get(0).getField(3));
        Assertions.assertEquals(9822, rows.get(1).getField(0));
        Assertions.assertEquals("jack", rows.get(1).getField(1));
        Assertions.assertEquals(18, rows.get(1).getField(2));
        Assertions.assertEquals("M", rows.get(1).getField(3));
    }

    private boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    private Map<String, Object> getOptionsForSpecialQuoteChar() {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.QUOTE_CHAR.key(), "`");
        map.put(FileBaseSourceOptions.ESCAPE_CHAR.key(), "\"");
        return map;
    }

    private Map<String, Object> getCsvBomOptions(boolean withHeader) {
        Map<String, Object> map = new HashMap<>();
        map.put(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key(), withHeader);
        return map;
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
}
