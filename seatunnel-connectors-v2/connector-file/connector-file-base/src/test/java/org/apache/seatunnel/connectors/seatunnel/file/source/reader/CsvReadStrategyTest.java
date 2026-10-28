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
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class CsvReadStrategyTest {

    @ParameterizedTest
    @ValueSource(strings = {"\n", "\r\n", "\r"})
    public void testSkipPreambleBeforeCsvHeader(String lineSeparator) throws Exception {
        String content =
                String.join(
                        lineSeparator,
                        "export generated today",
                        "name,id,age",
                        "Alice,1,10",
                        "Bob,2,20");
        List<SeaTunnelRow> rows = readCsvContent(content, true, 1);
        assertSimpleRows(rows);
    }

    @Test
    public void testSkipNonCsvPreambleBeforeHeader() throws Exception {
        List<SeaTunnelRow> rows =
                readCsvContent(
                        "\uFEFF\"unclosed quote in preamble\nreport\nid,name,age\n1,Alice,10\n2,Bob,20\n",
                        true,
                        2);
        assertSimpleRows(rows);
    }

    @Test
    public void testSkipPreamblePreservesMultilineCsvRecords() throws Exception {
        List<SeaTunnelRow> rows =
                readCsvContent("report\nid,name,age\n1,\"Alice\nSmith\",10\n2,Bob,20\n", true, 1);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertArrayEquals(new Object[] {1, "Alice\nSmith", 10}, rows.get(0).getFields());
        Assertions.assertArrayEquals(new Object[] {2, "Bob", 20}, rows.get(1).getFields());
    }

    @Test
    public void testSkipPreambleWithoutCsvHeader() throws Exception {
        assertSimpleRows(readCsvContent("report\n1,Alice,10\n2,Bob,20\n", false, 1));
    }

    @Test
    public void testReadCsvHeaderWithoutSkipping() throws Exception {
        assertSimpleRows(readCsvContent("id,name,age\n1,Alice,10\n2,Bob,20\n", true, 0));
    }

    @Test
    public void testReadCsvWithoutHeaderOrSkipping() throws Exception {
        assertSimpleRows(readCsvContent("1,Alice,10\n2,Bob,20\n", false, 0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSkipAllCsvLines(boolean useHeader) throws Exception {
        Assertions.assertTrue(readCsvContent("report\n", useHeader, 1).isEmpty());
        Assertions.assertTrue(readCsvContent("", useHeader, 0).isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testSkipMoreCsvLinesThanAvailable(boolean useHeader) {
        FileConnectorException error =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () -> readCsvContent("report\n", useHeader, 2));
        Assertions.assertInstanceOf(IOException.class, error.getCause());
        Assertions.assertTrue(error.getCause().getMessage().contains("fewer lines than expected"));
    }

    @Test
    public void testInputClosedWhenCsvPreambleCannotBeSkipped() throws Exception {
        AtomicBoolean closed = new AtomicBoolean();
        try (CsvReadStrategy strategy = createCsvReadStrategy(true, 1)) {
            ByteArrayInputStream input =
                    new ByteArrayInputStream(new byte[0]) {
                        @Override
                        public void close() throws IOException {
                            closed.set(true);
                            super.close();
                        }
                    };
            Assertions.assertThrows(
                    FileConnectorException.class,
                    () ->
                            strategy.readProcess(
                                    new FileSourceSplit("test", "preamble.csv"),
                                    new TestCollector(),
                                    input,
                                    Collections.emptyMap(),
                                    "preamble.csv"));
        }
        Assertions.assertTrue(closed.get());
    }

    @Test
    public void testCsvByteRangeDoesNotSkipPreambleAgain() throws Exception {
        String content = "1,Alice,10\n2,Bob,20\n";
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        try (CsvReadStrategy strategy = createCsvReadStrategy(false, 100)) {
            strategy.enableSplitFile = true;
            TestCollector collector = new TestCollector();
            strategy.readProcess(
                    new FileSourceSplit("test", "split.csv", 0, bytes.length),
                    collector,
                    new ByteArrayInputStream(bytes),
                    Collections.emptyMap(),
                    "split.csv");
            assertSimpleRows(collector.getRows());
        }
    }

    private List<SeaTunnelRow> readCsvContent(String content, boolean useHeader, long skip)
            throws Exception {
        try (CsvReadStrategy strategy = createCsvReadStrategy(useHeader, skip)) {
            TestCollector collector = new TestCollector();
            strategy.readProcess(
                    new FileSourceSplit("test", "preamble.csv"),
                    collector,
                    new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                    Collections.emptyMap(),
                    "preamble.csv");
            return collector.getRows();
        }
    }

    private CsvReadStrategy createCsvReadStrategy(boolean useHeader, long skip) {
        CsvReadStrategy strategy = new CsvReadStrategy();
        Map<String, Object> options = new HashMap<>();
        options.put(FileBaseSourceOptions.CSV_USE_HEADER_LINE.key(), useHeader);
        options.put(FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key(), skip);
        options.put(FileBaseSourceOptions.PARSE_PARTITION_FROM_PATH.key(), false);
        strategy.setPluginConfig(ConfigFactory.parseMap(options));
        strategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        return strategy;
    }

    private void assertSimpleRows(List<SeaTunnelRow> rows) {
        Assertions.assertEquals(2, rows.size());
        Assertions.assertArrayEquals(new Object[] {1, "Alice", 10}, rows.get(0).getFields());
        Assertions.assertArrayEquals(new Object[] {2, "Bob", 20}, rows.get(1).getFields());
    }

    @Test
    public void testReadCsv() throws Exception {
        URL resource = CsvReadStrategyTest.class.getResource("/test.csv");
        String path = Paths.get(resource.toURI()).toString();
        CsvReadStrategy csvReadStrategy = new CsvReadStrategy();
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
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
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
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
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
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
            LocalFileSystemConf.LocalConf localConf =
                    new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
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
}
