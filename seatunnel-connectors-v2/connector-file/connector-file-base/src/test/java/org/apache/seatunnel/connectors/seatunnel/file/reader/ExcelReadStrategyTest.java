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

package org.apache.seatunnel.connectors.seatunnel.file.reader;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.ExcelGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ExcelReadStrategy;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import lombok.Getter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExcelReadStrategyTest {

    @Test
    public void testExcelRead() throws IOException, URISyntaxException {
        URL excelFile = ExcelReadStrategyTest.class.getResource("/excel/test_read_excel.xlsx");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/test_read_excel.conf");
        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);
        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        SeaTunnelRow seaTunnelRow = testCollector.getRows().get(0);

        Assertions.assertEquals(seaTunnelRow.getArity(), 14);
        Assertions.assertEquals(seaTunnelRow.getField(0).getClass(), Byte.class);
        Assertions.assertEquals(seaTunnelRow.getField(1).getClass(), Short.class);
        Assertions.assertEquals(seaTunnelRow.getField(2).getClass(), Integer.class);
        Assertions.assertEquals(seaTunnelRow.getField(3).getClass(), Long.class);
        Assertions.assertEquals(seaTunnelRow.getField(4).getClass(), String.class);
        Assertions.assertEquals(seaTunnelRow.getField(5).getClass(), Double.class);
        Assertions.assertEquals(seaTunnelRow.getField(6).getClass(), Float.class);
        Assertions.assertEquals(seaTunnelRow.getField(7).getClass(), BigDecimal.class);
        Assertions.assertEquals(seaTunnelRow.getField(8).getClass(), Boolean.class);
        Assertions.assertEquals(seaTunnelRow.getField(9).getClass(), LinkedHashMap.class);
        Assertions.assertEquals(seaTunnelRow.getField(10).getClass(), String[].class);
        Assertions.assertEquals(seaTunnelRow.getField(11).getClass(), LocalDate.class);
        Assertions.assertEquals(seaTunnelRow.getField(12).getClass(), LocalDateTime.class);
        Assertions.assertEquals(seaTunnelRow.getField(13).getClass(), LocalTime.class);

        Assertions.assertEquals(seaTunnelRow.getField(0), (byte) 1);
        Assertions.assertEquals(seaTunnelRow.getField(1), (short) 22);
        Assertions.assertEquals(seaTunnelRow.getField(2), 333);
        Assertions.assertEquals(seaTunnelRow.getField(3), 4444L);
        Assertions.assertEquals(seaTunnelRow.getField(4), "Cosmos");
        Assertions.assertEquals(seaTunnelRow.getField(5), 5.555);
        Assertions.assertEquals(seaTunnelRow.getField(6), (float) 6.666);
        Assertions.assertEquals(seaTunnelRow.getField(7), new BigDecimal("7.78"));
        Assertions.assertEquals(seaTunnelRow.getField(8), Boolean.FALSE);
        Assertions.assertEquals(
                seaTunnelRow.getField(9),
                new LinkedHashMap<String, String>() {
                    {
                        put("name", "Ivan");
                        put("age", "26");
                    }
                });
        Assertions.assertArrayEquals(
                (String[]) seaTunnelRow.getField(10), new String[] {"Ivan", "Dusayi"});
        Assertions.assertEquals(
                seaTunnelRow.getField(11),
                DateUtils.parse("2024-01-31", DateUtils.Formatter.YYYY_MM_DD));
        Assertions.assertEquals(
                seaTunnelRow.getField(12),
                DateTimeUtils.parse(
                        "2024-01-31 16:00:48", DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS));
        Assertions.assertEquals(
                seaTunnelRow.getField(13),
                TimeUtils.parse("16:00:48", TimeUtils.Formatter.HH_MM_SS));

        SeaTunnelRow row2 = testCollector.getRows().get(1);
        Assertions.assertEquals(row2.getArity(), 14);
        // check number blank
        Assertions.assertEquals(row2.getField(0).getClass(), Byte.class);
        Assertions.assertNull(row2.getField(1));
        Assertions.assertNull(row2.getField(2));
        Assertions.assertNull(row2.getField(3));
        Assertions.assertEquals(row2.getField(4), "1");
        Assertions.assertNull(row2.getField(5));
        Assertions.assertNull(row2.getField(6));
        Assertions.assertNull(row2.getField(7));
        Assertions.assertNull(row2.getField(8));
        Assertions.assertNull(row2.getField(9));
        Assertions.assertNull(row2.getField(10));
        Assertions.assertNull(row2.getField(11));
        Assertions.assertNull(row2.getField(12));
        Assertions.assertNull(row2.getField(13));

        SeaTunnelRow row3 = testCollector.getRows().get(2);
        Assertions.assertEquals(row3.getArity(), 14);
        Assertions.assertEquals(row3.getField(0).getClass(), Byte.class);
        Assertions.assertNull(row3.getField(1));
        Assertions.assertNull(row3.getField(2));
        Assertions.assertNull(row3.getField(3));
        // check string blank
        Assertions.assertEquals(row3.getField(4), "");
        Assertions.assertNull(row3.getField(5));
        Assertions.assertNull(row3.getField(6));
        Assertions.assertNull(row3.getField(7));
        Assertions.assertNull(row3.getField(8));
        Assertions.assertNull(row3.getField(9));
        Assertions.assertNull(row3.getField(10));
        Assertions.assertNull(row3.getField(11));
        Assertions.assertNull(row3.getField(12));
        Assertions.assertNull(row3.getField(13));
    }

    @Test
    public void testExcelReadDateString() throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_date_string.xlsx");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/test_read_excel.conf");
        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);
        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        for (SeaTunnelRow seaTunnelRow : testCollector.getRows()) {
            Assertions.assertEquals(seaTunnelRow.getArity(), 14);
            Assertions.assertEquals(seaTunnelRow.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(seaTunnelRow.getField(1).getClass(), Short.class);
            Assertions.assertEquals(seaTunnelRow.getField(2).getClass(), Integer.class);
            Assertions.assertEquals(seaTunnelRow.getField(3).getClass(), Long.class);
            Assertions.assertEquals(seaTunnelRow.getField(4).getClass(), String.class);
            Assertions.assertEquals(seaTunnelRow.getField(5).getClass(), Double.class);
            Assertions.assertEquals(seaTunnelRow.getField(6).getClass(), Float.class);
            Assertions.assertEquals(seaTunnelRow.getField(7).getClass(), BigDecimal.class);
            Assertions.assertEquals(seaTunnelRow.getField(8).getClass(), Boolean.class);
            Assertions.assertEquals(seaTunnelRow.getField(9).getClass(), LinkedHashMap.class);
            Assertions.assertEquals(seaTunnelRow.getField(10).getClass(), String[].class);
            Assertions.assertEquals(seaTunnelRow.getField(11).getClass(), LocalDate.class);
            Assertions.assertEquals(seaTunnelRow.getField(12).getClass(), LocalDateTime.class);
            Assertions.assertEquals(seaTunnelRow.getField(13).getClass(), LocalTime.class);

            Assertions.assertEquals(seaTunnelRow.getField(0), (byte) 1);
            Assertions.assertEquals(seaTunnelRow.getField(1), (short) 22);
            Assertions.assertEquals(seaTunnelRow.getField(2), 333);
            Assertions.assertEquals(seaTunnelRow.getField(3), 4444L);
            Assertions.assertEquals(seaTunnelRow.getField(4), "Cosmos");
            Assertions.assertEquals(seaTunnelRow.getField(5), 5.555);
            Assertions.assertEquals(seaTunnelRow.getField(6), (float) 6.666);
            Assertions.assertEquals(seaTunnelRow.getField(7), new BigDecimal("7.78"));
            Assertions.assertEquals(seaTunnelRow.getField(8), Boolean.FALSE);
            Assertions.assertEquals(
                    seaTunnelRow.getField(9),
                    new LinkedHashMap<String, String>() {
                        {
                            put("name", "Ivan");
                            put("age", "26");
                        }
                    });
            Assertions.assertArrayEquals(
                    (String[]) seaTunnelRow.getField(10), new String[] {"Ivan", "Dusayi"});
            Assertions.assertEquals(
                    seaTunnelRow.getField(11),
                    DateUtils.parse("2024-01-31", DateUtils.Formatter.YYYY_MM_DD));
            Assertions.assertEquals(
                    seaTunnelRow.getField(12),
                    DateTimeUtils.parse(
                            "2024-01-31 16:00:48", DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS));
            Assertions.assertEquals(
                    seaTunnelRow.getField(13),
                    TimeUtils.parse("16:00:48", TimeUtils.Formatter.HH_MM_SS));
        }
    }

    @Test
    public void testEasyExcelRead() throws IOException, URISyntaxException {
        testLargeExcelRead(
                "/excel/test_read_excel_date_string.xlsx",
                "/excel/test_read_excel_data_string.conf",
                1);
        testLargeExcelRead("/excel/e2e.xls", "/excel/e2exls.conf", 5);
        testLargeExcelRead("/excel/e2e.xlsx", "/excel/e2exls.conf", 5);
    }

    @Test
    public void testEasyExcelReadColumnsUsesSourceColumnIndexes()
            throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_date_string.xlsx");
        URL conf =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_data_string.conf");
        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        Config pluginConfig =
                ConfigFactory.parseString("read_columns = [\"c_string\", \"c_int\"]")
                        .withFallback(ConfigFactory.parseFile(new File(conf.toURI())));
        ExcelReadStrategy readStrategy = new ExcelReadStrategy();
        readStrategy.setPluginConfig(pluginConfig);
        readStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        readStrategy.setCatalogTable(CatalogTableUtil.buildWithConfig(pluginConfig));

        TestCollector collector = new TestCollector();
        readStrategy.read(Paths.get(excelFile.toURI()).toString(), "", collector);

        Assertions.assertEquals(1, collector.getRows().size());
        SeaTunnelRow row = collector.getRows().get(0);
        Assertions.assertEquals(2, row.getArity());
        Assertions.assertEquals("Cosmos", row.getField(0));
        Assertions.assertEquals(333, row.getField(1));
    }

    @Test
    public void testReadColumnsMatchesExcelHeadersWithProjectedSchema()
            throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_date_string.xlsx");
        Assertions.assertNotNull(excelFile);
        for (String engine : new String[] {"POI", "EasyExcel"}) {
            for (String skipRows : new String[] {"", "skip_header_row_number = 2\n"}) {
                Config pluginConfig =
                        ConfigFactory.parseString(
                                "file_format_type = excel\n"
                                        + "excel_engine = "
                                        + engine
                                        + "\n"
                                        + "excel_use_header_line = true\n"
                                        + skipRows
                                        + "schema { columns = [{name = c_bytes, type = string}, "
                                        + "{name = c_int, type = int}, "
                                        + "{name = c_string, type = string}] }\n"
                                        + "read_columns = [c_string, c_int]");
                ExcelReadStrategy readStrategy = new ExcelReadStrategy();
                readStrategy.setPluginConfig(pluginConfig);
                readStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
                readStrategy.setCatalogTable(CatalogTableUtil.buildWithConfig(pluginConfig));

                TestCollector collector = new TestCollector();
                readStrategy.read(Paths.get(excelFile.toURI()).toString(), "", collector);

                Assertions.assertEquals(1, collector.getRows().size(), engine);
                SeaTunnelRow row = collector.getRows().get(0);
                Assertions.assertEquals("Cosmos", row.getField(0), engine);
                Assertions.assertEquals(333, row.getField(1), engine);
            }
        }
    }

    @Test
    public void testExcelHeaderWithoutMatchingSchemaKeepsPositionalReading()
            throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_date_string.xlsx");
        Assertions.assertNotNull(excelFile);
        for (String engine : new String[] {"POI", "EasyExcel"}) {
            Config pluginConfig =
                    ConfigFactory.parseString(
                            "file_format_type = excel\n"
                                    + "excel_engine = "
                                    + engine
                                    + "\n"
                                    + "skip_header_row_number = 1\n"
                                    + "schema { columns = [{name = first, type = string}, "
                                    + "{name = second, type = string}] }");
            ExcelReadStrategy readStrategy = new ExcelReadStrategy();
            readStrategy.setPluginConfig(pluginConfig);
            readStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
            readStrategy.setCatalogTable(CatalogTableUtil.buildWithConfig(pluginConfig));

            TestCollector collector = new TestCollector();
            readStrategy.read(Paths.get(excelFile.toURI()).toString(), "", collector);

            Assertions.assertEquals(1, collector.getRows().size(), engine);
            SeaTunnelRow row = collector.getRows().get(0);
            Assertions.assertEquals("1", row.getField(0), engine);
            Assertions.assertEquals("22", row.getField(1), engine);
        }
    }

    @Test
    public void testExcelPartialHeaderMatchWithoutOptInKeepsPositionalReading()
            throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_date_string.xlsx");
        Assertions.assertNotNull(excelFile);
        for (String engine : new String[] {"POI", "EasyExcel"}) {
            Config pluginConfig =
                    ConfigFactory.parseString(
                            "file_format_type = excel\n"
                                    + "excel_engine = "
                                    + engine
                                    + "\n"
                                    + "skip_header_row_number = 1\n"
                                    + "schema { columns = [{name = c_bytes, type = string}, "
                                    + "{name = second, type = string}] }");
            ExcelReadStrategy readStrategy = new ExcelReadStrategy();
            readStrategy.setPluginConfig(pluginConfig);
            readStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
            readStrategy.setCatalogTable(CatalogTableUtil.buildWithConfig(pluginConfig));

            TestCollector collector = new TestCollector();
            readStrategy.read(Paths.get(excelFile.toURI()).toString(), "", collector);

            Assertions.assertEquals(1, collector.getRows().size(), engine);
            SeaTunnelRow row = collector.getRows().get(0);
            Assertions.assertEquals("1", row.getField(0), engine);
            Assertions.assertEquals("22", row.getField(1), engine);
        }
    }

    @Test
    public void testExcelHeaderMissingProjectedFieldReturnsNull()
            throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_date_string.xlsx");
        Assertions.assertNotNull(excelFile);
        for (String engine : new String[] {"POI", "EasyExcel"}) {
            Config pluginConfig =
                    ConfigFactory.parseString(
                            "file_format_type = excel\n"
                                    + "excel_engine = "
                                    + engine
                                    + "\n"
                                    + "excel_use_header_line = true\n"
                                    + "schema { columns = [{name = c_string, type = string}, "
                                    + "{name = missing_field, type = string}] }\n"
                                    + "read_columns = [c_string, missing_field]");
            ExcelReadStrategy readStrategy = new ExcelReadStrategy();
            readStrategy.setPluginConfig(pluginConfig);
            readStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
            readStrategy.setCatalogTable(CatalogTableUtil.buildWithConfig(pluginConfig));

            TestCollector collector = new TestCollector();
            readStrategy.read(Paths.get(excelFile.toURI()).toString(), "", collector);

            Assertions.assertEquals(1, collector.getRows().size(), engine);
            SeaTunnelRow row = collector.getRows().get(0);
            Assertions.assertEquals("Cosmos", row.getField(0), engine);
            Assertions.assertNull(row.getField(1), engine);
        }
    }

    @Test
    public void testEasyExcelIgnoresPoiFileSizeLimit() throws IOException, URISyntaxException {
        URL excelFile = ExcelReadStrategyTest.class.getResource("/excel/e2e.xlsx");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/e2exls.conf");

        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseString("poi_excel_max_file_size = 1")
                        .withFallback(ConfigFactory.parseFile(new File(confPath)));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);

        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        Assertions.assertEquals(5, testCollector.getRows().size());
    }

    @Test
    public void testPoiRejectsExcelLargerThanConfiguredLimit()
            throws IOException, URISyntaxException {
        URL excelFile = ExcelReadStrategyTest.class.getResource("/excel/e2e.xlsx");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/e2exls.conf");

        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseString("poi_excel_max_file_size = 1")
                        .withFallback(ConfigFactory.parseFile(new File(confPath)))
                        .withoutPath("excel_engine");
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                excelReadStrategy.read(
                                        fileNamesByPath.get(0), "", new TestCollector()));
        Assertions.assertTrue(exception.getMessage().contains("larger than POI limit"));
        Assertions.assertTrue(exception.getMessage().contains("excel_engine = EasyExcel"));
    }

    @Test
    public void testPoiRejectsArchivedExcelEntryLargerThanConfiguredLimit()
            throws IOException, URISyntaxException {
        URL zipFile = ExcelReadStrategyTest.class.getResource("/excel/archive_zip/e2e_in_zip.zip");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/e2exls.conf");

        Assertions.assertNotNull(zipFile);
        Assertions.assertNotNull(conf);
        String zipFilePath = Paths.get(zipFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseString(
                                "poi_excel_max_file_size = 1\narchive_compress_codec = \"ZIP\"")
                        .withFallback(ConfigFactory.parseFile(new File(confPath)))
                        .withoutPath("excel_engine");
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(zipFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                excelReadStrategy.read(
                                        fileNamesByPath.get(0), "", new TestCollector()));
        Assertions.assertTrue(
                exception.getMessage().contains("larger than POI limit")
                        || exception.getMessage().contains("is %,d bytes, larger than POI limit"),
                "Expected archived entry guard error, got: " + exception.getMessage());
    }

    @Test
    public void testEasyExcelReadsArchivedExcelEntryAbovePoiLimit()
            throws IOException, URISyntaxException {
        URL zipFile = ExcelReadStrategyTest.class.getResource("/excel/archive_zip/e2e_in_zip.zip");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/e2exls.conf");

        Assertions.assertNotNull(zipFile);
        Assertions.assertNotNull(conf);
        String zipFilePath = Paths.get(zipFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseString(
                                "poi_excel_max_file_size = 1\narchive_compress_codec = \"ZIP\"")
                        .withFallback(ConfigFactory.parseFile(new File(confPath)));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(zipFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);

        TestCollector testCollector = new TestCollector();
        // EasyExcel streams rows lazily, so the POI size guard must NOT apply
        // to archived entries even when the configured limit is below the entry size.
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        Assertions.assertEquals(5, testCollector.getRows().size());
    }

    @Test
    public void testPoiAcceptsSmallExcelEntryInsideLargerArchive()
            throws IOException, URISyntaxException {
        // Regression for the archived-path size guard: the on-disk size of the
        // archive must NOT be used to judge an Excel entry bundled inside it. Build
        // a zip that is far larger than the configured POI limit on disk but whose
        // only Excel entry is well below the limit, then verify POI still reads it.
        // Previously the redundant POI-level guard re-statted the archive path and
        // falsely rejected such a small entry.
        URL excelFile = ExcelReadStrategyTest.class.getResource("/excel/e2e.xlsx");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/e2exls.conf");
        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);

        byte[] excelBytes = Files.readAllBytes(Paths.get(excelFile.toURI()));
        long entrySize = excelBytes.length;
        // Limit above the Excel entry, but below the padded archive on disk.
        long poiLimit = entrySize + 1024L;

        File archive = File.createTempFile("e2e_padded_", ".zip");
        archive.deleteOnExit();
        // STORED (uncompressed) padding so the archive's on-disk size is reliably
        // larger than the limit regardless of how the Excel entry compresses.
        byte[] padding = new byte[(int) (poiLimit * 4)];
        CRC32 crc = new CRC32();
        crc.update(padding);
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(archive))) {
            ZipEntry excelEntry = new ZipEntry("e2e.xlsx");
            zos.putNextEntry(excelEntry);
            zos.write(excelBytes);
            zos.closeEntry();

            ZipEntry paddingEntry = new ZipEntry("padding.bin");
            paddingEntry.setMethod(ZipEntry.STORED);
            paddingEntry.setSize(padding.length);
            paddingEntry.setCompressedSize(padding.length);
            paddingEntry.setCrc(crc.getValue());
            zos.putNextEntry(paddingEntry);
            zos.write(padding);
            zos.closeEntry();
        }

        Assertions.assertTrue(archive.length() > poiLimit, "archive must exceed the limit");
        Assertions.assertTrue(entrySize < poiLimit, "entry must be below the limit");

        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseString(
                                "poi_excel_max_file_size = "
                                        + poiLimit
                                        + "\narchive_compress_codec = \"ZIP\"")
                        .withFallback(ConfigFactory.parseFile(new File(confPath)))
                        .withoutPath("excel_engine");
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath =
                excelReadStrategy.getFileNamesByPath(archive.getAbsolutePath());
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);

        TestCollector testCollector = new TestCollector();
        // POI engine (default). The archive exceeds the limit, but the Excel entry
        // is below it, so the read must succeed rather than be falsely rejected.
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        Assertions.assertEquals(5, testCollector.getRows().size());
    }

    @Test
    public void testPoiReadKeepsNumericPrecision() throws IOException {
        File excelFile = writeNumericExcel();
        Config pluginConfig =
                ConfigFactory.parseString(
                        "skip_header_row_number = 1\n"
                                + "schema { fields { c_double = double, c_decimal = \"decimal(20, 4)\","
                                + " c_bigint = bigint, c_string = string } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(2, rows.size());
        // "General" display format would have produced 10.28571429 and 1012345.67
        Assertions.assertEquals(10.285714285, rows.get(0).getField(0));
        Assertions.assertEquals(new BigDecimal("1012345.6699"), rows.get(0).getField(1));
        Assertions.assertEquals(1234567890123L, rows.get(0).getField(2));
        // A numeric cell read into a string column keeps its displayed text
        Assertions.assertEquals("42", rows.get(0).getField(3));
        // A text cell holding a decimal that a double cannot hold is parsed in full
        Assertions.assertEquals(new BigDecimal("9215784885957046.9450"), rows.get(1).getField(1));
    }

    /**
     * DOUBLE and DECIMAL values written by the Excel sink are read back unchanged by both engines.
     * The DECIMAL values cover a number cell, a value with trailing zeros, a negative and a small
     * value, and a value with more digits than a double holds, which the sink writes as text.
     */
    @ParameterizedTest
    @ValueSource(strings = {"POI", "EasyExcel"})
    public void testSinkOutputRoundTripsDoubleAndDecimal(String excelEngine) throws IOException {
        Object[][] values = {
            {10.285714285, new BigDecimal("1012345.6699")},
            {7.142857142, new BigDecimal("5000.0000")},
            {-0.1, new BigDecimal("-3.2500")},
            {1.0E-5, new BigDecimal("0.0001")},
            {123456789.123, new BigDecimal("9215784885957046.9450")},
            // 17 significant digits: more than the 15 EasyExcel rounds its number value to
            {0.30000000000000004, new BigDecimal("12.5000")}
        };
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_double", "c_decimal"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, new DecimalType(20, 4)});
        File excelFile = writeWithExcelSink(rowType, values);
        Config pluginConfig =
                ConfigFactory.parseString(
                        "skip_header_row_number = 1\n"
                                + "excel_engine = "
                                + excelEngine
                                + "\n"
                                + "schema { fields { c_double = double,"
                                + " c_decimal = \"decimal(20, 4)\" } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(values.length, rows.size());
        for (int i = 0; i < values.length; i++) {
            Assertions.assertEquals(values[i][0], rows.get(i).getField(0), "c_double row " + i);
            // Number and text cells both read with the declared scale of 4
            Assertions.assertEquals(values[i][1], rows.get(i).getField(1), "c_decimal row " + i);
        }
    }

    /**
     * A decimal is read with the declared scale only when no digit is lost: a value with more
     * fractional digits, or one that does not fit the declared precision, is kept as it is.
     */
    @Test
    public void testDecimalReadsWithDeclaredScaleWithoutRounding() throws IOException {
        File excelFile = File.createTempFile("excel_decimal_scale_", ".xlsx");
        excelFile.deleteOnExit();
        String[] cells = {
            "12.5", "0", "1.123456", "123456789012345678", "1E+2147483647", "1000E+2147483647"
        };
        try (XSSFWorkbook workbook = new XSSFWorkbook();
                FileOutputStream output = new FileOutputStream(excelFile)) {
            Sheet sheet = workbook.createSheet("Sheet1");
            sheet.createRow(0).createCell(0).setCellValue("c_decimal");
            for (int i = 0; i < cells.length; i++) {
                sheet.createRow(i + 1).createCell(0).setCellValue(cells[i]);
            }
            workbook.write(output);
        }
        Config pluginConfig =
                ConfigFactory.parseString(
                        "skip_header_row_number = 1\n"
                                + "schema { fields { c_decimal = \"decimal(20, 4)\" } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(cells.length, rows.size());
        Assertions.assertEquals(new BigDecimal("12.5000"), rows.get(0).getField(0));
        Assertions.assertEquals(new BigDecimal("0.0000"), rows.get(1).getField(0));
        Assertions.assertEquals(new BigDecimal("1.123456"), rows.get(2).getField(0));
        Assertions.assertEquals(new BigDecimal("123456789012345678"), rows.get(3).getField(0));
        Assertions.assertEquals(new BigDecimal("1E+2147483647"), rows.get(4).getField(0));
        Assertions.assertEquals(new BigDecimal("1000E+2147483647"), rows.get(5).getField(0));
    }

    /** Numeric formulas and number cells preserve their stored double for every numeric type. */
    @ParameterizedTest
    @ValueSource(strings = {"POI", "EasyExcel"})
    public void testComputedNumbersReadWithoutRounding(String excelEngine) throws IOException {
        File excelFile = File.createTempFile("excel_computed_numbers_", ".xlsx");
        excelFile.deleteOnExit();
        try (XSSFWorkbook workbook = new XSSFWorkbook();
                FileOutputStream output = new FileOutputStream(excelFile)) {
            Sheet sheet = workbook.createSheet("Sheet1");
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("c_bigint");
            header.createCell(1).setCellValue("c_decimal");
            header.createCell(2).setCellValue("c_double");
            Row formulas = sheet.createRow(1);
            formulas.createCell(0).setCellFormula("(0.1+0.7)*10");
            formulas.createCell(1).setCellFormula("0.1+0.2");
            formulas.createCell(2).setCellFormula("0.1+0.2");
            Row numbers = sheet.createRow(2);
            numbers.createCell(0).setCellValue((0.1 + 0.7) * 10);
            numbers.createCell(1).setCellValue(0.1 + 0.2);
            numbers.createCell(2).setCellValue(0.1 + 0.2);
            Row realDigits = sheet.createRow(3);
            realDigits.createCell(0).setCellValue(123456789012345.67);
            realDigits.createCell(1).setCellValue(1234567.890123456);
            realDigits.createCell(2).setCellValue(1234567.890123456);
            // Keep low-order digits even when Excel's 15-digit display omits them.
            Row leadingNine = sheet.createRow(4);
            leadingNine.createCell(0).setCellValue(98765432109876.99);
            leadingNine.createCell(1).setCellValue(9.876543210987654);
            leadingNine.createCell(2).setCellValue(9.876543210987654);
            // EasyExcel reads the cached formula result, so store it
            workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();
            workbook.write(output);
        }
        Config pluginConfig =
                ConfigFactory.parseString(
                        "skip_header_row_number = 1\n"
                                + "excel_engine = "
                                + excelEngine
                                + "\n"
                                + "schema { fields { c_bigint = bigint,"
                                + " c_decimal = \"decimal(20, 9)\", c_double = double } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(4, rows.size());
        for (int i = 0; i < 2; i++) {
            Assertions.assertEquals(7L, rows.get(i).getField(0), "c_bigint row " + i);
            Assertions.assertEquals(
                    new BigDecimal("0.30000000000000004"),
                    rows.get(i).getField(1),
                    "c_decimal row " + i);
            Assertions.assertEquals(0.1 + 0.2, rows.get(i).getField(2), "c_double row " + i);
        }
        // Truncated as before, not rounded to the 15-digit 123456789012346
        Assertions.assertEquals(123456789012345L, rows.get(2).getField(0));
        Assertions.assertEquals(new BigDecimal("1234567.890123456"), rows.get(2).getField(1));
        Assertions.assertEquals(1234567.890123456, rows.get(2).getField(2));
        Assertions.assertEquals(98765432109876L, rows.get(3).getField(0));
        Assertions.assertEquals(new BigDecimal("9.876543210987654"), rows.get(3).getField(1));
        Assertions.assertEquals(9.876543210987654, rows.get(3).getField(2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"POI", "EasyExcel"})
    public void testDecimalNearIntegerValuesKeepStoredDigits(String excelEngine) throws IOException {
        File excelFile = File.createTempFile("excel_decimal_near_integer_", ".xlsx");
        excelFile.deleteOnExit();
        try (XSSFWorkbook workbook = new XSSFWorkbook();
                FileOutputStream output = new FileOutputStream(excelFile)) {
            Sheet sheet = workbook.createSheet("Sheet1");
            sheet.createRow(0).createCell(0).setCellValue("c_decimal");
            sheet.createRow(1).createCell(0).setCellValue(8.000000000000002);
            sheet.createRow(2).createCell(0).setCellValue(-8.000000000000002);
            workbook.write(output);
        }
        Config pluginConfig =
                ConfigFactory.parseString(
                        "skip_header_row_number = 1\n"
                                + "excel_engine = "
                                + excelEngine
                                + "\n"
                                + "schema { fields { c_decimal = \"decimal(20, 15)\" } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(new BigDecimal("8.000000000000002"), rows.get(0).getField(0));
        Assertions.assertEquals(new BigDecimal("-8.000000000000002"), rows.get(1).getField(0));
    }

    /**
     * Both engines read the stored double of {@code .xls} number cells and the cached result of a
     * formula. {@code 1/3} has 16 significant digits, one more than Excel's text of the result.
     */
    @ParameterizedTest
    @ValueSource(strings = {"POI", "EasyExcel"})
    public void testXlsReadKeepsNumericPrecision(String excelEngine) throws IOException {
        File excelFile = File.createTempFile("excel_numeric_precision_", ".xls");
        excelFile.deleteOnExit();
        try (HSSFWorkbook workbook = new HSSFWorkbook();
                FileOutputStream output = new FileOutputStream(excelFile)) {
            Sheet sheet = workbook.createSheet("Sheet1");
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("c_double");
            header.createCell(1).setCellValue("c_decimal");
            header.createCell(2).setCellValue("c_formula");
            Row row = sheet.createRow(1);
            row.createCell(0).setCellValue(10.285714285);
            row.createCell(1).setCellValue(1012345.6699);
            row.createCell(2).setCellFormula("1/3");
            // EasyExcel reads the cached formula result, so store it
            workbook.getCreationHelper().createFormulaEvaluator().evaluateAll();
            workbook.write(output);
        }
        Config pluginConfig =
                ConfigFactory.parseString(
                        "skip_header_row_number = 1\n"
                                + "excel_engine = "
                                + excelEngine
                                + "\n"
                                + "schema { fields { c_double = double,"
                                + " c_decimal = \"decimal(20, 4)\", c_formula = double } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(10.285714285, rows.get(0).getField(0));
        Assertions.assertEquals(new BigDecimal("1012345.6699"), rows.get(0).getField(1));
        Assertions.assertEquals(1.0 / 3, rows.get(0).getField(2));
    }

    /**
     * With {@code excel_use_header_line} the columns are mapped by header name, so the reader
     * schema can list them in another order; the stored numbers are still read in full.
     */
    @ParameterizedTest
    @ValueSource(strings = {"POI", "EasyExcel"})
    public void testHeaderMappedReadKeepsNumericPrecision(String excelEngine) throws IOException {
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"c_double", "c_decimal"},
                        new SeaTunnelDataType[] {BasicType.DOUBLE_TYPE, new DecimalType(20, 4)});
        File excelFile =
                writeWithExcelSink(
                        rowType, new Object[][] {{10.285714285, new BigDecimal("1012345.6699")}});
        Config pluginConfig =
                ConfigFactory.parseString(
                        "excel_use_header_line = true\n"
                                + "excel_engine = "
                                + excelEngine
                                + "\n"
                                + "schema { fields { c_decimal = \"decimal(20, 4)\","
                                + " c_double = double } }");

        List<SeaTunnelRow> rows = readExcel(excelFile, pluginConfig);

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(new BigDecimal("1012345.6699"), rows.get(0).getField(0));
        Assertions.assertEquals(10.285714285, rows.get(0).getField(1));
    }

    /**
     * Microsoft Excel stores 5.555 as {@code <v>5.5549999999999997</v>} and 6.666 as {@code
     * <v>6.6660000000000004</v>} (cells F2 and G2 of this file). Both engines read the value the
     * user typed, not the 17-digit stored text.
     */
    @ParameterizedTest
    @ValueSource(strings = {"POI", "EasyExcel"})
    public void testExcelWrittenNumberReadsAsTypedValue(String excelEngine)
            throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_formula.xlsx");
        Assertions.assertNotNull(excelFile);
        Config pluginConfig =
                ConfigFactory.parseString(
                        "sheet_name = \"Sheet1\"\n"
                                + "skip_header_row_number = 1\n"
                                + "excel_engine = "
                                + excelEngine
                                + "\n"
                                + "schema { fields { c_bytes = tinyint, c_short = smallint,"
                                + " c_int = int, c_bigint = bigint, c_string = string,"
                                + " c_decimal = \"decimal(10, 3)\", c_double = double } }");

        List<SeaTunnelRow> rows = readExcel(new File(excelFile.toURI()), pluginConfig);

        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals(new BigDecimal("5.555"), rows.get(0).getField(5));
        Assertions.assertEquals(6.666, rows.get(0).getField(6));
    }

    private File writeWithExcelSink(SeaTunnelRowType rowType, Object[][] values)
            throws IOException {
        FileSinkConfig fileSinkConfig = mock(FileSinkConfig.class);
        when(fileSinkConfig.getMaxRowsInMemory()).thenReturn(100);
        when(fileSinkConfig.getSheetName()).thenReturn("Sheet1");
        when(fileSinkConfig.getSheetMaxRows()).thenReturn(1048576);
        when(fileSinkConfig.getFieldDelimiter()).thenReturn(",");
        when(fileSinkConfig.getDateFormat()).thenReturn(DateUtils.Formatter.YYYY_MM_DD);
        when(fileSinkConfig.getDatetimeFormat())
                .thenReturn(DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
        when(fileSinkConfig.getTimeFormat()).thenReturn(TimeUtils.Formatter.HH_MM_SS);
        ExcelGenerator excelGenerator =
                new ExcelGenerator(Arrays.asList(0, 1), rowType, fileSinkConfig);
        for (Object[] row : values) {
            excelGenerator.writeData(new SeaTunnelRow(row));
        }
        File excelFile = File.createTempFile("excel_sink_round_trip_", ".xlsx");
        excelFile.deleteOnExit();
        try (FileOutputStream output = new FileOutputStream(excelFile)) {
            excelGenerator.flushAndCloseExcel(output);
        }
        return excelFile;
    }

    private File writeNumericExcel() throws IOException {
        File excelFile = File.createTempFile("excel_numeric_precision_", ".xlsx");
        excelFile.deleteOnExit();
        try (XSSFWorkbook workbook = new XSSFWorkbook();
                FileOutputStream output = new FileOutputStream(excelFile)) {
            Sheet sheet = workbook.createSheet("Sheet1");
            Row header = sheet.createRow(0);
            header.createCell(0).setCellValue("c_double");
            header.createCell(1).setCellValue("c_decimal");
            header.createCell(2).setCellValue("c_bigint");
            header.createCell(3).setCellValue("c_string");
            Row row = sheet.createRow(1);
            row.createCell(0).setCellValue(10.285714285);
            row.createCell(1).setCellValue(1012345.6699);
            row.createCell(2).setCellValue(1234567890123L);
            row.createCell(3).setCellValue(42);
            Row wide = sheet.createRow(2);
            wide.createCell(0).setCellValue(1.5);
            wide.createCell(1).setCellValue("9215784885957046.9450");
            wide.createCell(2).setCellValue(1);
            wide.createCell(3).setCellValue("x");
            workbook.write(output);
        }
        return excelFile;
    }

    private List<SeaTunnelRow> readExcel(File excelFile, Config pluginConfig) throws IOException {
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
        excelReadStrategy.setCatalogTable(CatalogTableUtil.buildWithConfig(pluginConfig));
        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(excelFile.getAbsolutePath(), "", testCollector);
        return testCollector.getRows();
    }

    private void testLargeExcelRead(String filePath, String configPath, int rowCount)
            throws IOException, URISyntaxException {
        URL excelFile = ExcelReadStrategyTest.class.getResource(filePath);
        URL conf = ExcelReadStrategyTest.class.getResource(configPath);

        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);

        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        Assertions.assertEquals(testCollector.getRows().size(), rowCount);
    }

    @Test
    public void testExcelReadFormulaXls() throws IOException, URISyntaxException {
        URL excelFile = ExcelReadStrategyTest.class.getResource("/excel/test_read_formula.xls");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/test_read_excel.conf");
        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);
        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        for (SeaTunnelRow seaTunnelRow : testCollector.getRows()) {
            Assertions.assertEquals(seaTunnelRow.getArity(), 14);
            Assertions.assertEquals(seaTunnelRow.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(seaTunnelRow.getField(1).getClass(), Short.class);
            Assertions.assertEquals(seaTunnelRow.getField(2).getClass(), Integer.class);
            Assertions.assertEquals(seaTunnelRow.getField(3).getClass(), Long.class);
            Assertions.assertEquals(seaTunnelRow.getField(4).getClass(), String.class);
            Assertions.assertEquals(seaTunnelRow.getField(5).getClass(), Double.class);
            Assertions.assertEquals(seaTunnelRow.getField(6).getClass(), Float.class);
            Assertions.assertEquals(seaTunnelRow.getField(7).getClass(), BigDecimal.class);
            Assertions.assertEquals(seaTunnelRow.getField(8).getClass(), Boolean.class);
            Assertions.assertEquals(seaTunnelRow.getField(9).getClass(), LinkedHashMap.class);
            Assertions.assertEquals(seaTunnelRow.getField(10).getClass(), String[].class);
            Assertions.assertEquals(seaTunnelRow.getField(11).getClass(), LocalDate.class);
            Assertions.assertEquals(seaTunnelRow.getField(12).getClass(), LocalDateTime.class);
            Assertions.assertEquals(seaTunnelRow.getField(13).getClass(), LocalTime.class);
            Assertions.assertEquals(seaTunnelRow.getField(0), (byte) 1);
            Assertions.assertEquals(seaTunnelRow.getField(1), (short) 22);
            Assertions.assertEquals(seaTunnelRow.getField(2), 333);
            Assertions.assertEquals(seaTunnelRow.getField(3), 355L);
            Assertions.assertEquals(seaTunnelRow.getField(4), "Cosmos");
            Assertions.assertEquals(seaTunnelRow.getField(5), 5.555);
            Assertions.assertEquals(seaTunnelRow.getField(6), (float) 6.666);
            Assertions.assertEquals(seaTunnelRow.getField(7), new BigDecimal("7.78"));
            Assertions.assertEquals(seaTunnelRow.getField(8), Boolean.FALSE);
            Assertions.assertEquals(
                    seaTunnelRow.getField(9),
                    new LinkedHashMap<String, String>() {
                        {
                            put("name", "Ivan");
                            put("age", "26");
                        }
                    });
            Assertions.assertArrayEquals(
                    (String[]) seaTunnelRow.getField(10), new String[] {"Ivan", "Dusayi"});
            Assertions.assertEquals(
                    seaTunnelRow.getField(11),
                    DateUtils.parse("2024-01-31", DateUtils.Formatter.YYYY_MM_DD));
            Assertions.assertEquals(
                    seaTunnelRow.getField(12),
                    DateTimeUtils.parse(
                            "2024-01-31 16:00:48", DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS));
            Assertions.assertEquals(
                    seaTunnelRow.getField(13),
                    TimeUtils.parse("16:00:48", TimeUtils.Formatter.HH_MM_SS));
        }
    }

    @Test
    public void testExcelReadFormula() throws IOException, URISyntaxException {
        URL excelFile =
                ExcelReadStrategyTest.class.getResource("/excel/test_read_excel_formula.xlsx");
        URL conf = ExcelReadStrategyTest.class.getResource("/excel/test_read_excel.conf");
        Assertions.assertNotNull(excelFile);
        Assertions.assertNotNull(conf);
        String excelFilePath = Paths.get(excelFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        ExcelReadStrategy excelReadStrategy = new ExcelReadStrategy();
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        excelReadStrategy.setPluginConfig(pluginConfig);
        excelReadStrategy.init(localConf);

        List<String> fileNamesByPath = excelReadStrategy.getFileNamesByPath(excelFilePath);
        CatalogTable userDefinedCatalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        excelReadStrategy.setCatalogTable(userDefinedCatalogTable);
        TestCollector testCollector = new TestCollector();
        excelReadStrategy.read(fileNamesByPath.get(0), "", testCollector);

        for (SeaTunnelRow seaTunnelRow : testCollector.getRows()) {
            Assertions.assertEquals(seaTunnelRow.getArity(), 14);
            Assertions.assertEquals(seaTunnelRow.getField(0).getClass(), Byte.class);
            Assertions.assertEquals(seaTunnelRow.getField(1).getClass(), Short.class);
            Assertions.assertEquals(seaTunnelRow.getField(2).getClass(), Integer.class);
            Assertions.assertEquals(seaTunnelRow.getField(3).getClass(), Long.class);
            Assertions.assertEquals(seaTunnelRow.getField(4).getClass(), String.class);
            Assertions.assertEquals(seaTunnelRow.getField(5).getClass(), Double.class);
            Assertions.assertEquals(seaTunnelRow.getField(6).getClass(), Float.class);
            Assertions.assertEquals(seaTunnelRow.getField(7).getClass(), BigDecimal.class);
            Assertions.assertEquals(seaTunnelRow.getField(8).getClass(), Boolean.class);
            Assertions.assertEquals(seaTunnelRow.getField(9).getClass(), LinkedHashMap.class);
            Assertions.assertEquals(seaTunnelRow.getField(10).getClass(), String[].class);
            Assertions.assertEquals(seaTunnelRow.getField(11).getClass(), LocalDate.class);
            Assertions.assertEquals(seaTunnelRow.getField(12).getClass(), LocalDateTime.class);
            Assertions.assertEquals(seaTunnelRow.getField(13).getClass(), LocalTime.class);
            Assertions.assertEquals(seaTunnelRow.getField(0), (byte) 1);
            Assertions.assertEquals(seaTunnelRow.getField(1), (short) 22);
            Assertions.assertEquals(seaTunnelRow.getField(2), 333);
            Assertions.assertEquals(seaTunnelRow.getField(3), 355L);
            Assertions.assertEquals(seaTunnelRow.getField(4), "Cosmos");
            Assertions.assertEquals(seaTunnelRow.getField(5), 5.555);
            Assertions.assertEquals(seaTunnelRow.getField(6), (float) 6.666);
            Assertions.assertEquals(seaTunnelRow.getField(7), new BigDecimal("7.78"));
            Assertions.assertEquals(seaTunnelRow.getField(8), Boolean.FALSE);
            Assertions.assertEquals(
                    seaTunnelRow.getField(9),
                    new LinkedHashMap<String, String>() {
                        {
                            put("name", "Ivan");
                            put("age", "26");
                        }
                    });
            Assertions.assertArrayEquals(
                    (String[]) seaTunnelRow.getField(10), new String[] {"Ivan", "Dusayi"});
            Assertions.assertEquals(
                    seaTunnelRow.getField(11),
                    DateUtils.parse("2024-01-31", DateUtils.Formatter.YYYY_MM_DD));
            Assertions.assertEquals(
                    seaTunnelRow.getField(12),
                    DateTimeUtils.parse(
                            "2024-01-31 16:00:48", DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS));
            Assertions.assertEquals(
                    seaTunnelRow.getField(13),
                    TimeUtils.parse("16:00:48", TimeUtils.Formatter.HH_MM_SS));
        }
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
