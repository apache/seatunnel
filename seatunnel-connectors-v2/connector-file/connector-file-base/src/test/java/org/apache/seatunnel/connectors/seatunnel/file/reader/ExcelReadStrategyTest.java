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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ExcelReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.Getter;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

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
