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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.AbstractReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.JsonReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.XmlReadStrategy;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class ReadStrategyEncodingTest {

    private static final Map<String, String> cMap = new HashMap<>();
    private static final Integer[] cArray = {101};
    private static final String[] cArrayString = {"测试ABC123!@#"};
    private static final String cString = "你好，世界ABC123!@#";
    private static final Boolean cBoolean = true;
    private static final Byte cTinyint = 117;
    private static final Short cSmallint = 15987;
    private static final Integer cInt = 56387395;
    private static final Long cBigint = 7084913402530365000L;
    private static final Float cFloat = 1.23f;
    private static final Double cDouble = 1.23;
    private static final BigDecimal cDecimal = new BigDecimal("2924137191386439303744.39292216");
    private static final byte[] cBytes = {
        -28, -67, -96, -27, -91, -67, -28, -72, -106, -25, -107, -116, 65, 66, 67, 97, 98, 99, 49,
        50, 51, 33, 64, 35
    };
    private static final LocalDate cDate = LocalDate.of(2023, 4, 22);
    private static final LocalDateTime cTimestamp = LocalDateTime.of(2023, 4, 22, 23, 20, 58);

    @BeforeAll
    public static void before() {
        cMap.put("a测试", "b测试");
    }

    @Test
    public void testTextRead() throws Exception {
        try (TextReadStrategy textReadStrategy = new TextReadStrategy()) {
            testRead("/encoding/gbk.txt", "/encoding/test_read_text.conf", textReadStrategy);
        }
    }

    @Test
    public void testJsonRead() throws Exception {
        try (JsonReadStrategy jsonReadStrategy = new JsonReadStrategy()) {
            testRead("/encoding/gbk.json", "/encoding/test_read_json.conf", jsonReadStrategy);
        }
    }

    @Test
    public void testXmlRead() throws Exception {
        try (XmlReadStrategy xmlReadStrategy = new XmlReadStrategy()) {
            testRead("/encoding/gbk.xml", "/encoding/test_read_xml.conf", xmlReadStrategy);
            testRead(
                    "/encoding/gbk_use_attr_format.xml",
                    "/encoding/test_read_xml_use_attr_format.conf",
                    xmlReadStrategy);
        }
    }

    private static void testRead(
            String sourcePathStr, String confPathStr, AbstractReadStrategy readStrategy)
            throws URISyntaxException, IOException {
        URL sourceFile = ReadStrategyEncodingTest.class.getResource(sourcePathStr);
        URL conf = ReadStrategyEncodingTest.class.getResource(confPathStr);
        Assertions.assertNotNull(sourceFile);
        Assertions.assertNotNull(conf);
        String sourceFilePath = Paths.get(sourceFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        TestCollector testCollector;
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        readStrategy.setPluginConfig(pluginConfig);
        readStrategy.init(localConf);
        readStrategy.getFileNamesByPath(sourceFilePath);
        testCollector = new TestCollector();
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        Assertions.assertNotNull(catalogTable.getSeaTunnelRowType());
        readStrategy.setCatalogTable(catalogTable);
        log.info(catalogTable.getSeaTunnelRowType().toString());
        readStrategy.read(sourceFilePath, "", testCollector);
        assertRows(testCollector);
    }

    private static void assertRows(TestCollector testCollector) {
        for (SeaTunnelRow row : testCollector.getRows()) {
            Assertions.assertEquals(row.getField(0), cMap);
            Assertions.assertArrayEquals(((Integer[]) row.getField(1)), cArray);
            Assertions.assertArrayEquals(((String[]) row.getField(2)), cArrayString);
            Assertions.assertEquals(row.getField(3), cString);
            Assertions.assertEquals(row.getField(4), cBoolean);
            Assertions.assertEquals(row.getField(5), cTinyint);
            Assertions.assertEquals(row.getField(6), cSmallint);
            Assertions.assertEquals(row.getField(7), cInt);
            Assertions.assertEquals(row.getField(8), cBigint);
            Assertions.assertEquals(row.getField(9), cFloat);
            Assertions.assertEquals(row.getField(10), cDouble);
            Assertions.assertEquals(row.getField(11), cDecimal);
            Assertions.assertTrue(StringUtils.isBlank((String) row.getField(12)));
            Assertions.assertArrayEquals((byte[]) row.getField(13), cBytes);
            Assertions.assertEquals(row.getField(14), cDate);
            Assertions.assertEquals(row.getField(15), cTimestamp);
        }
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
