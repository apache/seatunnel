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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.file.util.LocalFileSystemConf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class XmlReadStrategyTest {

    @Test
    public void testXmlRead() throws IOException, URISyntaxException {
        URL xmlFile = XmlReadStrategyTest.class.getResource("/xml/name=xmlTest/test_read.xml");
        URL conf = XmlReadStrategyTest.class.getResource("/xml/test_read_xml.conf");
        Assertions.assertNotNull(xmlFile);
        Assertions.assertNotNull(conf);
        String xmlFilePath = Paths.get(xmlFile.toURI()).toString();
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));
        XmlReadStrategy xmlReadStrategy = new XmlReadStrategy();
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        xmlReadStrategy.setPluginConfig(pluginConfig);
        xmlReadStrategy.init(localConf);
        List<String> fileNamesByPath = xmlReadStrategy.getFileNamesByPath(xmlFilePath);
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        xmlReadStrategy.setCatalogTable(catalogTable);
        TestCollector testCollector = new TestCollector();
        xmlReadStrategy.read(fileNamesByPath.get(0), "", testCollector);
        for (SeaTunnelRow seaTunnelRow : testCollector.getRows()) {
            Assertions.assertEquals(seaTunnelRow.getArity(), 15);
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
            Assertions.assertEquals(seaTunnelRow.getField(14).getClass(), String.class);

            Assertions.assertEquals(seaTunnelRow.getField(0), (byte) 1);
            Assertions.assertEquals(seaTunnelRow.getField(1), (short) 22);
            Assertions.assertEquals(seaTunnelRow.getField(2), 333);
            Assertions.assertEquals(seaTunnelRow.getField(3), 4444L);
            Assertions.assertEquals(seaTunnelRow.getField(4), "DusayI");
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
            Assertions.assertEquals(seaTunnelRow.getField(14), "xmlTest");
        }
    }

    @Test
    public void testXmlReadRejectsExternalEntityPayload(@TempDir Path tempDir) throws IOException {
        XmlReadStrategy xmlReadStrategy = createXmlReadStrategy();
        Path sentinel = tempDir.resolve("seatunnel-xxe.txt");
        Files.write(sentinel, Collections.singletonList("secret-from-temp-file"));
        String xxeXml =
                "<?xml version=\"1.0\"?>\n"
                        + "<!DOCTYPE row [<!ENTITY xxe SYSTEM \""
                        + sentinel.toUri()
                        + "\">]>\n"
                        + "<RECORDS><RECORD c_string=\"&xxe;\"/></RECORDS>";
        TestCollector collector = new TestCollector();

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class,
                        () ->
                                xmlReadStrategy.readProcess(
                                        new FileSourceSplit("xml", "poc.xml"),
                                        collector,
                                        new ByteArrayInputStream(
                                                xxeXml.getBytes(StandardCharsets.UTF_8)),
                                        Collections.emptyMap(),
                                        "poc.xml"));

        Assertions.assertEquals(
                FileConnectorErrorCode.FILE_READ_FAILED, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(collector.getRows().isEmpty());
        Assertions.assertTrue(
                containsMessage(exception, "DOCTYPE"),
                "expected secure XML parser to reject the DOCTYPE declaration");
        Assertions.assertFalse(
                containsMessage(exception, "secret-from-temp-file"),
                "expected the sentinel secret to never appear in the exception message/cause chain, "
                        + "confirming the external entity was rejected rather than resolved and merely dropped");
    }

    private boolean containsMessage(Throwable throwable, String message) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null && current.getMessage().contains(message)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /** Build a production-like XML reader instance with the shared test schema loaded. */
    private XmlReadStrategy createXmlReadStrategy() {
        Config pluginConfig = loadPluginConfig();
        XmlReadStrategy xmlReadStrategy = new XmlReadStrategy();
        LocalFileSystemConf.LocalConf localConf =
                new LocalFileSystemConf.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        xmlReadStrategy.setPluginConfig(pluginConfig);
        xmlReadStrategy.init(localConf);
        CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(pluginConfig);
        xmlReadStrategy.setCatalogTable(catalogTable);
        return xmlReadStrategy;
    }

    /** Load the reusable XML test configuration from the module resources. */
    private Config loadPluginConfig() {
        URL conf = XmlReadStrategyTest.class.getResource("/xml/test_read_xml.conf");
        Assertions.assertNotNull(conf);
        try {
            String confPath = Paths.get(conf.toURI()).toString();
            return ConfigFactory.parseFile(new File(confPath));
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Failed to load xml test configuration", e);
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
}
