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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.BaseTest;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.TextReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

/**
 * Verifies text row decoding and file metadata, including basename-only extension detection.
 *
 * <p>These tests exercise real temporary files and preserve decoded payload assertions.
 */
public class TextReadStrategyTest extends BaseTest {

    @TempDir private Path tempDir;

    @Test
    public void testExtensionlessFilesUnderDottedDirectory() throws Exception {
        Path directory = Files.createDirectories(tempDir.resolve("v1.0"));
        for (String fileName : new String[] {"README", "trailing."}) {
            Path sourceFile = directory.resolve(fileName);
            Files.write(sourceFile, "payload\n".getBytes(StandardCharsets.UTF_8));
            TextReadStrategy strategy = new TextReadStrategy();
            strategy.setPluginConfig(ConfigFactory.empty());
            strategy.init(new LocalConf(FS_DEFAULT_NAME_DEFAULT));
            strategy.setCatalogTable(
                    CatalogTableUtil.getCatalogTable(
                            "test",
                            new SeaTunnelRowType(
                                    new String[] {"value"},
                                    new SeaTunnelDataType[] {BasicType.STRING_TYPE})));
            TestCollector collector = new TestCollector();
            strategy.read(sourceFile.toString(), "", collector);
            Assertions.assertEquals(1, collector.getRows().size());
            SeaTunnelRow row = collector.getRows().get(0);
            Assertions.assertEquals("payload", row.getField(0));
            Assertions.assertEquals("", row.getOptions().get(CommonOptions.FILE_TYPE.getName()));
            strategy.close();
        }
    }

    @Test
    public void testTextRead() throws IOException, URISyntaxException {
        URL resource = TextReadStrategyTest.class.getResource("/text/test.txt");
        Assertions.assertNotNull(resource);
        String path = Paths.get(resource.toURI()).toString();
        URL conf = TextReadStrategyTest.class.getResource("/text/test_read_text.conf");
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig =
                ConfigFactory.parseFile(new File(confPath))
                        .withValue(
                                FileBaseSourceOptions.SKIP_HEADER_ROW_NUMBER.key(),
                                ConfigValueFactory.fromAnyRef(0));
        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        TextReadStrategy textReadStrategy = new TextReadStrategy();
        textReadStrategy.setPluginConfig(pluginConfig);
        textReadStrategy.init(localConf);
        textReadStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"name"},
                                new SeaTunnelDataType[] {
                                    BasicType.STRING_TYPE,
                                })));

        TestCollector testCollector = new TestCollector();
        textReadStrategy.read(path, "", testCollector);

        // assert file metadata
        SeaTunnelRow firstRow = testCollector.getRows().get(0);
        Assertions.assertTrue(
                firstRow.getOptions()
                        .get(CommonOptions.FILE_PATH.getName())
                        .toString()
                        .endsWith("test.txt"));
        Assertions.assertNotNull(
                firstRow.getOptions().get(CommonOptions.FILE_UPDATE_TIME.getName()));
        Assertions.assertNotNull(firstRow.getOptions().get(CommonOptions.FILE_SIZE.getName()));
        Assertions.assertEquals(
                "txt", firstRow.getOptions().get(CommonOptions.FILE_TYPE.getName()));
    }
}
