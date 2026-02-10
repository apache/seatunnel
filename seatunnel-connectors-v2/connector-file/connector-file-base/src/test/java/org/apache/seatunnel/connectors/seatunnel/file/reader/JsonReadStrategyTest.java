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

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.CommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.BaseTest;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.JsonReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

public class JsonReadStrategyTest extends BaseTest {

    @Test
    public void testJsonRead() throws IOException, URISyntaxException {
        URL resource = JsonReadStrategyTest.class.getResource("/filter-pattern/json/people.json");
        String path = Paths.get(resource.toURI()).toString();
        URL conf =
                JsonReadStrategyTest.class.getResource(
                        "/filter-pattern/json/json2025/test_read_json.conf");
        String confPath = Paths.get(conf.toURI()).toString();
        Config pluginConfig = ConfigFactory.parseFile(new File(confPath));

        LocalConf localConf = new LocalConf(FS_DEFAULT_NAME_DEFAULT);
        JsonReadStrategy jsonReadStrategy = new JsonReadStrategy();
        jsonReadStrategy.setPluginConfig(pluginConfig);
        jsonReadStrategy.init(localConf);
        jsonReadStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "test",
                        new SeaTunnelRowType(
                                new String[] {"id", "name", "age"},
                                new SeaTunnelDataType[] {
                                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                                })));
        TestCollector testCollector = new TestCollector();
        jsonReadStrategy.read(path, "", testCollector);

        // assert file metadata
        SeaTunnelRow firstRow = testCollector.getRows().get(0);
        Assertions.assertTrue(
                firstRow.getOptions()
                        .get(CommonOptions.FILE_PATH.getName())
                        .toString()
                        .endsWith("people.json"));
        Assertions.assertNotNull(
                firstRow.getOptions().get(CommonOptions.FILE_UPDATE_TIME.getName()));
        Assertions.assertNotNull(firstRow.getOptions().get(CommonOptions.FILE_SIZE.getName()));
        Assertions.assertEquals(
                "json", firstRow.getOptions().get(CommonOptions.FILE_TYPE.getName()));
    }
}
