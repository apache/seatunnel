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

package org.apache.seatunnel.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.config.utils.FileUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class ConfigTest {

    @Test
    public void testConfigKeyOrder() throws URISyntaxException {
        String expected =
                "{\"env\":{\"job.mode\":\"BATCH\"},\"source\":[{\"row.num\":100,\"schema\":{\"fields\":{\"name\":\"string\",\"age\":\"int\"}},\"plugin_name\":\"FakeSource\"}],\"sink\":[{\"plugin_name\":\"Console\"}]}";

        Config config =
                ConfigFactory.parseFile(
                        FileUtils.getFileFromResources("/seatunnel/serialize.conf"));
        Assertions.assertEquals(expected, config.root().render(ConfigRenderOptions.concise()));
    }

    @Test
    public void testQuoteAsKey() throws URISyntaxException {
        Config config =
                ConfigFactory.parseFile(
                        FileUtils.getFileFromResources("/seatunnel/configWithSpecialKey.conf"));
        List<String> keys = new ArrayList<>(config.getObject("object").keySet());
        Assertions.assertEquals("\"", keys.get(0));
        Assertions.assertEquals("\"\"", keys.get(1));
        Assertions.assertEquals("\\\"", keys.get(2));

        Assertions.assertEquals("\\\"", config.getObject("object").toConfig().getString("\""));
        Assertions.assertEquals(
                "\\\"", config.getObject("object").toConfig().getString("\"\\\"\""));
        Assertions.assertEquals(
                "\\\"\\\"", config.getObject("object").toConfig().getString("\"\\\"\\\"\""));
        Assertions.assertEquals(
                "\\\\\\\"", config.getObject("object").toConfig().getString("\\\""));
    }

    @Test
    public void testParseSchemaWithFields() throws URISyntaxException {
        Config config =
                ConfigFactory.parseFile(
                        FileUtils.getFileFromResources("/seatunnel/schema_fields.conf"));
        List<? extends Config> sourceRoot = config.getConfigList("source");
        Config row = getNestedConfig(sourceRoot.get(0), "schema", "fields", "row");
        Assertions.assertInstanceOf(ConfigObject.class, row.root());
        Assertions.assertInstanceOf(ConfigObject.class, row.getConfig("row").root());

        Config source = getNestedConfig(sourceRoot.get(0), "schema", "fields", "source");
        Assertions.assertInstanceOf(ConfigObject.class, source.root());
        Assertions.assertInstanceOf(ConfigObject.class, source.getConfig("source").root());
    }

    @Test
    public void testParseSchemaWithColumns() throws URISyntaxException {
        Config config =
                ConfigFactory.parseFile(
                        FileUtils.getFileFromResources("/seatunnel/schema_columns.conf"));
        List<? extends Config> sourceRoot = config.getConfigList("source");
        List<? extends Config> columns =
                sourceRoot.get(0).getConfig("schema").getConfigList("columns");
        Config row = getNestedConfig(columns.get(2), "type", "row");
        Assertions.assertInstanceOf(ConfigObject.class, row.root());

        Config source = getNestedConfig(columns.get(3), "type", "source");
        Assertions.assertInstanceOf(ConfigObject.class, source.root());
    }

    private Config getNestedConfig(Config initialConfig, String... pathSegments) {
        if (pathSegments == null || pathSegments.length == 0) {
            return initialConfig;
        }
        Config currentConfig = initialConfig;
        for (String segment : pathSegments) {
            currentConfig = currentConfig.getConfig(segment);
        }
        return currentConfig;
    }
}
