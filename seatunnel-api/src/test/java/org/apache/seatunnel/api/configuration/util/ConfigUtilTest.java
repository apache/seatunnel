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

package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

public class ConfigUtilTest {

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    private static Config config;

    private static Config errorConfig;

    @BeforeAll
    public static void init() throws URISyntaxException {
        config =
                ConfigFactory.parseFile(
                        Paths.get(
                                        ConfigUtilTest.class
                                                .getResource("/conf/option-test.conf")
                                                .toURI())
                                .toFile());

        errorConfig =
                ConfigFactory.parseFile(
                        Paths.get(
                                        ConfigUtilTest.class
                                                .getResource("/conf/config_with_error.conf")
                                                .toURI())
                                .toFile());
    }

    @Test
    public void convertToJsonString() {
        String configJson = ConfigUtil.convertToJsonString(config);
        Config parsedConfig = ConfigUtil.convertToConfig(configJson);
        Assertions.assertEquals(config.getConfig("env"), parsedConfig.getConfig("env"));
    }

    @Test
    public void treeMapFunctionTest() throws IOException, URISyntaxException {
        Map<String, Object> map =
                JACKSON_MAPPER.readValue(
                        config.root().render(ConfigRenderOptions.concise()),
                        new TypeReference<Map<String, Object>>() {});
        Map<String, Object> result = ConfigUtil.treeMap(map);
        Map<String, Object> expectResult =
                JACKSON_MAPPER.readValue(
                        ConfigUtilTest.class
                                .getResource("/conf/option-test-json-after-treemap.json")
                                .toURI()
                                .toURL(),
                        new TypeReference<Map<String, Object>>() {});
        Assertions.assertEquals(result, expectResult);

        Map<String, Object> errorConfigMap =
                JACKSON_MAPPER.readValue(
                        errorConfig.root().render(ConfigRenderOptions.concise()),
                        new TypeReference<Map<String, Object>>() {});
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ConfigUtil.treeMap(errorConfigMap),
                "Unsupported both value is map and string of key: start_mode");
    }
}
