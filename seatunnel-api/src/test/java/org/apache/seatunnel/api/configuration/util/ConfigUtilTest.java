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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.logging.log4j.core.util.IOUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Map;

public class ConfigUtilTest {

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    private static Config config;

    @BeforeAll
    public static void init() throws URISyntaxException {
        config =
                ConfigFactory.parseFile(
                                Paths.get(
                                                ConfigUtilTest.class
                                                        .getResource("/conf/option-test.conf")
                                                        .toURI())
                                        .toFile())
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    @Test
    public void convertToJsonString() {
        String configJson = ConfigUtil.convertToJsonString(config);
        Config parsedConfig = ConfigUtil.convertToConfig(configJson);
        Assertions.assertEquals(config.getConfig("env"), parsedConfig.getConfig("env"));
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    public void treeMapFunctionTest() throws IOException, URISyntaxException {
        Map<String, Object> map =
                JACKSON_MAPPER.readValue(
                        config.root().render(ConfigRenderOptions.concise()),
                        new TypeReference<Map<String, Object>>() {});
        Map<String, Object> result = ConfigUtil.treeMap(map);
        String prettyResult =
                JACKSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        String expectResult =
                IOUtils.toString(
                        new FileReader(
                                Paths.get(
                                                ConfigUtilTest.class
                                                        .getResource(
                                                                "/conf/option-test-json-after-treemap.json")
                                                        .toURI())
                                        .toFile()));
        Assertions.assertEquals(prettyResult, expectResult);
    }
}
