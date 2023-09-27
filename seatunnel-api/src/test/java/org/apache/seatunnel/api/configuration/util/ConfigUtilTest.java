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

import org.apache.logging.log4j.core.util.IOUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConfigUtilTest {

    private static final ObjectMapper JACKSON_MAPPER = new ObjectMapper();

    private static Config config;

    private static Config differentValueConfig;

    @BeforeAll
    public static void init() throws URISyntaxException {
        config =
                ConfigFactory.parseFile(
                        Paths.get(
                                        ConfigUtilTest.class
                                                .getResource("/conf/option-test.conf")
                                                .toURI())
                                .toFile());

        differentValueConfig =
                ConfigFactory.parseFile(
                        Paths.get(
                                        ConfigUtilTest.class
                                                .getResource(
                                                        "/conf/config_with_key_with_different_type_value.conf")
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
    @DisabledOnOs(OS.WINDOWS)
    public void treeMapFunctionTest() throws IOException, URISyntaxException {
        Map<String, Object> map =
                JACKSON_MAPPER.readValue(
                        config.root().render(ConfigRenderOptions.concise()),
                        new TypeReference<Map<String, Object>>() {});
        Map<String, Object> result = ConfigUtil.treeMap(map);
        String expectResult =
                IOUtils.toString(
                        new FileReader(
                                new File(
                                        ConfigUtilTest.class
                                                .getResource(
                                                        "/conf/option-test-json-after-treemap.json")
                                                .toURI())));
        Assertions.assertEquals(
                JACKSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(result),
                expectResult);

        // Check the order of option after treemap
        Iterator<String> resultIterator = result.keySet().iterator();
        for (String entry : map.keySet()) {
            String r = resultIterator.next();
            Assertions.assertEquals(entry, r);
        }
        String data =
                String.join(
                        ",",
                        ((Map<String, Object>)
                                        ((List<Map<String, Object>>) result.get("source"))
                                                .get(0)
                                                .get("option"))
                                .keySet());
        String data2 =
                String.join(
                        ",",
                        ((Map<String, Object>)
                                        ((List<Map<String, Object>>) map.get("source"))
                                                .get(0)
                                                .get("option"))
                                .keySet());
        String sameOrder = "bool,bool-str,int,int-str,float,float-str,double,double-str,map";
        Assertions.assertEquals(
                "string,enum,list-json,list-str,complex-type,long,list,numeric-list,long-str,enum-list,"
                        + sameOrder,
                data);
        Assertions.assertEquals(sameOrder + ",map.name", data2);
        Map<String, Object> differentValueMap =
                JACKSON_MAPPER.readValue(
                        differentValueConfig.root().render(ConfigRenderOptions.concise()),
                        new TypeReference<Map<String, Object>>() {});

        Map<String, Object> value = ConfigUtil.treeMap(differentValueMap);
        Assertions.assertEquals(value.size(), 2);
        Map<String, Object> expect = new HashMap<>();
        Map<String, Object> offsets = new HashMap<>();
        expect.put("", "specific_offsets");
        expect.put("offsets", offsets);
        offsets.put("test_topic_source-0", "50");
        Assertions.assertEquals(
                ((Map) ((List) value.get("source")).get(0)).get("start_mode"), expect);
    }
}
