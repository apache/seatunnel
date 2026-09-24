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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.core.starter.utils.ConfigShadeUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

class RestUtilTest {

    private static final String REGEX_FIELD = "^t_nova_.*$";

    @Test
    void buildConfigShouldPreserveRegexKeysFromJson() throws IOException {
        Config config = RestUtil.buildConfig(jsonNode(jobConfigJson()));

        assertJobConfig(config);
        assertJobConfig(ConfigShadeUtils.decryptConfig(config));
    }

    @Test
    void buildConfigShouldPreserveDottedKeyAsLiteral() throws IOException {
        Config config = RestUtil.buildConfig(jsonNode("{\"a.b\":\"value\"}"));

        Assertions.assertEquals("value", config.getString("a.b"));
        Assertions.assertEquals("value", config.root().unwrapped().get("a.b"));
    }

    @Test
    void buildConfigShouldKeepValidPathExpressions() throws IOException {
        Config config = RestUtil.buildConfig(jsonNode("{\"a->b\":\"value\"}"));

        Assertions.assertEquals("value", config.getConfig("a").getString("b"));
        Assertions.assertFalse(config.root().unwrapped().containsKey("a->b"));
    }

    @Test
    void buildConfigListShouldPreserveRegexKeysFromJson() throws IOException {
        List<Tuple2<Map<String, String>, Config>> configs =
                RestUtil.buildConfigList(
                        jsonNode("[{\"params\":{}," + jobConfigJson().substring(1) + "]"));

        Assertions.assertEquals(1, configs.size());
        Assertions.assertTrue(configs.get(0)._1.isEmpty());
        assertJobConfig(configs.get(0)._2);
        assertJobConfig(ConfigShadeUtils.decryptConfig(configs.get(0)._2));
    }

    @Test
    void buildConfigShouldPreserveValuesRequiredForConfigShadeDecryption() throws IOException {
        Config decryptedConfig =
                ConfigShadeUtils.decryptConfig(RestUtil.buildConfig(jsonNode(configShadeJson())));

        Config source = decryptedConfig.getConfigList("source").get(0);
        Assertions.assertEquals("seatunnel", source.getString("username"));
        Assertions.assertEquals("seatunnel_password", source.getString("password"));
    }

    private void assertJobConfig(Config config) {
        Assertions.assertEquals(
                "BATCH", config.getConfig("env").root().unwrapped().get("job.mode"));
        Config fields =
                config.getConfigList("source").get(0).getConfig("schema").getConfig("fields");
        Assertions.assertEquals("string", fields.root().unwrapped().get(REGEX_FIELD));
    }

    private String jobConfigJson() {
        return "{"
                + "\"env\":{\"job.mode\":\"BATCH\"},"
                + "\"source\":[{\"schema\":{\"fields\":{\""
                + REGEX_FIELD
                + "\":\"string\"}}}],"
                + "\"transform\":[],"
                + "\"sink\":[{}]"
                + "}";
    }

    private String configShadeJson() {
        return "{"
                + "\"env\":{\"shade.identifier\":\"base64\"},"
                + "\"source\":[{\"username\":\"c2VhdHVubmVs\","
                + "\"password\":\"c2VhdHVubmVsX3Bhc3N3b3Jk\"}],"
                + "\"transform\":[],"
                + "\"sink\":[{}]"
                + "}";
    }

    private org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode jsonNode(String json)
            throws IOException {
        return RestUtil.convertByteToJsonNode(json.getBytes(StandardCharsets.UTF_8));
    }
}
