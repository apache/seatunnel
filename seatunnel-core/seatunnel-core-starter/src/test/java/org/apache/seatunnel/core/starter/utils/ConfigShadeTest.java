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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;

import org.apache.seatunnel.api.configuration.ConfigShade;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.apache.seatunnel.core.starter.utils.ConfigBuilder.CONFIG_RENDER_OPTIONS;

@Slf4j
public class ConfigShadeTest {

    private static final String USERNAME = "seatunnel";

    private static final String PASSWORD = "seatunnel_password";

    @Test
    public void testParseConfig() throws URISyntaxException {
        URL resource = ConfigShadeTest.class.getResource("/config.shade.conf");
        Assertions.assertNotNull(resource);
        Config config = ConfigBuilder.of(Paths.get(resource.toURI()));
        Config fields =
                config.getConfigList("source").get(0).getConfig("schema").getConfig("fields");
        log.info("Schema fields: {}", fields.root().render(CONFIG_RENDER_OPTIONS));
        ObjectNode jsonNodes = JsonUtils.parseObject(fields.root().render(CONFIG_RENDER_OPTIONS));
        List<String> field = new ArrayList<>();
        jsonNodes.fieldNames().forEachRemaining(field::add);
        Assertions.assertEquals(field.size(), jsonNodes.size());
        Assertions.assertEquals(field.get(0), "name");
        Assertions.assertEquals(field.get(1), "age");
        Assertions.assertEquals(field.get(2), "sex");
        log.info("Decrypt config: {}", config.root().render(CONFIG_RENDER_OPTIONS));
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("username"), USERNAME);
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("password"), PASSWORD);
    }

    @Test
    public void testVariableReplacement() throws URISyntaxException {
        String jobName = "seatunnel variable test job";
        String resName = "fake";
        int rowNum = 10;
        String nameType = "string";
        String username = "seatunnel=2.3.1";
        String password = "$a^b%c.d~e0*9(";
        String blankSpace = "2023-12-26 11:30:00";
        List<String> variables = new ArrayList<>();
        variables.add("jobName=" + jobName);
        variables.add("resName=" + resName);
        variables.add("rowNum=" + rowNum);
        variables.add("strTemplate=[abc,de~,f h]");
        variables.add("nameType=" + nameType);
        variables.add("nameVal=abc");
        variables.add("username=" + username);
        variables.add("password=" + password);
        variables.add("blankSpace=" + blankSpace);
        URL resource = ConfigShadeTest.class.getResource("/config.variables.conf");
        Assertions.assertNotNull(resource);
        Config config = ConfigBuilder.of(Paths.get(resource.toURI()), variables);
        Config envConfig = config.getConfig("env");
        Assertions.assertEquals(envConfig.getString("job.name"), jobName);
        List<? extends ConfigObject> sourceConfigs = config.getObjectList("source");
        for (ConfigObject configObject : sourceConfigs) {
            Config sourceConfig = configObject.toConfig();
            List<String> list1 = sourceConfig.getStringList("string.template");
            Assertions.assertEquals(list1.get(0), "abc");
            Assertions.assertEquals(list1.get(1), "de~");
            Assertions.assertEquals(list1.get(2), "f h");
            Assertions.assertEquals(sourceConfig.getInt("row.num"), rowNum);
            Assertions.assertEquals(sourceConfig.getString("result_table_name"), resName);
        }
        List<? extends ConfigObject> transformConfigs = config.getObjectList("transform");
        for (ConfigObject configObject : transformConfigs) {
            Config transformConfig = configObject.toConfig();
            Assertions.assertEquals(
                    transformConfig.getString("query"), "select * from fake where name = 'abc' ");
        }
        List<? extends ConfigObject> sinkConfigs = config.getObjectList("sink");
        for (ConfigObject sinkObject : sinkConfigs) {
            Config sinkConfig = sinkObject.toConfig();
            Assertions.assertEquals(sinkConfig.getString("username"), username);
            Assertions.assertEquals(sinkConfig.getString("password"), password);
            Assertions.assertEquals(sinkConfig.getString("blankSpace"), blankSpace);
        }
    }

    @Test
    public void testDecryptAndEncrypt() {
        String encryptUsername = ConfigShadeUtils.encryptOption("base64", USERNAME);
        String decryptUsername = ConfigShadeUtils.decryptOption("base64", encryptUsername);
        String encryptPassword = ConfigShadeUtils.encryptOption("base64", PASSWORD);
        String decryptPassword = ConfigShadeUtils.decryptOption("base64", encryptPassword);
        Assertions.assertEquals("c2VhdHVubmVs", encryptUsername);
        Assertions.assertEquals("c2VhdHVubmVsX3Bhc3N3b3Jk", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);
    }

    public static class Base64ConfigShade implements ConfigShade {

        private static final Base64.Encoder ENCODER = Base64.getEncoder();

        private static final Base64.Decoder DECODER = Base64.getDecoder();

        private static final String IDENTIFIER = "base64";

        @Override
        public String getIdentifier() {
            return IDENTIFIER;
        }

        @Override
        public String encrypt(String content) {
            return ENCODER.encodeToString(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public String decrypt(String content) {
            return new String(DECODER.decode(content));
        }
    }
}
