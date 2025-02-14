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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.api.configuration.ConfigShade;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import com.beust.jcommander.internal.Lists;
import lombok.extern.slf4j.Slf4j;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.core.starter.utils.ConfigBuilder.CONFIG_RENDER_OPTIONS;

@Slf4j
public class ConfigShadeTest {

    private static final String USERNAME = "seatunnel";

    private static final String PASSWORD = "seatunnel_password";

    private static final String ACCESS_KEY = "access_key";
    private static final String SECRET_KEY = "secret_key";

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
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("access_key"), ACCESS_KEY);
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("secret_key"), SECRET_KEY);
    }

    @Test
    public void testUsePrivacyHandlerHocon() throws URISyntaxException {
        URL resource = ConfigShadeTest.class.getResource("/config.shade.conf");
        Assertions.assertNotNull(resource);
        Config config = ConfigBuilder.of(Paths.get(resource.toURI()), Lists.newArrayList());
        config =
                ConfigFactory.parseMap(
                                ConfigBuilder.configDesensitization(config.root().unwrapped()))
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("username"), "******");
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("password"), "******");
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("access_key"), "******");
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("secret_key"), "******");
        String conf = ConfigBuilder.mapToString(config.root().unwrapped());
        Assertions.assertTrue(conf.contains("\"password\" : \"******\""));
    }

    @Test
    public void testUsePrivacyHandlerJson() throws URISyntaxException {
        URL resource = ConfigShadeTest.class.getResource("/config.shade.json");
        Assertions.assertNotNull(resource);
        Config config = ConfigBuilder.of(Paths.get(resource.toURI()), Lists.newArrayList());
        config =
                ConfigFactory.parseMap(
                                ConfigBuilder.configDesensitization(config.root().unwrapped()))
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("username"), "******");
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("password"), "******");
        String json = ConfigBuilder.mapToString(config.root().unwrapped());
        Assertions.assertTrue(json.contains("\"password\" : \"******\""));
    }

    @Test
    public void testConfNull() throws URISyntaxException {
        URL resource = ConfigShadeTest.class.getResource("/config.shade_caseNull.conf");
        Assertions.assertNotNull(resource);
        Config config = ConfigBuilder.of(Paths.get(resource.toURI()), Lists.newArrayList());
        config =
                ConfigFactory.parseMap(
                                ConfigBuilder.configDesensitization(config.root().unwrapped()))
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("username"), "******");
        Assertions.assertEquals(
                config.getConfigList("source").get(0).getString("password"), "******");
        String conf = ConfigBuilder.mapToString(config.root().unwrapped());
        Assertions.assertTrue(conf.contains("\"password\" : \"******\""));
        Assertions.assertTrue(conf.contains("\"test\" : null"));
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
            Assertions.assertEquals(sourceConfig.getString("plugin_output"), resName);
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

    // Set the system environment variables through SetEnvironmentVariable to verify whether the
    // parameters set by the system environment variables are effective
    @SetEnvironmentVariable(key = "jobName", value = "seatunnel variable test job")
    @Test
    public void testVariableReplacementWithDefaultValue() throws URISyntaxException {
        String jobName = "seatunnel variable test job";
        Assertions.assertEquals(System.getenv("jobName"), jobName);
        String pluginInputIdentifier = "sql";
        String containSpaceString = "f h";
        List<String> variables = new ArrayList<>();
        variables.add("strTemplate=[abc,de~," + containSpaceString + "]");
        // Set the environment variable value nameVal to `f h` to verify whether setting the space
        // through the environment variable is effective
        System.setProperty("nameValForEnv", containSpaceString);
        variables.add("pluginInputIdentifier=" + pluginInputIdentifier);
        URL resource =
                ConfigShadeTest.class.getResource("/config_variables_with_default_value.conf");
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
            Assertions.assertEquals(list1.get(2), containSpaceString);
            Assertions.assertEquals(sourceConfig.getInt("row.num"), 50);
            // Verify when verifying without setting variables, ${xxx} should be retained
            Assertions.assertEquals(
                    sourceConfig.getConfig("schema").getConfig("fields").getString("age"),
                    "${ageType}");
            Assertions.assertEquals(sourceConfig.getString("plugin_output"), "fake_test_table");
        }
        List<? extends ConfigObject> transformConfigs = config.getObjectList("transform");
        for (ConfigObject configObject : transformConfigs) {
            Config transformConfig = configObject.toConfig();
            Assertions.assertEquals(
                    transformConfig.getString("query"),
                    "select * from fake_test_table where name = 'f h' ");
        }
        List<? extends ConfigObject> sinkConfigs = config.getObjectList("sink");
        for (ConfigObject sinkObject : sinkConfigs) {
            Config sinkConfig = sinkObject.toConfig();
            Assertions.assertEquals(sinkConfig.getString("plugin_input"), pluginInputIdentifier);
        }
    }

    @Test
    public void testVariableReplacementWithReservedPlaceholder() {
        List<String> variables = new ArrayList<>();
        variables.add("strTemplate=[abc,de~,f h]");
        // Set up a reserved placeholder
        variables.add("table_name=sql");
        URL resource =
                ConfigShadeTest.class.getResource(
                        "/config_variables_with_reserved_placeholder.conf");
        Assertions.assertNotNull(resource);
        ConfigCheckException configCheckException =
                Assertions.assertThrows(
                        ConfigCheckException.class,
                        () -> ConfigBuilder.of(Paths.get(resource.toURI()), variables));
        Assertions.assertEquals(
                "System placeholders cannot be used. Incorrect config parameter: table_name",
                configCheckException.getMessage());
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

    @Test
    public void testDecryptWithProps() throws URISyntaxException {
        URL resource = ConfigShadeTest.class.getResource("/config.shade_with_props.json");
        Assertions.assertNotNull(resource);
        Config decryptedProps = ConfigBuilder.of(Paths.get(resource.toURI()), Lists.newArrayList());

        String suffix = "666";
        String rawUsername = "un";
        String rawPassword = "pd";
        Assertions.assertEquals(
                rawUsername, decryptedProps.getConfigList("source").get(0).getString("username"));
        Assertions.assertEquals(
                rawPassword, decryptedProps.getConfigList("source").get(0).getString("password"));

        Config encryptedConfig = ConfigShadeUtils.encryptConfig(decryptedProps);
        Assertions.assertEquals(
                rawUsername + suffix,
                encryptedConfig.getConfigList("source").get(0).getString("username"));
        Assertions.assertEquals(
                rawPassword + suffix,
                encryptedConfig.getConfigList("source").get(0).getString("password"));
    }

    public static class ConfigShadeWithProps implements ConfigShade {

        private String suffix;
        private String identifier = "withProps";

        @Override
        public void open(Map<String, Object> props) {
            this.suffix = String.valueOf(props.get("suffix"));
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public String encrypt(String content) {
            return content + suffix;
        }

        @Override
        public String decrypt(String content) {
            return content.substring(0, content.length() - suffix.length());
        }
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
