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

package org.apache.seatunnel.core.starter.command;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;

import org.apache.seatunnel.common.config.DeployMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConfEncryptCommandTest {

    public static Path getFilePath(String path) throws URISyntaxException {
        URL resource = ConfEncryptCommandTest.class.getResource(path);
        Assertions.assertNotNull(resource);
        return Paths.get(resource.toURI());
    }

    @Test
    public void testEncrypt() throws URISyntaxException {
        TestCommandArgs testCommandArgs = new TestCommandArgs();
        Path filePath = getFilePath("/origin.conf");
        testCommandArgs.setEncrypt(true);
        testCommandArgs.setConfigFile(filePath.toString());
        ConfEncryptCommand confEncryptCommand = new ConfEncryptCommand(testCommandArgs);
        confEncryptCommand.execute();
    }

    @Test
    public void testEncryptWithJsonUserVariables() throws Exception {
        TestCommandArgs testCommandArgs = new TestCommandArgs();
        Path filePath = getFilePath("/json_params.conf");
        testCommandArgs.setEncrypt(true);
        testCommandArgs.setConfigFile(filePath.toString());

        List<String> variables = new ArrayList<>();
        variables.add("mysql_password=123456");
        variables.add("mysql_props={\"useSSL\":\"false\",\"allowPublicKeyRetrieval\":\"true\"}");
        variables.add(
                "mysql_tables=["
                        + "{\"table_path\":\"test.ml_*\",\"use_regex\":\"true\"},"
                        + "{\"table_path\":\"test.ratings\"}"
                        + "]");

        testCommandArgs.setVariables(variables);

        ConfEncryptCommand command = new ConfEncryptCommand(testCommandArgs);
        command.execute();
        Config encryptedConfig = command.getEncryptedConfig();

        List<? extends ConfigObject> sourceConfigs = encryptedConfig.getObjectList("source");
        for (ConfigObject configObject : sourceConfigs) {
            Config sourceConfig = configObject.toConfig();
            Assertions.assertTrue(
                    sourceConfig.hasPath("password"),
                    "password key should exist in encrypted config");

            Assertions.assertTrue(
                    sourceConfig.hasPath("properties"),
                    "properties key should exist in encrypted config");

            Assertions.assertTrue(
                    sourceConfig.hasPath("table_list"),
                    "table_list key should exist in encrypted config");

            String mysql_password = sourceConfig.getString("password");
            Assertions.assertNotEquals(mysql_password, "123456");

            Map<String, Object> mysqlProperties = sourceConfig.getObject("properties").unwrapped();
            Assertions.assertTrue(
                    mysqlProperties.containsKey("allowPublicKeyRetrieval"),
                    "properties should contain key: 'allowPublicKeyRetrieval'");

            List<? extends ConfigObject> tableList = sourceConfig.getObjectList("table_list");
            boolean useRegex =
                    tableList.stream()
                            .map(tableObject -> tableObject.toConfig().getBoolean("use_regex"))
                            .findFirst()
                            .get();

            Assertions.assertTrue(
                    useRegex, "useRegex should contain replaced placeholder value: " + useRegex);
        }
    }

    public static class TestCommandArgs extends AbstractCommandArgs {

        @Override
        public DeployMode getDeployMode() {
            return null;
        }

        @Override
        public Command<?> buildCommand() {
            return null;
        }
    }
}
