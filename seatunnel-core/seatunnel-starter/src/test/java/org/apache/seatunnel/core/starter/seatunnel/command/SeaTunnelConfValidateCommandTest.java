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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SeaTunnelConfValidateCommandTest {

    @Test
    public void testValidStaticDryRun() {
        ClientCommandArgs args = getCommandArgs("config/valid_static_dryrun.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        Assertions.assertDoesNotThrow(command::execute);
    }

    @Test
    public void testInvalidHoconSyntax() {
        ClientCommandArgs args = getCommandArgs("config/invalid_hocon_syntax.conf");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("Key 'source' may not be followed by token: '['"),
                "Exception should mention token error. Actual message: " + exception.getMessage());
    }

    @Test
    public void testInvalidYamlSyntax() {
        ClientCommandArgs args = getCommandArgs("config/invalid_yaml_syntax.yaml");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("invalid_yaml_syntax.yaml")
                        && exception
                                .getMessage()
                                .contains("Expecting end of input or a comma, got ':'"),
                "Exception should mention YAML file and colon syntax error. Actual message: "
                        + exception.getMessage());
    }

    @Test
    public void testUnknownKeyFailsValidation() {
        ClientCommandArgs args = getCommandArgs("config/invalid_dryrun_unknown_key.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("typo_unknown_key"),
                "Exception should mention the unknown key. Actual message: "
                        + exception.getMessage());
    }

    @Test
    public void testMissingRequiredKeyFailsValidation() {
        ClientCommandArgs args = getCommandArgs("config/invalid_dryrun_missing_required.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("schema"),
                "Exception should mention the missing required 'schema' key. Actual message: "
                        + exception.getMessage());
    }

    @Test
    public void testInvalidOptionType() {
        ClientCommandArgs args = getCommandArgs("config/invalid_option_type.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("invalid_string_value")
                        && exception.getMessage().contains("java.lang.Integer"),
                "Exception should mention the invalid value and expected type. Actual message: "
                        + exception.getMessage());
    }

    @Test
    public void testInvalidPluginLoadability() {
        ClientCommandArgs args = getCommandArgs("config/invalid_plugin_loadability.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("NonExistentConnector"),
                "Exception should mention 'NonExistentConnector'. Actual message: "
                        + exception.getMessage());
    }

    @Test
    public void testInvalidDagTopology() {
        ClientCommandArgs args = getCommandArgs("config/invalid_dag_topology.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("Miss <Sink> config!"),
                "Exception should mention missing Sink config. Actual message: "
                        + exception.getMessage());
    }

    @Test
    public void testInvalidSqlTransform() {
        ClientCommandArgs args = getCommandArgs("config/invalid_sql_transform.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);

        Assertions.assertTrue(
                exception.getMessage().contains("TableTransformFactory"),
                "Exception should mention TableTransformFactory not found. Actual message: "
                        + exception.getMessage());
    }

    private ClientCommandArgs getCommandArgs(String configFile) {
        Path configPath;
        try {
            configPath =
                    Paths.get(
                            SeaTunnelConfValidateCommandTest.class
                                    .getResource("/" + configFile)
                                    .toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        String[] args = {"-c", configPath.toString(), "--dry-run", "static"};
        return CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel.sh", true);
    }
}
