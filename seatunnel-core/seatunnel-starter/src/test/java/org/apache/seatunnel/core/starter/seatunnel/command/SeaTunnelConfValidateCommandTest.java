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
import java.nio.file.Paths;

public class SeaTunnelConfValidateCommandTest {

    @Test
    public void testValidStaticDryRun() {
        ClientCommandArgs args = buildArgs("config/valid_static_dryrun.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        Assertions.assertDoesNotThrow(command::execute);
    }

    @Test
    public void testCheckFlagRoutesToValidation() {
        ClientCommandArgs args = buildCheckArgs("config/valid_static_dryrun.json");
        Assertions.assertInstanceOf(SeaTunnelConfValidateCommand.class, args.buildCommand());

        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);
        Assertions.assertDoesNotThrow(command::execute);
    }

    @Test
    public void testEnvUnknownKeyFailsValidation() {
        ClientCommandArgs args = buildArgs("config/invalid_env_unknown_key.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("Connector 'env' has unknown option keys"));
    }

    @Test
    public void testInvalidHoconSyntax() {
        ClientCommandArgs args = buildArgs("config/invalid_hocon_syntax.conf");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("Static analysis failed"),
                "Actual: " + exception.getMessage());
    }

    @Test
    public void testInvalidYamlSyntax() {
        ClientCommandArgs args = buildArgs("config/invalid_yaml_syntax.yaml");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("Static analysis failed"),
                "Actual: " + exception.getMessage());
    }

    @Test
    public void testUnknownKeyFailsValidation() {
        ClientCommandArgs args = buildArgs("config/invalid_dryrun_unknown_key.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("typo_unknown_key"),
                "Should detect unknown key. Actual: " + exception.getMessage());
    }

    @Test
    public void testMissingRequiredKeyFailsValidation() {
        ClientCommandArgs args = buildArgs("config/invalid_dryrun_missing_required.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("unconfigured options"),
                "Should detect missing required option. Actual: " + exception.getMessage());
    }

    @Test
    public void testInvalidOptionType() {
        ClientCommandArgs args = buildArgs("config/invalid_option_type.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("Json parsing exception"),
                "Should detect type mismatch. Actual: " + exception.getMessage());
    }

    @Test
    public void testInvalidPluginLoadability() {
        ClientCommandArgs args = buildArgs("config/invalid_plugin_loadability.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("NonExistentConnector"),
                "Should mention the unloadable plugin. Actual: " + exception.getMessage());
    }

    @Test
    public void testInvalidDagTopology() {
        ClientCommandArgs args = buildArgs("config/invalid_dag_topology.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("Miss <Sink> config"),
                "Should detect invalid DAG. Actual: " + exception.getMessage());
    }

    @Test
    public void testInvalidSqlTransform() {
        ClientCommandArgs args = buildArgs("config/invalid_sql_transform.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        Assertions.assertTrue(
                exception.getMessage().contains("SQL Syntax Error in Sql Transform"),
                "Should detect SQL syntax error. Actual: " + exception.getMessage());
    }

    private ClientCommandArgs buildArgs(String configFile) {
        String[] args = {"-c", resolveConfigPath(configFile), "--dry-run", "static"};
        return CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel.sh", true);
    }

    private ClientCommandArgs buildCheckArgs(String configFile) {
        String[] args = {"-c", resolveConfigPath(configFile), "--check"};
        return CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel.sh", true);
    }

    private String resolveConfigPath(String configFile) {
        try {
            return Paths.get(
                            SeaTunnelConfValidateCommandTest.class
                                    .getResource("/" + configFile)
                                    .toURI())
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
