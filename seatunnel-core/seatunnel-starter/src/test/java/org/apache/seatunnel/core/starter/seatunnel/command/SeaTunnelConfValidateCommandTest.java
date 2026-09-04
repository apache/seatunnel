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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.core.starter.validation.ConfigValidationResult;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class SeaTunnelConfValidateCommandTest {

    @Test
    public void testValidStaticDryRun() {
        ClientCommandArgs args = buildArgs("config/valid_static_dryrun.json");
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        Assertions.assertDoesNotThrow(command::execute);
    }

    @Test
    public void testValidationResultForValidConfig() {
        SeaTunnelConfValidateCommand command =
                new SeaTunnelConfValidateCommand(buildArgs("config/valid_static_dryrun.json"));

        ConfigValidationResult result = command.validateResult();

        Assertions.assertTrue(result.isValid());
        Assertions.assertEquals("static", result.getPhase());
        Assertions.assertTrue(result.getErrors().isEmpty());
    }

    @Test
    public void testValidationResultClassifiesMissingPluginAsOptionError() throws Exception {
        Path configFile = Files.createTempFile("seatunnel-validation-result", ".conf");
        Files.write(
                configFile,
                ("source { FakeSource { plugin_output = output } }\n" + "sink { InMemory {} }")
                        .getBytes(StandardCharsets.UTF_8));
        configFile.toFile().deleteOnExit();

        SeaTunnelConfValidateCommand command =
                new SeaTunnelConfValidateCommand(buildArgsFromPath(configFile.toString()));
        ConfigValidationResult result = command.validateResult();

        Assertions.assertFalse(result.isValid());
        Assertions.assertEquals(1, result.getErrors().size());
        Assertions.assertEquals("option", result.getErrors().get(0).getRuleCategory());
        Assertions.assertTrue(result.toJson().contains("\"schemaVersion\":\"1.0\""));
    }

    @Test
    public void testValidationResultClassifiesParseFailure() {
        SeaTunnelConfValidateCommand command =
                new SeaTunnelConfValidateCommand(buildArgs("config/invalid_hocon_syntax.conf"));

        ConfigValidationResult result = command.validateResult();

        Assertions.assertFalse(result.isValid());
        Assertions.assertEquals("parse", result.getErrors().get(0).getRuleCategory());
    }

    @Test
    public void testValidationResultClassifiesOptionFailure() {
        SeaTunnelConfValidateCommand command =
                new SeaTunnelConfValidateCommand(buildArgs("config/invalid_option_type.json"));

        ConfigValidationResult result = command.validateResult();

        Assertions.assertFalse(result.isValid());
        Assertions.assertEquals("option", result.getErrors().get(0).getRuleCategory());
    }

    @Test
    public void testValidationResultClassifiesPluginLoadFailure() {
        SeaTunnelConfValidateCommand command =
                new SeaTunnelConfValidateCommand(
                        buildArgs("config/invalid_plugin_loadability.json"));

        ConfigValidationResult result = command.validateResult();

        Assertions.assertFalse(result.isValid());
        Assertions.assertEquals("plugin", result.getErrors().get(0).getRuleCategory());
    }

    @Test
    public void testValidConnectDryRun() {
        ClientCommandArgs args = buildConnectArgs("config/valid_static_dryrun.json");
        Assertions.assertInstanceOf(SeaTunnelConfValidateCommand.class, args.buildCommand());

        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);
        Assertions.assertDoesNotThrow(command::execute);
    }

    @Test
    public void testConnectDryRunReportsValidatedSourceAndSkippedSink() {
        // FakeSource implements SupportSourceDryRunValidation -> VALIDATED.
        // InMemory sink does not -> SKIPPED, never silently treated as validated.
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidator("config/valid_static_dryrun.json");

        Assertions.assertEquals(2, results.size());
        DryRunConnectValidator.PluginResult sourceResult = results.get(0);
        Assertions.assertEquals(PluginType.SOURCE, sourceResult.getPluginType());
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.VALIDATED, sourceResult.getStatus());
        DryRunConnectValidator.PluginResult sinkResult = results.get(1);
        Assertions.assertEquals(PluginType.SINK, sinkResult.getPluginType());
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.SKIPPED, sinkResult.getStatus());
        Assertions.assertTrue(
                sinkResult.getDetail().contains("does not support connect dry-run validation"),
                "Actual: " + sinkResult.getDetail());
    }

    @Test
    public void testConnectDryRunSkipsPipelineWhenSourceSchemaUnknown() {
        // InMemorySource neither supports dry-run validation nor declares a schema in config,
        // so no placeholder schema may be propagated: source and sink must both be SKIPPED.
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidator("config/inmemory_to_inmemory_multi_table.conf");

        Assertions.assertEquals(2, results.size());
        DryRunConnectValidator.PluginResult sourceResult = results.get(0);
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.SKIPPED, sourceResult.getStatus());
        DryRunConnectValidator.PluginResult sinkResult = results.get(1);
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.SKIPPED, sinkResult.getStatus());
        Assertions.assertTrue(
                sinkResult.getDetail().contains("upstream schema not available"),
                "Actual: " + sinkResult.getDetail());
    }

    @Test
    public void testConnectDryRunValidatesTransformAndPropagatesSchema() {
        // Source schema is trusted, so the transform must be built for real: its produced
        // schema (with the copied column) flows downstream instead of the source schema.
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidatorFromString(
                        "source {\n"
                                + "  FakeSource {\n"
                                + "    plugin_output = fake_out\n"
                                + "    schema = { fields { id = int, name = string } }\n"
                                + "  }\n"
                                + "}\n"
                                + "transform {\n"
                                + "  Copy {\n"
                                + "    plugin_input = [fake_out]\n"
                                + "    plugin_output = copied\n"
                                + "    src_field = name\n"
                                + "    dest_field = name_copy\n"
                                + "  }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  InMemory { plugin_input = [copied] }\n"
                                + "}");

        Assertions.assertEquals(3, results.size());
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.VALIDATED, results.get(0).getStatus());
        DryRunConnectValidator.PluginResult transformResult = results.get(1);
        Assertions.assertEquals(PluginType.TRANSFORM, transformResult.getPluginType());
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.VALIDATED, transformResult.getStatus());
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.SKIPPED, results.get(2).getStatus());
    }

    @Test
    public void testConnectDryRunSkipsTransformWhenUpstreamSchemaUnknown() {
        // The unknown-schema marker must flow through transforms: neither the transform nor
        // the sink may be validated against a placeholder schema.
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidatorFromString(
                        "source {\n"
                                + "  InMemorySource { plugin_output = mem_out }\n"
                                + "}\n"
                                + "transform {\n"
                                + "  Copy {\n"
                                + "    plugin_input = [mem_out]\n"
                                + "    plugin_output = copied\n"
                                + "    src_field = name\n"
                                + "    dest_field = name_copy\n"
                                + "  }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  InMemory { plugin_input = [copied] }\n"
                                + "}");

        Assertions.assertEquals(3, results.size());
        for (DryRunConnectValidator.PluginResult result : results) {
            Assertions.assertEquals(
                    DryRunConnectValidator.PluginResult.Status.SKIPPED,
                    result.getStatus(),
                    "Expected SKIPPED but got: " + result);
        }
        Assertions.assertTrue(
                results.get(1).getDetail().contains("upstream schema not available"),
                "Actual: " + results.get(1).getDetail());
    }

    @Test
    public void testConnectDryRunUsesConfigSchemaForUnsupportedSource() {
        // A source without dry-run support but WITH an explicit config schema must still feed
        // meaningful schemas downstream: the sink is skipped for lack of support, not for lack
        // of schema.
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidatorFromString(
                        "source {\n"
                                + "  InMemorySource {\n"
                                + "    plugin_output = mem_out\n"
                                + "    schema = { fields { id = int, name = string } }\n"
                                + "  }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  InMemory { plugin_input = [mem_out] }\n"
                                + "}");

        Assertions.assertEquals(2, results.size());
        Assertions.assertTrue(
                results.get(0).getDetail().contains("schema taken from config"),
                "Actual: " + results.get(0).getDetail());
        Assertions.assertTrue(
                results.get(1).getDetail().contains("does not support connect dry-run validation"),
                "Sink must be skipped for missing support, not missing schema. Actual: "
                        + results.get(1).getDetail());
    }

    @Test
    public void testConnectDryRunDoesNotTrustSchemaBlockWithoutFields() {
        // A schema block without fields/columns resolves to the synthetic single-text-column
        // placeholder table, so it must be treated as unknown instead of trusted: both the
        // source and the sink must be SKIPPED.
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidatorFromString(
                        "source {\n"
                                + "  InMemorySource {\n"
                                + "    plugin_output = mem_out\n"
                                + "    schema = { table = \"placeholder_table\" }\n"
                                + "  }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  InMemory { plugin_input = [mem_out] }\n"
                                + "}");

        Assertions.assertEquals(2, results.size());
        DryRunConnectValidator.PluginResult sourceResult = results.get(0);
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.SKIPPED, sourceResult.getStatus());
        Assertions.assertTrue(
                sourceResult.getDetail().contains("declares no schema fields"),
                "Actual: " + sourceResult.getDetail());
        DryRunConnectValidator.PluginResult sinkResult = results.get(1);
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.SKIPPED, sinkResult.getStatus());
        Assertions.assertTrue(
                sinkResult.getDetail().contains("upstream schema not available"),
                "Actual: " + sinkResult.getDetail());
    }

    @Test
    public void testConnectDryRunReportsValidatedSink() {
        List<DryRunConnectValidator.PluginResult> results =
                runConnectValidatorFromString(
                        "source {\n"
                                + "  FakeSource {\n"
                                + "    plugin_output = fake_out\n"
                                + "    schema = { fields { id = int, name = string } }\n"
                                + "  }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  DryRunTestSink { plugin_input = [fake_out] }\n"
                                + "}");

        Assertions.assertEquals(2, results.size());
        DryRunConnectValidator.PluginResult sinkResult = results.get(1);
        Assertions.assertEquals(PluginType.SINK, sinkResult.getPluginType());
        Assertions.assertEquals(
                DryRunConnectValidator.PluginResult.Status.VALIDATED, sinkResult.getStatus());
    }

    @Test
    public void testConnectDryRunSinkValidationFailurePropagatesWithLocation() {
        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class,
                        () ->
                                runConnectValidatorFromString(
                                        "source {\n"
                                                + "  FakeSource {\n"
                                                + "    plugin_output = fake_out\n"
                                                + "    schema = { fields { id = int } }\n"
                                                + "  }\n"
                                                + "}\n"
                                                + "sink {\n"
                                                + "  DryRunTestSink {\n"
                                                + "    plugin_input = [fake_out]\n"
                                                + "    fail_validation = true\n"
                                                + "  }\n"
                                                + "}"));

        Assertions.assertTrue(
                exception.getMessage().contains("sink[0](DryRunTestSink)"),
                "Error must carry the plugin location. Actual: " + exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("simulated sink validation failure"),
                "Actual: " + exception.getMessage());
    }

    @Test
    public void testConnectDryRunFailsWhenSourceInfersNoSchema() {
        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class,
                        () ->
                                runConnectValidatorFromString(
                                        "source {\n"
                                                + "  DryRunTestSource { empty_schema = true }\n"
                                                + "}\n"
                                                + "sink {\n"
                                                + "  InMemory {}\n"
                                                + "}"));

        Assertions.assertTrue(
                exception.getMessage().contains("did not infer any source schema"),
                "Actual: " + exception.getMessage());
    }

    @Test
    public void testConnectDryRunSourceConnectionFailurePropagatesWithLocation() {
        ConfigCheckException exception =
                Assertions.assertThrows(
                        ConfigCheckException.class,
                        () ->
                                runConnectValidatorFromString(
                                        "source {\n"
                                                + "  DryRunTestSource { fail_connection = true }\n"
                                                + "}\n"
                                                + "sink {\n"
                                                + "  InMemory {}\n"
                                                + "}"));

        Assertions.assertTrue(
                exception.getMessage().contains("source[0](DryRunTestSource)"),
                "Error must carry the plugin location. Actual: " + exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("simulated connection failure"),
                "Actual: " + exception.getMessage());
    }

    @Test
    public void testConnectDryRunSourceConnectionFailureSanitizesSensitiveJdbcUrl()
            throws Exception {
        Path configFile =
                Files.createTempFile("seatunnel-sensitive-dryrun-connect-failure", ".conf");
        Files.write(
                configFile,
                ("source {\n"
                                + "  DryRunTestSource { sensitive_connection_failure = true }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  InMemory {}\n"
                                + "}")
                        .getBytes(StandardCharsets.UTF_8));
        configFile.toFile().deleteOnExit();

        ClientCommandArgs args = buildConnectArgsFromPath(configFile.toString());
        SeaTunnelConfValidateCommand command = new SeaTunnelConfValidateCommand(args);

        ConfigCheckException exception =
                Assertions.assertThrows(ConfigCheckException.class, command::execute);
        String message = exception.getMessage();
        Assertions.assertTrue(
                message.contains("source[0](DryRunTestSource)"),
                "Error must carry the plugin location. Actual: " + message);
        Assertions.assertTrue(
                message.contains("the configured JDBC URL"),
                "Error should keep a useful sanitized JDBC failure hint. Actual: " + message);
        Assertions.assertFalse(message.contains("alice:secret-password@"), "Actual: " + message);
        Assertions.assertFalse(message.contains("secret-password"), "Actual: " + message);
        Assertions.assertFalse(message.contains("secret-token"), "Actual: " + message);
        Assertions.assertFalse(message.contains("token=secret-token"), "Actual: " + message);

        StringWriter stackTrace = new StringWriter();
        exception.printStackTrace(new PrintWriter(stackTrace));
        Assertions.assertFalse(stackTrace.toString().contains("jdbc:"), stackTrace.toString());
        Assertions.assertFalse(
                stackTrace.toString().contains("secret-password"), stackTrace.toString());
        Assertions.assertFalse(
                stackTrace.toString().contains("secret-token"), stackTrace.toString());
    }

    private List<DryRunConnectValidator.PluginResult> runConnectValidator(String configFile) {
        Config config = ConfigBuilder.of(Paths.get(resolveConfigPath(configFile)));
        return runConnectValidator(config);
    }

    private List<DryRunConnectValidator.PluginResult> runConnectValidatorFromString(String hocon) {
        return runConnectValidator(ConfigFactory.parseString(hocon));
    }

    private List<DryRunConnectValidator.PluginResult> runConnectValidator(Config config) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        DryRunConnectValidator validator =
                new DryRunConnectValidator(
                        config.getConfigList("source"),
                        config.hasPath("transform")
                                ? config.getConfigList("transform")
                                : Collections.emptyList(),
                        config.getConfigList("sink"),
                        classLoader,
                        classLoader);
        return validator.validate();
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
                exception.getMessage().contains("Option validation failed"),
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
        return buildArgsFromPath(resolveConfigPath(configFile));
    }

    private ClientCommandArgs buildArgsFromPath(String configPath) {
        String[] args = {"-c", configPath, "--dry-run", "static"};
        return CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel.sh", true);
    }

    private ClientCommandArgs buildConnectArgs(String configFile) {
        return buildConnectArgsFromPath(resolveConfigPath(configFile));
    }

    private ClientCommandArgs buildConnectArgsFromPath(String configPath) {
        String[] args = {"-c", configPath, "--dry-run", "connect"};
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
