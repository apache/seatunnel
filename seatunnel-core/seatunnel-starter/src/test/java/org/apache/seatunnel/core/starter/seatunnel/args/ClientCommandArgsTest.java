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

package org.apache.seatunnel.core.starter.seatunnel.args;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.enums.DryRun;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandExecuteException;
import org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand;
import org.apache.seatunnel.core.starter.seatunnel.multitable.MultiTableSinkTest;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static com.github.stefanbirkner.systemlambda.SystemLambda.catchSystemExit;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;

public class ClientCommandArgsTest {
    @Test
    public void testExecuteClientCommandArgsWithPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = buildClientCommandArgs(configFile);
        Assertions.assertDoesNotThrow(() -> SeaTunnel.run(clientCommandArgs.buildCommand()));
    }

    @Test
    public void testSetJobId() throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        long jobId = 999;
        ClientCommandArgs clientCommandArgs = buildClientCommandArgs(configFile, jobId);
        Assertions.assertDoesNotThrow(() -> SeaTunnel.run(clientCommandArgs.buildCommand()));
    }

    @Test
    public void testExecuteClientCommandArgsWithoutPluginName()
            throws FileNotFoundException, URISyntaxException {
        String configurePath = "/config/fake_to_inmemory_without_pluginname.json";
        String configFile = MultiTableSinkTest.getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = buildClientCommandArgs(configFile);
        CommandExecuteException commandExecuteException =
                Assertions.assertThrows(
                        CommandExecuteException.class,
                        () -> SeaTunnel.run(clientCommandArgs.buildCommand()));
        Assertions.assertEquals(
                String.format(
                        "The '%s' option is not configured, please configure it.",
                        PLUGIN_NAME.key()),
                commandExecuteException.getCause().getMessage());
    }

    @Test
    public void testDryRunParam() {
        String[] args = {"-c", "app.conf", "--dry-run", "static"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);
        Assertions.assertEquals(DryRun.STATIC, clientCommandArgs.getDryRun());
    }

    @Test
    public void testConnectDryRunParam() {
        String[] args = {"-c", "app.conf", "--dry-run", "connect"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);
        Assertions.assertEquals(DryRun.CONNECT, clientCommandArgs.getDryRun());
    }

    @Test
    public void testSampleDryRunParam() {
        String[] args = {
            "-c",
            "app.conf",
            "--master",
            "local",
            "--dry-run",
            "sample",
            "--sample-limit",
            "5",
            "--sample-print-data"
        };
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);

        Assertions.assertEquals(DryRun.SAMPLE, clientCommandArgs.getDryRun());
        Assertions.assertEquals(5, clientCommandArgs.getSampleLimit());
        Assertions.assertTrue(clientCommandArgs.isSamplePrintData());
        Assertions.assertInstanceOf(ClientExecuteCommand.class, clientCommandArgs.buildCommand());
    }

    @Test
    public void testSampleDryRunDefaultLimit() {
        String[] args = {"-c", "app.conf", "--master", "local", "--dry-run", "sample"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);

        Assertions.assertEquals(10, clientCommandArgs.getSampleLimit());
    }

    @Test
    public void testSampleDryRunRequiresLocalMaster() {
        String[] args = {"-c", "app.conf", "--dry-run", "sample"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);

        Assertions.assertThrows(
                com.beust.jcommander.ParameterException.class, clientCommandArgs::buildCommand);
    }

    @Test
    public void testSampleDryRunRejectsInvalidLimit() {
        ClientCommandArgs.PositiveIntegerValidator validator =
                new ClientCommandArgs.PositiveIntegerValidator();
        Assertions.assertThrows(
                com.beust.jcommander.ParameterException.class,
                () -> validator.validate("--sample-limit", "0"));
        Assertions.assertThrows(
                com.beust.jcommander.ParameterException.class,
                () -> validator.validate("--sample-limit", "10001"));
    }

    @Test
    public void testSampleOptionsRequireSampleMode() {
        String[] args = {"-c", "app.conf", "--sample-limit", "5"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);

        Assertions.assertThrows(
                com.beust.jcommander.ParameterException.class, clientCommandArgs::buildCommand);
    }

    @Test
    public void testSampleOptionsUseCommandLineUsagePath() throws Exception {
        String[] args = {"-c", "app.conf", "--sample-limit", "5"};

        int statusCode =
                catchSystemExit(
                        () ->
                                CommandLineUtils.parse(
                                        args,
                                        new ClientCommandArgs(),
                                        "seatunnel-client",
                                        true,
                                        ClientCommandArgs::validateCommandOptions));

        Assertions.assertEquals(USAGE_EXIT_CODE, statusCode);
    }

    @Test
    public void testSampleDryRunRejectsJobControlOptions() {
        String[] args = {"-c", "app.conf", "--master", "local", "--dry-run", "sample", "--check"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);

        Assertions.assertThrows(
                com.beust.jcommander.ParameterException.class, clientCommandArgs::buildCommand);
    }

    @Test
    public void testSampleDryRunRejectsRestoreWithCheckpoint() {
        String[] args = {
            "-c",
            "app.conf",
            "--master",
            "local",
            "--dry-run",
            "sample",
            "--restore-with-checkpoint",
            "123"
        };
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);

        Assertions.assertThrows(
                com.beust.jcommander.ParameterException.class, clientCommandArgs::buildCommand);
    }

    @Test
    public void testDryRunConverterWithValidStatic() {
        ClientCommandArgs.DryRunConverter converter = new ClientCommandArgs.DryRunConverter();
        Assertions.assertEquals(DryRun.STATIC, converter.convert("static"));
        Assertions.assertEquals(DryRun.STATIC, converter.convert("STATIC"));
        Assertions.assertEquals(DryRun.CONNECT, converter.convert("connect"));
        Assertions.assertEquals(DryRun.CONNECT, converter.convert("CONNECT"));
        Assertions.assertEquals(DryRun.SAMPLE, converter.convert("sample"));
        Assertions.assertEquals(DryRun.SAMPLE, converter.convert("SAMPLE"));
    }

    @Test
    public void testDryRunConverterRejectsUnsupportedModes() {
        ClientCommandArgs.DryRunConverter converter = new ClientCommandArgs.DryRunConverter();
        Assertions.assertThrows(IllegalArgumentException.class, () -> converter.convert("shadow"));
    }

    @Test
    public void testDryRunConverterRejectsInvalidMode() {
        ClientCommandArgs.DryRunConverter converter = new ClientCommandArgs.DryRunConverter();
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> converter.convert("nonexistent_mode"));
        Assertions.assertTrue(
                ex.getMessage().contains("Currently only [static, connect, sample] are supported"),
                "Actual: " + ex.getMessage());
    }

    @Test
    public void testParseRestoreFromCheckpointJobArg() {
        String[] args = {"-c", "app.conf", "--restore-with-checkpoint", "123"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);
        Assertions.assertEquals("123", clientCommandArgs.getRestoreWithCheckpointJobId());
        Assertions.assertNull(clientCommandArgs.getRestoreJobId());
    }

    @Test
    public void testRejectRestoreAndRestoreFromCheckpointTogether() {
        String[] args = {"-c", "app.conf", "--restore", "100", "--restore-with-checkpoint", "200"};
        ClientCommandArgs clientCommandArgs =
                CommandLineUtils.parse(args, new ClientCommandArgs(), "seatunnel-client", true);
        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class, clientCommandArgs::buildCommand);
        Assertions.assertTrue(
                ex.getMessage()
                        .contains("--restore and --restore-with-checkpoint are mutually exclusive"),
                "Actual: " + ex.getMessage());
    }

    @Test
    public void testRejectBlankRestoreWithCheckpointJobId() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setRestoreWithCheckpointJobId("   ");

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class, clientCommandArgs::buildCommand);
        Assertions.assertTrue(
                ex.getMessage()
                        .contains(
                                "restoreSourceJobId is required when using --restore-with-checkpoint"),
                "Actual: " + ex.getMessage());
    }

    @Test
    public void testRejectNonNumericRestoreWithCheckpointJobId() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setRestoreWithCheckpointJobId("abc");

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class, clientCommandArgs::buildCommand);
        Assertions.assertTrue(
                ex.getMessage()
                        .contains("--restore-with-checkpoint requires a numeric jobId, got: abc"),
                "Actual: " + ex.getMessage());
    }

    @Test
    public void testTrimRestoreWithCheckpointJobId() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setRestoreWithCheckpointJobId(" 123 ");

        Assertions.assertDoesNotThrow(clientCommandArgs::buildCommand);
        Assertions.assertEquals("123", clientCommandArgs.getRestoreWithCheckpointJobId());
    }

    @Test
    public void testRejectSavepointAndRestoreFromCheckpointTogether() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setSavePointJobId("100");
        clientCommandArgs.setRestoreWithCheckpointJobId("200");

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class, clientCommandArgs::buildCommand);
        Assertions.assertTrue(
                ex.getMessage()
                        .contains(
                                "--savepoint and --restore-with-checkpoint are mutually exclusive"),
                "Actual: " + ex.getMessage());
    }

    @Test
    public void testRejectNonNumericCustomJobId() {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setCustomJobId("not-a-number");

        IllegalArgumentException ex =
                Assertions.assertThrows(
                        IllegalArgumentException.class, clientCommandArgs::buildCommand);
        Assertions.assertTrue(
                ex.getMessage()
                        .contains("--set-job-id requires a numeric jobId, got: not-a-number"),
                "Actual: " + ex.getMessage());
    }

    private static ClientCommandArgs buildClientCommandArgs(String configFile, Long jobId) {
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setVariables(new ArrayList<>());
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setMasterType(MasterType.LOCAL);
        clientCommandArgs.setCheckConfig(false);
        if (jobId != null) {
            clientCommandArgs.setCustomJobId(String.valueOf(jobId));
        }
        return clientCommandArgs;
    }

    private static ClientCommandArgs buildClientCommandArgs(String configFile) {
        return buildClientCommandArgs(configFile, null);
    }
}
