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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.core.starter.command.Command;
import org.apache.seatunnel.core.starter.command.CommandArgs;
import org.apache.seatunnel.core.starter.utils.CommandLineUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

class NativeHelpExitCodeTest {

    @Test
    void nativeHelpCommandsExitSuccessfully() throws Exception {
        assertHelpExitsSuccessfully(SeaTunnelServer.class, "-h");
        assertHelpExitsSuccessfully(SeaTunnelServer.class, "-r", "master", "--help");
        assertHelpExitsSuccessfully(SeaTunnelServer.class, "-r", "worker", "-d", "--help");
        assertHelpExitsSuccessfully(SeaTunnelConnector.class, "--help");
        assertHelpExitsSuccessfully(SeaTunnelMetadataExporter.class, "-h");
    }

    @Test
    void invalidNativeCommandStillExitsUnsuccessfully() throws Exception {
        assertInvalidCommandExitsUnsuccessfully(SeaTunnelServer.class, "-r");
        assertInvalidCommandExitsUnsuccessfully(SeaTunnelConnector.class, "-pt", "invalid");
        assertInvalidCommandExitsUnsuccessfully(SeaTunnelMetadataExporter.class, "-pt", "invalid");
    }

    private static void assertInvalidCommandExitsUnsuccessfully(Class<?> mainClass, String... args)
            throws Exception {
        ProcessResult result = invokeMain(mainClass, args);

        Assertions.assertNotEquals(0, result.exitCode);
    }

    @Test
    void sharedParserKeepsEngineLauncherHelpExitCode() throws Exception {
        ProcessResult result = invokeMain(DefaultHelpParserMain.class, "--help");

        Assertions.assertEquals(234, result.exitCode, result.output);
    }

    private static void assertHelpExitsSuccessfully(Class<?> mainClass, String... args)
            throws Exception {
        ProcessResult result = invokeMain(mainClass, args);

        Assertions.assertEquals(0, result.exitCode, result.output);
        Assertions.assertTrue(result.output.contains("Usage:"), result.output);
    }

    private static ProcessResult invokeMain(Class<?> mainClass, String... args) throws Exception {
        Path java = Paths.get(System.getProperty("java.home"), "bin", "java");
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(java.toString(), "-cp", classpath, mainClass.getName()));
        command.addAll(Arrays.asList(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        Assertions.assertTrue(process.waitFor(15, TimeUnit.SECONDS), "CLI process timed out");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new ProcessResult(process.exitValue(), output);
    }

    public static class DefaultHelpParserMain {

        public static void main(String[] args) {
            CommandLineUtils.parse(args, new HelpOnlyCommandArgs(), "engine-starter", true);
        }
    }

    private static final class HelpOnlyCommandArgs extends CommandArgs {
        @Override
        public Command<?> buildCommand() {
            return null;
        }
    }

    private static final class ProcessResult {
        private final int exitCode;
        private final String output;

        private ProcessResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
