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

package org.apache.seatunnel.edge.agent.starter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

class EdgeAgentHelpExitCodeTest {

    @Test
    void edgeAgentHelpCommandsExitSuccessfully() throws Exception {
        assertHelpExitsSuccessfully("-h");
        assertHelpExitsSuccessfully("--help");
        assertHelpExitsSuccessfully("db", "-h");
        assertHelpExitsSuccessfully("db", "--help");
        assertHelpExitsSuccessfully("db", "help");
    }

    @Test
    void invalidEdgeAgentArgumentsRemainUnsuccessful() throws Exception {
        ProcessResult result = invokeMain("--invalid-option");

        Assertions.assertNotEquals(0, result.exitCode);
    }

    private static void assertHelpExitsSuccessfully(String... args) throws Exception {
        ProcessResult result = invokeMain(args);

        Assertions.assertEquals(0, result.exitCode, result.output);
        Assertions.assertTrue(result.output.contains("Usage:"), result.output);
    }

    private static ProcessResult invokeMain(String... args) throws Exception {
        Path java = Paths.get(System.getProperty("java.home"), "bin", "java");
        String classpath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        List<String> command =
                new ArrayList<>(
                        Arrays.asList(
                                java.toString(),
                                "-cp",
                                classpath,
                                EdgeAgentStarter.class.getName()));
        command.addAll(Arrays.asList(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        Assertions.assertTrue(process.waitFor(15, TimeUnit.SECONDS), "CLI process timed out");
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new ProcessResult(process.exitValue(), output);
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
