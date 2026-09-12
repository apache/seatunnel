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

package org.apache.seatunnel.edge.agent.starter.command;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EdgeAgentRunCommandTest {

    @TempDir Path tempDir;

    private String savedUserDir;

    @AfterEach
    void restoreProperties() {
        System.clearProperty(EdgeAgentEnvConstants.PROP_AGENT_HOME);
        if (savedUserDir != null) {
            System.setProperty("user.dir", savedUserDir);
            savedUserDir = null;
        }
    }

    private void useTempDirAsUserDir() {
        savedUserDir = System.getProperty("user.dir");
        System.setProperty("user.dir", tempDir.toAbsolutePath().toString());
    }

    private static EdgeAgentCommand<?> buildRun(String... args) {
        return EdgeAgentTopLevelCommand.resolve(args).buildCommand(args);
    }

    private static RunCommandArgs parseRun(String... args) {
        return EdgeAgentCommandLineUtils.parse(
                args, new RunCommandArgs(), EdgeAgentStarterConstants.PROGRAM_NAME);
    }

    @Nested
    class TopLevelResolve {

        @Test
        void emptyArgsResolveToRun() {
            Assertions.assertEquals(
                    EdgeAgentTopLevelCommand.RUN,
                    EdgeAgentTopLevelCommand.resolve(new String[] {}));
        }

        @Test
        void configFlagResolveToRun() {
            Assertions.assertEquals(
                    EdgeAgentTopLevelCommand.RUN,
                    EdgeAgentTopLevelCommand.resolve(new String[] {"--config", "agent.yaml"}));
        }

        @Test
        void dbPrefixResolveToDb() {
            Assertions.assertEquals(
                    EdgeAgentTopLevelCommand.DB,
                    EdgeAgentTopLevelCommand.resolve(new String[] {"db", "info"}));
        }
    }

    @Nested
    class RunArgsParsing {

        @Test
        void parseEmptyArgs() {
            Assertions.assertNull(parseRun().getExplicitConfigPath());
        }

        @Test
        void parseConfigFlag() {
            Assertions.assertEquals(
                    Paths.get("config/agent.yaml"),
                    parseRun("--config", "config/agent.yaml").getExplicitConfigPath());
        }

        @Test
        void parsePositionalPath() {
            Assertions.assertEquals(
                    Paths.get("/tmp/agent.yaml"),
                    parseRun("/tmp/agent.yaml").getExplicitConfigPath());
        }

        @Test
        void buildRunProducesRunAgentCommand() {
            Assertions.assertTrue(buildRun("--config", "agent.yaml") instanceof RunAgentCommand);
        }
    }

    @Nested
    class ConfigLocator {

        @Test
        void resolveExplicitPath() throws Exception {
            Path yaml = tempDir.resolve("agent.yaml");
            Files.write(yaml, "input:\n  id: x\n".getBytes(StandardCharsets.UTF_8));
            Path resolved = EdgeAgentConfigLocator.resolve(yaml);
            Assertions.assertEquals(yaml.toAbsolutePath().normalize(), resolved);
        }

        @Test
        void resolveFailsWhenExplicitMissing() {
            Assertions.assertThrows(
                    FileNotFoundException.class,
                    () -> EdgeAgentConfigLocator.resolve(tempDir.resolve("missing.yaml")));
        }

        @Test
        void resolveFailsWhenNoCandidateInWorkingDirectory() {
            useTempDirAsUserDir();
            FileNotFoundException ex =
                    Assertions.assertThrows(
                            FileNotFoundException.class,
                            () -> EdgeAgentConfigLocator.resolve(null));
            Assertions.assertTrue(
                    ex.getMessage().contains(EdgeAgentEnvConstants.ENV_EDGE_AGENT_CONFIG));
        }
    }
}
