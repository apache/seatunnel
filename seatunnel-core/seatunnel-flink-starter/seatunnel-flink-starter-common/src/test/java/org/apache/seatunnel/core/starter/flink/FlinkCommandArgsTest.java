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

package org.apache.seatunnel.core.starter.flink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Verifies Flink restore command construction and the actual POSIX wrapper argument boundaries.
 *
 * <p>These cases build commands without starting Flink jobs. Job execution remains in the Flink
 * 1.15 starter so its existing planner/runtime test classpath is preserved.
 */
public class FlinkCommandArgsTest {
    @Test
    public void testBuildFlinkCommandArgsWithSavePoint() {
        String checkpointPath =
                "hdfs:///flink/checkpoints/3c298a925d9a2a7837bbf5a8e4966b4f/chk-7902";

        assertRestorePath("-s", checkpointPath);
        assertRestorePath("--fromSavepoint", checkpointPath);
        assertRestorePath("--fromCheckpoint", checkpointPath);
    }

    @Test
    public void testBuildFlinkCommandArgsWithoutSavePoint() {
        FlinkStarter flinkStarter =
                new FlinkStarter(
                        new String[] {
                            "--target", "local",
                            "--deploy-mode", "run",
                            "--config", "/config/fake_to_inmemory1.json",
                        });
        List<String> commands = flinkStarter.buildCommands();
        Assertions.assertFalse(
                commands.contains("-s") || commands.contains("--fromSavepoint"),
                "The flink commands should not include either `-s` or `--fromSavepoint`");
    }

    @Test
    public void testBuildFlinkCommandArgsWithLocalCheckpointPath() {
        assertRestorePath("--fromCheckpoint", "/tmp/flink/chk-1");
    }

    @Test
    public void testBuildFlinkCommandArgsWithBlankCheckpointPath() {
        FlinkStarter flinkStarter =
                new FlinkStarter(
                        new String[] {
                            "--target", "local",
                            "--deploy-mode", "run",
                            "--config", "/config/fake_to_inmemory1.json",
                            "--fromCheckpoint", " "
                        });
        Assertions.assertThrows(IllegalArgumentException.class, flinkStarter::buildCommands);
    }

    @Test
    public void testRestorePathInApplicationModeRemainsNativeFlinkOption() {
        FlinkStarter starter =
                new FlinkStarter(
                        new String[] {
                            "--target",
                            "yarn-application",
                            "--deploy-mode",
                            "run-application",
                            "--config",
                            "/config/fake_to_inmemory1.json",
                            "--fromCheckpoint",
                            "hdfs:///checkpoints/chk-1"
                        });
        List<String> commands = starter.buildCommands();
        Assertions.assertEquals("run-application", commands.get(1));
        Assertions.assertEquals(
                Arrays.asList("-s", "'hdfs:///checkpoints/chk-1'"), commands.subList(2, 4));
        Assertions.assertTrue(commands.indexOf("-s") < commands.indexOf("-c"));
    }

    @Test
    public void testAllowNonRestoredStateRemainsBeforeTheJobArguments() {
        for (String mode : new String[] {"run", "run-application"}) {
            FlinkStarter starter =
                    new FlinkStarter(
                            new String[] {
                                "--target",
                                "run".equals(mode) ? "local" : "yarn-application",
                                "--deploy-mode",
                                mode,
                                "--config",
                                "job.conf",
                                "--fromCheckpoint",
                                "hdfs:///checkpoints/chk-1",
                                "--allowNonRestoredState"
                            });
            List<String> commands = starter.buildCommands();
            Assertions.assertEquals(
                    Arrays.asList("-s", "'hdfs:///checkpoints/chk-1'"), commands.subList(2, 4));
            int flagIndex = commands.indexOf("--allowNonRestoredState");
            Assertions.assertTrue(
                    flagIndex >= 4 && flagIndex < commands.indexOf("-c"),
                    "The native Flink restore flag must precede the SeaTunnel main class");
            Assertions.assertEquals(flagIndex, commands.lastIndexOf("--allowNonRestoredState"));
        }
    }

    @Test
    public void testRestorePathIsQuotedAsOneLiteralShellArgument() {
        FlinkStarter starter =
                new FlinkStarter(
                        new String[] {
                            "--target",
                            "local",
                            "--deploy-mode",
                            "run",
                            "--config",
                            "/config/fake_to_inmemory1.json",
                            "--fromCheckpoint",
                            "/tmp/check point's/$USER;echo"
                        });
        Assertions.assertEquals(
                "'/tmp/check point'\"'\"'s/$USER;echo'", starter.buildCommands().get(3));
    }

    @Test
    public void testRestorePathRejectsLineSeparators() {
        for (String path : new String[] {"/tmp/check\npoint", "/tmp/check\rpoint"}) {
            FlinkStarter starter =
                    new FlinkStarter(
                            new String[] {
                                "--target",
                                "local",
                                "--deploy-mode",
                                "run",
                                "--config",
                                "job.conf",
                                "-s",
                                path
                            });
            Assertions.assertThrows(IllegalArgumentException.class, starter::buildCommands);
        }
    }

    /**
     * Exercises both real shell hops using capture-only Java and Flink executables.
     *
     * <p>The path includes repeated spaces and shell metacharacters to detect argv flattening.
     */
    @EnabledOnOs({OS.LINUX, OS.MAC})
    @Test
    public void testShellWrappersPreserveRestoreArguments(@TempDir Path temporaryDirectory)
            throws Exception {
        Path repository = Paths.get("").toAbsolutePath();
        while (repository != null && !Files.isDirectory(repository.resolve("seatunnel-core"))) {
            repository = repository.getParent();
        }
        Assertions.assertNotNull(
                repository, "The wrapper source tree must be available to this test");
        String restorePath = "/tmp/check  point's/$USER;echo";
        String[] arguments = {
            "--target",
            "local",
            "--deploy-mode",
            "run",
            "--config",
            "job.conf",
            "--fromCheckpoint",
            restorePath
        };
        String generatedCommand = String.join(" ", new FlinkStarter(arguments).buildCommands());
        for (String version : new String[] {"13", "15", "20"}) {
            Path app = Files.createDirectories(temporaryDirectory.resolve("wrapper-" + version));
            Path bin = Files.createDirectories(app.resolve("bin"));
            Path script = bin.resolve("start-seatunnel-flink.sh");
            Files.copy(
                    repository.resolve(
                            "seatunnel-core/seatunnel-flink-starter/seatunnel-flink-"
                                    + version
                                    + "-starter/src/main/bin/start-seatunnel-flink-"
                                    + version
                                    + "-connector-v2.sh"),
                    script);
            Path java = bin.resolve("java");
            Files.write(
                    java,
                    "#!/bin/sh\nprintf '%s\\0' \"$@\" > \"$CAPTURE_JAVA_ARGS\"\nprintf '%s\\n' \"$COMMAND_TO_EXECUTE\"\n"
                            .getBytes(StandardCharsets.UTF_8));
            Path flink = bin.resolve("flink");
            Files.write(
                    flink,
                    "#!/bin/sh\nprintf '%s\\0' \"$@\" > \"$CAPTURE_FLINK_ARGS\"\n"
                            .getBytes(StandardCharsets.UTF_8));
            Assertions.assertTrue(java.toFile().setExecutable(true));
            Assertions.assertTrue(flink.toFile().setExecutable(true));
            Path javaArgs = app.resolve("java-args");
            Path flinkArgs = app.resolve("flink-args");
            Path log = app.resolve("wrapper.log");
            List<String> command = new ArrayList<>(Arrays.asList("/bin/bash", script.toString()));
            command.addAll(Arrays.asList(arguments));
            ProcessBuilder processBuilder =
                    new ProcessBuilder(command)
                            .redirectErrorStream(true)
                            .redirectOutput(log.toFile());
            processBuilder
                    .environment()
                    .put("PATH", bin + File.pathSeparator + System.getenv("PATH"));
            processBuilder.environment().put("FLINK_HOME", app.toString());
            processBuilder.environment().put("COMMAND_TO_EXECUTE", generatedCommand);
            processBuilder.environment().put("CAPTURE_JAVA_ARGS", javaArgs.toString());
            processBuilder.environment().put("CAPTURE_FLINK_ARGS", flinkArgs.toString());
            Process process = processBuilder.start();
            try {
                Assertions.assertTrue(
                        process.waitFor(30, TimeUnit.SECONDS), "Wrapper did not finish");
                Assertions.assertEquals(
                        0,
                        process.exitValue(),
                        new String(Files.readAllBytes(log), StandardCharsets.UTF_8));
            } finally {
                process.destroyForcibly();
            }
            List<String> input =
                    Arrays.asList(
                            new String(Files.readAllBytes(javaArgs), StandardCharsets.UTF_8)
                                    .split("\\x00"));
            Assertions.assertEquals(restorePath, input.get(input.indexOf("--fromCheckpoint") + 1));
            List<String> output =
                    Arrays.asList(
                            new String(Files.readAllBytes(flinkArgs), StandardCharsets.UTF_8)
                                    .split("\\x00"));
            Assertions.assertEquals(restorePath, output.get(output.indexOf("-s") + 1));
        }
    }

    private static void assertRestorePath(String option, String restorePath) {
        FlinkStarter flinkStarter =
                new FlinkStarter(
                        new String[] {
                            "--target",
                            "local",
                            "--deploy-mode",
                            "run",
                            "--config",
                            "/config/fake_to_inmemory1.json",
                            option,
                            restorePath
                        });

        List<String> commands = flinkStarter.buildCommands();

        Assertions.assertEquals(
                Arrays.asList("-s", "\'" + restorePath + "\'"), commands.subList(2, 4));
        Assertions.assertTrue(commands.indexOf("-s") < commands.indexOf("-c"));
    }
}
