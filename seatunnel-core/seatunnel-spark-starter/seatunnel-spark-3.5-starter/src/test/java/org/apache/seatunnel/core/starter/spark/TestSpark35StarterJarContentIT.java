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

package org.apache.seatunnel.core.starter.spark;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;

class TestSpark35StarterJarContentIT {

    @TempDir Path tempDir;

    @Test
    void shadedJarUsesSparkScalaAndExternalLoggingBindings() throws IOException {
        try (JarFile jar = new JarFile(findStarterJar().toFile())) {
            Assertions.assertFalse(
                    jar.stream().anyMatch(entry -> entry.getName().startsWith("scala/")),
                    "The starter must use Spark's Scala runtime");
            Assertions.assertFalse(
                    jar.stream()
                            .anyMatch(
                                    entry ->
                                            entry.getName().startsWith("org/apache/logging/slf4j/")
                                                    || entry.getName()
                                                            .startsWith("org/slf4j/impl/")),
                    "Logging bindings must not be embedded in the starter jar");
        }
    }

    @Test
    void usagePreservesLegacyLauncherNameWhenUnspecified() throws Exception {
        assertUsage(null, "start-seatunnel-spark-3-connector-v2.sh");
    }

    @Test
    void usageNamesTheInvokingSpark35Launcher() throws Exception {
        assertUsage(
                "start-seatunnel-spark-3.5-connector-v2.sh",
                "start-seatunnel-spark-3.5-connector-v2.sh");
        assertUsage(
                "start-seatunnel-spark-3.5-connector-v2.cmd",
                "start-seatunnel-spark-3.5-connector-v2.cmd");
    }

    private void assertUsage(String configuredName, String expectedName) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(
                Paths.get(
                                System.getProperty("java.home"),
                                "bin",
                                System.getProperty("os.name").startsWith("Windows")
                                        ? "java.exe"
                                        : "java")
                        .toString());
        if (configuredName != null) {
            command.add("-Dseatunnel.spark.starter.shell.name=" + configuredName);
        }
        command.add("-cp");
        command.add(Paths.get("target/logging-e2e", "*") + File.pathSeparator + findStarterJar());
        command.add("org.apache.seatunnel.core.starter.spark.SparkStarter");
        command.add("-h");
        Path outputFile = Files.createTempFile(tempDir, "usage-", ".log");
        Process process =
                new ProcessBuilder(command)
                        .redirectErrorStream(true)
                        .redirectOutput(outputFile.toFile())
                        .start();
        try {
            Assertions.assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Starter help timed out");
            String output = new String(Files.readAllBytes(outputFile), StandardCharsets.UTF_8);
            Assertions.assertEquals(USAGE_EXIT_CODE, process.exitValue(), output);
            Assertions.assertTrue(output.contains("Usage: " + expectedName), output);
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                Assertions.assertTrue(
                        process.waitFor(10, TimeUnit.SECONDS), "Starter did not exit");
            }
        }
    }

    private static Path findStarterJar() throws IOException {
        try (Stream<Path> files = Files.list(Paths.get("target"))) {
            return files.filter(Files::isRegularFile)
                    .filter(
                            path -> {
                                String name = path.getFileName().toString();
                                return name.startsWith("seatunnel-spark-3.5-starter")
                                        && name.endsWith(".jar")
                                        && !name.endsWith("-sources.jar")
                                        && !name.endsWith("-javadoc.jar")
                                        && !name.endsWith("-tests.jar");
                            })
                    .sorted()
                    .findFirst()
                    .orElseThrow(
                            () -> new IllegalStateException("Spark 3.5 starter jar is missing"));
        }
    }
}
