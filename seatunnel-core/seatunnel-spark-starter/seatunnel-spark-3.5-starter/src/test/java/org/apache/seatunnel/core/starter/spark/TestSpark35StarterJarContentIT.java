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
import org.junit.jupiter.api.Assumptions;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;

class TestSpark35StarterJarContentIT {

    private static final String WINDOWS_LAUNCHER = "start-seatunnel-spark-3.5-connector-v2.cmd";

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

    @Test
    void windowsLauncherPrintsHelpFromPathWithSpacesAndMetacharacters() throws Exception {
        Path home = prepareWindowsDistribution();
        for (String[] arguments : new String[][] {{"-h"}, {}}) {
            ProcessResult result = runWindowsLauncher(home, "-Xmx128m", arguments);
            Assertions.assertEquals(0, result.exitCode, result.diagnostic());
            Assertions.assertTrue(
                    result.stdout.contains("Usage: " + WINDOWS_LAUNCHER), result.diagnostic());
            assertNoSubmissionOrTemporaryOutput();
        }
    }

    @Test
    void windowsLauncherPreservesJavaFailureWithEmptyStdout() throws Exception {
        Path home = prepareWindowsDistribution();
        // Fail the real JVM before main(), without replacing java with a batch-file stub.
        String invalidOption = "-XX:SeaTunnelInvalidOption";
        ProcessResult javaFailure =
                runProcess(
                        new ProcessBuilder(
                                javaExecutable().toString(),
                                "-Xmx128m",
                                invalidOption,
                                "-version"));
        Assertions.assertNotEquals(0, javaFailure.exitCode, javaFailure.diagnostic());
        Assertions.assertEquals("", javaFailure.stdout, javaFailure.diagnostic());
        Assertions.assertTrue(
                javaFailure.stderr.contains("SeaTunnelInvalidOption"), javaFailure.diagnostic());

        ProcessResult result = runWindowsLauncher(home, "-Xmx128m " + invalidOption, "-h");
        Assertions.assertEquals(javaFailure.exitCode, result.exitCode, result.diagnostic());
        Assertions.assertEquals("", result.stdout, result.diagnostic());
        Assertions.assertTrue(
                result.stderr.contains("SeaTunnelInvalidOption"), result.diagnostic());
        assertNoSubmissionOrTemporaryOutput();
    }

    @Test
    void windowsLauncherPassesQuotedConfigPathWithSpaces() throws Exception {
        assertWindowsConfigPath("distribution with spaces", "job with spaces.conf");
    }

    @Test
    void windowsLauncherPassesQuotedConfigPathWithMetacharacters() throws Exception {
        assertWindowsConfigPath(
                "distribution !seatunnel_missing! & spaces",
                "job !seatunnel_missing! & spaces.conf");
    }

    private void assertWindowsConfigPath(String directoryName, String fileName) throws Exception {
        Path home = prepareWindowsDistribution(directoryName);
        Path config = home.resolve("config").resolve(fileName);
        // A parse error proves the real starter opened this file, not a split or expanded path.
        // This intentionally stops before the legacy command-string handoff to spark-submit.
        Files.write(config, "env {\n".getBytes(StandardCharsets.UTF_8));
        // First prove the fixture's cmd-to-Java boundary without the production launcher.
        ProcessResult direct =
                runWindowsCommand(
                        home,
                        "-Xmx128m",
                        "java %JAVA_OPTS% -cp \"%SEATUNNEL_TEST_CLASSPATH%\" "
                                + "org.apache.seatunnel.core.starter.spark.SparkStarter",
                        "--config",
                        config.toString());
        assertConfigParseFailure(direct, fileName, "Direct cmd-to-Java control");
        ProcessResult result = runWindowsLauncher(home, "-Xmx128m", "--config", config.toString());
        assertConfigParseFailure(result, fileName, "Production launcher");
        assertNoSubmissionOrTemporaryOutput();
    }

    private void assertConfigParseFailure(ProcessResult result, String fileName, String boundary) {
        String diagnostic = boundary + "\n" + result.diagnostic();
        Assertions.assertNotEquals(0, result.exitCode, diagnostic);
        Assertions.assertTrue(result.stderr.contains("ConfigException$Parse"), diagnostic);
        Assertions.assertTrue(result.stderr.contains(fileName), diagnostic);
    }

    private Path prepareWindowsDistribution() throws IOException {
        return prepareWindowsDistribution("distribution !seatunnel_missing! & spaces");
    }

    private Path prepareWindowsDistribution(String directoryName) throws IOException {
        Assumptions.assumeTrue(
                System.getProperty("os.name").startsWith("Windows"),
                "Requires native Windows cmd.exe; not exercised by non-Windows builds");
        Path home = Files.createDirectories(tempDir.resolve(directoryName));
        Path bin = Files.createDirectories(home.resolve("bin"));
        Files.createDirectories(home.resolve("config"));
        Path logging = Files.createDirectories(home.resolve("starter/logging"));
        Files.copy(Paths.get("src/main/bin", WINDOWS_LAUNCHER), bin.resolve(WINDOWS_LAUNCHER));
        Files.copy(findStarterJar(), home.resolve("starter/seatunnel-spark-3.5-starter.jar"));
        try (Stream<Path> jars = Files.list(Paths.get("target/logging-e2e"))) {
            for (Path jar :
                    (Iterable<Path>)
                            jars.filter(path -> path.toString().endsWith(".jar"))::iterator) {
                Files.copy(jar, logging.resolve(jar.getFileName()));
            }
        }
        Files.createDirectories(tempDir.resolve("cmd-temp !seatunnel_missing! & spaces"));
        Path sparkBin = Files.createDirectories(tempDir.resolve("spark/bin"));
        // Only a sentinel: none of these help/failure cases should reach Spark submission.
        Files.write(
                sparkBin.resolve("spark-submit.cmd"),
                ("@echo off\r\n> \"%SEATUNNEL_SUBMIT_MARKER%\" echo unexpected\r\nexit /b 97\r\n")
                        .getBytes(StandardCharsets.UTF_8));
        return home;
    }

    private ProcessResult runWindowsLauncher(Path home, String javaOptions, String... arguments)
            throws Exception {
        return runWindowsCommand(home, javaOptions, WINDOWS_LAUNCHER, arguments);
    }

    private ProcessResult runWindowsCommand(
            Path home, String javaOptions, String command, String... arguments) throws Exception {
        // ProcessBuilder receives only a simple batch filename, not a cmd command string with
        // Java-quoted paths. The driver quotes each fixture argument at the cmd boundary.
        ProcessBuilder builder =
                new ProcessBuilder(
                                Paths.get(System.getenv("SystemRoot"), "System32", "cmd.exe")
                                        .toString(),
                                "/d",
                                "/v:off",
                                "/c",
                                "launcher-test.cmd")
                        .directory(home.resolve("bin").toFile());
        StringBuilder driver =
                new StringBuilder("@echo off\r\nsetlocal disabledelayedexpansion\r\n")
                        .append(command);
        for (int i = 0; i < arguments.length; i++) {
            String name = "SEATUNNEL_TEST_ARG_" + i;
            builder.environment().put(name, arguments[i]);
            driver.append(" \"%").append(name).append("%\"");
        }
        driver.append("\r\n");
        Files.write(
                home.resolve("bin/launcher-test.cmd"),
                driver.toString().getBytes(StandardCharsets.UTF_8));
        builder.environment()
                .put(
                        "SEATUNNEL_TEST_CLASSPATH",
                        home.resolve("starter/logging")
                                + File.separator
                                + "*"
                                + File.pathSeparator
                                + home.resolve("starter/seatunnel-spark-3.5-starter.jar"));
        builder.environment().put("JAVA_OPTS", javaOptions);
        builder.environment().put("SPARK_HOME", tempDir.resolve("spark").toString());
        builder.environment()
                .put("SEATUNNEL_SUBMIT_MARKER", tempDir.resolve("submitted.txt").toString());
        builder.environment()
                .put("TEMP", tempDir.resolve("cmd-temp !seatunnel_missing! & spaces").toString());
        builder.environment().put("TMP", builder.environment().get("TEMP"));
        builder.environment()
                .put(
                        "PATH",
                        javaExecutable().getParent() + File.pathSeparator + System.getenv("PATH"));
        return runProcess(builder);
    }

    private ProcessResult runProcess(ProcessBuilder builder) throws Exception {
        Map<String, String> environment = builder.environment();
        environment
                .keySet()
                .removeIf(
                        name ->
                                name.equalsIgnoreCase("JAVA_TOOL_OPTIONS")
                                        || name.equalsIgnoreCase("JDK_JAVA_OPTIONS")
                                        || name.equalsIgnoreCase("_JAVA_OPTIONS")
                                        || name.equalsIgnoreCase("seatunnel_missing"));
        Path stdout = Files.createTempFile(tempDir, "stdout-", ".log");
        Path stderr = Files.createTempFile(tempDir, "stderr-", ".log");
        Process process =
                builder.redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
        try {
            Assertions.assertTrue(
                    process.waitFor(30, TimeUnit.SECONDS), "Launcher process timed out");
            return new ProcessResult(
                    process.exitValue(),
                    new String(Files.readAllBytes(stdout), StandardCharsets.UTF_8),
                    new String(Files.readAllBytes(stderr), StandardCharsets.UTF_8));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                Assertions.assertTrue(
                        process.waitFor(10, TimeUnit.SECONDS), "Launcher did not exit");
            }
        }
    }

    private void assertNoSubmissionOrTemporaryOutput() throws IOException {
        Assertions.assertFalse(
                Files.exists(tempDir.resolve("submitted.txt")), "Unexpected Spark submission");
        try (Stream<Path> files =
                Files.list(tempDir.resolve("cmd-temp !seatunnel_missing! & spaces"))) {
            List<Path> leftovers =
                    files.filter(
                                    path ->
                                            path.getFileName()
                                                    .toString()
                                                    .startsWith("seatunnel-spark-"))
                            .collect(Collectors.toList());
            Assertions.assertTrue(
                    leftovers.isEmpty(),
                    "Launcher did not clean its temporary output: " + leftovers);
        }
    }

    @Test
    void temporaryOutputCheckIgnoresOtherOwners() throws IOException {
        Path directory =
                Files.createDirectories(tempDir.resolve("cmd-temp !seatunnel_missing! & spaces"));
        Files.createDirectory(directory.resolve("hsperfdata-test"));
        Files.createFile(directory.resolve("unrelated.tmp"));
        assertNoSubmissionOrTemporaryOutput();
    }

    @Test
    void temporaryOutputCheckDetectsLauncherOutput() throws IOException {
        Path directory =
                Files.createDirectories(tempDir.resolve("cmd-temp !seatunnel_missing! & spaces"));
        Files.createDirectory(directory.resolve("seatunnel-spark-123-456"));
        Assertions.assertThrows(AssertionError.class, this::assertNoSubmissionOrTemporaryOutput);
    }

    private static Path javaExecutable() {
        return Paths.get(System.getProperty("java.home"), "bin", "java.exe");
    }

    private static class ProcessResult {
        private final int exitCode;
        private final String stdout;
        private final String stderr;

        private ProcessResult(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        private String diagnostic() {
            return "exit=" + exitCode + "\nstdout:\n" + stdout + "\nstderr:\n" + stderr;
        }
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
        command.add(
                Paths.get("target/logging-e2e")
                        + File.separator
                        + "*"
                        + File.pathSeparator
                        + findStarterJar());
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
