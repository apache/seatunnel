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

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;

public class TestSparkStarterJarContentIT {

    @TempDir Path tempDir;

    private static final String ARTIFACT_ID = "seatunnel-spark-3-starter";
    private static final String SPARK_STARTER_MAIN =
            "org.apache.seatunnel.core.starter.spark.SparkStarter";

    @Test
    public void testShadedJarDoesNotContainScalaClasses() throws IOException {
        try (JarFile jarFile = new JarFile(findStarterJar().toFile())) {
            Assertions.assertFalse(
                    jarFile.stream().anyMatch(entry -> entry.getName().startsWith("scala/")),
                    "The Spark starter must use Spark's Scala runtime instead of embedding one");
        }
    }

    @Test
    public void testStarterUsageRunsFromShadedJar() throws Exception {
        Process process =
                new ProcessBuilder(
                                javaExecutable().toString(),
                                "-cp",
                                starterClasspath(),
                                SPARK_STARTER_MAIN,
                                "-h")
                        .redirectErrorStream(true)
                        .start();

        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        String output = readOutput(process);
        Assertions.assertTrue(finished, output);
        Assertions.assertEquals(USAGE_EXIT_CODE, process.exitValue(), output);
        Assertions.assertTrue(output.contains("Usage:"), output);
    }

    @Test
    public void testStarterBuildsSparkSubmitCommandFromShadedJar() throws Exception {
        Path configFile =
                Paths.get("src/test/resources/config/fake_to_inmemory.json").toAbsolutePath();
        Process process =
                new ProcessBuilder(
                                javaExecutable().toString(),
                                "-cp",
                                starterClasspath(),
                                SPARK_STARTER_MAIN,
                                "--config",
                                configFile.toString(),
                                "--deploy-mode",
                                "client")
                        .redirectErrorStream(true)
                        .start();

        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
        }
        String output = readOutput(process);
        Assertions.assertTrue(finished, output);
        Assertions.assertEquals(0, process.exitValue(), output);
        Assertions.assertTrue(output.contains("${SPARK_HOME}/bin/spark-submit"), output);
        Assertions.assertTrue(output.contains("SeaTunnelSpark"), output);
    }

    @Test
    public void testClusterPluginArchivesThroughStarter() throws Exception {
        Path home = Files.createDirectories(tempDir.resolve("home"));
        Path plugins = Files.createDirectories(home.resolve("plugins"));
        Path payload = plugins.resolve("payload.txt");
        Files.write(payload, "first".getBytes(StandardCharsets.UTF_8));
        Path first = Files.createDirectory(tempDir.resolve("submission one"));
        Path second = Files.createDirectory(tempDir.resolve("submission two"));
        String firstOutput = runClusterStarter(home, first.toString(), true);
        Assertions.assertTrue(firstOutput.contains("--files"), firstOutput);
        Assertions.assertTrue(
                firstOutput.contains(first.resolve("plugins.tar.gz").toString()), firstOutput);
        byte[] firstArchive = Files.readAllBytes(first.resolve("plugins.tar.gz"));
        Files.write(payload, "second".getBytes(StandardCharsets.UTF_8));
        runClusterStarter(home, second.toString(), true);
        Assertions.assertArrayEquals(
                firstArchive, Files.readAllBytes(first.resolve("plugins.tar.gz")));
        assertArchivePayload(first.resolve("plugins.tar.gz"), "first");
        assertArchivePayload(second.resolve("plugins.tar.gz"), "second");
        Assertions.assertTrue(
                runClusterStarter(home, plugins.toString(), false)
                        .contains("outside the plugins directory"));
        Assertions.assertFalse(Files.exists(plugins.resolve("plugins.tar.gz")));
        Path nested = Files.createDirectory(plugins.resolve("nested"));
        Assertions.assertTrue(
                runClusterStarter(home, nested.toString(), false)
                        .contains("outside the plugins directory"));
        Assertions.assertFalse(Files.exists(nested.resolve("plugins.tar.gz")));
        runClusterStarter(home, tempDir.resolve("missing").toString(), false);
        Path file = Files.createFile(tempDir.resolve("not-directory"));
        runClusterStarter(home, file.toString(), false);
        String defaultOutput = runClusterStarter(home, null, true);
        Assertions.assertTrue(
                defaultOutput.contains(home.resolve("plugins.tar.gz").toString()), defaultOutput);
        assertArchivePayload(home.resolve("plugins.tar.gz"), "second");
        runClusterStarter(home, "  ", true);
        assertArchivePayload(home.resolve("plugins.tar.gz"), "second");
    }

    @Test
    public void testClusterRejectsSymlinkIntoPlugins() throws Exception {
        Path home = Files.createDirectories(tempDir.resolve("symlink-home"));
        Path nested = Files.createDirectories(home.resolve("plugins/nested"));
        Path link = tempDir.resolve("staging-link");
        try {
            Files.createSymbolicLink(link, nested);
        } catch (IOException | UnsupportedOperationException | SecurityException error) {
            Assumptions.assumeTrue(false, "Symlinks unavailable: " + error);
        }
        Assertions.assertTrue(
                runClusterStarter(home, link.toString(), false)
                        .contains("outside the plugins directory"));
        Path staging = Files.createDirectory(tempDir.resolve("external"));
        Path input = Files.write(nested.resolve("input.txt"), new byte[] {1});
        Files.createSymbolicLink(staging.resolve("plugins.tar.gz"), input);
        Assertions.assertTrue(
                runClusterStarter(home, staging.toString(), false)
                        .contains("outside the plugins directory"));
        Assertions.assertArrayEquals(new byte[] {1}, Files.readAllBytes(input));
        Assertions.assertFalse(Files.exists(nested.resolve("plugins.tar.gz")));
    }

    private String runClusterStarter(Path home, String directory, boolean success)
            throws Exception {
        // Copy the jar so the legacy default archive also stays inside this test's directory.
        Path starter = home.resolve("starter").resolve(findStarterJar().getFileName());
        Files.createDirectories(starter.getParent());
        if (!Files.exists(starter)) {
            Files.copy(findStarterJar(), starter);
        }
        Path config = home.resolve("job.conf");
        Files.write(
                config,
                "env {}\nsource { FakeSource {} }\nsink { Console {} }\n"
                        .getBytes(StandardCharsets.UTF_8));
        Path output = Files.createTempFile(tempDir, "starter-", ".log");
        ProcessBuilder builder =
                new ProcessBuilder(
                        javaExecutable().toString(),
                        "-cp",
                        Paths.get("target/logging-e2e").toAbsolutePath()
                                + File.separator
                                + "*"
                                + File.pathSeparator
                                + starter,
                        SPARK_STARTER_MAIN,
                        "--config",
                        config.toString(),
                        "--deploy-mode",
                        "cluster");
        builder.environment().put("SEATUNNEL_HOME", home.toString());
        builder.environment().remove("SEATUNNEL_PLUGIN_TARBALL_DIR");
        if (directory != null) {
            builder.environment().put("SEATUNNEL_PLUGIN_TARBALL_DIR", directory);
        }
        Process process = builder.redirectErrorStream(true).redirectOutput(output.toFile()).start();
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
        }
        String text = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
        Assertions.assertTrue(finished, text);
        if (success) {
            Assertions.assertEquals(0, process.exitValue(), text);
        } else {
            Assertions.assertNotEquals(0, process.exitValue(), text);
        }
        return text;
    }

    private static void assertArchivePayload(Path archive, String expected) throws IOException {
        try (TarArchiveInputStream input =
                new TarArchiveInputStream(new GZIPInputStream(Files.newInputStream(archive)))) {
            Assertions.assertEquals("payload.txt", input.getNextTarEntry().getName());
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            byte[] buffer = new byte[128];
            int length;
            while ((length = input.read(buffer)) != -1) {
                bytes.write(buffer, 0, length);
            }
            Assertions.assertEquals(expected, bytes.toString("UTF-8"));
            Assertions.assertNull(input.getNextTarEntry());
        }
    }

    private static Path findStarterJar() throws IOException {
        try (Stream<Path> files = Files.list(Paths.get("target"))) {
            return files.filter(Files::isRegularFile)
                    .filter(path -> isStarterJar(path.getFileName().toString()))
                    .sorted()
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Spark starter jar is missing"));
        }
    }

    private static boolean isStarterJar(String fileName) {
        return (fileName.equals(ARTIFACT_ID + ".jar") || fileName.startsWith(ARTIFACT_ID + "-"))
                && fileName.endsWith(".jar")
                && !fileName.startsWith("original-")
                && !fileName.endsWith("-sources.jar")
                && !fileName.endsWith("-javadoc.jar")
                && !fileName.endsWith("-tests.jar");
    }

    private static Path javaExecutable() {
        String executable =
                System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
        return Paths.get(System.getProperty("java.home"), "bin", executable);
    }

    private static String starterClasspath() throws IOException {
        return Paths.get("target/logging-e2e")
                + File.separator
                + "*"
                + File.pathSeparator
                + findStarterJar();
    }

    private static String readOutput(Process process) throws IOException {
        try (InputStream inputStream = process.getInputStream();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, length);
            }
            return outputStream.toString("UTF-8");
        }
    }
}
