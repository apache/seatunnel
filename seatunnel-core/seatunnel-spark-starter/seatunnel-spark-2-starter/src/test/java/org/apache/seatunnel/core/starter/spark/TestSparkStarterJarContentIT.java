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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static org.apache.seatunnel.core.starter.constants.SeaTunnelStarterConstants.USAGE_EXIT_CODE;

public class TestSparkStarterJarContentIT {

    private static final String ARTIFACT_ID = "seatunnel-spark-2-starter";
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
        Path configFile = Paths.get("src/main/resources/spark_application.conf").toAbsolutePath();
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
        return Paths.get("target/logging-e2e", "*") + File.pathSeparator + findStarterJar();
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
