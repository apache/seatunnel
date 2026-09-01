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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

@EnabledOnOs({OS.LINUX, OS.MAC})
public class SeaTunnelClusterScriptTest {

    @TempDir private Path temporaryDirectory;

    /**
     * Verifies that cluster startup publishes the distribution home to Java system properties.
     *
     * <p>The server uses these properties to resolve connector metadata and connector jars when
     * jobs are submitted through cluster-side paths such as REST.
     */
    @Test
    public void testClusterScriptPassesSeatunnelHomeToServerJvm() throws Exception {
        Path appDirectory = createMinimalDistribution();
        List<String> arguments = runClusterScript(appDirectory, null);

        Assertions.assertTrue(
                arguments.contains("-Dseatunnel.home=" + appDirectory),
                "seatunnel.home should default to the script distribution directory");
        Assertions.assertTrue(
                arguments.contains("-DSEATUNNEL_HOME=" + appDirectory),
                "SEATUNNEL_HOME should default to the script distribution directory");
    }

    /**
     * Verifies that an externally configured SeaTunnel home is forwarded without being overwritten
     * by the script fallback.
     */
    @Test
    public void testClusterScriptPassesCustomSeatunnelHomeToServerJvm() throws Exception {
        Path appDirectory = createMinimalDistribution();
        Path customSeaTunnelHome = temporaryDirectory.resolve("custom-seatunnel-home");
        Files.createDirectories(customSeaTunnelHome);
        List<String> arguments = runClusterScript(appDirectory, customSeaTunnelHome);

        Assertions.assertTrue(
                arguments.contains("-Dseatunnel.home=" + customSeaTunnelHome),
                "seatunnel.home should use the externally configured SeaTunnel home");
        Assertions.assertTrue(
                arguments.contains("-DSEATUNNEL_HOME=" + customSeaTunnelHome),
                "SEATUNNEL_HOME should use the externally configured SeaTunnel home");
    }

    /**
     * Verifies that the Windows cluster script keeps custom SeaTunnel home values and forwards the
     * effective value before later JVM option appends.
     */
    @Test
    public void testWindowsClusterScriptPassesSeatunnelHomeToServerJvm() throws Exception {
        String script =
                new String(Files.readAllBytes(locateWindowsClusterScript()), StandardCharsets.UTF_8)
                        .replace("\r\n", "\n");

        int fallbackIndex = script.indexOf("if not defined SEATUNNEL_HOME");
        int fallbackValueIndex = script.indexOf("set \"SEATUNNEL_HOME=%APP_DIR%\"");
        int seaTunnelHomeJvmOptionIndex =
                script.indexOf(
                        "set \"JAVA_OPTS=!JAVA_OPTS! -Dseatunnel.home=!SEATUNNEL_HOME! "
                                + "-DSEATUNNEL_HOME=!SEATUNNEL_HOME!\"");
        int log4jJvmOptionIndex =
                script.indexOf(
                        "set \"JAVA_OPTS=!JAVA_OPTS! "
                                + "-Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector\"");

        Assertions.assertTrue(
                fallbackIndex >= 0,
                "Windows cluster script should only fallback when SEATUNNEL_HOME is undefined");
        Assertions.assertTrue(
                fallbackValueIndex > fallbackIndex,
                "Windows cluster script should fallback to APP_DIR as the distribution home");
        Assertions.assertTrue(
                seaTunnelHomeJvmOptionIndex > fallbackValueIndex,
                "Windows cluster script should pass the effective SeaTunnel home to the JVM");
        Assertions.assertTrue(
                seaTunnelHomeJvmOptionIndex < log4jJvmOptionIndex,
                "SeaTunnel home JVM options should be appended before later JVM options");
    }

    /**
     * Verifies that a foreground server replaces the launcher shell, allowing container runtimes to
     * deliver SIGTERM directly to the JVM shutdown hook.
     */
    @Test
    public void testClusterScriptExecsForegroundServer() throws Exception {
        String script =
                new String(Files.readAllBytes(locateClusterScript()), StandardCharsets.UTF_8)
                        .replace("\r\n", "\n");

        Assertions.assertTrue(
                script.contains("exec java ${JAVA_OPTS} -cp ${CLASS_PATH} ${APP_MAIN} ${args}"),
                "foreground startup should replace the launcher shell with the server JVM");
    }

    private List<String> runClusterScript(Path appDirectory, Path seaTunnelHome) throws Exception {
        Path capturedArguments = temporaryDirectory.resolve("java-args.txt");
        Path fakeJavaDirectory = createFakeJava(capturedArguments);
        ProcessBuilder processBuilder =
                new ProcessBuilder(
                        "/bin/bash",
                        appDirectory.resolve("bin").resolve("seatunnel-cluster.sh").toString());
        processBuilder.directory(temporaryDirectory.resolve("working-dir").toFile());
        Map<String, String> environment = processBuilder.environment();
        environment.put(
                "PATH",
                fakeJavaDirectory
                        + System.getProperty("path.separator")
                        + environment.getOrDefault("PATH", ""));
        environment.put("CAPTURE_FILE", capturedArguments.toString());
        if (seaTunnelHome == null) {
            environment.remove("SEATUNNEL_HOME");
        } else {
            environment.put("SEATUNNEL_HOME", seaTunnelHome.toString());
        }
        environment.remove("JAVA_OPTS");
        environment.remove("JvmOption");
        environment.remove("SEATUNNEL_CONFIG");
        environment.remove("HAZELCAST_CONFIG");

        Process process = processBuilder.start();
        int exitCode = process.waitFor();

        Assertions.assertEquals(0, exitCode);
        return Files.readAllLines(capturedArguments, StandardCharsets.UTF_8);
    }

    private Path createMinimalDistribution() throws Exception {
        Path appDirectory = temporaryDirectory.resolve("apache-seatunnel");
        Path binDirectory = appDirectory.resolve("bin");
        Path configDirectory = appDirectory.resolve("config");
        Files.createDirectories(binDirectory);
        Files.createDirectories(configDirectory);
        Files.createDirectories(appDirectory.resolve("starter"));
        Files.createDirectories(appDirectory.resolve("lib"));
        Files.createDirectories(temporaryDirectory.resolve("working-dir"));

        Files.copy(locateClusterScript(), binDirectory.resolve("seatunnel-cluster.sh"));
        Files.createFile(configDirectory.resolve("hazelcast.yaml"));
        Files.createFile(configDirectory.resolve("seatunnel.yaml"));
        Files.createFile(configDirectory.resolve("jvm_options"));
        Files.createFile(configDirectory.resolve("jvm_master_options"));
        Files.createFile(configDirectory.resolve("jvm_worker_options"));
        Files.createFile(appDirectory.resolve("starter").resolve("seatunnel-starter.jar"));
        return appDirectory;
    }

    private Path locateClusterScript() {
        Path modulePath = Paths.get("src/main/bin/seatunnel-cluster.sh");
        if (Files.exists(modulePath)) {
            return modulePath;
        }
        return Paths.get("seatunnel-core/seatunnel-starter/src/main/bin/seatunnel-cluster.sh");
    }

    private Path locateWindowsClusterScript() {
        Path modulePath = Paths.get("src/main/bin/seatunnel-cluster.cmd");
        if (Files.exists(modulePath)) {
            return modulePath;
        }
        return Paths.get("seatunnel-core/seatunnel-starter/src/main/bin/seatunnel-cluster.cmd");
    }

    private Path createFakeJava(Path capturedArguments) throws Exception {
        Path fakeJavaDirectory = temporaryDirectory.resolve("fake-java-bin");
        Files.createDirectories(fakeJavaDirectory);
        Path fakeJava = fakeJavaDirectory.resolve("java");
        Files.write(
                fakeJava,
                ("#!/bin/sh\n"
                                + "for arg in \"$@\"; do\n"
                                + "  printf '%s\\n' \"$arg\"\n"
                                + "done > \"$CAPTURE_FILE\"\n")
                        .getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(fakeJava.toFile().setExecutable(true));
        return fakeJavaDirectory;
    }
}
