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

package org.apache.seatunnel.edge.agent.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

public final class EdgeAgentDistPaths {

    private static final String EDGE_AGENT_MODULE = "seatunnel-edge-agent";
    private static final String STARTER_MODULE = "seatunnel-edge-agent-starter";
    private static final String STARTER_JAR_NAME = "seatunnel-edge-agent-starter.jar";

    private EdgeAgentDistPaths() {}

    /** Validates starter package output and config/bin layout exist under the repo root. */
    public static void validateAgentDistribution() {
        assertRegularFile(starterJar(), "starter jar");
        assertDirectoryWithJars(loggingDirectory(), "logging-e2e");
        assertRegularFile(log4jConfig(), "log4j2.properties");
        assertRegularFile(startScript(), "start script");
    }

    public static Path starterJar() {
        return Paths.get(
                        PROJECT_ROOT_PATH,
                        EDGE_AGENT_MODULE,
                        STARTER_MODULE,
                        "target",
                        STARTER_JAR_NAME)
                .toAbsolutePath()
                .normalize();
    }

    public static Path loggingDirectory() {
        return Paths.get(
                        PROJECT_ROOT_PATH,
                        EDGE_AGENT_MODULE,
                        STARTER_MODULE,
                        "target",
                        "logging-e2e")
                .toAbsolutePath()
                .normalize();
    }

    public static Path log4jConfig() {
        return Paths.get(PROJECT_ROOT_PATH, EDGE_AGENT_MODULE, "config", "log4j2.properties")
                .toAbsolutePath()
                .normalize();
    }

    public static Path startScript() {
        return Paths.get(PROJECT_ROOT_PATH, EDGE_AGENT_MODULE, "bin", "seatunnel-edge-agent.sh")
                .toAbsolutePath()
                .normalize();
    }

    public static void copyAgentConfigFromClasspath(String classpathResource, Path workDir)
            throws IOException {
        String resourcePath =
                classpathResource.startsWith("/") ? classpathResource : "/" + classpathResource;
        try (InputStream in = EdgeAgentDistPaths.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalArgumentException("Missing classpath resource: " + resourcePath);
            }
            Files.createDirectories(workDir);
            Files.copy(in, workDir.resolve("agent.yaml"), StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void assertRegularFile(Path path, String label) {
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException(
                    "Missing edge-agent "
                            + label
                            + " at "
                            + path
                            + ". Build with: ./mvnw -pl "
                            + EDGE_AGENT_MODULE
                            + "/"
                            + STARTER_MODULE
                            + " -DskipTests package");
        }
    }

    private static void assertDirectoryWithJars(Path directory, String label) {
        if (!Files.isDirectory(directory)) {
            throw new IllegalStateException(
                    "Missing edge-agent "
                            + label
                            + " directory at "
                            + directory
                            + ". Build with: ./mvnw -pl "
                            + EDGE_AGENT_MODULE
                            + "/"
                            + STARTER_MODULE
                            + " -DskipTests package");
        }
        try (Stream<Path> jars = Files.list(directory)) {
            if (jars.noneMatch(path -> path.toString().endsWith(".jar"))) {
                throw new IllegalStateException(
                        "No logging jars under "
                                + directory
                                + ". Build with: ./mvnw -pl "
                                + EDGE_AGENT_MODULE
                                + "/"
                                + STARTER_MODULE
                                + " -DskipTests package");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list " + directory, e);
        }
    }
}
