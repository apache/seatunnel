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

package org.apache.seatunnel.plugin.discovery.seatunnel;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnOs(OS.WINDOWS)
class SeaTunnelSourcePluginDiscoveryTest {

    private String originSeatunnelHome = null;
    private DeployMode originMode = null;
    private static final String seatunnelHome =
            SeaTunnelSourcePluginDiscoveryTest.class.getResource("/duplicate").getPath();

    private static final List<Object[]> pluginJars =
            Lists.newArrayList(
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar"),
                        "connector-http-jira"
                    },
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-http.jar"),
                        "connector-http"
                    },
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-kafka.jar"),
                        "connector-kafka"
                    },
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-kafka-alcs.jar"),
                        "connector-kafka-alcs"
                    },
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-kafka-blcs.jar"),
                        "connector-kafka-blcs"
                    },
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-jdbc-release-1.1.jar"),
                        "connector-jdbc"
                    },
                    new Object[] {
                        Paths.get(seatunnelHome, "connectors", "connector-jdbc-hive1.jar"),
                        "connector-jdbc-hive1"
                    });

    @BeforeEach
    public void before() throws IOException {
        originMode = Common.getDeployMode();
        Common.setDeployMode(DeployMode.CLIENT);
        originSeatunnelHome = Common.getSeaTunnelHome();
        Common.setSeaTunnelHome(seatunnelHome);

        // The file is created under target directory.
        for (Object[] pluginJarTuple : pluginJars) {
            generateJarWithPomProperties(
                    ((Path) pluginJarTuple[0]).toString(),
                    "org.apache.seatunnel",
                    pluginJarTuple[1].toString(),
                    "1.0.0-SNAPSHOT");
            // Files.createFile( ((Path) pluginJarTuple[0]));
        }
    }

    @Test
    void getPluginBaseClass() {
        List<PluginIdentifier> pluginIdentifiers =
                Lists.newArrayList(
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpJira"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpBase"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "Kafka"),
                        PluginIdentifier.of("seatunnel", PluginType.SINK.getType(), "Kafka-Blcs"),
                        PluginIdentifier.of("seatunnel", PluginType.SINK.getType(), "Jdbc"));
        SeaTunnelSourcePluginDiscovery seaTunnelSourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery();
        List<String> collect =
                seaTunnelSourcePluginDiscovery.getPluginJarPaths(pluginIdentifiers).stream()
                        .map(URL::getPath)
                        .collect(Collectors.toList());
        collect.forEach(System.out::println);
        Assertions.assertIterableEquals(
                Stream.of(
                                Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-http.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-kafka.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-kafka-blcs.jar")
                                        .toString(),
                                Paths.get(
                                                seatunnelHome,
                                                "connectors",
                                                "connector-jdbc-release-1.1.jar")
                                        .toString())
                        .collect(Collectors.toList()),
                collect);
    }

    @AfterEach
    public void after() throws IOException {
        for (Object[] pluginJar : pluginJars) {
            Files.deleteIfExists(((Path) pluginJar[0]));
        }
        Common.setSeaTunnelHome(originSeatunnelHome);
        Common.setDeployMode(originMode);
    }

    public static void generateJarWithPomProperties(
            String jarFilePath, String groupId, String artifactId, String version)
            throws IOException {
        File jarFile = new File(jarFilePath);

        File parentDir = jarFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            parentDir.mkdirs();
        }
        if (jarFile.exists()) {
            jarFile.delete();
        }

        try (JarOutputStream jos = new JarOutputStream(Files.newOutputStream(jarFile.toPath()))) {
            String pomDir = "META-INF/maven/" + groupId + "/" + artifactId + "/";
            JarEntry dirEntry = new JarEntry(pomDir);
            jos.putNextEntry(dirEntry);
            jos.closeEntry();

            String pomContent = buildPomContent(groupId, artifactId, version);
            String pomPath = pomDir + "pom.properties";
            JarEntry pomEntry = new JarEntry(pomPath);
            jos.putNextEntry(pomEntry);
            jos.write(pomContent.getBytes("UTF-8"));
            jos.closeEntry();

            addPerturbedPomProperties(jos, groupId, artifactId, version, 10);
        }
    }

    private static String buildPomContent(String groupId, String artifactId, String version) {
        return "groupId="
                + groupId
                + "\n"
                + "artifactId="
                + artifactId
                + "\n"
                + "version="
                + version
                + "\n";
    }

    private static void addDirectoryEntry(JarOutputStream jos, String directoryPath) {
        if (!directoryPath.endsWith("/")) {
            directoryPath += "/";
        }
        JarEntry dirEntry = new JarEntry(directoryPath);
        try {
            jos.putNextEntry(dirEntry);
            jos.closeEntry();
        } catch (IOException e) {
        }
    }

    private static void addPerturbedPomProperties(
            JarOutputStream jos, String groupId, String artifactId, String version, int count)
            throws IOException {
        Random random = new Random();
        for (int i = 1; i <= count; i++) {
            String perturbedGroupId = perturbString(groupId, i);
            String perturbedArtifactId = perturbString(artifactId, i);
            String perturbedPomContent =
                    buildPomContent(perturbedGroupId, perturbedArtifactId, version);
            String perturbedDir =
                    "META-INF/maven/" + perturbedGroupId + "/" + perturbedArtifactId + "/";
            addDirectoryEntry(jos, perturbedDir);
            String perturbedPomPath = perturbedDir + "pom.properties";
            JarEntry entry = new JarEntry(perturbedPomPath);
            jos.putNextEntry(entry);
            jos.write(perturbedPomContent.getBytes(StandardCharsets.UTF_8));
            jos.closeEntry();
        }
    }

    private static String perturbString(String s, int i) {
        return i + s;
    }
}
