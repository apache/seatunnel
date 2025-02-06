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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnOs(OS.WINDOWS)
class SeaTunnelSourcePluginDiscoveryTest {

    private String originSeatunnelHome = null;
    private DeployMode originMode = null;
    private static final String seatunnelHome =
            SeaTunnelSourcePluginDiscoveryTest.class.getResource("/duplicate").getPath();
    private static final List<Path> pluginJars =
            Lists.newArrayList(
                    Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-http.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka-alcs.jar"),
                    Paths.get(seatunnelHome, "connectors", "connector-kafka-blcs.jar"));

    @BeforeEach
    public void before() throws IOException {
        originMode = Common.getDeployMode();
        Common.setDeployMode(DeployMode.CLIENT);
        originSeatunnelHome = Common.getSeaTunnelHome();
        Common.setSeaTunnelHome(seatunnelHome);

        // The file is created under target directory.
        for (Path pluginJar : pluginJars) {
            Files.createFile(pluginJar);
        }
    }

    @Test
    void getPluginBaseClass() {
        List<PluginIdentifier> pluginIdentifiers =
                Lists.newArrayList(
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpJira"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpBase"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "Kafka"),
                        PluginIdentifier.of("seatunnel", PluginType.SINK.getType(), "Kafka-Blcs"));
        SeaTunnelSourcePluginDiscovery seaTunnelSourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery();
        Assertions.assertIterableEquals(
                Stream.of(
                                Paths.get(seatunnelHome, "connectors", "connector-http-jira.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-http.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-kafka.jar")
                                        .toString(),
                                Paths.get(seatunnelHome, "connectors", "connector-kafka-blcs.jar")
                                        .toString())
                        .collect(Collectors.toList()),
                seaTunnelSourcePluginDiscovery.getPluginJarPaths(pluginIdentifiers).stream()
                        .map(URL::getPath)
                        .collect(Collectors.toList()));
    }

    @AfterEach
    public void after() throws IOException {
        for (Path pluginJar : pluginJars) {
            Files.deleteIfExists(pluginJar);
        }
        Common.setSeaTunnelHome(originSeatunnelHome);
        Common.setDeployMode(originMode);
    }
}
