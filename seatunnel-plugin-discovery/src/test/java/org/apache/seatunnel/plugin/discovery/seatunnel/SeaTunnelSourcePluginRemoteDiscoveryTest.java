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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnOs(OS.WINDOWS)
public class SeaTunnelSourcePluginRemoteDiscoveryTest extends SeaTunnelSourcePluginDiscoveryTest {

    private static final BiConsumer<ClassLoader, URL> DEFAULT_URL_TO_CLASSLOADER =
            (classLoader, url) -> {
                if (classLoader instanceof URLClassLoader) {
                    ReflectionUtils.invoke(classLoader, "addURL", url);
                } else {
                    throw new UnsupportedOperationException("can't support custom load jar");
                }
            };

    private static final List<URL> remoteConnectors =
            pluginJars.stream()
                    .map(
                            path -> {
                                try {
                                    return path.toUri().toURL();
                                } catch (MalformedURLException e) {
                                    throw new SeaTunnelException(e);
                                }
                            })
                    .collect(Collectors.toList());

    private Config pluginMappingConfig;

    @BeforeEach
    public void before() throws IOException {
        originSeatunnelHome = Common.getSeaTunnelHome();
        // The file is created under target directory.
        for (Path pluginJar : pluginJars) {
            if (Files.exists(pluginJar)) {
                continue;
            }
            Files.createFile(pluginJar);
        }

        final String pluginMappingPath =
                SeaTunnelSourcePluginRemoteDiscoveryTest.class
                        .getResource("/plugin-mapping.properties")
                        .getPath();

        pluginMappingConfig =
                ConfigFactory.parseFile(new File(pluginMappingPath))
                        .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                        .resolveWith(
                                ConfigFactory.systemProperties(),
                                ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    @Test
    void getPluginBaseClass() {
        List<PluginIdentifier> pluginIdentifiers =
                Lists.newArrayList(
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpJira"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "HttpBase"),
                        PluginIdentifier.of("seatunnel", PluginType.SOURCE.getType(), "Kafka"),
                        PluginIdentifier.of("seatunnel", PluginType.SINK.getType(), "Kafka-Blcs"));
        SeaTunnelSourcePluginRemoteDiscovery seaTunnelSourcePluginRemoteDiscovery =
                new SeaTunnelSourcePluginRemoteDiscovery(
                        remoteConnectors, pluginMappingConfig, DEFAULT_URL_TO_CLASSLOADER);
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
                seaTunnelSourcePluginRemoteDiscovery.getPluginJarPaths(pluginIdentifiers).stream()
                        .map(URL::getPath)
                        .collect(Collectors.toList()));
    }
}
