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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

class SinkExecuteProcessorTest {

    @TempDir Path seatunnelHome;

    private String originalSeatunnelHome;
    private DeployMode originalDeployMode;
    private ClassLoader originalClassLoader;
    private URLClassLoader pluginClassLoader;

    @BeforeEach
    void setUp() {
        originalSeatunnelHome = Common.getSeaTunnelHome();
        originalDeployMode = Common.getDeployMode();
        originalClassLoader = Thread.currentThread().getContextClassLoader();
        Common.setSeaTunnelHome(seatunnelHome.toString());
        Common.setDeployMode(DeployMode.CLIENT);
        // The processor adds the plugin jars to the context class loader.
        pluginClassLoader = new URLClassLoader(new URL[0], originalClassLoader);
        Thread.currentThread().setContextClassLoader(pluginClassLoader);
    }

    @AfterEach
    void tearDown() throws IOException {
        Thread.currentThread().setContextClassLoader(originalClassLoader);
        Common.setSeaTunnelHome(originalSeatunnelHome);
        Common.setDeployMode(originalDeployMode);
        // An open loader keeps the jars locked on Windows, and @TempDir then cannot delete them.
        pluginClassLoader.close();
    }

    @Test
    void sinkJarPathsIncludeTheConnectorPluginDirectory() throws IOException {
        Path connectors = Files.createDirectories(seatunnelHome.resolve("connectors"));
        Files.write(
                connectors.resolve("plugin-mapping.properties"),
                "seatunnel.sink.Demo = connector-demo\n".getBytes(StandardCharsets.UTF_8));
        Path connectorJar = emptyJar(connectors.resolve("connector-demo.jar"));
        Path dependencyJar =
                emptyJar(
                        Files.createDirectories(
                                        seatunnelHome.resolve("plugins").resolve("connector-demo"))
                                .resolve("demo-client.jar"));
        Path otherConnectorJar =
                emptyJar(
                        Files.createDirectories(
                                        seatunnelHome.resolve("plugins").resolve("connector-other"))
                                .resolve("other-client.jar"));

        List<URL> jarPaths = new ArrayList<>();
        Config sinkConfig = ConfigFactory.parseString("plugin_name = Demo");
        new SinkExecuteProcessor(
                jarPaths,
                ConfigFactory.empty(),
                Collections.singletonList(sinkConfig),
                new JobContext());

        Assertions.assertTrue(jarPaths.contains(connectorJar.toUri().toURL()), jarPaths::toString);
        Assertions.assertTrue(jarPaths.contains(dependencyJar.toUri().toURL()), jarPaths::toString);
        Assertions.assertFalse(
                jarPaths.contains(otherConnectorJar.toUri().toURL()), jarPaths::toString);
    }

    private static Path emptyJar(Path path) throws IOException {
        try (OutputStream out = Files.newOutputStream(path);
                JarOutputStream ignored = new JarOutputStream(out, new Manifest())) {
            return path;
        }
    }
}
