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

package org.apache.seatunnel.plugin.discovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.PluginType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

public class AbstractPluginDiscoveryTest {

    private String originSeatunnelHome = null;
    private DeployMode originMode = null;
    private ClassLoader originContextClassLoader = null;
    private static final String seatunnelHome;
    private static final Path fallbackWorkDir =
            Paths.get(System.getProperty("user.dir"), "target", "fallback-plugin");

    static {
        String rootModuleDir = "seatunnel-plugin-discovery";
        Path path = Paths.get(System.getProperty("user.dir"));
        while (!path.endsWith(Paths.get(rootModuleDir))) {
            path = path.getParent();
        }
        seatunnelHome =
                Paths.get(
                                path.getParent().toString(),
                                rootModuleDir,
                                "target",
                                "test-classes",
                                "home")
                        .toString();
    }

    @BeforeEach
    public void before() {
        originMode = Common.getDeployMode();
        Common.setDeployMode(DeployMode.CLIENT);
        originSeatunnelHome = Common.getSeaTunnelHome();
        Common.setSeaTunnelHome(seatunnelHome);
        originContextClassLoader = Thread.currentThread().getContextClassLoader();
    }

    @Test
    public void testGetAllPlugins() {
        Map<PluginIdentifier, String> sourcePlugins =
                AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SOURCE);
        Assertions.assertEquals(31, sourcePlugins.size());

        Map<PluginIdentifier, String> sinkPlugins =
                AbstractPluginDiscovery.getAllSupportedPlugins(PluginType.SINK);
        Assertions.assertEquals(35, sinkPlugins.size());
    }

    /**
     * When the context classloader is not a {@link URLClassLoader}, the default consumer throws
     * {@link UnsupportedOperationException} (JDK 9+ application classloader behaves like that) and
     * the discovery must fall back to building its own {@link URLClassLoader}. The fallback array
     * used to contain a trailing {@code null} which made the {@code URLClassLoader} constructor
     * throw a {@link NullPointerException}, so no plugin could ever be loaded through it.
     */
    @Test
    public void testCreateOptionalPluginInstanceFallbackWithoutUrlClassLoader() throws Exception {
        Path pluginDir = fallbackWorkDir.resolve("connectors");
        Files.createDirectories(pluginDir);

        // The plugin jar only has to exist so that discovery resolves a plugin jar path.
        Path fallbackPluginJar = pluginDir.resolve("connector-fallback.jar");
        try (JarOutputStream ignored =
                new JarOutputStream(Files.newOutputStream(fallbackPluginJar))) {
            // A valid but empty jar is enough, the service descriptor is provided below.
        }

        // The additional jar carries the service descriptor while the implementation class
        // itself is visible from the test classloader.
        Path additionalJar = fallbackWorkDir.resolve("fallback-additional.jar");
        try (OutputStream outputStream = Files.newOutputStream(additionalJar);
                JarOutputStream jarOutputStream = new JarOutputStream(outputStream)) {
            jarOutputStream.putNextEntry(
                    new JarEntry("META-INF/services/" + FallbackPlugin.class.getName()));
            jarOutputStream.write(
                    FallbackPluginImpl.class.getName().getBytes(StandardCharsets.UTF_8));
            jarOutputStream.closeEntry();
        }

        Config pluginMappingConfig =
                ConfigFactory.parseString("seatunnel { source { Fallback = connector-fallback } }");
        TestPluginDiscovery pluginDiscovery =
                new TestPluginDiscovery(pluginDir, pluginMappingConfig);

        ClassLoader plainClassLoader =
                new ClassLoader(Thread.currentThread().getContextClassLoader()) {};
        Assertions.assertFalse(plainClassLoader instanceof URLClassLoader);
        Thread.currentThread().setContextClassLoader(plainClassLoader);
        try {
            Optional<FallbackPlugin> plugin =
                    pluginDiscovery.createOptionalPluginInstance(
                            PluginIdentifier.of(
                                    "seatunnel", PluginType.SOURCE.getType(), "Fallback"),
                            Collections.singletonList(additionalJar.toUri().toURL()));
            Assertions.assertTrue(plugin.isPresent());
            Assertions.assertEquals(FallbackPluginImpl.class, plugin.get().getClass());
            Assertions.assertEquals("Fallback", plugin.get().getPluginName());
        } finally {
            Thread.currentThread().setContextClassLoader(originContextClassLoader);
        }
    }

    @AfterEach
    public void after() throws IOException {
        Thread.currentThread().setContextClassLoader(originContextClassLoader);
        Common.setSeaTunnelHome(originSeatunnelHome);
        Common.setDeployMode(originMode);
        Files.deleteIfExists(
                fallbackWorkDir.resolve("connectors").resolve("connector-fallback.jar"));
        Files.deleteIfExists(fallbackWorkDir.resolve("fallback-additional.jar"));
        Files.deleteIfExists(fallbackWorkDir.resolve("connectors"));
        Files.deleteIfExists(fallbackWorkDir);
    }

    /** Test plugin interface, acts as the plugin base class of the test discovery. */
    public interface FallbackPlugin extends PluginIdentifierInterface {}

    /** Test plugin implementation loaded through the fallback URLClassLoader. */
    public static class FallbackPluginImpl implements FallbackPlugin {

        @Override
        public String getPluginName() {
            return "Fallback";
        }
    }

    /** Plugin discovery used to exercise the fallback of the default URL consumer. */
    static class TestPluginDiscovery extends AbstractPluginDiscovery<FallbackPlugin> {

        TestPluginDiscovery(Path pluginDir, Config pluginMappingConfig) {
            super(pluginDir, pluginMappingConfig);
        }

        @Override
        protected Class<FallbackPlugin> getPluginBaseClass() {
            return FallbackPlugin.class;
        }
    }
}
