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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

@Slf4j
public abstract class AbstractPluginLocalDiscovery<T> extends AbstractPluginDiscovery<T> {

    private static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";

    /**
     * Add jar url to classloader. The different engine should have different logic to add url into
     * their own classloader
     */
    private static final BiConsumer<ClassLoader, URL> DEFAULT_URL_TO_CLASSLOADER =
            (classLoader, url) -> {
                if (classLoader instanceof URLClassLoader) {
                    ReflectionUtils.invoke(classLoader, "addURL", url);
                } else {
                    throw new UnsupportedOperationException("can't support custom load jar");
                }
            };

    private final Path pluginDir;

    public AbstractPluginLocalDiscovery(BiConsumer<ClassLoader, URL> addURLToClassloader) {
        this(Common.connectorDir(), loadConnectorPluginConfig(), addURLToClassloader);
    }

    public AbstractPluginLocalDiscovery() {
        this(Common.connectorDir(), loadConnectorPluginConfig());
    }

    public AbstractPluginLocalDiscovery(Path pluginDir) {
        this(pluginDir, loadConnectorPluginConfig());
    }

    public AbstractPluginLocalDiscovery(Path pluginDir, Config pluginMappingConfig) {
        this(pluginDir, pluginMappingConfig, DEFAULT_URL_TO_CLASSLOADER);
    }

    public AbstractPluginLocalDiscovery(
            Path pluginDir,
            Config pluginMappingConfig,
            BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer) {
        super(pluginMappingConfig, addURLToClassLoaderConsumer);
        this.pluginDir = pluginDir;
        log.info("Load {} Plugin from {}", getPluginBaseClass().getSimpleName(), pluginDir);
    }

    protected static Config loadConnectorPluginConfig() {
        return ConfigFactory.parseFile(Common.connectorDir().resolve(PLUGIN_MAPPING_FILE).toFile())
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(
                        ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    /**
     * Get all support plugin by plugin type
     *
     * @param pluginType plugin type, not support transform
     * @return the all plugin identifier of the engine with artifactId
     */
    public static Map<PluginIdentifier, String> getAllSupportedPlugins(PluginType pluginType) {
        Config config = loadConnectorPluginConfig();
        Map<PluginIdentifier, String> pluginIdentifiers = new HashMap<>();
        if (config.isEmpty() || !config.hasPath(CollectionConstants.SEATUNNEL_PLUGIN)) {
            return pluginIdentifiers;
        }
        Config engineConfig = config.getConfig(CollectionConstants.SEATUNNEL_PLUGIN);
        if (engineConfig.hasPath(pluginType.getType())) {
            engineConfig
                    .getConfig(pluginType.getType())
                    .entrySet()
                    .forEach(
                            entry -> {
                                pluginIdentifiers.put(
                                        PluginIdentifier.of(
                                                CollectionConstants.SEATUNNEL_PLUGIN,
                                                pluginType.getType(),
                                                entry.getKey()),
                                        entry.getValue().unwrapped().toString());
                            });
        }
        return pluginIdentifiers;
    }

    /**
     * Get all support plugin already in SEATUNNEL_HOME, only support connector-v2
     *
     * @return the all plugin identifier of the engine
     */
    public Map<PluginType, LinkedHashMap<PluginIdentifier, OptionRule>> getAllPlugin() {
        List<Factory> factories = getPluginFactories();

        Map<PluginType, LinkedHashMap<PluginIdentifier, OptionRule>> plugins = new HashMap<>();

        factories.forEach(
                plugin -> {
                    if (TableSourceFactory.class.isAssignableFrom(plugin.getClass())) {
                        TableSourceFactory tableSourceFactory = (TableSourceFactory) plugin;
                        plugins.computeIfAbsent(PluginType.SOURCE, k -> new LinkedHashMap<>());

                        plugins.get(PluginType.SOURCE)
                                .put(
                                        PluginIdentifier.of(
                                                "seatunnel",
                                                PluginType.SOURCE.getType(),
                                                plugin.factoryIdentifier()),
                                        FactoryUtil.sourceFullOptionRule(tableSourceFactory));
                        return;
                    }

                    if (TableSinkFactory.class.isAssignableFrom(plugin.getClass())) {
                        plugins.computeIfAbsent(PluginType.SINK, k -> new LinkedHashMap<>());

                        plugins.get(PluginType.SINK)
                                .put(
                                        PluginIdentifier.of(
                                                "seatunnel",
                                                PluginType.SINK.getType(),
                                                plugin.factoryIdentifier()),
                                        FactoryUtil.sinkFullOptionRule((TableSinkFactory) plugin));
                        return;
                    }

                    if (TableTransformFactory.class.isAssignableFrom(plugin.getClass())) {
                        plugins.computeIfAbsent(PluginType.TRANSFORM, k -> new LinkedHashMap<>());

                        plugins.get(PluginType.TRANSFORM)
                                .put(
                                        PluginIdentifier.of(
                                                "seatunnel",
                                                PluginType.TRANSFORM.getType(),
                                                plugin.factoryIdentifier()),
                                        plugin.optionRule());
                        return;
                    }
                });
        return plugins;
    }

    @Override
    protected List<Factory> getPluginFactories() {
        List<Factory> factories;
        if (this.pluginDir.toFile().exists()) {
            log.debug("load plugin from plugin dir: {}", this.pluginDir);
            List<URL> files;
            try {
                files = FileUtils.searchJarFiles(this.pluginDir);
            } catch (IOException e) {
                throw new RuntimeException(
                        String.format(
                                "Can not find any plugin(source/sink/transform) in the dir: %s",
                                this.pluginDir));
            }
            factories =
                    FactoryUtil.discoverFactories(new URLClassLoader(files.toArray(new URL[0])));
        } else {
            log.warn("plugin dir: {} not exists, load plugin from classpath", this.pluginDir);
            factories =
                    FactoryUtil.discoverFactories(Thread.currentThread().getContextClassLoader());
        }
        return factories;
    }

    @Override
    protected Optional<URL> findPluginJarPath(PluginIdentifier pluginIdentifier) {
        final String engineType = pluginIdentifier.getEngineType().toLowerCase();
        final String pluginType = pluginIdentifier.getPluginType().toLowerCase();
        final String pluginName = pluginIdentifier.getPluginName().toLowerCase();
        if (!pluginMappingConfig.hasPath(engineType)) {
            return Optional.empty();
        }
        Config engineConfig = pluginMappingConfig.getConfig(engineType);
        if (!engineConfig.hasPath(pluginType)) {
            return Optional.empty();
        }
        Config typeConfig = engineConfig.getConfig(pluginType);
        Optional<Map.Entry<String, ConfigValue>> optional =
                typeConfig.entrySet().stream()
                        .filter(entry -> StringUtils.equalsIgnoreCase(entry.getKey(), pluginName))
                        .findFirst();
        if (!optional.isPresent()) {
            return Optional.empty();
        }
        String pluginJarPrefix = optional.get().getValue().unwrapped().toString();
        File[] targetPluginFiles =
                pluginDir
                        .toFile()
                        .listFiles(
                                new FileFilter() {
                                    @Override
                                    public boolean accept(File pathname) {
                                        return pathname.getName().endsWith(".jar")
                                                && StringUtils.startsWithIgnoreCase(
                                                        pathname.getName(), pluginJarPrefix);
                                    }
                                });
        if (ArrayUtils.isEmpty(targetPluginFiles)) {
            return Optional.empty();
        }
        try {
            URL pluginJarPath;
            if (targetPluginFiles.length == 1) {
                pluginJarPath = targetPluginFiles[0].toURI().toURL();
            } else {
                pluginJarPath =
                        findMostSimlarPluginJarFile(
                                        targetPluginFiles, File::getName, pluginJarPrefix)
                                .toURI()
                                .toURL();
            }
            log.info("Discovery plugin jar for: {} at: {}", pluginIdentifier, pluginJarPath);
            return Optional.of(pluginJarPath);
        } catch (MalformedURLException e) {
            log.warn(
                    "Cannot get plugin URL: {} for pluginIdentifier: {}" + targetPluginFiles[0],
                    pluginIdentifier,
                    e);
            return Optional.empty();
        }
    }
}
