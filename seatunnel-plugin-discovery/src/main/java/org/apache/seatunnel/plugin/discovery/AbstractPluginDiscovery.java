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

import org.apache.seatunnel.api.common.PluginIdentifierInterface;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.apis.base.plugin.Plugin;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {

    private static final String PLUGIN_MAPPING_FILE = "plugin-mapping.properties";

    /**
     * Add jar url to classloader. The different engine should have different logic to add url into
     * their own classloader
     */
    private static final BiConsumer DEFAULT_URL_TO_CLASSLOADER = (classLoader, url) -> {
        if (classLoader instanceof URLClassLoader) {
            ReflectionUtils.invoke(classLoader, "addURL", url);
        } else {
            throw new UnsupportedOperationException("can't support custom load jar");
        }
    };

    private final Path pluginDir;
    private final Config pluginConfig;
    private final BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer;
    protected final ConcurrentHashMap<PluginIdentifier, Optional<URL>> pluginJarPath =
        new ConcurrentHashMap<>(Common.COLLECTION_SIZE);

    public AbstractPluginDiscovery(String pluginSubDir, BiConsumer<ClassLoader, URL> addURLToClassloader) {
        this(Common.connectorJarDir(pluginSubDir), loadConnectorPluginConfig(), addURLToClassloader);
    }

    public AbstractPluginDiscovery(String pluginSubDir) {
        this(Common.connectorJarDir(pluginSubDir), loadConnectorPluginConfig());
    }

    public AbstractPluginDiscovery(Path pluginDir) {
        this(pluginDir, loadConnectorPluginConfig());
    }

    public AbstractPluginDiscovery(Path pluginDir,
                                   Config pluginConfig) {
        this(pluginDir, pluginConfig, DEFAULT_URL_TO_CLASSLOADER);
    }

    public AbstractPluginDiscovery(Path pluginDir,
                                   Config pluginConfig,
                                   BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer) {
        this.pluginDir = pluginDir;
        this.pluginConfig = pluginConfig;
        this.addURLToClassLoaderConsumer = addURLToClassLoaderConsumer;
        log.info("Load {} Plugin from {}", getPluginBaseClass().getSimpleName(), pluginDir);
    }

    protected static Config loadConnectorPluginConfig() {
        return ConfigFactory
            .parseFile(Common.connectorDir().resolve(PLUGIN_MAPPING_FILE).toFile())
            .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
            .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }

    @Override
    public List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
            .map(this::getPluginJarPath)
            .filter(Optional::isPresent)
            .map(Optional::get).distinct()
            .collect(Collectors.toList());
    }

    @Override
    public List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
            .map(this::createPluginInstance).distinct()
            .collect(Collectors.toList());
    }

    /**
     * Get all support plugin by plugin type
     *
     * @param pluginType plugin type, not support transform
     * @return the all plugin identifier of the engine with artifactId
     */
    public static @Nonnull Map<PluginIdentifier, String> getAllSupportedPlugins(PluginType pluginType) {
        Config config = loadConnectorPluginConfig();
        Map<PluginIdentifier, String> pluginIdentifiers = new HashMap<>();
        if (config.isEmpty() || !config.hasPath(CollectionConstants.SEATUNNEL_PLUGIN)) {
            return pluginIdentifiers;
        }
        Config engineConfig = config.getConfig(CollectionConstants.SEATUNNEL_PLUGIN);
        if (engineConfig.hasPath(pluginType.getType())) {
            engineConfig.getConfig(pluginType.getType()).entrySet().forEach(entry -> {
                pluginIdentifiers.put(
                    PluginIdentifier.of(CollectionConstants.SEATUNNEL_PLUGIN, pluginType.getType(), entry.getKey()),
                    entry.getValue().unwrapped().toString());
            });
        }
        return pluginIdentifiers;
    }

    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier) {
        return (T) createPluginInstance(pluginIdentifier, Collections.EMPTY_LIST);
    }

    @Override
    public T createPluginInstance(PluginIdentifier pluginIdentifier, Collection<URL> pluginJars) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        T pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
        if (pluginInstance != null) {
            log.info("Load plugin: {} from classpath", pluginIdentifier);
            return pluginInstance;
        }
        Optional<URL> pluginJarPath = getPluginJarPath(pluginIdentifier);
        // if the plugin jar not exist in classpath, will load from plugin dir.
        if (pluginJarPath.isPresent()) {
            try {
                // use current thread classloader to avoid different classloader load same class error.
                this.addURLToClassLoaderConsumer.accept(classLoader, pluginJarPath.get());
                for (URL jar : pluginJars) {
                    addURLToClassLoaderConsumer.accept(classLoader, jar);
                }
            } catch (Exception e) {
                log.warn("can't load jar use current thread classloader, use URLClassLoader instead now." +
                    " message: " + e.getMessage());
                URL[] urls = new URL[pluginJars.size() + 1];
                int i = 0;
                for (URL pluginJar : pluginJars) {
                    urls[i++] = pluginJar;
                }
                urls[i] = pluginJarPath.get();
                classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
            }
            pluginInstance = loadPluginInstance(pluginIdentifier, classLoader);
            if (pluginInstance != null) {
                log.info("Load plugin: {} from path: {} use classloader: {}",
                    pluginIdentifier, pluginJarPath.get(), classLoader.getClass().getName());
                return pluginInstance;
            }
        }
        throw new RuntimeException("Plugin " + pluginIdentifier + " not found.");
    }

    /**
     * Get all support plugin already in SEATUNNEL_HOME, only support connector-v2
     * @return the all plugin identifier of the engine
     */
    @SuppressWarnings("checkstyle:WhitespaceAfter")
    public Map<PluginType, LinkedHashMap<PluginIdentifier, OptionRule>> getAllPlugin() throws IOException {
        List<Factory> factories;
        if (this.pluginDir.toFile().exists()) {
            log.info("load plugin from plugin dir: {}", this.pluginDir);
            List<URL> files = FileUtils.searchJarFiles(this.pluginDir);
            factories = FactoryUtil.discoverFactories(new URLClassLoader((URL[]) files.toArray(new URL[0])));
        } else {
            log.info("plugin dir: {} not exists, load plugin from classpath", this.pluginDir);
            factories = FactoryUtil.discoverFactories(Thread.currentThread().getContextClassLoader());
        }

        Map<PluginType, LinkedHashMap<PluginIdentifier, OptionRule>> plugins = new HashMap<>();

        factories.forEach(plugin -> {
            if (TableSourceFactory.class.isAssignableFrom(plugin.getClass())) {
                if (plugins.get(PluginType.SOURCE) == null) {
                    plugins.put(PluginType.SOURCE, new LinkedHashMap());
                }

                plugins.get(PluginType.SOURCE).put(PluginIdentifier.of(
                        "seatunnel",
                        PluginType.SOURCE.getType(),
                        plugin.factoryIdentifier()
                    ),
                    FactoryUtil.sourceFullOptionRule(plugin));
                return;
            }

            if (TableSinkFactory.class.isAssignableFrom(plugin.getClass())) {
                if (plugins.get(PluginType.SINK) == null) {
                    plugins.put(PluginType.SINK, new LinkedHashMap());
                }

                plugins.get(PluginType.SINK).put(PluginIdentifier.of(
                        "seatunnel",
                        PluginType.SINK.getType(),
                        plugin.factoryIdentifier()
                    ),
                    plugin.optionRule());
                return;
            }

            if (TableTransformFactory.class.isAssignableFrom(plugin.getClass())) {
                if (plugins.get(PluginType.TRANSFORM) == null) {
                    plugins.put(PluginType.TRANSFORM, new LinkedHashMap());
                }

                plugins.get(PluginType.TRANSFORM).put(PluginIdentifier.of(
                        "seatunnel",
                        PluginType.TRANSFORM.getType(),
                        plugin.factoryIdentifier()
                    ),
                    plugin.optionRule());
                return;
            }
        });
        return plugins;
    }

    @Nullable
    private T loadPluginInstance(PluginIdentifier pluginIdentifier, ClassLoader classLoader) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (T t : serviceLoader) {
            if (t instanceof Plugin) {
                // old api
                Plugin<?> pluginInstance = (Plugin<?>) t;
                if (StringUtils.equalsIgnoreCase(pluginInstance.getPluginName(), pluginIdentifier.getPluginName())) {
                    return (T) pluginInstance;
                }
            } else if (t instanceof PluginIdentifierInterface) {
                // new api
                PluginIdentifierInterface pluginIdentifierInstance = (PluginIdentifierInterface) t;
                if (StringUtils.equalsIgnoreCase(pluginIdentifierInstance.getPluginName(),
                    pluginIdentifier.getPluginName())) {
                    return (T) pluginIdentifierInstance;
                }
            } else {
                throw new UnsupportedOperationException("Plugin instance: " + t + " is not supported.");
            }
        }
        return null;
    }

    /**
     * Get the plugin instance.
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin instance.
     */
    protected Optional<URL> getPluginJarPath(PluginIdentifier pluginIdentifier) {
        return pluginJarPath.computeIfAbsent(pluginIdentifier, this::findPluginJarPath);
    }

    /**
     * Get spark plugin interface.
     *
     * @return plugin base class.
     */
    protected abstract Class<T> getPluginBaseClass();

    /**
     * Find the plugin jar path;
     *
     * @param pluginIdentifier plugin identifier.
     * @return plugin jar path.
     */
    private Optional<URL> findPluginJarPath(PluginIdentifier pluginIdentifier) {
        if (pluginConfig.isEmpty()) {
            return Optional.empty();
        }
        final String engineType = pluginIdentifier.getEngineType().toLowerCase();
        final String pluginType = pluginIdentifier.getPluginType().toLowerCase();
        final String pluginName = pluginIdentifier.getPluginName().toLowerCase();
        if (!pluginConfig.hasPath(engineType)) {
            return Optional.empty();
        }
        Config engineConfig = pluginConfig.getConfig(engineType);
        if (!engineConfig.hasPath(pluginType)) {
            return Optional.empty();
        }
        Config typeConfig = engineConfig.getConfig(pluginType);
        Optional<Map.Entry<String, ConfigValue>> optional = typeConfig.entrySet().stream()
            .filter(entry -> StringUtils.equalsIgnoreCase(entry.getKey(), pluginName))
            .findFirst();
        if (!optional.isPresent()) {
            return Optional.empty();
        }
        String pluginJarPrefix = optional.get().getValue().unwrapped().toString();
        File[] targetPluginFiles = pluginDir.toFile().listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(".jar") &&
                    StringUtils.startsWithIgnoreCase(pathname.getName(), pluginJarPrefix);
            }
        });
        if (ArrayUtils.isEmpty(targetPluginFiles)) {
            return Optional.empty();
        }
        try {
            URL pluginJarPath = targetPluginFiles[0].toURI().toURL();
            log.info("Discovery plugin jar: {} at: {}", pluginIdentifier.getPluginName(), pluginJarPath);
            return Optional.of(pluginJarPath);
        } catch (MalformedURLException e) {
            log.warn("Cannot get plugin URL: " + targetPluginFiles[0], e);
            return Optional.empty();
        }
    }
}
