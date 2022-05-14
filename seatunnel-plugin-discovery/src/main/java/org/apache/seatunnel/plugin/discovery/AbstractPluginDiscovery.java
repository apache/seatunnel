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

import org.apache.seatunnel.apis.base.plugin.Plugin;
import org.apache.seatunnel.common.config.Common;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class AbstractPluginDiscovery<T> implements PluginDiscovery<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPluginDiscovery.class);
    private final Path pluginDir;

    protected final ConcurrentHashMap<PluginIdentifier, Optional<T>> pluginInstanceMap = new ConcurrentHashMap<>(Common.COLLECTION_SIZE);
    protected final ConcurrentHashMap<PluginIdentifier, Optional<URL>> pluginJarPath = new ConcurrentHashMap<>(Common.COLLECTION_SIZE);

    public AbstractPluginDiscovery(String pluginSubDir) {
        this.pluginDir = Common.connectorJarDir(pluginSubDir);
    }

    @Override
    public List<URL> getPluginJarPaths(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
            .map(this::getPluginJarPath)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    }

    @Override
    public List<T> getAllPlugins(List<PluginIdentifier> pluginIdentifiers) {
        return pluginIdentifiers.stream()
            .map(this::getPluginInstance)
            .collect(Collectors.toList());
    }

    @Override
    public T getPluginInstance(PluginIdentifier pluginIdentifier) {
        Optional<T> pluginInstance = pluginInstanceMap.computeIfAbsent(pluginIdentifier, this::createPluginInstance);
        if (!pluginInstance.isPresent()) {
            throw new IllegalArgumentException("Can't find plugin: " + pluginIdentifier);
        }
        return pluginInstance.get();
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
        final String engineType = pluginIdentifier.getEngineType().toLowerCase();
        final String pluginType = pluginIdentifier.getPluginType().toLowerCase();
        final String pluginName = pluginIdentifier.getPluginName().toLowerCase();
        if (!PLUGIN_JAR_MAPPING.hasPath(engineType)) {
            return Optional.empty();
        }
        Config sparkConfig = PLUGIN_JAR_MAPPING.getConfig(engineType);
        if (!sparkConfig.hasPath(pluginType)) {
            return Optional.empty();
        }
        Config typeConfig = sparkConfig.getConfig(pluginType);
        Optional<Map.Entry<String, ConfigValue>> optional = typeConfig.entrySet().stream()
            .filter(entry -> StringUtils.equalsIgnoreCase(entry.getKey(), pluginName))
            .findFirst();
        if (!optional.isPresent()) {
            return Optional.empty();
        }
        String pluginJar = optional.get().getValue().unwrapped().toString() + ".jar";
        try {
            return Optional.of(pluginDir.resolve(pluginJar).toUri().toURL());
        } catch (MalformedURLException e) {
            LOGGER.warn("Cannot get plugin URL: " + pluginJar, e);
            return Optional.empty();
        }
    }

    private Optional<T> createPluginInstance(PluginIdentifier pluginIdentifier) {
        Optional<URL> pluginJarPath = getPluginJarPath(pluginIdentifier);
        ClassLoader classLoader;
        // if the plugin jar not exist in plugin dir, will load from classpath.
        if (pluginJarPath.isPresent()) {
            classLoader = new URLClassLoader(new URL[]{pluginJarPath.get()}, Thread.currentThread().getContextClassLoader());
        } else {
            classLoader = Thread.currentThread().getContextClassLoader();
        }
        ServiceLoader<T> serviceLoader = ServiceLoader.load(getPluginBaseClass(), classLoader);
        for (T t : serviceLoader) {
            // todo: add plugin identifier interface to support new api interface.
            Plugin<?> pluginInstance = (Plugin<?>) t;
            if (StringUtils.equalsIgnoreCase(pluginInstance.getPluginName(), pluginIdentifier.getPluginName())) {
                return Optional.of((T) pluginInstance);
            }
        }
        return Optional.empty();
    }
}
