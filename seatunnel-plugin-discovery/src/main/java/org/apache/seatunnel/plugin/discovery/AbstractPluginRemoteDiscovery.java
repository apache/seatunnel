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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractPluginRemoteDiscovery<T> extends AbstractPluginDiscovery<T> {

    protected final Collection<URL> provideLibUrls;

    public AbstractPluginRemoteDiscovery(
            Collection<URL> provideLibUrls,
            Config pluginMappingConfig,
            BiConsumer<ClassLoader, URL> addURLToClassLoaderConsumer) {
        super(pluginMappingConfig, addURLToClassLoaderConsumer);
        this.provideLibUrls = provideLibUrls;
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
        List<URL> targetPluginFiles =
                provideLibUrls.stream()
                        .filter(
                                url -> {
                                    String fileName = getFileName(url);
                                    return fileName.endsWith("jar")
                                            && StringUtils.startsWithIgnoreCase(
                                                    fileName, pluginJarPrefix);
                                })
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(targetPluginFiles)) {
            return Optional.empty();
        }
        URL pluginJarPath;
        if (targetPluginFiles.size() == 1) {
            pluginJarPath = targetPluginFiles.get(0);
        } else {
            pluginJarPath =
                    findMostSimlarPluginJarFile(
                            targetPluginFiles.toArray(new URL[0]),
                            this::getFileName,
                            pluginJarPrefix);
        }
        log.info("Discovery plugin jar for: {} at: {}", pluginIdentifier, pluginJarPath);
        return Optional.of(pluginJarPath);
    }

    @Override
    protected List<Factory> getPluginFactories() {
        List<Factory> factories;
        if (CollectionUtils.isNotEmpty(provideLibUrls)) {
            factories =
                    FactoryUtil.discoverFactories(
                            new URLClassLoader(provideLibUrls.toArray(new URL[0])));
        } else {
            log.warn("provide library not exists, load plugin from classpath");
            factories =
                    FactoryUtil.discoverFactories(Thread.currentThread().getContextClassLoader());
        }
        return factories;
    }

    private String getFileName(URL url) {
        String file = url.getFile();
        int index = file.lastIndexOf(File.separator);
        return file.substring(index + 1);
    }
}
