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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;

public abstract class FlinkAbstractPluginExecuteProcessor<T>
        implements PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment> {

    protected static final BiConsumer<ClassLoader, URL> ADD_URL_TO_CLASSLOADER =
            (classLoader, url) -> {
                if (classLoader.getClass().getName().endsWith("SafetyNetWrapperClassLoader")) {
                    URLClassLoader c =
                            (URLClassLoader) ReflectionUtils.getField(classLoader, "inner").get();
                    ReflectionUtils.invoke(c, "addURL", url);
                } else if (classLoader instanceof URLClassLoader) {
                    ReflectionUtils.invoke(classLoader, "addURL", url);
                } else {
                    throw new RuntimeException(
                            "Unsupported classloader: " + classLoader.getClass().getName());
                }
            };

    protected FlinkRuntimeEnvironment flinkRuntimeEnvironment;
    protected final List<? extends Config> pluginConfigs;
    protected JobContext jobContext;
    protected final List<T> plugins;
    protected final Config envConfig;
    protected final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    protected FlinkAbstractPluginExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        this.pluginConfigs = pluginConfigs;
        this.jobContext = jobContext;
        this.plugins = initializePlugins(jarPaths, pluginConfigs);
        this.envConfig = envConfig;
    }

    @Override
    public void setRuntimeEnvironment(FlinkRuntimeEnvironment flinkRuntimeEnvironment) {
        this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    }

    protected Optional<DataStreamTableInfo> fromSourceTable(
            Config pluginConfig, List<DataStreamTableInfo> upstreamDataStreams) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);

        if (readonlyConfig.getOptional(PLUGIN_INPUT).isPresent()) {
            List<String> pluginInputIdentifiers = readonlyConfig.get(PLUGIN_INPUT);
            if (pluginInputIdentifiers.size() > 1) {
                throw new UnsupportedOperationException(
                        "Multiple input tables are not supported in flink plugin");
            }

            String tableName = pluginInputIdentifiers.get(0);
            DataStreamTableInfo dataStreamTableInfo =
                    upstreamDataStreams.stream()
                            .filter(info -> tableName.equals(info.getTableName()))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new SeaTunnelException(
                                                    String.format(
                                                            "table %s not found", tableName)));
            return Optional.of(
                    new DataStreamTableInfo(
                            dataStreamTableInfo.getDataStream(),
                            dataStreamTableInfo.getCatalogTables(),
                            tableName));
        }
        return Optional.empty();
    }

    protected abstract List<T> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs);
}
