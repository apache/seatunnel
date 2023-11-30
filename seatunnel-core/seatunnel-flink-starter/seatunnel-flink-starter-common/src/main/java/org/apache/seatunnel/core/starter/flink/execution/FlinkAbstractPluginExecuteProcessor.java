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
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.core.starter.flink.utils.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

public abstract class FlinkAbstractPluginExecuteProcessor<T>
        implements PluginExecuteProcessor<DataStreamTableInfo, FlinkRuntimeEnvironment> {
    protected static final String ENGINE_TYPE = "seatunnel";
    protected static final String PLUGIN_NAME_KEY = "plugin_name";
    protected static final String SOURCE_TABLE_NAME = "source_table_name";
    protected static HashMap<String, Boolean> isAppendMap = new HashMap<>();

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

    protected FlinkAbstractPluginExecuteProcessor(
            List<URL> jarPaths, List<? extends Config> pluginConfigs, JobContext jobContext) {
        this.pluginConfigs = pluginConfigs;
        this.jobContext = jobContext;
        this.plugins = initializePlugins(jarPaths, pluginConfigs);
    }

    @Override
    public void setRuntimeEnvironment(FlinkRuntimeEnvironment flinkRuntimeEnvironment) {
        this.flinkRuntimeEnvironment = flinkRuntimeEnvironment;
    }

    protected Optional<DataStreamTableInfo> fromSourceTable(
            Config pluginConfig, List<DataStreamTableInfo> upstreamDataStreams) {
        if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            StreamTableEnvironment tableEnvironment =
                    flinkRuntimeEnvironment.getStreamTableEnvironment();
            String tableName = pluginConfig.getString(SOURCE_TABLE_NAME);
            Table table = tableEnvironment.from(tableName);
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
                            TableUtil.tableToDataStream(
                                    tableEnvironment,
                                    table,
                                    isAppendMap.getOrDefault(tableName, true)),
                            dataStreamTableInfo.getCatalogTable(),
                            tableName));
        }
        return Optional.empty();
    }

    protected void registerResultTable(Config pluginConfig, DataStream<Row> dataStream) {
        if (pluginConfig.hasPath(RESULT_TABLE_NAME.key())) {
            String resultTable = pluginConfig.getString(RESULT_TABLE_NAME.key());
            if (pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
                String sourceTable = pluginConfig.getString(SOURCE_TABLE_NAME);
                flinkRuntimeEnvironment.registerResultTable(
                        pluginConfig,
                        dataStream,
                        resultTable,
                        isAppendMap.getOrDefault(sourceTable, true));
                registerAppendStream(pluginConfig);
                return;
            }
            flinkRuntimeEnvironment.registerResultTable(
                    pluginConfig,
                    dataStream,
                    resultTable,
                    isAppendMap.getOrDefault(resultTable, true));
        }
    }

    protected void registerAppendStream(Config pluginConfig) {
        if (pluginConfig.hasPath(RESULT_TABLE_NAME.key())) {
            String tableName = pluginConfig.getString(RESULT_TABLE_NAME.key());
            isAppendMap.put(tableName, false);
        }
    }

    protected abstract List<T> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs);
}
