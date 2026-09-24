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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.core.starter.execution.PluginExecuteProcessor;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;

@Slf4j
public abstract class SparkAbstractPluginExecuteProcessor<T>
        implements PluginExecuteProcessor<DatasetTableInfo, SparkRuntimeEnvironment> {
    protected SparkRuntimeEnvironment sparkRuntimeEnvironment;
    protected final List<? extends Config> pluginConfigs;
    protected final JobContext jobContext;
    protected final List<T> plugins;
    protected final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    protected SparkAbstractPluginExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            List<? extends Config> pluginConfigs) {
        this.sparkRuntimeEnvironment = sparkRuntimeEnvironment;
        this.jobContext = jobContext;
        this.pluginConfigs = pluginConfigs;
        this.plugins = initializePlugins(pluginConfigs);
    }

    @Override
    public void setRuntimeEnvironment(SparkRuntimeEnvironment sparkRuntimeEnvironment) {
        this.sparkRuntimeEnvironment = sparkRuntimeEnvironment;
    }

    protected abstract List<T> initializePlugins(List<? extends Config> pluginConfigs);

    protected void registerInputTempView(Config pluginConfig, Dataset<Row> dataStream) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(pluginConfig);
        if (readonlyConfig.getOptional(PLUGIN_OUTPUT).isPresent()) {
            String tableName = readonlyConfig.get(PLUGIN_OUTPUT);
            registerTempView(tableName, dataStream);
        }
    }

    protected Optional<DatasetTableInfo> fromSourceTable(
            Config pluginConfig,
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            List<DatasetTableInfo> upstreamDataStreams) {
        List<String> pluginInputIdentifiers =
                ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_INPUT);
        if (pluginInputIdentifiers == null || pluginInputIdentifiers.isEmpty()) {
            return Optional.empty();
        }
        if (pluginInputIdentifiers.size() > 1) {
            throw new UnsupportedOperationException(
                    "Multiple input tables are not supported in the current version");
        }
        String pluginInputIdentifier = pluginInputIdentifiers.get(0);
        DatasetTableInfo datasetTableInfo =
                upstreamDataStreams.stream()
                        .filter(info -> pluginInputIdentifier.equals(info.getTableName()))
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new SeaTunnelException(
                                                String.format(
                                                        "table %s not found",
                                                        pluginInputIdentifier)));
        return Optional.of(
                new DatasetTableInfo(
                        sparkRuntimeEnvironment
                                .getSparkSession()
                                .read()
                                .table(pluginInputIdentifier),
                        datasetTableInfo.getCatalogTables(),
                        pluginInputIdentifier));
    }

    // if not support multi table, rollback
    protected SeaTunnelSink tryGenerateMultiTableSink(
            Map<TablePath, SeaTunnelSink> sinks,
            ReadonlyConfig sinkConfig,
            ClassLoader classLoader) {
        if (sinks.values().stream().anyMatch(sink -> !(sink instanceof SupportMultiTableSink))) {
            log.info("Unsupported multi table sink api, rollback to sink template");
            // choose the first sink
            return sinks.values().iterator().next();
        }
        return FactoryUtil.createMultiTableSink(sinks, sinkConfig, classLoader);
    }

    private void registerTempView(String tableName, Dataset<Row> ds) {
        ds.createOrReplaceTempView(tableName);
    }
}
