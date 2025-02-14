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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverOptionalFactory;

public class SinkExecuteProcessor
        extends SparkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {

    protected SinkExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            List<? extends Config> pluginConfigs) {
        super(sparkRuntimeEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<Optional<? extends Factory>> initializePlugins(
            List<? extends Config> pluginConfigs) {
        List<URL> pluginJars = new ArrayList<>();
        SeaTunnelFactoryDiscovery sinkPluginDiscovery =
                new SeaTunnelFactoryDiscovery(TableSinkFactory.class);
        List<Optional<? extends Factory>> sinks =
                pluginConfigs.stream()
                        .map(
                                sinkConfig -> {
                                    pluginJars.addAll(
                                            sinkPluginDiscovery.getPluginJarPaths(
                                                    Lists.newArrayList(
                                                            PluginIdentifier.of(
                                                                    EngineType.SEATUNNEL
                                                                            .getEngine(),
                                                                    PluginType.SINK.getType(),
                                                                    sinkConfig.getString(
                                                                            PLUGIN_NAME.key())))));
                                    return discoverOptionalFactory(
                                            classLoader,
                                            TableSinkFactory.class,
                                            sinkConfig.getString(PLUGIN_NAME.key()));
                                })
                        .distinct()
                        .collect(Collectors.toList());
        sparkRuntimeEnvironment.registerPlugin(pluginJars);
        return sinks;
    }

    @Override
    public List<DatasetTableInfo> execute(List<DatasetTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        DatasetTableInfo input = upstreamDataStreams.get(upstreamDataStreams.size() - 1);
        Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink =
                sinkPluginDiscovery::createPluginInstance;
        for (int i = 0; i < plugins.size(); i++) {
            Config sinkConfig = pluginConfigs.get(i);
            DatasetTableInfo datasetTableInfo =
                    fromSourceTable(sinkConfig, sparkRuntimeEnvironment, upstreamDataStreams)
                            .orElse(input);
            Dataset<Row> dataset = datasetTableInfo.getDataset();

            int parallelism;
            if (sinkConfig.hasPath(EnvCommonOptions.PARALLELISM.key())) {
                parallelism = sinkConfig.getInt(EnvCommonOptions.PARALLELISM.key());
            } else {
                parallelism =
                        sparkRuntimeEnvironment
                                .getSparkConf()
                                .getInt(
                                        EnvCommonOptions.PARALLELISM.key(),
                                        EnvCommonOptions.PARALLELISM.defaultValue());
            }
            dataset.sparkSession().read().option(EnvCommonOptions.PARALLELISM.key(), parallelism);
            Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
            datasetTableInfo.getCatalogTables().stream()
                    .forEach(
                            catalogTable -> {
                                SeaTunnelSink<Object, Object, Object, Object> sink =
                                        FactoryUtil.createAndPrepareSink(
                                                catalogTable,
                                                ReadonlyConfig.fromConfig(sinkConfig),
                                                classLoader,
                                                sinkConfig.getString(PLUGIN_NAME.key()),
                                                fallbackCreateSink,
                                                null);
                                sink.setJobContext(jobContext);
                                sinks.put(catalogTable.getTableId().toTablePath(), sink);
                            });

            SeaTunnelSink sink =
                    tryGenerateMultiTableSink(
                            sinks, ReadonlyConfig.fromConfig(sinkConfig), classLoader);
            // TODO modify checkpoint location
            handleSaveMode(sink);
            String applicationId =
                    sparkRuntimeEnvironment.getSparkSession().sparkContext().applicationId();
            CatalogTable[] catalogTables =
                    datasetTableInfo.getCatalogTables().toArray(new CatalogTable[0]);
            SparkSinkInjector.inject(
                            dataset.write(), sink, catalogTables, applicationId, parallelism)
                    .option("checkpointLocation", "/tmp")
                    .save();
        }
        // the sink is the last stream
        return null;
    }

    public void handleSaveMode(SeaTunnelSink sink) {
        if (sink instanceof SupportSaveMode) {
            Optional<SaveModeHandler> saveModeHandler =
                    ((SupportSaveMode) sink).getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    handler.open();
                    new SaveModeExecuteWrapper(handler).execute();
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        } else if (sink instanceof MultiTableSink) {
            Map<TablePath, SeaTunnelSink> sinks = ((MultiTableSink) sink).getSinks();
            for (SeaTunnelSink seaTunnelSink : sinks.values()) {
                handleSaveMode(seaTunnelSink);
            }
        }
    }
}
