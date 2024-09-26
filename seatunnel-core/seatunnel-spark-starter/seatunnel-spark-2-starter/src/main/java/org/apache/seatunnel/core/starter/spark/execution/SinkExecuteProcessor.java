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

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginLocalDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

public class SinkExecuteProcessor
        extends SparkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {
    private static final String PLUGIN_TYPE = PluginType.SINK.getType();

    protected SinkExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            List<? extends Config> pluginConfigs) {
        super(sparkRuntimeEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<Optional<? extends Factory>> initializePlugins(
            List<? extends Config> pluginConfigs) {
        SeaTunnelFactoryLocalDiscovery factoryDiscovery =
                new SeaTunnelFactoryLocalDiscovery(TableSinkFactory.class);
        SeaTunnelSinkPluginLocalDiscovery sinkPluginDiscovery =
                new SeaTunnelSinkPluginLocalDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        List<Optional<? extends Factory>> sinks =
                pluginConfigs.stream()
                        .map(
                                sinkConfig ->
                                        PluginUtil.createSinkFactory(
                                                factoryDiscovery,
                                                sinkPluginDiscovery,
                                                sinkConfig,
                                                pluginJars))
                        .distinct()
                        .collect(Collectors.toList());
        sparkRuntimeEnvironment.registerPlugin(pluginJars);
        return sinks;
    }

    @Override
    public List<DatasetTableInfo> execute(List<DatasetTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        SeaTunnelSinkPluginLocalDiscovery sinkPluginDiscovery =
                new SeaTunnelSinkPluginLocalDiscovery();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        DatasetTableInfo input = upstreamDataStreams.get(0);
        for (int i = 0; i < plugins.size(); i++) {
            Config sinkConfig = pluginConfigs.get(i);
            DatasetTableInfo datasetTableInfo =
                    fromSourceTable(sinkConfig, sparkRuntimeEnvironment, upstreamDataStreams)
                            .orElse(input);
            SeaTunnelDataType<?> inputType =
                    datasetTableInfo.getCatalogTable().getSeaTunnelRowType();
            Dataset<Row> dataset = datasetTableInfo.getDataset();

            int parallelism;
            if (sinkConfig.hasPath(CommonOptions.PARALLELISM.key())) {
                parallelism = sinkConfig.getInt(CommonOptions.PARALLELISM.key());
            } else {
                parallelism =
                        sparkRuntimeEnvironment
                                .getSparkConf()
                                .getInt(
                                        CommonOptions.PARALLELISM.key(),
                                        CommonOptions.PARALLELISM.defaultValue());
            }
            dataset.sparkSession().read().option(CommonOptions.PARALLELISM.key(), parallelism);
            Optional<? extends Factory> factory = plugins.get(i);
            boolean fallBack = !factory.isPresent() || isFallback(factory.get());
            SeaTunnelSink sink;
            if (fallBack) {
                sink =
                        fallbackCreateSink(
                                sinkPluginDiscovery,
                                PluginIdentifier.of(
                                        ENGINE_TYPE,
                                        PLUGIN_TYPE,
                                        sinkConfig.getString(PLUGIN_NAME.key())),
                                sinkConfig);
                sink.setJobContext(jobContext);
                sink.setTypeInfo((SeaTunnelRowType) inputType);
            } else {
                TableSinkFactoryContext context =
                        TableSinkFactoryContext.replacePlaceholderAndCreate(
                                datasetTableInfo.getCatalogTable(),
                                ReadonlyConfig.fromConfig(sinkConfig),
                                classLoader,
                                ((TableSinkFactory) factory.get())
                                        .excludeTablePlaceholderReplaceKeys());
                ConfigValidator.of(context.getOptions()).validate(factory.get().optionRule());
                sink = ((TableSinkFactory) factory.get()).createSink(context).createSink();
                sink.setJobContext(jobContext);
            }
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

    public boolean isFallback(Factory factory) {
        try {
            ((TableSinkFactory) factory).createSink(null);
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                return true;
            }
        }
        return false;
    }

    public SeaTunnelSink fallbackCreateSink(
            SeaTunnelSinkPluginLocalDiscovery sinkPluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        SeaTunnelSink source = sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
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
            Map<String, SeaTunnelSink> sinks = ((MultiTableSink) sink).getSinks();
            for (SeaTunnelSink seaTunnelSink : sinks.values()) {
                handleSaveMode(seaTunnelSink);
            }
        }
    }
}
