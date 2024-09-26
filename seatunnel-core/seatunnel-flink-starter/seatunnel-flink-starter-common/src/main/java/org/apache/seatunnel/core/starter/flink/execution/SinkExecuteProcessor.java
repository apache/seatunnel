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

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.types.Row;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.enums.DiscoveryType;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginRemoteDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.core.starter.flink.utils.ResourceUtils.getPluginMappingConfigFromClasspath;

@Slf4j
@SuppressWarnings("unchecked,rawtypes")
public class SinkExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {

    private static final String PLUGIN_TYPE = PluginType.SINK.getType();

    protected SinkExecuteProcessor(
            DiscoveryType discoveryType,
            List<URL> provideJarPaths,
            List<URL> actualUseJarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(
                discoveryType,
                provideJarPaths,
                actualUseJarPaths,
                envConfig,
                pluginConfigs,
                jobContext);
    }

    @Override
    protected List<Optional<? extends Factory>> initializePlugins(
            List<URL> actualUseJarPaths, List<? extends Config> pluginConfigs) {
        Config pluginMappingConfig =
                discoveryType == DiscoveryType.REMOTE
                        ? getPluginMappingConfigFromClasspath()
                        : null;

        PluginDiscovery<Factory> factoryDiscovery =
                getSeaTunnelFactoryDiscovery(connectors, pluginMappingConfig);

        PluginDiscovery<SeaTunnelSink> sinkPluginDiscovery =
                getSeaTunnelSinkPluginDiscovery(connectors, pluginMappingConfig);

        return pluginConfigs.stream()
                .map(
                        sinkConfig ->
                                PluginUtil.createSinkFactory(
                                        factoryDiscovery,
                                        sinkPluginDiscovery,
                                        sinkConfig,
                                        actualUseJarPaths))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        Config pluginMappingConfig =
                discoveryType == DiscoveryType.REMOTE
                        ? getPluginMappingConfigFromClasspath()
                        : null;
        PluginDiscovery<SeaTunnelSink> sinkPluginDiscovery =
                getSeaTunnelSinkPluginDiscovery(connectors, pluginMappingConfig);
        DataStreamTableInfo input = upstreamDataStreams.get(0);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < plugins.size(); i++) {
            Config sinkConfig = pluginConfigs.get(i);
            DataStreamTableInfo stream =
                    fromSourceTable(sinkConfig, upstreamDataStreams).orElse(input);
            Optional<? extends Factory> factory = plugins.get(i);
            boolean fallBack = !factory.isPresent() || isFallback(factory.get());
            Map<String, SeaTunnelSink> sinks = new HashMap<>();
            if (fallBack) {
                for (CatalogTable catalogTable : stream.getCatalogTables()) {
                    SeaTunnelSink fallBackSink =
                            fallbackCreateSink(
                                    sinkPluginDiscovery,
                                    PluginIdentifier.of(
                                            ENGINE_TYPE,
                                            PLUGIN_TYPE,
                                            sinkConfig.getString(PLUGIN_NAME.key())),
                                    sinkConfig);
                    fallBackSink.setJobContext(jobContext);
                    SeaTunnelRowType sourceType = catalogTable.getSeaTunnelRowType();
                    fallBackSink.setTypeInfo(sourceType);
                    handleSaveMode(fallBackSink);
                    TableIdentifier tableId = catalogTable.getTableId();
                    String tableIdName = tableId.toTablePath().toString();
                    sinks.put(tableIdName, fallBackSink);
                }
            } else {
                for (CatalogTable catalogTable : stream.getCatalogTables()) {
                    SeaTunnelSink seaTunnelSink;
                    TableSinkFactoryContext context =
                            TableSinkFactoryContext.replacePlaceholderAndCreate(
                                    catalogTable,
                                    ReadonlyConfig.fromConfig(sinkConfig),
                                    classLoader,
                                    ((TableSinkFactory) factory.get())
                                            .excludeTablePlaceholderReplaceKeys());
                    ConfigValidator.of(context.getOptions()).validate(factory.get().optionRule());
                    seaTunnelSink =
                            ((TableSinkFactory) factory.get()).createSink(context).createSink();
                    seaTunnelSink.setJobContext(jobContext);
                    handleSaveMode(seaTunnelSink);
                    TableIdentifier tableId = catalogTable.getTableId();
                    String tableIdName = tableId.toTablePath().toString();
                    sinks.put(tableIdName, seaTunnelSink);
                }
            }
            SeaTunnelSink sink =
                    tryGenerateMultiTableSink(
                            sinks, ReadonlyConfig.fromConfig(sinkConfig), classLoader);
            boolean sinkParallelism = sinkConfig.hasPath(CommonOptions.PARALLELISM.key());
            boolean envParallelism = envConfig.hasPath(CommonOptions.PARALLELISM.key());
            int parallelism =
                    sinkParallelism
                            ? sinkConfig.getInt(CommonOptions.PARALLELISM.key())
                            : envParallelism
                                    ? envConfig.getInt(CommonOptions.PARALLELISM.key())
                                    : 1;
            DataStreamSink<SeaTunnelRow> dataStreamSink =
                    stream.getDataStream()
                            .sinkTo(
                                    SinkV1Adapter.wrap(
                                            new FlinkSink<>(
                                                    sink, stream.getCatalogTables(), parallelism)))
                            .name(String.format("%s-Sink", sink.getPluginName()));
            if (sinkParallelism || envParallelism) {
                dataStreamSink.setParallelism(parallelism);
            }
        }
        // the sink is the last stream
        return null;
    }

    // if not support multi table, rollback
    public SeaTunnelSink tryGenerateMultiTableSink(
            Map<String, SeaTunnelSink> sinks, ReadonlyConfig sinkConfig, ClassLoader classLoader) {
        if (sinks.values().stream().anyMatch(sink -> !(sink instanceof SupportMultiTableSink))) {
            log.info("Unsupported multi table sink api, rollback to sink template");
            // choose the first sink
            return sinks.values().iterator().next();
        }
        return FactoryUtil.createMultiTableSink(sinks, sinkConfig, classLoader);
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
            PluginDiscovery<SeaTunnelSink> sinkPluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        SeaTunnelSink source = sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
    }


    public void handleSaveMode(SeaTunnelSink seaTunnelSink) {
        if (seaTunnelSink instanceof SupportSaveMode) {
            SupportSaveMode saveModeSink = (SupportSaveMode) seaTunnelSink;
            Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    handler.open();
                    new SaveModeExecuteWrapper(handler).execute();
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
    }

    private PluginDiscovery<Factory> getSeaTunnelFactoryDiscovery(List<URL> jarPaths, Config cfg) {
        switch (discoveryType) {
            case LOCAL:
                return new SeaTunnelFactoryLocalDiscovery(
                        TableSinkFactory.class, ADD_URL_TO_CLASSLOADER);
            case REMOTE:
                return new SeaTunnelFactoryRemoteDiscovery(
                        jarPaths, cfg, ADD_URL_TO_CLASSLOADER, TableSinkFactory.class);
            default:
                throw new IllegalArgumentException("unsupported discovery type: " + discoveryType);
        }
    }

    private PluginDiscovery<SeaTunnelSink> getSeaTunnelSinkPluginDiscovery(
            List<URL> jarPaths, Config cfg) {
        switch (discoveryType) {
            case LOCAL:
                return new SeaTunnelSinkPluginLocalDiscovery(ADD_URL_TO_CLASSLOADER);
            case REMOTE:
                return new SeaTunnelSinkPluginRemoteDiscovery(
                        jarPaths, cfg, ADD_URL_TO_CLASSLOADER);
            default:
                throw new IllegalArgumentException("unsupported discovery type: " + discoveryType);

        }
    }
}
