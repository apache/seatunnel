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
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.flink.utils.TableUtil;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
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

@SuppressWarnings("unchecked,rawtypes")
@Slf4j
public class SinkExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {

    private static final String PLUGIN_TYPE = PluginType.SINK.getType();

    protected SinkExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(jarPaths, envConfig, pluginConfigs, jobContext);
    }

    @Override
    protected List<Optional<? extends Factory>> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSinkFactory.class, ADD_URL_TO_CLASSLOADER);
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery =
                new SeaTunnelSinkPluginDiscovery(ADD_URL_TO_CLASSLOADER);
        return pluginConfigs.stream()
                .map(
                        sinkConfig ->
                                PluginUtil.createSinkFactory(
                                        factoryDiscovery,
                                        sinkPluginDiscovery,
                                        sinkConfig,
                                        jarPaths))
                .distinct()
                .collect(Collectors.toList());
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery =
                new SeaTunnelSinkPluginDiscovery(ADD_URL_TO_CLASSLOADER);
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
                    String tableIdName = TableUtil.extractTableIdName(catalogTable.getTableId());
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
                    // remove catalog name
                    String tableIdName = TableUtil.extractTableIdName(catalogTable.getTableId());
                    sinks.put(tableIdName, seaTunnelSink);
                }
            }
            SeaTunnelSink sink =
                    tryGenerateMultiTableSink(
                            sinks, ReadonlyConfig.fromConfig(sinkConfig), classLoader);
            DataStreamSink<SeaTunnelRow> dataStreamSink =
                    stream.getDataStream()
                            .sinkTo(
                                    SinkV1Adapter.wrap(
                                            new FlinkSink<>(sink, stream.getCatalogTables())))
                            .name(String.format("%s-Sink", sink.getPluginName()));
            if (sinkConfig.hasPath(CommonOptions.PARALLELISM.key())) {
                int parallelism = sinkConfig.getInt(CommonOptions.PARALLELISM.key());
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
            SeaTunnelSinkPluginDiscovery sinkPluginDiscovery,
            PluginIdentifier pluginIdentifier,
            Config pluginConfig) {
        SeaTunnelSink source = sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        source.prepare(pluginConfig);
        return source;
    }

    public void handleSaveMode(SeaTunnelSink seaTunnelSink) {
        if (SupportSaveMode.class.isAssignableFrom(seaTunnelSink.getClass())) {
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
}
