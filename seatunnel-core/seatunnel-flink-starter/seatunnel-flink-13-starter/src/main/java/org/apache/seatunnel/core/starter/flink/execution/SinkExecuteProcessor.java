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
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;

import org.apache.flink.streaming.api.datastream.DataStreamSink;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

@SuppressWarnings({"unchecked", "rawtypes"})
@Slf4j
public class SinkExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<Optional<? extends Factory>> {

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
        DataStreamTableInfo input = upstreamDataStreams.get(upstreamDataStreams.size() - 1);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink =
                sinkPluginDiscovery::createPluginInstance;
        for (int i = 0; i < plugins.size(); i++) {
            Optional<? extends Factory> factory = plugins.get(i);
            Config sinkConfig = pluginConfigs.get(i);
            DataStreamTableInfo stream =
                    fromSourceTable(sinkConfig, upstreamDataStreams).orElse(input);
            Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
            for (CatalogTable catalogTable : stream.getCatalogTables()) {
                SeaTunnelSink sink =
                        FactoryUtil.createAndPrepareSink(
                                catalogTable,
                                ReadonlyConfig.fromConfig(sinkConfig),
                                classLoader,
                                sinkConfig.getString(PLUGIN_NAME.key()),
                                fallbackCreateSink,
                                ((TableSinkFactory) (factory.orElse(null))));
                sink.setJobContext(jobContext);
                handleSaveMode(sink);
                TableIdentifier tableId = catalogTable.getTableId();
                sinks.put(tableId.toTablePath(), sink);
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
                            .sinkTo(new FlinkSink<>(sink, stream.getCatalogTables(), parallelism))
                            .name(String.format("%s-Sink", sink.getPluginName()));
            dataStreamSink.setParallelism(parallelism);
        }
        // the sink is the last stream
        return null;
    }

    // if not support multi table, rollback
    public SeaTunnelSink tryGenerateMultiTableSink(
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
}
