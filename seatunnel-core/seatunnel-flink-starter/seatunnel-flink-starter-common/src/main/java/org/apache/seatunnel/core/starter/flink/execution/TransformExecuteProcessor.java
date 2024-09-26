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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.enums.DiscoveryType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginRemoteDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;
import static org.apache.seatunnel.core.starter.flink.utils.ResourceUtils.getPluginMappingConfigFromClasspath;

@SuppressWarnings("unchecked,rawtypes")
public class TransformExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<TableTransformFactory> {

    protected TransformExecuteProcessor(
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
    protected List<TableTransformFactory> initializePlugins(
            List<URL> actualUseJarPaths, List<? extends Config> pluginConfigs) {

        Config pluginMappingConfig =
                discoveryType == DiscoveryType.REMOTE
                        ? getPluginMappingConfigFromClasspath()
                        : null;

        PluginDiscovery<Factory> factoryDiscovery =
                getSeaTunnelFactoryDiscovery(connectors, pluginMappingConfig);

        PluginDiscovery<SeaTunnelTransform> transformPluginDiscovery =
                getSeaTunnelTransformPluginLocalDiscovery(connectors, pluginMappingConfig);

        return pluginConfigs.stream()
                .map(
                        transformConfig ->
                                PluginUtil.createTransformFactory(
                                        factoryDiscovery,
                                        transformPluginDiscovery,
                                        transformConfig,
                                        actualUseJarPaths))
                .distinct()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(e -> (TableTransformFactory) e)
                .collect(Collectors.toList());
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        DataStreamTableInfo input = upstreamDataStreams.get(0);
        Map<String, DataStreamTableInfo> outputTables =
                upstreamDataStreams.stream()
                        .collect(
                                Collectors.toMap(
                                        DataStreamTableInfo::getTableName,
                                        e -> e,
                                        (a, b) -> b,
                                        LinkedHashMap::new));

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < plugins.size(); i++) {
            try {
                Config pluginConfig = pluginConfigs.get(i);
                DataStreamTableInfo stream =
                        fromSourceTable(pluginConfig, new ArrayList<>(outputTables.values()))
                                .orElse(input);
                TableTransformFactory factory = plugins.get(i);
                TableTransformFactoryContext context =
                        new TableTransformFactoryContext(
                                stream.getCatalogTables(),
                                ReadonlyConfig.fromConfig(pluginConfig),
                                classLoader);
                ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
                SeaTunnelTransform transform = factory.createTransform(context).createTransform();

                transform.setJobContext(jobContext);
                DataStream<SeaTunnelRow> inputStream =
                        flinkTransform(transform, stream.getDataStream());
                String resultTableName =
                        pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                : null;
                // TODO transform support multi tables
                outputTables.put(
                        resultTableName,
                        new DataStreamTableInfo(
                                inputStream,
                                Collections.singletonList(transform.getProducedCatalogTable()),
                                resultTableName));
            } catch (Exception e) {
                throw new TaskExecuteException(
                        String.format(
                                "SeaTunnel transform task: %s execute error",
                                plugins.get(i).factoryIdentifier()),
                        e);
            }
        }
        return new ArrayList<>(outputTables.values());
    }

    protected DataStream<SeaTunnelRow> flinkTransform(
            SeaTunnelTransform transform, DataStream<SeaTunnelRow> stream) {
        return stream.transform(
                String.format("%s-Transform", transform.getPluginName()),
                TypeInformation.of(SeaTunnelRow.class),
                new StreamMap<>(
                        flinkRuntimeEnvironment
                                .getStreamExecutionEnvironment()
                                .clean(
                                        row ->
                                                ((SeaTunnelTransform<SeaTunnelRow>) transform)
                                                        .map(row))));
    }

    private PluginDiscovery<Factory> getSeaTunnelFactoryDiscovery(List<URL> jarPaths, Config cfg) {
        switch (discoveryType) {
            case LOCAL:
                return new SeaTunnelFactoryLocalDiscovery(
                        TableTransformFactory.class, ADD_URL_TO_CLASSLOADER);
            case REMOTE:
                return new SeaTunnelFactoryRemoteDiscovery(
                        jarPaths, cfg, ADD_URL_TO_CLASSLOADER, TableTransformFactory.class);
            default:
                throw new IllegalArgumentException("unsupported discovery type: " + discoveryType);
        }
    }

    private PluginDiscovery<SeaTunnelTransform> getSeaTunnelTransformPluginLocalDiscovery(
            List<URL> jarPaths, Config cfg) {
        switch (discoveryType) {
            case LOCAL:
                return new SeaTunnelTransformPluginLocalDiscovery();
            case REMOTE:
                return new SeaTunnelTransformPluginRemoteDiscovery(
                        jarPaths, cfg, ADD_URL_TO_CLASSLOADER);
            default:
                throw new IllegalArgumentException("unsupported discovery type: " + discoveryType);
        }
    }
}
