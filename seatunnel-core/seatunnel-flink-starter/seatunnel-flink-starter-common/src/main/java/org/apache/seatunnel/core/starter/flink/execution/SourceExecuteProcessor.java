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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.core.starter.enums.DiscoveryType;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.PluginDiscovery;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryRemoteDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginLocalDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginRemoteDiscovery;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.translation.flink.source.FlinkSource;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;
import static org.apache.seatunnel.core.starter.flink.utils.ResourceUtils.getPluginMappingConfigFromClasspath;

@Slf4j
@SuppressWarnings("unchecked,rawtypes")
public class SourceExecuteProcessor extends FlinkAbstractPluginExecuteProcessor<SourceTableInfo> {
    private static final String PLUGIN_TYPE = PluginType.SOURCE.getType();

    public SourceExecuteProcessor(
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
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams) {
        StreamExecutionEnvironment executionEnvironment =
                flinkRuntimeEnvironment.getStreamExecutionEnvironment();
        List<DataStreamTableInfo> sources = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            SourceTableInfo sourceTableInfo = plugins.get(i);
            SeaTunnelSource internalSource = sourceTableInfo.getSource();
            Config pluginConfig = pluginConfigs.get(i);
            FlinkSource flinkSource = new FlinkSource<>(internalSource, envConfig);

            DataStreamSource<SeaTunnelRow> sourceStream =
                    executionEnvironment.fromSource(
                            flinkSource,
                            WatermarkStrategy.noWatermarks(),
                            String.format("%s-Source", internalSource.getPluginName()));

            if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
                int parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
                sourceStream.setParallelism(parallelism);
            }
            sources.add(
                    new DataStreamTableInfo(
                            sourceStream,
                            sourceTableInfo.getCatalogTables(),
                            pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                    ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                    : null));
        }
        return sources;
    }

    @Override
    protected List<SourceTableInfo> initializePlugins(
            List<URL> actualUseJarPaths, List<? extends Config> pluginConfigs) {

        Config pluginMappingConfig =
                discoveryType == DiscoveryType.REMOTE
                        ? getPluginMappingConfigFromClasspath()
                        : null;

        PluginDiscovery<SeaTunnelSource> sourcePluginDiscovery =
                getSeaTunnelSourceDiscovery(connectors, pluginMappingConfig);

        PluginDiscovery<Factory> factoryDiscovery =
                getSeaTunnelFactoryDiscovery(connectors, pluginMappingConfig);

        List<SourceTableInfo> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of(
                            ENGINE_TYPE, PLUGIN_TYPE, sourceConfig.getString(PLUGIN_NAME.key()));
            jars.addAll(
                    sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SourceTableInfo source =
                    PluginUtil.createSource(
                            factoryDiscovery,
                            sourcePluginDiscovery,
                            pluginIdentifier,
                            sourceConfig,
                            jobContext);
            sources.add(source);
        }
        actualUseJarPaths.addAll(jars);
        return sources;
    }

    private PluginDiscovery<SeaTunnelSource> getSeaTunnelSourceDiscovery(
            List<URL> jarPaths, Config cfg) {
        switch (discoveryType) {
            case LOCAL:
                return new SeaTunnelSourcePluginLocalDiscovery(ADD_URL_TO_CLASSLOADER);
            case REMOTE:
                return new SeaTunnelSourcePluginRemoteDiscovery(
                        jarPaths, cfg, ADD_URL_TO_CLASSLOADER);
            default:
                throw new IllegalArgumentException("unsupported discovery type: " + discoveryType);
        }
    }

    private PluginDiscovery<Factory> getSeaTunnelFactoryDiscovery(List<URL> jarPaths, Config cfg) {
        switch (discoveryType) {
            case LOCAL:
                return new SeaTunnelFactoryLocalDiscovery(
                        TableSourceFactory.class, ADD_URL_TO_CLASSLOADER);
            case REMOTE:
                return new SeaTunnelFactoryRemoteDiscovery(
                        jarPaths, cfg, ADD_URL_TO_CLASSLOADER, TableSourceFactory.class);
            default:
                throw new IllegalArgumentException("unsupported discovery type: " + discoveryType);
        }
    }
}
