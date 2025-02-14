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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.flink.source.FlinkSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.ensureJobModeMatch;

@Slf4j
@SuppressWarnings("unchecked,rawtypes")
public class SourceExecuteProcessor extends FlinkAbstractPluginExecuteProcessor<SourceTableInfo> {

    public SourceExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(jarPaths, envConfig, pluginConfigs, jobContext);
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

            if (pluginConfig.hasPath(EnvCommonOptions.PARALLELISM.key())) {
                int parallelism = pluginConfig.getInt(EnvCommonOptions.PARALLELISM.key());
                sourceStream.setParallelism(parallelism);
            }
            sources.add(
                    new DataStreamTableInfo(
                            sourceStream,
                            sourceTableInfo.getCatalogTables(),
                            ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_OUTPUT)));
        }
        return sources;
    }

    @Override
    protected List<SourceTableInfo> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSourceFactory.class, ADD_URL_TO_CLASSLOADER);
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery(ADD_URL_TO_CLASSLOADER);
        Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource =
                sourcePluginDiscovery::createPluginInstance;

        List<SourceTableInfo> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of(
                            EngineType.SEATUNNEL.getEngine(),
                            PluginType.SOURCE.getType(),
                            sourceConfig.getString(PLUGIN_NAME.key()));
            jars.addAll(
                    sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));

            Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> source =
                    FactoryUtil.createAndPrepareSource(
                            ReadonlyConfig.fromConfig(sourceConfig),
                            classLoader,
                            pluginIdentifier.getPluginName(),
                            fallbackCreateSource,
                            (TableSourceFactory)
                                    factoryDiscovery
                                            .createOptionalPluginInstance(pluginIdentifier)
                                            .orElse(null));

            source._1().setJobContext(jobContext);
            ensureJobModeMatch(jobContext, source._1());

            sources.add(new SourceTableInfo(source._1(), source._2()));
        }
        jarPaths.addAll(jars);
        return sources;
    }
}
