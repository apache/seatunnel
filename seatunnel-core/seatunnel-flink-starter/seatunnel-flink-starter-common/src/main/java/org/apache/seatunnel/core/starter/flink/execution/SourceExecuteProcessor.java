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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.core.starter.enums.PluginType;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.flink.source.BaseSeaTunnelSourceFunction;
import org.apache.seatunnel.translation.flink.source.SeaTunnelCoordinatedSource;
import org.apache.seatunnel.translation.flink.source.SeaTunnelParallelSource;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.types.Row;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

@Slf4j
@SuppressWarnings("unchecked,rawtypes")
public class SourceExecuteProcessor extends FlinkAbstractPluginExecuteProcessor<SourceTableInfo> {
    private static final String PLUGIN_TYPE = PluginType.SOURCE.getType();
    private Config envConfigs;

    public SourceExecuteProcessor(List<URL> jarPaths, Config ConfigsInfo, JobContext jobContext) {
        super(jarPaths, ConfigsInfo.getConfigList(Constants.SOURCE), jobContext);
        this.envConfigs = ConfigsInfo.getConfig("env");
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
            BaseSeaTunnelSourceFunction sourceFunction;
            if (internalSource instanceof SupportCoordinate) {
                sourceFunction = new SeaTunnelCoordinatedSource(internalSource, envConfigs);

                registerAppendStream(pluginConfig);
            } else {
                sourceFunction = new SeaTunnelParallelSource(internalSource, envConfigs);
            }
            boolean bounded =
                    internalSource.getBoundedness()
                            == org.apache.seatunnel.api.source.Boundedness.BOUNDED;

            DataStreamSource<Row> sourceStream =
                    addSource(
                            executionEnvironment,
                            sourceFunction,
                            "SeaTunnel " + internalSource.getClass().getSimpleName(),
                            bounded);

            if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
                int parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
                sourceStream.setParallelism(parallelism);
            }
            registerResultTable(pluginConfig, sourceStream);
            sources.add(
                    new DataStreamTableInfo(
                            sourceStream,
                            sourceTableInfo.getCatalogTables().get(0),
                            pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                    ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                    : null));
        }
        return sources;
    }

    private DataStreamSource<Row> addSource(
            StreamExecutionEnvironment streamEnv,
            BaseSeaTunnelSourceFunction function,
            String sourceName,
            boolean bounded) {
        checkNotNull(function);
        checkNotNull(sourceName);
        checkNotNull(bounded);

        TypeInformation<Row> resolvedTypeInfo = function.getProducedType();

        boolean isParallel = function instanceof ParallelSourceFunction;

        streamEnv.clean(function);

        final StreamSource<Row, ?> sourceOperator = new StreamSource<>(function);
        return new DataStreamSource<>(
                streamEnv,
                resolvedTypeInfo,
                sourceOperator,
                isParallel,
                sourceName,
                bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Override
    protected List<SourceTableInfo> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery =
                new SeaTunnelSourcePluginDiscovery(ADD_URL_TO_CLASSLOADER);

        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSourceFactory.class, ADD_URL_TO_CLASSLOADER);

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
        jarPaths.addAll(jars);
        return sources;
    }
}
