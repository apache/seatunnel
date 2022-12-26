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

import static org.apache.flink.util.Preconditions.checkNotNull;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceCommonOptions;
import org.apache.seatunnel.api.source.SupportCoordinate;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.flink.source.BaseSeaTunnelSourceFunction;
import org.apache.seatunnel.translation.flink.source.SeaTunnelCoordinatedSource;
import org.apache.seatunnel.translation.flink.source.SeaTunnelParallelSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SourceExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelSource> {

    private static final String PLUGIN_TYPE = "source";

    public SourceExecuteProcessor(List<URL> jarPaths, List<? extends Config> sourceConfigs, JobContext jobContext) {
        super(jarPaths, sourceConfigs, jobContext);
    }

    @Override
    public List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams) {
        StreamExecutionEnvironment executionEnvironment = flinkEnvironment.getStreamExecutionEnvironment();
        List<DataStream<Row>> sources = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            SeaTunnelSource internalSource = plugins.get(i);
            BaseSeaTunnelSourceFunction sourceFunction;
            if (internalSource instanceof SupportCoordinate) {
                sourceFunction = new SeaTunnelCoordinatedSource(internalSource);
            } else {
                sourceFunction = new SeaTunnelParallelSource(internalSource);
            }
            DataStreamSource<Row> sourceStream = addSource(executionEnvironment,
                sourceFunction,
                "SeaTunnel " + internalSource.getClass().getSimpleName(),
                internalSource.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.BOUNDED);
            Config pluginConfig = pluginConfigs.get(i);
            if (pluginConfig.hasPath(SourceCommonOptions.PARALLELISM.key())) {
                int parallelism = pluginConfig.getInt(SourceCommonOptions.PARALLELISM.key());
                sourceStream.setParallelism(parallelism);
            }
            registerResultTable(pluginConfig, sourceStream);
            sources.add(sourceStream);
        }
        return sources;
    }

    private DataStreamSource<Row> addSource(
        final StreamExecutionEnvironment streamEnv,
        final BaseSeaTunnelSourceFunction function,
        final String sourceName,
        boolean bounded) {
        checkNotNull(function);
        checkNotNull(sourceName);
        checkNotNull(bounded);

        TypeInformation<Row> resolvedTypeInfo = function.getProducedType();

        boolean isParallel = function instanceof ParallelSourceFunction;

        streamEnv.clean(function);

        final StreamSource<Row, ?> sourceOperator = new StreamSource<>(function);
        return new DataStreamSource<>(streamEnv, resolvedTypeInfo, sourceOperator, isParallel, sourceName, bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Override
    protected List<SeaTunnelSource> initializePlugins(List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery(addUrlToClassloader);
        List<SeaTunnelSource> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                ENGINE_TYPE, PLUGIN_TYPE, sourceConfig.getString(PLUGIN_NAME));
            jars.addAll(sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SeaTunnelSource seaTunnelSource = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
            seaTunnelSource.prepare(sourceConfig);
            seaTunnelSource.setJobContext(jobContext);
            if (jobContext.getJobMode() == JobMode.BATCH
                && seaTunnelSource.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
                throw new UnsupportedOperationException(String.format("'%s' source don't support off-line job.", seaTunnelSource.getPluginName()));
            }
            sources.add(seaTunnelSource);
        }
        jarPaths.addAll(jars);
        return sources;
    }
}
