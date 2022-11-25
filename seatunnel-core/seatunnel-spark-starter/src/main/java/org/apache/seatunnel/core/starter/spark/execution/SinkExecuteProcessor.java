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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.translation.spark.common.utils.TypeConverterUtils;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SinkExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelSink<?, ?, ?, ?>> {

    private static final String PLUGIN_TYPE = "sink";

    protected SinkExecuteProcessor(SparkEnvironment sparkEnvironment,
                                   JobContext jobContext,
                                   List<? extends Config> pluginConfigs) {
        super(sparkEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<SeaTunnelSink<?, ?, ?, ?>> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        List<SeaTunnelSink<?, ?, ?, ?>> sinks = pluginConfigs.stream().map(sinkConfig -> {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(ENGINE_TYPE, PLUGIN_TYPE, sinkConfig.getString(PLUGIN_NAME));
            pluginJars.addAll(sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SeaTunnelSink<?, ?, ?, ?> seaTunnelSink = sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
            seaTunnelSink.prepare(sinkConfig);
            seaTunnelSink.setJobContext(jobContext);
            return seaTunnelSink;
        }).distinct().collect(Collectors.toList());
        sparkEnvironment.registerPlugin(pluginJars);
        return sinks;
    }

    @Override
    public List<Dataset<Row>> execute(List<Dataset<Row>> upstreamDataStreams) throws TaskExecuteException {
        Dataset<Row> input = upstreamDataStreams.get(0);
        for (int i = 0; i < plugins.size(); i++) {
            Config sinkConfig = pluginConfigs.get(i);
            SeaTunnelSink<?, ?, ?, ?> seaTunnelSink = plugins.get(i);
            Dataset<Row> dataset = fromSourceTable(sinkConfig, sparkEnvironment).orElse(input);
            int parallelism;
            if (sinkConfig.hasPath(SinkCommonOptions.PARALLELISM.key())) {
                parallelism = sinkConfig.getInt(SinkCommonOptions.PARALLELISM.key());
            } else {
                parallelism = sparkEnvironment.getSparkConf().getInt(EnvCommonOptions.PARALLELISM.key(), EnvCommonOptions.PARALLELISM.defaultValue());
            }
            dataset.sparkSession().read().option(SinkCommonOptions.PARALLELISM.key(), parallelism);
            // TODO modify checkpoint location
            seaTunnelSink.setTypeInfo((SeaTunnelRowType) TypeConverterUtils.convert(dataset.schema()));
            SparkSinkInjector.inject(dataset.write(), seaTunnelSink).option("checkpointLocation", "/tmp").save();
        }
        // the sink is the last stream
        return null;
    }

}
