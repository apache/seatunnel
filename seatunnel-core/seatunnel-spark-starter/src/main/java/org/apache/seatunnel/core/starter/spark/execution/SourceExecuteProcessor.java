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
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceCommonOptions;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.translation.spark.common.utils.TypeConverterUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SourceExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelSource<?, ?, ?>> {

    private static final String PLUGIN_TYPE = "source";

    public SourceExecuteProcessor(SparkEnvironment sparkEnvironment,
                                  JobContext jobContext,
                                  List<? extends Config> sourceConfigs) {
        super(sparkEnvironment, jobContext, sourceConfigs);
    }

    @Override
    public List<Dataset<Row>> execute(List<Dataset<Row>> upstreamDataStreams) {
        List<Dataset<Row>> sources = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            SeaTunnelSource<?, ?, ?> source = plugins.get(i);
            Config pluginConfig = pluginConfigs.get(i);
            int parallelism;
            if (pluginConfig.hasPath(SourceCommonOptions.PARALLELISM.key())) {
                parallelism = pluginConfig.getInt(SourceCommonOptions.PARALLELISM.key());
            } else {
                parallelism = sparkEnvironment.getSparkConf().getInt(EnvCommonOptions.PARALLELISM.key(), EnvCommonOptions.PARALLELISM.defaultValue());
            }
            Dataset<Row> dataset = sparkEnvironment.getSparkSession()
                .read()
                .format(SeaTunnelSource.class.getSimpleName())
                .option(SourceCommonOptions.PARALLELISM.key(), parallelism)
                .option(Constants.SOURCE_SERIALIZATION, SerializationUtils.objectToString(source))
                .schema((StructType) TypeConverterUtils.convert(source.getProducedType())).load();
            sources.add(dataset);
            registerInputTempView(pluginConfigs.get(i), dataset);
        }
        return sources;
    }

    @Override
    protected List<SeaTunnelSource<?, ?, ?>> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        List<SeaTunnelSource<?, ?, ?>> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                ENGINE_TYPE, PLUGIN_TYPE, sourceConfig.getString(PLUGIN_NAME));
            jars.addAll(sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            SeaTunnelSource<?, ?, ?> seaTunnelSource = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
            seaTunnelSource.prepare(sourceConfig);
            seaTunnelSource.setJobContext(jobContext);
            sources.add(seaTunnelSource);
        }
        sparkEnvironment.registerPlugin(new ArrayList<>(jars));
        return sources;
    }
}
