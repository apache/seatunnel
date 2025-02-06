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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.core.starter.execution.PluginUtil.ensureJobModeMatch;

@SuppressWarnings("rawtypes")
public class SourceExecuteProcessor extends SparkAbstractPluginExecuteProcessor<SourceTableInfo> {
    private static final String PLUGIN_TYPE = "source";
    private Map envOption = new HashMap<String, String>();

    public SourceExecuteProcessor(
            SparkRuntimeEnvironment sparkEnvironment,
            JobContext jobContext,
            List<? extends Config> sourceConfigs) {
        super(sparkEnvironment, jobContext, sourceConfigs);
        for (Map.Entry<String, ConfigValue> entry : sparkEnvironment.getConfig().entrySet()) {
            String envKey = entry.getKey();
            String envValue = entry.getValue().render();
            if (envKey != null && envValue != null) {
                envOption.put(envKey, envValue);
            }
        }
    }

    @Override
    public List<DatasetTableInfo> execute(List<DatasetTableInfo> upstreamDataStreams) {
        List<DatasetTableInfo> sources = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            SourceTableInfo sourceTableInfo = plugins.get(i);
            SeaTunnelSource<?, ?, ?> source = sourceTableInfo.getSource();
            Config pluginConfig = pluginConfigs.get(i);
            int parallelism;
            if (pluginConfig.hasPath(CommonOptions.PARALLELISM.key())) {
                parallelism = pluginConfig.getInt(CommonOptions.PARALLELISM.key());
            } else {
                parallelism =
                        sparkRuntimeEnvironment
                                .getSparkConf()
                                .getInt(
                                        CommonOptions.PARALLELISM.key(),
                                        CommonOptions.PARALLELISM.defaultValue());
            }
            Dataset<Row> dataset =
                    sparkRuntimeEnvironment
                            .getSparkSession()
                            .read()
                            .format(SeaTunnelSource.class.getSimpleName())
                            .option(CommonOptions.PARALLELISM.key(), parallelism)
                            .option(
                                    Constants.SOURCE_SERIALIZATION,
                                    SerializationUtils.objectToString(source))
                            .options(envOption)
                            .load();
            sources.add(
                    new DatasetTableInfo(
                            dataset,
                            sourceTableInfo.getCatalogTables(),
                            ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_OUTPUT)));
            registerInputTempView(pluginConfigs.get(i), dataset);
        }
        return sources;
    }

    @Override
    protected List<SourceTableInfo> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();

        Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource =
                sourcePluginDiscovery::createPluginInstance;

        List<SourceTableInfo> sources = new ArrayList<>();
        Set<URL> jars = new HashSet<>();
        for (Config sourceConfig : pluginConfigs) {
            PluginIdentifier pluginIdentifier =
                    PluginIdentifier.of(
                            ENGINE_TYPE, PLUGIN_TYPE, sourceConfig.getString(PLUGIN_NAME.key()));
            jars.addAll(
                    sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> source =
                    FactoryUtil.createAndPrepareSource(
                            ReadonlyConfig.fromConfig(sourceConfig),
                            Thread.currentThread().getContextClassLoader(),
                            pluginIdentifier.getPluginName(),
                            fallbackCreateSource,
                            null);

            source._1().setJobContext(jobContext);
            ensureJobModeMatch(jobContext, source._1());
            sources.add(new SourceTableInfo(source._1(), source._2()));
        }
        sparkRuntimeEnvironment.registerPlugin(new ArrayList<>(jars));
        return sources;
    }
}
