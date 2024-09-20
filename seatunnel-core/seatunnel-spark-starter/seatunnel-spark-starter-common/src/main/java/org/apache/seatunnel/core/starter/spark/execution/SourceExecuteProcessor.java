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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.execution.SourceTableInfo;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.common.collect.Lists;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.common.CommonOptions.RESULT_TABLE_NAME;

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
            SparkSession sparkSession = sparkRuntimeEnvironment.getSparkSession();
            Dataset<Row> dataset = null;
            if (isStreaming()) {
                dataset =
                        sparkSession
                                .readStream()
                                .format(SeaTunnelSource.class.getSimpleName())
                                .option(CommonOptions.PARALLELISM.key(), parallelism)
                                .option(
                                        Constants.SOURCE_SERIALIZATION,
                                        SerializationUtils.objectToString(source))
                                .options(envOption)
                                .load();
            } else {
                dataset =
                        sparkSession
                                .read()
                                .format(SeaTunnelSource.class.getSimpleName())
                                .option(CommonOptions.PARALLELISM.key(), parallelism)
                                .option(
                                        Constants.SOURCE_SERIALIZATION,
                                        SerializationUtils.objectToString(source))
                                .options(envOption)
                                .load();
            }
            sources.add(
                    new DatasetTableInfo(
                            dataset,
                            sourceTableInfo.getCatalogTables(),
                            pluginConfig.hasPath(RESULT_TABLE_NAME.key())
                                    ? pluginConfig.getString(RESULT_TABLE_NAME.key())
                                    : null));
            registerInputTempView(pluginConfigs.get(i), dataset);
        }
        return sources;
    }

    @Override
    protected List<SourceTableInfo> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableSourceFactory.class);

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
        sparkRuntimeEnvironment.registerPlugin(new ArrayList<>(jars));
        return sources;
    }
}
