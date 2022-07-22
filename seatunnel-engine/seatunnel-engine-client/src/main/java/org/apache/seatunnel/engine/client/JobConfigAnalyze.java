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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.apis.base.plugin.Plugin;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckExceptionSeaTunnel;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFlinkTransformPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.Data;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Serializable;

@Data
public class JobConfigAnalyze {
    private static final ILogger LOGGER = Logger.getLogger(JobConfigAnalyze.class);
    private String jobDefineFilePath;

    private Map<Action, String> alreadyTransformActionMap = new HashMap<>();

    private Map<String, List<SeaTunnelTransform>> transformResultTableNameMap = new HashMap<>();
    private Map<String, List<SeaTunnelTransform>> transformSourceTableNameMap = new HashMap<>();

    private Map<String, List<SeaTunnelSource>> sourceResultTableNameMap = new HashMap<>();

    public List<Action> analyzeJobConfig() {
        List<Action> actions = new ArrayList<>();
        Config seaTunnelJobConfig = new ConfigBuilder(Paths.get(jobDefineFilePath)).getConfig();
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        List<? extends Config> transformConfigs = seaTunnelJobConfig.getConfigList("transform");
        List<? extends Config> sourceConfigs = seaTunnelJobConfig.getConfigList("source");

        if (sinkConfigs.size() == 1 && sourceConfigs.size() == 1 & transformConfigs.size() <= 1) {
            return sampleAnalyze(sourceConfigs, transformConfigs, sinkConfigs);
        } else {
            return complexAnalyze(sourceConfigs, transformConfigs, sinkConfigs);
        }

        for (Config config : sinkConfigs) {
            if (!config.hasPath(Plugin.SOURCE_TABLE_NAME)) {
                // If sink is not configured with source_table_name, we use the last transform as the upstream of the sink
                if (transformConfigs.size() == 0) {
                    // If the job have no transform, we use the last source as the upstream of the sink
                    Config lastSourceConfig = sourceConfigs.get(sourceConfigs.size() - 1);
                    String pluginName = lastSourceConfig.getString("plugin_name");
                } else {
                    Config lastTransformConfig = transformConfigs.get(transformConfigs.size() - 1);
                    String pluginName = lastTransformConfig.getString("plugin_name");
                    analyzeTransform();
                }
            } else {

            }

            String sourceTableName = config.getString(Plugin.SOURCE_TABLE_NAME);

        }
    }

    private List<Action> complexAnalyze(List<? extends Config> sourceConfigs, List<? extends Config> transformConfigs, List<? extends Config> sinkConfigs) {
        for (Config config : sourceConfigs) {
            if (!config.hasPath(Plugin.RESULT_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.RESULT_TABLE_NAME + " must be set in the source plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(Plugin.RESULT_TABLE_NAME);
            if (sourceResultTableNameMap.get(resultTableName) == null) {
                sourceResultTableNameMap.put(resultTableName, new ArrayList<>());
            }
            sourceResultTableNameMap.get(resultTableName).add(analyzeSource(config));
        }

        for (Config config : transformConfigs) {
            if (!config.hasPath(Plugin.RESULT_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.RESULT_TABLE_NAME + " must be set in the transform plugin config when the job have complex dependencies");
            }

            if (!config.hasPath(Plugin.SOURCE_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.SOURCE_TABLE_NAME + " must be set in the transform plugin config when the job have complex dependencies");
            }

            SeaTunnelTransform seaTunnelTransform = analyzeTransform(config);

            String resultTableName = config.getString(Plugin.RESULT_TABLE_NAME);
            String sourceTableName = config.getString(Plugin.SOURCE_TABLE_NAME);

            if (transformResultTableNameMap.get(resultTableName) == null) {
                transformResultTableNameMap.put(resultTableName, new ArrayList<>());
            }
            transformResultTableNameMap.get(resultTableName).add(seaTunnelTransform);

            if (transformSourceTableNameMap.get(sourceTableName) == null) {
                transformSourceTableNameMap.put(sourceTableName, new ArrayList<>());
            }
            transformSourceTableNameMap.get(sourceTableName).add(seaTunnelTransform);

        }

        for (Config config : sinkConfigs) {
            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink = analyzeSink(config);
            if (!config.hasPath(Plugin.SOURCE_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.SOURCE_TABLE_NAME + " must be set in the sink plugin config when the job have complex dependencies");
            }
            String sourceTableName = config.getString(Plugin.SOURCE_TABLE_NAME);
            List<SeaTunnelTransform> seaTunnelTransforms = transformResultTableNameMap.get(sourceTableName);
            if (seaTunnelTransforms == null) {
                List<SeaTunnelSource> seaTunnelSources = sourceResultTableNameMap.get(sourceTableName);
                if (seaTunnelSources == null) {
                    throw new JobDefineCheckExceptionSeaTunnel("Sink Connector " + seaTunnelSink.getPluginName() + " " + Plugin.SOURCE_TABLE_NAME + " can not be found in transform and source connectors");
                } else {
                    SourceAction sourceAction = new SourceAction(seaTunnelSource.getPluginName(), seaTunnelSource);
                    SinkAction sinkAction = new SinkAction(seaTunnelSink.getPluginName(), List.)
                }
            }
        }
    }

    private List<Action> sampleAnalyze(List<? extends Config> sourceConfigs, List<? extends Config> transformConfigs, List<? extends Config> sinkConfigs) {
        SeaTunnelSource seaTunnelSource = analyzeSource(sourceConfigs.get(0));
        SourceAction sourceAction = new SourceAction(seaTunnelSource.getPluginName(), seaTunnelSource);
        if (transformConfigs.size() != 0) {
            SeaTunnelTransform seaTunnelTransform = analyzeTransform(transformConfigs.get(0));
            TransformAction transformAction = new TransformAction(seaTunnelTransform.getPluginName(), Lists.newArrayList(sourceAction), seaTunnelTransform);

            SeaTunnelSink seaTunnelSink = analyzeSink(sinkConfigs.get(0));
            SinkAction sinkAction = new SinkAction(seaTunnelSink.getPluginName(), Lists.newArrayList(transformAction), seaTunnelSink);
            return Lists.newArrayList(sinkAction);
        } else {
            SeaTunnelSink seaTunnelSink = analyzeSink(sinkConfigs.get(0));
            SinkAction sinkAction = new SinkAction(seaTunnelSink.getPluginName(), Lists.newArrayList(sourceAction), seaTunnelSink);
            return Lists.newArrayList(sinkAction);
        }
    }

    private SeaTunnelSource analyzeSource(Config sourceConfig) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        PluginIdentifier pluginIdentifier = PluginIdentifier.of(
            CollectionConstants.SEATUNNEL_PLUGIN, CollectionConstants.SOURCE_PLUGIN, sourceConfig.getString(CollectionConstants.PLUGIN_NAME));
        SeaTunnelSource seaTunnelSource = sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
        seaTunnelSource.prepare(sourceConfig);
        seaTunnelSource.setSeaTunnelContext(SeaTunnelContext.getContext());
        if (SeaTunnelContext.getContext().getJobMode() == JobMode.BATCH
            && seaTunnelSource.getBoundedness() == org.apache.seatunnel.api.source.Boundedness.UNBOUNDED) {
            throw new UnsupportedOperationException(String.format("'%s' source don't support off-line job.", seaTunnelSource.getPluginName()));
        }
        return seaTunnelSource;
    }

    private SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> analyzeSink(Config sinkConfig) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        PluginIdentifier pluginIdentifier = PluginIdentifier.of(
            CollectionConstants.SEATUNNEL_PLUGIN, CollectionConstants.SINK_PLUGIN, sinkConfig.getString(CollectionConstants.PLUGIN_NAME));
        SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink =
            sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
        seaTunnelSink.prepare(sinkConfig);
        seaTunnelSink.setSeaTunnelContext(SeaTunnelContext.getContext());
        return seaTunnelSink;
    }

    private SeaTunnelTransform analyzeTransform(Config transformConfig) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery = new SeaTunnelTransformPluginDiscovery();
        PluginIdentifier pluginIdentifier = PluginIdentifier.of(
            CollectionConstants.SEATUNNEL_PLUGIN, CollectionConstants.TRANSFORM_PLUGIN, transformConfig.getString(CollectionConstants.PLUGIN_NAME));
        SeaTunnelTransform seaTunnelTransform = transformPluginDiscovery.createPluginInstance(pluginIdentifier);
        return seaTunnelTransform;
    }
}
