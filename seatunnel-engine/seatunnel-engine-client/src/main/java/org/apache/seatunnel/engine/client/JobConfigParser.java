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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.PartitionSeaTunnelTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.apis.base.plugin.Plugin;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckExceptionSeaTunnel;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.Data;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import scala.Serializable;

@Data
public class JobConfigParser {
    private static final ILogger LOGGER = Logger.getLogger(JobConfigParser.class);
    private String jobDefineFilePath;
    private IdGenerator idGenerator;

    private Map<Action, String> alreadyTransformActionMap = new HashMap<>();

    private Map<String, List<Config>> transformResultTableNameMap = new HashMap<>();
    private Map<String, List<Config>> transformSourceTableNameMap = new HashMap<>();

    private Map<String, List<Config>> sourceResultTableNameMap = new HashMap<>();

    protected JobConfigParser(@NonNull String jobDefineFilePath, @NonNull IdGenerator idGenerator) {
        this.jobDefineFilePath = jobDefineFilePath;
        this.idGenerator = idGenerator;
    }

    public List<Action> parse() {
        Config seaTunnelJobConfig = new ConfigBuilder(Paths.get(jobDefineFilePath)).getConfig();
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        List<? extends Config> transformConfigs = seaTunnelJobConfig.getConfigList("transform");
        List<? extends Config> sourceConfigs = seaTunnelJobConfig.getConfigList("source");

        if (sinkConfigs.size() == 1 && sourceConfigs.size() == 1 & transformConfigs.size() <= 1) {
            return sampleAnalyze(sourceConfigs, transformConfigs, sinkConfigs);
        } else {
            return complexAnalyze(sourceConfigs, transformConfigs, sinkConfigs);
        }
    }

    /**
     * If there are multiple sources or multiple transforms or multiple sink, We will rely on
     * source_table_name and result_table_name to build actions pipeline.
     * So in this case result_table_name is necessary for the Source Connector and all of
     * result_table_name and source_table_name are necessary for Transform Connector.
     * By the end, source_table_name is necessary for Sink Connector.
     */
    private List<Action> complexAnalyze(List<? extends Config> sourceConfigs,
                                        List<? extends Config> transformConfigs,
                                        List<? extends Config> sinkConfigs) {
        initRelationMap(sourceConfigs, transformConfigs);

        List<Action> actions = new ArrayList<>();
        for (Config config : sinkConfigs) {
            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink =
                ConnectorInstanceLoader.loadSinkInstance(config);

            SinkAction sinkAction =
                new SinkAction(idGenerator.getNextId(), seaTunnelSink.getPluginName(), seaTunnelSink);
            actions.add(sinkAction);
            if (!config.hasPath(Plugin.SOURCE_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.SOURCE_TABLE_NAME
                    + " must be set in the sink plugin config when the job have complex dependencies");
            }
            String sourceTableName = config.getString(Plugin.SOURCE_TABLE_NAME);
            List<Config> transformConfigList = transformResultTableNameMap.get(sourceTableName);
            if (CollectionUtils.isEmpty(transformConfigList)) {
                sourceAnalyze(sourceTableName, sinkAction);
            } else if (transformConfigList.size() > 1) {
                throw new JobDefineCheckExceptionSeaTunnel("Only UnionTransform can have more than one upstream, "
                    + sinkAction.getName()
                    + " is not UnionTransform Connector");
            } else {
                transformAnalyze(sourceTableName, sinkAction);
            }

        }
        return actions;
    }

    private void sourceAnalyze(String sourceTableName, Action action) {
        List<Config> sourceConfigList = sourceResultTableNameMap.get(sourceTableName);
        if (CollectionUtils.isEmpty(sourceConfigList)) {
            throw new JobDefineCheckExceptionSeaTunnel(action.getName()
                + " source table name [" + sourceTableName + "] can not be found");
        }

        // If a transform have more than one upstream action, the parallelism of this transform is the sum of the parallelism
        // of its upstream action.
        AtomicInteger totalParallelism = new AtomicInteger();
        sourceConfigList.stream().forEach(sourceConfig -> {
            SourceAction sourceAction =
                new SourceAction(idGenerator.getNextId(),
                    sourceConfig.getString(CollectionConstants.PLUGIN_NAME),
                    ConnectorInstanceLoader.loadSourceInstance(sourceConfig));
            int sourceParallelism = getSourceParallelism(sourceConfig);
            sourceAction.setParallelism(sourceParallelism);
            totalParallelism.set(totalParallelism.get() + sourceParallelism);
            action.addUpstream(sourceAction);
            action.setParallelism(totalParallelism.get());
        });
    }

    private void transformAnalyze(String sourceTableName, Action action) {
        // find upstream transform node
        List<Config> transformConfigList = transformResultTableNameMap.get(sourceTableName);
        if (CollectionUtils.isEmpty(transformConfigList)) {
            sourceAnalyze(sourceTableName, action);
        } else {
            AtomicInteger totalParallelism = new AtomicInteger();
            transformConfigList.stream().forEach(config -> {
                SeaTunnelTransform seaTunnelTransform = ConnectorInstanceLoader.loadTransformInstance(config);
                TransformAction transformAction =
                    new TransformAction(idGenerator.getNextId(), seaTunnelTransform.getPluginName(),
                        seaTunnelTransform);
                action.addUpstream(transformAction);
                transformAnalyze(config.getString(Plugin.SOURCE_TABLE_NAME), transformAction);
                totalParallelism.set(totalParallelism.get() + transformAction.getParallelism());
                action.setParallelism(totalParallelism.get());
            });
        }
    }

    private void initRelationMap(List<? extends Config> sourceConfigs, List<? extends Config> transformConfigs) {
        for (Config config : sourceConfigs) {
            if (!config.hasPath(Plugin.RESULT_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.RESULT_TABLE_NAME
                    + " must be set in the source plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(Plugin.RESULT_TABLE_NAME);
            if (sourceResultTableNameMap.get(resultTableName) == null) {
                sourceResultTableNameMap.put(resultTableName, new ArrayList<>());
            }
            sourceResultTableNameMap.get(resultTableName).add(config);
        }

        for (Config config : transformConfigs) {
            if (!config.hasPath(Plugin.RESULT_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.RESULT_TABLE_NAME
                    + " must be set in the transform plugin config when the job have complex dependencies");
            }

            if (!config.hasPath(Plugin.SOURCE_TABLE_NAME)) {
                throw new JobDefineCheckExceptionSeaTunnel(Plugin.SOURCE_TABLE_NAME
                    + " must be set in the transform plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(Plugin.RESULT_TABLE_NAME);
            String sourceTableName = config.getString(Plugin.SOURCE_TABLE_NAME);

            if (transformResultTableNameMap.get(resultTableName) == null) {
                transformResultTableNameMap.put(resultTableName, new ArrayList<>());
            }
            transformResultTableNameMap.get(resultTableName).add(config);

            if (transformSourceTableNameMap.get(sourceTableName) == null) {
                transformSourceTableNameMap.put(sourceTableName, new ArrayList<>());
            }
            transformSourceTableNameMap.get(sourceTableName).add(config);

        }
    }

    /**
     * If there is only one Source and one Sink and at most one Transform, We simply build actions pipeline in the following order
     * Source
     * |
     * Transform(If have)
     * |
     * Sink
     */
    private List<Action> sampleAnalyze(List<? extends Config> sourceConfigs,
                                       List<? extends Config> transformConfigs,
                                       List<? extends Config> sinkConfigs) {
        SeaTunnelSource seaTunnelSource = ConnectorInstanceLoader.loadSourceInstance(sourceConfigs.get(0));
        SourceAction sourceAction =
            new SourceAction(idGenerator.getNextId(), seaTunnelSource.getPluginName(), seaTunnelSource);
        sourceAction.setParallelism(getSourceParallelism(sourceConfigs.get(0)));
        if (transformConfigs.size() != 0) {
            SeaTunnelTransform seaTunnelTransform =
                ConnectorInstanceLoader.loadTransformInstance(transformConfigs.get(0));
            TransformAction transformAction = new TransformAction(
                idGenerator.getNextId(),
                seaTunnelTransform.getPluginName(),
                Lists.newArrayList(sourceAction),
                seaTunnelTransform);

            initTransformParallelism(transformConfigs, sourceAction, seaTunnelTransform, transformAction);

            SeaTunnelSink seaTunnelSink = ConnectorInstanceLoader.loadSinkInstance(sinkConfigs.get(0));
            SinkAction sinkAction = new SinkAction(
                idGenerator.getNextId(),
                seaTunnelSink.getPluginName(),
                Lists.newArrayList(transformAction),
                seaTunnelSink
            );
            sinkAction.setParallelism(transformAction.getParallelism());
            return Lists.newArrayList(sinkAction);
        } else {
            SeaTunnelSink seaTunnelSink = ConnectorInstanceLoader.loadSinkInstance(sinkConfigs.get(0));
            SinkAction sinkAction = new SinkAction(
                idGenerator.getNextId(),
                seaTunnelSink.getPluginName(),
                Lists.newArrayList(sourceAction),
                seaTunnelSink
            );
            sinkAction.setParallelism(sourceAction.getParallelism());
            return Lists.newArrayList(sinkAction);
        }
    }

    private void initTransformParallelism(List<? extends Config> transformConfigs, Action upstreamAction,
                                          SeaTunnelTransform seaTunnelTransform, TransformAction transformAction) {
        if ((seaTunnelTransform instanceof PartitionSeaTunnelTransform)
            && transformConfigs.get(0).hasPath(CollectionConstants.PARALLELISM)) {
            transformAction.setParallelism(transformConfigs
                .get(0)
                .getInt(CollectionConstants.PARALLELISM));
        } else {
            // If transform type is not RePartitionTransform, Using the parallelism of its upstream operators.
            transformAction.setParallelism(upstreamAction.getParallelism());
        }
    }

    private int getSourceParallelism(Config sourceConfig) {
        if (sourceConfig.hasPath(CollectionConstants.PARALLELISM)) {
            int sourceParallelism = sourceConfig.getInt(CollectionConstants.PARALLELISM);
            return sourceParallelism < 1 ? 1 : sourceParallelism;
        }
        return 1;
    }
}
