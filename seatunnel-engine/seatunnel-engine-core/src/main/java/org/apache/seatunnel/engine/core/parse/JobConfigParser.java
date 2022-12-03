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

package org.apache.seatunnel.engine.core.parse;

import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceCommonOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.PartitionSeaTunnelTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.api.transform.TransformCommonOptions;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.config.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    private List<Action> actions = new ArrayList<>();
    private Set<URL> jarUrlsSet = new HashSet<>();

    private JobConfig jobConfig;

    private Config seaTunnelJobConfig;

    private Config envConfigs;

    private List<URL> commonPluginJars;

    public JobConfigParser(@NonNull String jobDefineFilePath,
                           @NonNull IdGenerator idGenerator,
                           @NonNull JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList());
    }

    public JobConfigParser(@NonNull String jobDefineFilePath,
                           @NonNull IdGenerator idGenerator,
                           @NonNull JobConfig jobConfig,
                           @NonNull List<URL> commonPluginJars) {
        this.jobDefineFilePath = jobDefineFilePath;
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.seaTunnelJobConfig = new ConfigBuilder(Paths.get(jobDefineFilePath)).getConfig();
        this.envConfigs = seaTunnelJobConfig.getConfig("env");
        this.commonPluginJars = commonPluginJars;
    }

    public ImmutablePair<List<Action>, Set<URL>> parse() {
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        List<? extends Config> transformConfigs =
            TypesafeConfigUtils.getConfigList(seaTunnelJobConfig, "transform", Collections.emptyList());
        List<? extends Config> sourceConfigs = seaTunnelJobConfig.getConfigList("source");

        if (CollectionUtils.isEmpty(sinkConfigs) || CollectionUtils.isEmpty(sourceConfigs)) {
            throw new JobDefineCheckException("Source And Sink can not be null");
        }

        jobConfigAnalyze(envConfigs);

        if (sinkConfigs.size() == 1
            && sourceConfigs.size() == 1
            && (CollectionUtils.isEmpty(transformConfigs) || transformConfigs.size() == 1)) {
            sampleAnalyze(sourceConfigs, transformConfigs, sinkConfigs);
        } else {
            complexAnalyze(sourceConfigs, transformConfigs, sinkConfigs);
        }
        actions.forEach(this::addCommonPluginJarsToAction);
        jarUrlsSet.addAll(commonPluginJars);
        return new ImmutablePair<>(actions, jarUrlsSet);
    }

    private void addCommonPluginJarsToAction(Action action) {
        action.getJarUrls().addAll(commonPluginJars);
        if (!action.getUpstream().isEmpty()) {
            action.getUpstream().forEach(this::addCommonPluginJarsToAction);
        }
    }

    private void jobConfigAnalyze(@NonNull Config envConfigs) {
        if (envConfigs.hasPath(EnvCommonOptions.JOB_MODE.key())) {
            jobConfig.getJobContext().setJobMode(envConfigs.getEnum(JobMode.class, EnvCommonOptions.JOB_MODE.key()));
        } else {
            jobConfig.getJobContext().setJobMode(EnvCommonOptions.JOB_MODE.defaultValue());
        }

        if (StringUtils.isEmpty(jobConfig.getName())) {
            if (envConfigs.hasPath(EnvCommonOptions.JOB_NAME.key())) {
                jobConfig.setName(envConfigs.getString(EnvCommonOptions.JOB_NAME.key()));
            } else {
                jobConfig.setName(EnvCommonOptions.JOB_NAME.defaultValue());
            }
        }

        if (envConfigs.hasPath(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            jobConfig.getEnvOptions()
                .put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(),
                    envConfigs.getInt(EnvCommonOptions.CHECKPOINT_INTERVAL.key()));
        }
    }

    /**
     * If there are multiple sources or multiple transforms or multiple sink, We will rely on
     * source_table_name and result_table_name to build actions pipeline.
     * So in this case result_table_name is necessary for the Source Connector and all of
     * result_table_name and source_table_name are necessary for Transform Connector.
     * By the end, source_table_name is necessary for Sink Connector.
     */
    private void complexAnalyze(List<? extends Config> sourceConfigs,
                                List<? extends Config> transformConfigs,
                                List<? extends Config> sinkConfigs) {
        initRelationMap(sourceConfigs, transformConfigs);

        for (Config config : sinkConfigs) {
            ImmutablePair<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>, Set<URL>>
                sinkListImmutablePair =
                ConnectorInstanceLoader.loadSinkInstance(config, jobConfig.getJobContext(), commonPluginJars);

            SinkAction sinkAction =
                createSinkAction(idGenerator.getNextId(), sinkListImmutablePair.getLeft().getPluginName(),
                    sinkListImmutablePair.getLeft(), sinkListImmutablePair.getRight());

            actions.add(sinkAction);
            if (!config.hasPath(SinkCommonOptions.SOURCE_TABLE_NAME.key())) {
                throw new JobDefineCheckException(SinkCommonOptions.SOURCE_TABLE_NAME
                    + " must be set in the sink plugin config when the job have complex dependencies");
            }
            String sourceTableName = config.getString(SinkCommonOptions.SOURCE_TABLE_NAME.key());
            List<Config> transformConfigList = transformResultTableNameMap.get(sourceTableName);
            SeaTunnelDataType<?> dataType;
            if (CollectionUtils.isEmpty(transformConfigList)) {
                dataType = sourceAnalyze(sourceTableName, sinkAction);
            } else if (transformConfigList.size() > 1) {
                throw new JobDefineCheckException("Only UnionTransform can have more than one upstream, "
                    + sinkAction.getName()
                    + " is not UnionTransform Connector");
            } else {
                dataType = transformAnalyze(sourceTableName, sinkAction);
            }
            sinkListImmutablePair.getLeft().setTypeInfo((SeaTunnelRowType) dataType);
        }
    }

    private SeaTunnelDataType sourceAnalyze(String sourceTableName, Action action) {
        List<Config> sourceConfigList = sourceResultTableNameMap.get(sourceTableName);
        if (CollectionUtils.isEmpty(sourceConfigList)) {
            throw new JobDefineCheckException(action.getName()
                + " source table name [" + sourceTableName + "] can not be found");
        }

        // If a transform have more than one upstream action, the parallelism of this transform is the sum of the parallelism
        // of its upstream action.
        SeaTunnelDataType dataType = null;
        AtomicInteger totalParallelism = new AtomicInteger();
        for (Config sourceConfig : sourceConfigList) {
            ImmutablePair<SeaTunnelSource, Set<URL>> seaTunnelSourceListImmutablePair =
                ConnectorInstanceLoader.loadSourceInstance(sourceConfig, jobConfig.getJobContext(), commonPluginJars);
            dataType = seaTunnelSourceListImmutablePair.getLeft().getProducedType();
            SourceAction sourceAction = createSourceAction(
                idGenerator.getNextId(),
                sourceConfig.getString(CollectionConstants.PLUGIN_NAME),
                seaTunnelSourceListImmutablePair.getLeft(),
                seaTunnelSourceListImmutablePair.getRight());

            int sourceParallelism = getSourceParallelism(sourceConfig);
            sourceAction.setParallelism(sourceParallelism);
            totalParallelism.set(totalParallelism.get() + sourceParallelism);
            action.addUpstream(sourceAction);
            action.setParallelism(totalParallelism.get());
        }
        return dataType;
    }

    private SeaTunnelDataType<?> transformAnalyze(String sourceTableName, Action action) {
        // find upstream transform node
        List<Config> transformConfigList = transformResultTableNameMap.get(sourceTableName);
        if (CollectionUtils.isEmpty(transformConfigList)) {
            return sourceAnalyze(sourceTableName, action);
        } else {
            AtomicInteger totalParallelism = new AtomicInteger();
            SeaTunnelDataType<?> dataTypeResult = null;
            for (Config config : transformConfigList) {
                ImmutablePair<SeaTunnelTransform<?>, Set<URL>> transformListImmutablePair =
                    ConnectorInstanceLoader.loadTransformInstance(config, jobConfig.getJobContext(), commonPluginJars);
                TransformAction transformAction = createTransformAction(
                    idGenerator.getNextId(),
                    transformListImmutablePair.getLeft().getPluginName(),
                    transformListImmutablePair.getLeft(),
                    transformListImmutablePair.getRight());

                action.addUpstream(transformAction);
                SeaTunnelDataType dataType =
                    transformAnalyze(config.getString(SinkCommonOptions.SOURCE_TABLE_NAME.key()),
                        transformAction);
                transformListImmutablePair.getLeft().setTypeInfo(dataType);
                dataTypeResult = transformListImmutablePair.getLeft().getProducedType();
                totalParallelism.set(totalParallelism.get() + transformAction.getParallelism());
                action.setParallelism(totalParallelism.get());
            }
            return dataTypeResult;
        }
    }

    private void initRelationMap(List<? extends Config> sourceConfigs, List<? extends Config> transformConfigs) {
        for (Config config : sourceConfigs) {
            if (!config.hasPath(SourceCommonOptions.RESULT_TABLE_NAME.key())) {
                throw new JobDefineCheckException(SourceCommonOptions.RESULT_TABLE_NAME.key()
                    + " must be set in the source plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(SourceCommonOptions.RESULT_TABLE_NAME.key());
            sourceResultTableNameMap.computeIfAbsent(resultTableName, k -> new ArrayList<>());
            sourceResultTableNameMap.get(resultTableName).add(config);
        }

        for (Config config : transformConfigs) {
            if (!config.hasPath(SourceCommonOptions.RESULT_TABLE_NAME.key())) {
                throw new JobDefineCheckException(SourceCommonOptions.RESULT_TABLE_NAME.key()
                    + " must be set in the transform plugin config when the job have complex dependencies");
            }

            if (!config.hasPath(SinkCommonOptions.SOURCE_TABLE_NAME.key())) {
                throw new JobDefineCheckException(SinkCommonOptions.SOURCE_TABLE_NAME.key()
                    + " must be set in the transform plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(SourceCommonOptions.RESULT_TABLE_NAME.key());
            String sourceTableName = config.getString(SinkCommonOptions.SOURCE_TABLE_NAME.key());
            if (Objects.equals(sourceTableName, resultTableName)) {
                throw new JobDefineCheckException(String.format(
                    "Source{%s} and result{%s} table name cannot be equals", sourceTableName, resultTableName));
            }

            transformResultTableNameMap.computeIfAbsent(resultTableName, k -> new ArrayList<>());
            transformResultTableNameMap.get(resultTableName).add(config);

            transformSourceTableNameMap.computeIfAbsent(sourceTableName, k -> new ArrayList<>());
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
    private void sampleAnalyze(List<? extends Config> sourceConfigs,
                               List<? extends Config> transformConfigs,
                               List<? extends Config> sinkConfigs) {
        ImmutablePair<SeaTunnelSource, Set<URL>> pair =
            ConnectorInstanceLoader.loadSourceInstance(sourceConfigs.get(0), jobConfig.getJobContext(),
                commonPluginJars);
        SourceAction sourceAction =
            createSourceAction(idGenerator.getNextId(), pair.getLeft().getPluginName(), pair.getLeft(),
                pair.getRight());
        sourceAction.setParallelism(getSourceParallelism(sourceConfigs.get(0)));
        SeaTunnelDataType dataType = sourceAction.getSource().getProducedType();

        Action sinkUpstreamAction = sourceAction;

        if (!CollectionUtils.isEmpty(transformConfigs)) {
            ImmutablePair<SeaTunnelTransform<?>, Set<URL>> transformListImmutablePair =
                ConnectorInstanceLoader.loadTransformInstance(transformConfigs.get(0), jobConfig.getJobContext(),
                    commonPluginJars);
            transformListImmutablePair.getLeft().setTypeInfo(dataType);

            dataType = transformListImmutablePair.getLeft().getProducedType();
            TransformAction transformAction = createTransformAction(
                idGenerator.getNextId(),
                transformListImmutablePair.getLeft().getPluginName(),
                Lists.newArrayList(sourceAction),
                transformListImmutablePair.getLeft(),
                transformListImmutablePair.getRight());

            initTransformParallelism(transformConfigs, sourceAction, transformListImmutablePair.getLeft(),
                transformAction);

            sinkUpstreamAction = transformAction;
        }

        ImmutablePair<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>, Set<URL>>
            sinkListImmutablePair =
            ConnectorInstanceLoader.loadSinkInstance(sinkConfigs.get(0), jobConfig.getJobContext(), commonPluginJars);
        SinkAction sinkAction = createSinkAction(
            idGenerator.getNextId(),
            sinkListImmutablePair.getLeft().getPluginName(),
            Lists.newArrayList(sinkUpstreamAction),
            sinkListImmutablePair.getLeft(),
            sinkListImmutablePair.getRight()
        );
        sinkAction.getSink().setTypeInfo((SeaTunnelRowType) dataType);
        sinkAction.setParallelism(sinkUpstreamAction.getParallelism());
        actions.add(sinkAction);
    }

    private void initTransformParallelism(List<? extends Config> transformConfigs, Action upstreamAction,
                                          SeaTunnelTransform seaTunnelTransform, TransformAction transformAction) {
        if (seaTunnelTransform instanceof PartitionSeaTunnelTransform
            && transformConfigs.get(0).hasPath(TransformCommonOptions.PARALLELISM.key())) {
            transformAction.setParallelism(transformConfigs
                .get(0)
                .getInt(TransformCommonOptions.PARALLELISM.key()));
        } else {
            // If transform type is not RePartitionTransform, Using the parallelism of its upstream operators.
            transformAction.setParallelism(upstreamAction.getParallelism());
        }
    }

    private int getSourceParallelism(Config sourceConfig) {
        if (sourceConfig.hasPath(SourceCommonOptions.PARALLELISM.key())) {
            int sourceParallelism = sourceConfig.getInt(SourceCommonOptions.PARALLELISM.key());
            return Math.max(sourceParallelism, 1);
        }
        int executionParallelism = 0;
        if (envConfigs.hasPath(EnvCommonOptions.PARALLELISM.key())) {
            executionParallelism = envConfigs.getInt(EnvCommonOptions.PARALLELISM.key());
        }
        return Math.max(executionParallelism, 1);
    }

    private SourceAction createSourceAction(long id,
                                            @NonNull String name,
                                            @NonNull SeaTunnelSource source,
                                            Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new SourceAction(id, name, source, jarUrls);
    }

    private TransformAction createTransformAction(long id,
                                                  @NonNull String name,
                                                  @NonNull List<Action> upstreams,
                                                  @NonNull SeaTunnelTransform transformation,
                                                  Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new TransformAction(id, name, upstreams, transformation, jarUrls);
    }

    private SinkAction createSinkAction(long id,
                                        @NonNull String name,
                                        @NonNull List<Action> upstreams,
                                        @NonNull SeaTunnelSink sink,
                                        Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new SinkAction(id, name, upstreams, sink, jarUrls);
    }

    private TransformAction createTransformAction(long id,
                                                  @NonNull String name,
                                                  @NonNull SeaTunnelTransform transformation,
                                                  Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new TransformAction(id, name, transformation, jarUrls);
    }

    private SinkAction createSinkAction(long id,
                                        @NonNull String name,
                                        @NonNull SeaTunnelSink sink,
                                        Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new SinkAction(id, name, sink, jarUrls);
    }
}
