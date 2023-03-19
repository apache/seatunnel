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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.PartitionSeaTunnelTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.common.collect.Lists;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.Data;
import lombok.NonNull;
import scala.Serializable;

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

    public JobConfigParser(
            @NonNull String jobDefineFilePath,
            @NonNull IdGenerator idGenerator,
            @NonNull JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList());
    }

    public JobConfigParser(
            @NonNull String jobDefineFilePath,
            @NonNull IdGenerator idGenerator,
            @NonNull JobConfig jobConfig,
            @NonNull List<URL> commonPluginJars) {
        this.jobDefineFilePath = jobDefineFilePath;
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.seaTunnelJobConfig = ConfigBuilder.of(Paths.get(jobDefineFilePath));
        this.envConfigs = seaTunnelJobConfig.getConfig("env");
        this.commonPluginJars = commonPluginJars;
    }

    public ImmutablePair<List<Action>, Set<URL>> parse() {
        Thread.currentThread()
                .setContextClassLoader(new SeaTunnelChildFirstClassLoader(new ArrayList<>()));
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        List<? extends Config> transformConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "transform", Collections.emptyList());
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

    void jobConfigAnalyze(@NonNull Config envConfigs) {
        if (envConfigs.hasPath(EnvCommonOptions.JOB_MODE.key())) {
            jobConfig
                    .getJobContext()
                    .setJobMode(envConfigs.getEnum(JobMode.class, EnvCommonOptions.JOB_MODE.key()));
        } else {
            jobConfig.getJobContext().setJobMode(EnvCommonOptions.JOB_MODE.defaultValue());
        }

        if (StringUtils.isEmpty(jobConfig.getName())
                || jobConfig.getName().equals(Constants.LOGO)) {
            if (envConfigs.hasPath(EnvCommonOptions.JOB_NAME.key())) {
                jobConfig.setName(envConfigs.getString(EnvCommonOptions.JOB_NAME.key()));
            } else {
                jobConfig.setName(EnvCommonOptions.JOB_NAME.defaultValue());
            }
        }

        if (envConfigs.hasPath(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            jobConfig
                    .getEnvOptions()
                    .put(
                            EnvCommonOptions.CHECKPOINT_INTERVAL.key(),
                            envConfigs.getLong(EnvCommonOptions.CHECKPOINT_INTERVAL.key()));
        }
    }

    /**
     * If there are multiple sources or multiple transforms or multiple sink, We will rely on
     * source_table_name and result_table_name to build actions pipeline. So in this case
     * result_table_name is necessary for the Source Connector and all of result_table_name and
     * source_table_name are necessary for Transform Connector. By the end, source_table_name is
     * necessary for Sink Connector.
     */
    private void complexAnalyze(
            List<? extends Config> sourceConfigs,
            List<? extends Config> transformConfigs,
            List<? extends Config> sinkConfigs) {
        initRelationMap(sourceConfigs, transformConfigs);

        for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
            Config config = sinkConfigs.get(configIndex);
            ImmutablePair<
                            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>,
                            Set<URL>>
                    sinkListImmutablePair =
                            ConnectorInstanceLoader.loadSinkInstance(
                                    config, jobConfig.getJobContext(), commonPluginJars);

            String sinkActionName =
                    createSinkActionName(
                            configIndex,
                            sinkListImmutablePair.getLeft().getPluginName(),
                            getTableName(config));
            SinkAction sinkAction =
                    createSinkAction(
                            idGenerator.getNextId(),
                            sinkActionName,
                            sinkListImmutablePair.getLeft(),
                            sinkListImmutablePair.getRight());

            actions.add(sinkAction);
            if (!config.hasPath(CommonOptions.SOURCE_TABLE_NAME.key())) {
                throw new JobDefineCheckException(
                        CommonOptions.SOURCE_TABLE_NAME
                                + " must be set in the sink plugin config when the job have complex dependencies");
            }
            String sourceTableName = config.getString(CommonOptions.SOURCE_TABLE_NAME.key());
            List<Config> transformConfigList = transformResultTableNameMap.get(sourceTableName);
            SeaTunnelDataType<?> dataType;
            if (CollectionUtils.isEmpty(transformConfigList)) {
                dataType = sourceAnalyze(sourceTableName, sinkAction);
            } else if (transformConfigList.size() > 1) {
                throw new JobDefineCheckException(
                        "Only UnionTransform can have more than one upstream, "
                                + sinkAction.getName()
                                + " is not UnionTransform Connector");
            } else {
                dataType = transformAnalyze(sourceTableName, sinkAction);
            }
            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink =
                    sinkListImmutablePair.getLeft();
            seaTunnelSink.setTypeInfo((SeaTunnelRowType) dataType);
            if (SupportDataSaveMode.class.isAssignableFrom(seaTunnelSink.getClass())) {
                SupportDataSaveMode saveModeSink = (SupportDataSaveMode) seaTunnelSink;
                DataSaveMode dataSaveMode = saveModeSink.getDataSaveMode();
                saveModeSink.handleSaveMode(dataSaveMode);
            }
        }
    }

    private SeaTunnelDataType sourceAnalyze(String sourceTableName, Action action) {
        List<Config> sourceConfigList = sourceResultTableNameMap.get(sourceTableName);
        if (CollectionUtils.isEmpty(sourceConfigList)) {
            throw new JobDefineCheckException(
                    action.getName()
                            + " source table name ["
                            + sourceTableName
                            + "] can not be found");
        }

        // If a transform have more than one upstream action, the parallelism of this transform is
        // the sum of the parallelism
        // of its upstream action.
        SeaTunnelDataType dataType = null;
        AtomicInteger totalParallelism = new AtomicInteger();
        for (int configIndex = 0; configIndex < sourceConfigList.size(); configIndex++) {
            Config sourceConfig = sourceConfigList.get(configIndex);
            ImmutablePair<SeaTunnelSource, Set<URL>> seaTunnelSourceListImmutablePair =
                    ConnectorInstanceLoader.loadSourceInstance(
                            sourceConfig, jobConfig.getJobContext(), commonPluginJars);
            dataType = seaTunnelSourceListImmutablePair.getLeft().getProducedType();
            String sourceActionName =
                    createSourceActionName(
                            configIndex,
                            sourceConfig.getString(CollectionConstants.PLUGIN_NAME),
                            getTableName(sourceConfig));
            SourceAction sourceAction =
                    createSourceAction(
                            idGenerator.getNextId(),
                            sourceActionName,
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
            for (int configIndex = 0; configIndex < transformConfigList.size(); configIndex++) {
                Config config = transformConfigList.get(configIndex);
                ImmutablePair<SeaTunnelTransform<?>, Set<URL>> transformListImmutablePair =
                        ConnectorInstanceLoader.loadTransformInstance(
                                config, jobConfig.getJobContext(), commonPluginJars);
                String transformActionName =
                        createTransformActionName(
                                configIndex,
                                transformListImmutablePair.getLeft().getPluginName(),
                                getTableName(config));
                TransformAction transformAction =
                        createTransformAction(
                                idGenerator.getNextId(),
                                transformActionName,
                                transformListImmutablePair.getLeft(),
                                transformListImmutablePair.getRight());

                action.addUpstream(transformAction);
                SeaTunnelDataType dataType =
                        transformAnalyze(
                                config.getString(CommonOptions.SOURCE_TABLE_NAME.key()),
                                transformAction);
                transformListImmutablePair.getLeft().setTypeInfo(dataType);
                dataTypeResult = transformListImmutablePair.getLeft().getProducedType();
                totalParallelism.set(totalParallelism.get() + transformAction.getParallelism());
                action.setParallelism(totalParallelism.get());
            }
            return dataTypeResult;
        }
    }

    private void initRelationMap(
            List<? extends Config> sourceConfigs, List<? extends Config> transformConfigs) {
        for (Config config : sourceConfigs) {
            if (!config.hasPath(CommonOptions.RESULT_TABLE_NAME.key())) {
                throw new JobDefineCheckException(
                        CommonOptions.RESULT_TABLE_NAME.key()
                                + " must be set in the source plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(CommonOptions.RESULT_TABLE_NAME.key());
            sourceResultTableNameMap.computeIfAbsent(resultTableName, k -> new ArrayList<>());
            sourceResultTableNameMap.get(resultTableName).add(config);
        }

        for (Config config : transformConfigs) {
            if (!config.hasPath(CommonOptions.RESULT_TABLE_NAME.key())) {
                throw new JobDefineCheckException(
                        CommonOptions.RESULT_TABLE_NAME.key()
                                + " must be set in the transform plugin config when the job have complex dependencies");
            }

            if (!config.hasPath(CommonOptions.SOURCE_TABLE_NAME.key())) {
                throw new JobDefineCheckException(
                        CommonOptions.SOURCE_TABLE_NAME.key()
                                + " must be set in the transform plugin config when the job have complex dependencies");
            }
            String resultTableName = config.getString(CommonOptions.RESULT_TABLE_NAME.key());
            String sourceTableName = config.getString(CommonOptions.SOURCE_TABLE_NAME.key());
            if (Objects.equals(sourceTableName, resultTableName)) {
                throw new JobDefineCheckException(
                        String.format(
                                "Source{%s} and result{%s} table name cannot be equals",
                                sourceTableName, resultTableName));
            }

            transformResultTableNameMap.computeIfAbsent(resultTableName, k -> new ArrayList<>());
            transformResultTableNameMap.get(resultTableName).add(config);

            transformSourceTableNameMap.computeIfAbsent(sourceTableName, k -> new ArrayList<>());
            transformSourceTableNameMap.get(sourceTableName).add(config);
        }
    }

    /**
     * If there is only one Source and one Sink and at most one Transform, We simply build actions
     * pipeline in the following order Source | Transform(If have) | Sink
     */
    private void sampleAnalyze(
            List<? extends Config> sourceConfigs,
            List<? extends Config> transformConfigs,
            List<? extends Config> sinkConfigs) {
        ImmutablePair<SeaTunnelSource, Set<URL>> pair =
                ConnectorInstanceLoader.loadSourceInstance(
                        sourceConfigs.get(0), jobConfig.getJobContext(), commonPluginJars);
        String sourceActionName =
                createSourceActionName(0, pair.getLeft().getPluginName(), "default");
        SourceAction sourceAction =
                createSourceAction(
                        idGenerator.getNextId(), sourceActionName, pair.getLeft(), pair.getRight());
        sourceAction.setParallelism(getSourceParallelism(sourceConfigs.get(0)));
        SeaTunnelDataType dataType = sourceAction.getSource().getProducedType();

        Action sinkUpstreamAction = sourceAction;

        if (!CollectionUtils.isEmpty(transformConfigs)) {
            ImmutablePair<SeaTunnelTransform<?>, Set<URL>> transformListImmutablePair =
                    ConnectorInstanceLoader.loadTransformInstance(
                            transformConfigs.get(0), jobConfig.getJobContext(), commonPluginJars);
            transformListImmutablePair.getLeft().setTypeInfo(dataType);

            dataType = transformListImmutablePair.getLeft().getProducedType();
            String transformActionName =
                    createTransformActionName(
                            0, transformListImmutablePair.getLeft().getPluginName(), "default");
            TransformAction transformAction =
                    createTransformAction(
                            idGenerator.getNextId(),
                            transformActionName,
                            Lists.newArrayList(sourceAction),
                            transformListImmutablePair.getLeft(),
                            transformListImmutablePair.getRight());

            initTransformParallelism(
                    transformConfigs,
                    sourceAction,
                    transformListImmutablePair.getLeft(),
                    transformAction);

            sinkUpstreamAction = transformAction;
        }

        ImmutablePair<
                        SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>,
                        Set<URL>>
                sinkListImmutablePair =
                        ConnectorInstanceLoader.loadSinkInstance(
                                sinkConfigs.get(0), jobConfig.getJobContext(), commonPluginJars);
        String sinkActionName =
                createSinkActionName(0, sinkListImmutablePair.getLeft().getPluginName(), "default");
        SinkAction sinkAction =
                createSinkAction(
                        idGenerator.getNextId(),
                        sinkActionName,
                        Lists.newArrayList(sinkUpstreamAction),
                        sinkListImmutablePair.getLeft(),
                        sinkListImmutablePair.getRight());
        SeaTunnelSink<?, ?, ?, ?> seaTunnelSink = sinkAction.getSink();
        seaTunnelSink.setTypeInfo((SeaTunnelRowType) dataType);
        sinkAction.setParallelism(sinkUpstreamAction.getParallelism());
        if (SupportDataSaveMode.class.isAssignableFrom(seaTunnelSink.getClass())) {
            SupportDataSaveMode saveModeSink = (SupportDataSaveMode) seaTunnelSink;
            DataSaveMode dataSaveMode = saveModeSink.getDataSaveMode();
            saveModeSink.handleSaveMode(dataSaveMode);
        }
        actions.add(sinkAction);
    }

    private void initTransformParallelism(
            List<? extends Config> transformConfigs,
            Action upstreamAction,
            SeaTunnelTransform seaTunnelTransform,
            TransformAction transformAction) {
        if (seaTunnelTransform instanceof PartitionSeaTunnelTransform
                && transformConfigs.get(0).hasPath(CommonOptions.PARALLELISM.key())) {
            transformAction.setParallelism(
                    transformConfigs.get(0).getInt(CommonOptions.PARALLELISM.key()));
        } else {
            // If transform type is not RePartitionTransform, Using the parallelism of its upstream
            // operators.
            transformAction.setParallelism(upstreamAction.getParallelism());
        }
    }

    private int getSourceParallelism(Config sourceConfig) {
        if (sourceConfig.hasPath(CommonOptions.PARALLELISM.key())) {
            int sourceParallelism = sourceConfig.getInt(CommonOptions.PARALLELISM.key());
            return Math.max(sourceParallelism, 1);
        }
        int executionParallelism = 0;
        if (envConfigs.hasPath(CommonOptions.PARALLELISM.key())) {
            executionParallelism = envConfigs.getInt(CommonOptions.PARALLELISM.key());
        }
        return Math.max(executionParallelism, 1);
    }

    private SourceAction createSourceAction(
            long id, @NonNull String name, @NonNull SeaTunnelSource source, Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new SourceAction(id, name, source, jarUrls);
    }

    private TransformAction createTransformAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull SeaTunnelTransform transformation,
            Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new TransformAction(id, name, upstreams, transformation, jarUrls);
    }

    private SinkAction createSinkAction(
            long id,
            @NonNull String name,
            @NonNull List<Action> upstreams,
            @NonNull SeaTunnelSink sink,
            Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new SinkAction(id, name, upstreams, sink, jarUrls);
    }

    private TransformAction createTransformAction(
            long id,
            @NonNull String name,
            @NonNull SeaTunnelTransform transformation,
            Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new TransformAction(id, name, transformation, jarUrls);
    }

    private SinkAction createSinkAction(
            long id, @NonNull String name, @NonNull SeaTunnelSink sink, Set<URL> jarUrls) {
        if (!CollectionUtils.isEmpty(jarUrls)) {
            jarUrlsSet.addAll(jarUrls);
        }
        return new SinkAction(id, name, sink, jarUrls);
    }

    static String createSourceActionName(int configIndex, String pluginName, String tableName) {
        return String.format("Source[%s]-%s-%s", configIndex, pluginName, tableName);
    }

    static String createSinkActionName(int configIndex, String pluginName, String tableName) {
        return String.format("Sink[%s]-%s-%s", configIndex, pluginName, tableName);
    }

    static String createTransformActionName(int configIndex, String pluginName, String tableName) {
        return String.format("Transform[%s]-%s-%s", configIndex, pluginName, tableName);
    }

    static String getTableName(Config config) {
        return getTableName(config, "default");
    }

    static String getTableName(Config config, String defaultValue) {
        String sourceTableName = null;
        if (config.hasPath(CommonOptions.SOURCE_TABLE_NAME.key())) {
            sourceTableName = config.getString(CommonOptions.SOURCE_TABLE_NAME.key());
        }
        String resultTableName = null;
        if (config.hasPath(CommonOptions.RESULT_TABLE_NAME.key())) {
            resultTableName = config.getString(CommonOptions.RESULT_TABLE_NAME.key());
        }
        if (sourceTableName != null && resultTableName != null) {
            return String.format("%s_%s", sourceTableName, resultTableName);
        }
        if (sourceTableName == null) {
            return resultTableName;
        }
        if (resultTableName == null) {
            return sourceTableName;
        }
        return defaultValue;
    }
}
