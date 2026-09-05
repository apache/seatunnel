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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.api.metadata.MetadataProviderManager;
import org.apache.seatunnel.api.metalake.MetalakeConfigUtils;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.EnvOptionRule;
import org.apache.seatunnel.api.sink.SaveModeExecuteLocation;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.source.DynamicLookupSourceCapability;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceCheckpoint;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupDescriptor;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupProjectionField;
import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupSideSpec;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getFactoryId;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getInputIds;

@Slf4j
public class MultipleTableJobConfigParser {

    private static final long KIB = 1024L;
    private static final long MIB = KIB * 1024L;
    private static final long GIB = MIB * 1024L;
    private static final long DYNAMIC_LOOKUP_M0_MAX_LOGICAL_STATE_BYTES = 512L * MIB;
    private static final long DYNAMIC_LOOKUP_M0_MAX_RESIDENT_STATE_BYTES = 512L * MIB;

    static {
        // Load DriverManager first to avoid deadlock between DriverManager's
        // static initialization block and specific driver class's static
        // initialization block when two different driver classes are loading
        // concurrently using Class.forName while DriverManager is uninitialized
        // before.
        //
        // This could happen in JDK 8 but not above as driver loading has been
        // moved out of DriverManager's static initialization block since JDK 9.
        DriverManager.getDrivers();
    }

    private final IdGenerator idGenerator;
    private final JobConfig jobConfig;

    private final List<URL> commonPluginJars;
    private final Config seaTunnelJobConfig;

    private final ReadonlyConfig envOptions;

    private final boolean isStartWithSavePoint;
    private final List<JobPipelineCheckpointData> pipelineCheckpoints;
    private final List<MultiTableFailedTable> failedTables = new ArrayList<>();
    private final List<MultiTableFailedTable> sourceFailedTables = new ArrayList<>();

    private final MetadataConfig metaDataConfig;

    @VisibleForTesting
    public MultipleTableJobConfigParser(
            String jobDefineFilePath, IdGenerator idGenerator, JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList(), false);
    }

    @VisibleForTesting
    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig, IdGenerator idGenerator, JobConfig jobConfig) {
        this(
                seaTunnelJobConfig,
                idGenerator,
                jobConfig,
                Collections.emptyList(),
                false,
                Collections.emptyList(),
                new MetadataConfig());
    }

    @VisibleForTesting
    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint) {
        this(
                jobDefineFilePath,
                null,
                idGenerator,
                jobConfig,
                commonPluginJars,
                isStartWithSavePoint,
                Collections.emptyList(),
                new MetadataConfig());
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            List<String> variables,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint,
            List<JobPipelineCheckpointData> pipelineCheckpoints,
            MetadataConfig metaDataConfig) {
        this(
                ConfigBuilder.of(Paths.get(jobDefineFilePath), variables),
                idGenerator,
                jobConfig,
                commonPluginJars,
                isStartWithSavePoint,
                pipelineCheckpoints,
                metaDataConfig);
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint,
            List<JobPipelineCheckpointData> pipelineCheckpoints,
            MetadataConfig metaDataConfig) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = handleDataSource(seaTunnelJobConfig, metaDataConfig);
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.pipelineCheckpoints = pipelineCheckpoints;
        this.metaDataConfig = metaDataConfig;
        ConfigValidator.of(this.envOptions).validate(new EnvOptionRule().optionRule());
    }

    public ImmutablePair<List<Action>, Set<URL>> parse(ClassLoaderService classLoaderService) {
        failedTables.clear();
        sourceFailedTables.clear();
        this.fillJobConfigAndCommonJars();
        List<? extends Config> sourceConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "source", Collections.emptyList());
        List<? extends Config> transformConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "transform", Collections.emptyList());
        List<NamedDynamicLookupConfig> dynamicLookupConfigs =
                getDynamicLookupConfigs(seaTunnelJobConfig);
        List<? extends Config> sinkConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "sink", Collections.emptyList());

        List<URL> sourceConnectorJars = getConnectorJarList(sourceConfigs, PluginType.SOURCE);
        List<URL> transformConnectorJars =
                getConnectorJarList(transformConfigs, PluginType.TRANSFORM);
        List<URL> sinkConnectorJars =
                DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())
                        ? Collections.emptyList()
                        : getConnectorJarList(sinkConfigs, PluginType.SINK);
        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();

        // source and transform use the same classloader
        List<URL> sourceJars =
                Stream.of(sourceConnectorJars, transformConnectorJars)
                        .flatMap(Collection::stream)
                        .distinct()
                        .collect(Collectors.toList());
        ClassLoader sourceAndTransformClassLoader =
                getClassLoader(classLoaderService, parentClassLoader, sourceJars);
        ClassLoader sinkClassLoader =
                getClassLoader(classLoaderService, parentClassLoader, sinkConnectorJars);

        try {
            Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
            ConfigParserUtil.checkGraph(
                    sourceConfigs,
                    mergeGraphMiddleConfigs(transformConfigs, dynamicLookupConfigs),
                    sinkConfigs);
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap =
                    new LinkedHashMap<>();

            log.info("start generating all sources.");
            if (isStartWithSavePoint
                    && pipelineCheckpoints != null
                    && !pipelineCheckpoints.isEmpty()) {
                Preconditions.checkState(
                        sourceConfigs.size() == pipelineCheckpoints.size(),
                        "The number of source configurations and pipeline checkpoints must be"
                                + " equal.");
            }
            for (int configIndex = 0; configIndex < sourceConfigs.size(); configIndex++) {
                Config sourceConfig = sourceConfigs.get(configIndex);
                Tuple2<String, List<Tuple2<CatalogTable, Action>>> tuple2 =
                        parseSource(configIndex, sourceConfig, sourceAndTransformClassLoader);
                tableWithActionMap.put(tuple2._1(), tuple2._2());
            }
            boolean hasSourceTables =
                    tableWithActionMap.values().stream().anyMatch(actions -> !actions.isEmpty());
            if (!sourceConfigs.isEmpty()
                    && !hasSourceTables
                    && MultiTableFailureHelper.shouldContinueOtherTables(envOptions)) {
                throw new JobDefineCheckException(
                        "No source tables were available after discovery. "
                                + "Check source-side failed-table warnings for details.");
            }

            log.info("start generating all transforms.");
            parseTransforms(transformConfigs, sourceAndTransformClassLoader, tableWithActionMap);

            log.info("start generating all dynamic lookups.");
            parseDynamicLookups(dynamicLookupConfigs, tableWithActionMap);

            Thread.currentThread().setContextClassLoader(sinkClassLoader);
            log.info("start generating all sinks.");
            List<Action> sinkActions = new ArrayList<>();
            for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
                Config sinkConfig = sinkConfigs.get(configIndex);
                sinkActions.addAll(
                        parseSink(configIndex, sinkConfig, sinkClassLoader, tableWithActionMap));
            }
            if (sinkActions.isEmpty() && !failedTables.isEmpty()) {
                throw new JobDefineCheckException(
                        buildFailureSummary(
                                "All candidate sink tables were skipped during job parsing."));
            }
            if (!failedTables.isEmpty()) {
                log.warn(
                        buildFailureSummary(
                                "Some tables were skipped during multi-table job parsing."));
            }
            Set<URL> factoryUrls = getUsedFactoryUrls(sinkActions);
            return new ImmutablePair<>(sinkActions, factoryUrls);
        } finally {
            Thread.currentThread().setContextClassLoader(parentClassLoader);
            if (classLoaderService != null) {
                classLoaderService.releaseClassLoader(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), sourceJars);
                classLoaderService.releaseClassLoader(
                        Long.parseLong(jobConfig.getJobContext().getJobId()), sinkConnectorJars);
            }
        }
    }

    private List<Config> mergeGraphMiddleConfigs(
            List<? extends Config> transformConfigs,
            List<NamedDynamicLookupConfig> dynamicLookupConfigs) {
        List<Config> middleConfigs = new ArrayList<>(transformConfigs);
        for (NamedDynamicLookupConfig dynamicLookupConfig : dynamicLookupConfigs) {
            middleConfigs.add(toGraphConfig(dynamicLookupConfig));
        }
        return middleConfigs;
    }

    private static List<NamedDynamicLookupConfig> getDynamicLookupConfigs(Config rootConfig) {
        if (!rootConfig.hasPath("dynamic_lookup")) {
            return Collections.emptyList();
        }
        Config dynamicLookupRoot = rootConfig.getConfig("dynamic_lookup");
        List<NamedDynamicLookupConfig> configs = new ArrayList<>();
        for (Map.Entry<String, ConfigValue> entry : dynamicLookupRoot.root().entrySet()) {
            if (entry.getValue().valueType() != ConfigValueType.OBJECT) {
                continue;
            }
            configs.add(
                    new NamedDynamicLookupConfig(
                            entry.getKey(), dynamicLookupRoot.getConfig(entry.getKey())));
        }
        return configs;
    }

    private static Config toGraphConfig(NamedDynamicLookupConfig namedConfig) {
        List<String> inputIds =
                Arrays.asList(
                        namedConfig.config.getConfig("fact").getString("input"),
                        namedConfig.config.getConfig("dimension").getString("input"));
        return ConfigFactory.empty()
                .withValue("plugin_name", ConfigValueFactory.fromAnyRef("DynamicLookup"))
                .withValue(
                        "plugin_output",
                        ConfigValueFactory.fromAnyRef(getLookupOutputId(namedConfig.config)))
                .withValue("plugin_input", ConfigValueFactory.fromIterable(inputIds));
    }

    private static String getLookupOutputId(Config config) {
        return config.getString("plugin_output");
    }

    private ClassLoader getClassLoader(
            ClassLoaderService classLoaderService,
            ClassLoader parentClassLoader,
            List<URL> connectorJars) {
        ClassLoader classLoader;
        if (classLoaderService == null) {
            classLoader = new SeaTunnelChildFirstClassLoader(connectorJars, parentClassLoader);
        } else {
            classLoader =
                    classLoaderService.getClassLoader(
                            Long.parseLong(jobConfig.getJobContext().getJobId()), connectorJars);
        }
        return classLoader;
    }

    public Set<URL> getUsedFactoryUrls(List<Action> sinkActions) {
        Set<URL> urls = new HashSet<>();
        fillUsedFactoryUrls(sinkActions, urls);
        return urls;
    }

    /**
     * Resolves connector JAR paths for the given plugin configs and type.
     *
     * <p>Delegates to {@link JobPluginClasspathHelper#connectorJarList} so that dry-run validation
     * ({@link org.apache.seatunnel.core.starter.seatunnel.command.SeaTunnelConfValidateCommand})
     * and the normal runtime parse path use the same discovery contract.
     */
    private List<URL> getConnectorJarList(List<? extends Config> configs, PluginType type) {
        return JobPluginClasspathHelper.connectorJarList(configs, type, commonPluginJars);
    }

    private void fillUsedFactoryUrls(List<Action> actions, Set<URL> result) {
        actions.forEach(
                action -> {
                    result.addAll(action.getJarUrls());
                    if (!action.getUpstream().isEmpty()) {
                        fillUsedFactoryUrls(action.getUpstream(), result);
                    }
                });
    }

    private void fillJobConfigAndCommonJars() {
        JobMode jobMode = envOptions.get(EnvCommonOptions.JOB_MODE);
        jobConfig
                .getJobContext()
                .setJobMode(jobMode)
                .setEnableCheckpoint(
                        (envOptions.get(EnvCommonOptions.CHECKPOINT_INTERVAL) != null)
                                || jobMode == JobMode.STREAMING);
        if (StringUtils.isEmpty(jobConfig.getName())
                || jobConfig.getName().equals(Constants.LOGO)
                || jobConfig.getName().equals(EnvCommonOptions.JOB_NAME.defaultValue())) {
            jobConfig.setName(envOptions.get(EnvCommonOptions.JOB_NAME));
        }
        jobConfig.getEnvOptions().putAll(envOptions.getSourceMap());
        DryRunSampleConfig.applyTrustedConfiguration(jobConfig);
        if (DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())) {
            jobConfig.getJobContext().setEnableCheckpoint(false);
        }
        this.commonPluginJars.addAll(JobPluginClasspathHelper.thirdPartyJarsFromEnv(envOptions));
        log.info("add common jar in plugins :{}", commonPluginJars);
    }

    private int getParallelism(ReadonlyConfig config) {
        if (DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())) {
            // Keep task-local sample counters source-wide and the preview output deterministic.
            return 1;
        }
        return Math.max(
                1,
                config.getOptional(EnvCommonOptions.PARALLELISM)
                        .orElse(envOptions.get(EnvCommonOptions.PARALLELISM)));
    }

    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
            int configIndex, Config sourceConfig, ClassLoader classLoader) {
        final ReadonlyConfig readonlyConfig =
                MultiTableFailureHelper.withMultiTableFailurePolicy(
                        ReadonlyConfig.fromConfig(sourceConfig), envOptions);
        final String factoryId = getFactoryId(readonlyConfig);
        final String tableId =
                readonlyConfig.getOptional(ConnectorCommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);

        final int parallelism = getParallelism(readonlyConfig);

        Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource =
                pluginIdentifier -> {
                    SeaTunnelSourcePluginDiscovery sourcePluginDiscovery =
                            new SeaTunnelSourcePluginDiscovery();
                    return sourcePluginDiscovery.createPluginInstance(pluginIdentifier);
                };

        List<MultiTableFailedTable> discoveryFailedTables = new ArrayList<>();
        Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> tuple2 =
                MultiTableFailureHelper.collectFailedTables(
                        discoveryFailedTables,
                        () ->
                                createAndPrepareSource(
                                        configIndex,
                                        readonlyConfig,
                                        classLoader,
                                        factoryId,
                                        fallbackCreateSource));
        failedTables.addAll(discoveryFailedTables);
        sourceFailedTables.addAll(discoveryFailedTables);

        Set<URL> factoryUrls = new HashSet<>();
        factoryUrls.addAll(getSourcePluginJarPaths(sourceConfig));

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        long id = idGenerator.getNextId();
        String actionName = JobConfigParser.createSourceActionName(configIndex, factoryId);
        SeaTunnelSource<Object, SourceSplit, Serializable> source = tuple2._1();
        source.setJobContext(jobConfig.getJobContext());
        FactoryUtil.ensureJobModeMatch(jobConfig.getJobContext(), source);
        SourceAction<Object, SourceSplit, Serializable> action =
                new SourceAction<>(id, actionName, tuple2._1(), factoryUrls, new HashSet<>());
        action.setParallelism(parallelism);
        for (CatalogTable catalogTable : tuple2._2()) {
            actions.add(new Tuple2<>(catalogTable, action));
        }
        return new Tuple2<>(tableId, actions);
    }

    protected Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>>
            createAndPrepareSource(
                    int configIndex,
                    ReadonlyConfig readonlyConfig,
                    ClassLoader classLoader,
                    String factoryId,
                    Function<PluginIdentifier, SeaTunnelSource> fallbackCreateSource) {
        if (isStartWithSavePoint && pipelineCheckpoints != null && !pipelineCheckpoints.isEmpty()) {
            ChangeStreamTableSourceCheckpoint checkpoint =
                    getSourceCheckpoint(configIndex, factoryId);
            return FactoryUtil.restoreAndPrepareSource(
                    readonlyConfig,
                    classLoader,
                    factoryId,
                    checkpoint,
                    fallbackCreateSource,
                    null,
                    metaDataConfig);
        }
        return FactoryUtil.createAndPrepareSource(
                readonlyConfig, classLoader, factoryId, fallbackCreateSource, null, metaDataConfig);
    }

    public void parseTransforms(
            List<? extends Config> transformConfigs,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        if (CollectionUtils.isEmpty(transformConfigs) || transformConfigs.isEmpty()) {
            return;
        }
        Set<String> usedTransformNames = new HashSet<>();
        Queue<Config> configList = new LinkedList<>(transformConfigs);
        int index = 0;
        while (!configList.isEmpty()) {
            parseTransform(
                    index++, configList, classLoader, tableWithActionMap, usedTransformNames);
        }
    }

    private void parseTransform(
            int index,
            Queue<Config> transforms,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap,
            Set<String> usedTransformNames) {
        Config config = transforms.poll();
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        final String factoryId = getFactoryId(readonlyConfig);
        // get jar urls
        Set<URL> jarUrls = new HashSet<>();
        jarUrls.addAll(getTransformPluginJarPaths(config));
        final List<String> inputIds = getInputIds(readonlyConfig);

        List<Tuple2<CatalogTable, Action>> inputs =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
        if (inputs.isEmpty()) {
            if (transforms.isEmpty()) {
                // Tolerates incorrect configuration of simple graph
                inputs = findLast(tableWithActionMap);
            } else {
                // The previous transform has not been created
                transforms.offer(config);
                return;
            }
        }

        final String tableId =
                readonlyConfig.getOptional(ConnectorCommonOptions.PLUGIN_OUTPUT).orElse(DEFAULT_ID);

        Set<Action> inputActions =
                inputs.stream()
                        .map(Tuple2::_2)
                        .collect(Collectors.toCollection(LinkedHashSet::new));

        LinkedHashSet<CatalogTable> catalogTables =
                inputs.stream()
                        .map(Tuple2::_1)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        checkProducedTypeEquals(inputActions);
        int spareParallelism = inputs.get(0)._2().getParallelism();
        int parallelism =
                DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())
                        ? 1
                        : readonlyConfig
                                .getOptional(EnvCommonOptions.PARALLELISM)
                                .orElse(spareParallelism);
        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareMultiTableTransform(
                        new ArrayList<>(catalogTables), readonlyConfig, classLoader, factoryId);

        transform.setJobContext(jobConfig.getJobContext());
        long id = idGenerator.getNextId();
        String legacyActionName = JobConfigParser.createTransformActionName(index, factoryId);
        String actionName = legacyActionName;
        String configuredName = getOptionalName(config);
        if (StringUtils.isNotBlank(configuredName)) {
            if (!usedTransformNames.add(configuredName)) {
                throw new JobDefineCheckException(
                        String.format(
                                "Duplicated transform name '%s'. Transform names must be unique"
                                        + " within a job.",
                                configuredName));
            }
            actionName = configuredName;
        }

        TransformAction transformAction =
                new TransformAction(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        transform,
                        jarUrls,
                        new HashSet<>());
        transformAction.setParallelism(parallelism);

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        List<CatalogTable> producedCatalogTables = transform.getProducedCatalogTables();

        for (CatalogTable catalogTable : producedCatalogTables) {
            actions.add(new Tuple2<>(catalogTable, transformAction));
        }

        tableWithActionMap.put(tableId, actions);
    }

    private static String getOptionalName(Config config) {
        if (config == null || !config.hasPath("name")) {
            return null;
        }
        String name = config.getString("name");
        if (name == null) {
            return null;
        }
        return name.trim();
    }

    public static SeaTunnelDataType<?> getProducedType(Action action) {
        if (action instanceof SourceAction) {
            try {
                return ((SourceAction<?, ?, ?>) action)
                        .getSource()
                        .getProducedCatalogTables()
                        .get(0)
                        .getSeaTunnelRowType();
            } catch (UnsupportedOperationException e) {
                // TODO remove it when all connector use `getProducedCatalogTables`
                return ((SourceAction<?, ?, ?>) action).getSource().getProducedType();
            }
        } else if (action instanceof TransformAction) {
            return ((TransformAction) action)
                    .getTransform()
                    .getProducedCatalogTable()
                    .getSeaTunnelRowType();
        } else if (action instanceof DynamicLookupAction) {
            return ((DynamicLookupAction) action).getProducedCatalogTable().getSeaTunnelRowType();
        }
        throw new UnsupportedOperationException();
    }

    private void parseDynamicLookups(
            List<NamedDynamicLookupConfig> dynamicLookupConfigs,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        for (NamedDynamicLookupConfig namedConfig : dynamicLookupConfigs) {
            parseDynamicLookup(namedConfig, tableWithActionMap);
        }
    }

    private void parseDynamicLookup(
            NamedDynamicLookupConfig namedConfig,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        Config config = namedConfig.config;
        Config factConfig = config.getConfig("fact");
        Config dimensionConfig = config.getConfig("dimension");
        validateDynamicLookupModeConfig(config, factConfig, dimensionConfig);
        validateDynamicLookupResourceConfig(config);
        String factInputId = factConfig.getString("input");
        String dimensionInputId = dimensionConfig.getString("input");
        Tuple2<CatalogTable, Action> factInput =
                getSingleLookupInput(namedConfig, tableWithActionMap, factInputId, "fact");
        Tuple2<CatalogTable, Action> dimensionInput =
                getSingleLookupInput(
                        namedConfig, tableWithActionMap, dimensionInputId, "dimension");
        validateRequiredCapabilities(namedConfig, factConfig, factInput._2(), "fact");
        validateRequiredCapabilities(
                namedConfig, dimensionConfig, dimensionInput._2(), "dimension");
        validateDimensionTableBinding(namedConfig, dimensionConfig, dimensionInput._1());
        validateJoinKeyCompatibility(
                namedConfig,
                factInput._1(),
                factConfig.getStringList("key"),
                dimensionInput._1(),
                dimensionConfig.getStringList("key"));
        if (factInput._2().getParallelism() != dimensionInput._2().getParallelism()) {
            throw new JobDefineCheckException(
                    "Dynamic lookup requires equal fact and dimension parallelism, but got "
                            + factInput._2().getParallelism()
                            + " and "
                            + dimensionInput._2().getParallelism());
        }

        String operatorUid = config.hasPath("uid") ? config.getString("uid") : namedConfig.name;
        String actionName = namedConfig.name;
        String outputId = getLookupOutputId(config);
        DynamicLookupDescriptor.JoinType joinType = parseJoinType(config.getString("join.type"));
        List<DynamicLookupProjectionField> projectionFields =
                parseProjectionFields(
                        config.getStringList("join.fields"), factInput._1(), dimensionInput._1());
        CatalogTable producedCatalogTable =
                buildDynamicLookupCatalogTable(
                        outputId, factInput._1(), dimensionInput._1(), joinType, projectionFields);
        DynamicLookupDescriptor descriptor =
                new DynamicLookupDescriptor(
                        outputId,
                        new DynamicLookupSideSpec(
                                factInputId,
                                factInput._1().getTablePath().getFullName(),
                                factConfig.getStringList("key"),
                                resolveFieldIndexes(
                                        factInput._1(), factConfig.getStringList("key"))),
                        new DynamicLookupSideSpec(
                                dimensionInputId,
                                dimensionInput._1().getTablePath().getFullName(),
                                dimensionConfig.getStringList("key"),
                                resolveFieldIndexes(
                                        dimensionInput._1(), dimensionConfig.getStringList("key"))),
                        joinType,
                        projectionFields);
        Set<URL> jarUrls = new HashSet<>();
        jarUrls.addAll(factInput._2().getJarUrls());
        jarUrls.addAll(dimensionInput._2().getJarUrls());
        Set<ConnectorJarIdentifier> connectorJarIdentifiers = new HashSet<>();
        connectorJarIdentifiers.addAll(factInput._2().getConnectorJarIdentifiers());
        connectorJarIdentifiers.addAll(dimensionInput._2().getConnectorJarIdentifiers());
        DynamicLookupAction action =
                new DynamicLookupAction(
                        idGenerator.getNextId(),
                        actionName,
                        operatorUid,
                        factInput._2(),
                        stableSourceUid(factInputId, factInput._2()),
                        dimensionInput._2(),
                        stableSourceUid(dimensionInputId, dimensionInput._2()),
                        descriptor,
                        producedCatalogTable,
                        getDynamicLookupBytes(
                                config, "resource.max-logical-state-bytes-per-subtask"),
                        getDynamicLookupBytes(
                                config, "resource.max-resident-state-bytes-per-subtask"),
                        jarUrls,
                        connectorJarIdentifiers);
        action.setParallelism(factInput._2().getParallelism());
        tableWithActionMap.put(
                outputId, Collections.singletonList(new Tuple2<>(producedCatalogTable, action)));
    }

    private static Tuple2<CatalogTable, Action> getSingleLookupInput(
            NamedDynamicLookupConfig namedConfig,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap,
            String inputId,
            String sideName) {
        List<Tuple2<CatalogTable, Action>> candidates = tableWithActionMap.get(inputId);
        if (CollectionUtils.isEmpty(candidates)) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' cannot resolve "
                            + sideName
                            + " input '"
                            + inputId
                            + "'");
        }
        if (candidates.size() != 1) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' requires exactly one table for "
                            + sideName
                            + " input '"
                            + inputId
                            + "'");
        }
        return candidates.get(0);
    }

    private static String stableSourceUid(String inputId, Action action) {
        return inputId + "-" + action.getId();
    }

    private static void validateRequiredCapabilities(
            NamedDynamicLookupConfig namedConfig,
            Config sideConfig,
            Action inputAction,
            String sideName) {
        List<String> requiredCapabilities = getRequiredCapabilities(sideConfig);
        if (requiredCapabilities.isEmpty()) {
            return;
        }
        if (!(inputAction instanceof SourceAction)) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' "
                            + sideName
                            + " input must be a SourceAction");
        }
        SeaTunnelSource<?, ?, ?> source = ((SourceAction<?, ?, ?>) inputAction).getSource();
        if (!(source instanceof DynamicLookupSourceCapability)) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' "
                            + sideName
                            + " source does not declare dynamic lookup capabilities "
                            + requiredCapabilities);
        }
        Set<String> actualCapabilities =
                ((DynamicLookupSourceCapability) source).dynamicLookupCapabilities();
        List<String> missingCapabilities =
                requiredCapabilities.stream()
                        .filter(required -> !actualCapabilities.contains(required))
                        .collect(Collectors.toList());
        if (!missingCapabilities.isEmpty()) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' "
                            + sideName
                            + " source misses required capabilities "
                            + missingCapabilities);
        }
    }

    private static List<String> getRequiredCapabilities(Config sideConfig) {
        if (!sideConfig.hasPath("required-capability")) {
            return Collections.emptyList();
        }
        List<String> requiredCapabilities;
        ConfigValue capabilityValue = sideConfig.getValue("required-capability");
        if (capabilityValue.valueType() == ConfigValueType.STRING) {
            requiredCapabilities =
                    Collections.singletonList(sideConfig.getString("required-capability"));
        } else {
            requiredCapabilities = sideConfig.getStringList("required-capability");
        }
        List<String> normalizedCapabilities =
                requiredCapabilities.stream()
                        .map(String::trim)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList());
        if (normalizedCapabilities.isEmpty()) {
            throw new JobDefineCheckException(
                    "Dynamic lookup required-capability must not be empty when declared");
        }
        return normalizedCapabilities;
    }

    private static void validateDimensionTableBinding(
            NamedDynamicLookupConfig namedConfig,
            Config dimensionConfig,
            CatalogTable dimensionCatalogTable) {
        if (!dimensionConfig.hasPath("table")) {
            return;
        }
        String expectedTable = dimensionConfig.getString("table");
        String actualTable = dimensionCatalogTable.getTablePath().getFullName();
        if (!expectedTable.equals(actualTable)) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' dimension.table expects '"
                            + expectedTable
                            + "' but input table is '"
                            + actualTable
                            + "'");
        }
    }

    private static CatalogTable buildDynamicLookupCatalogTable(
            String outputId,
            CatalogTable factCatalogTable,
            CatalogTable dimensionCatalogTable,
            DynamicLookupDescriptor.JoinType joinType,
            List<DynamicLookupProjectionField> projectionFields) {
        List<Column> columns = new ArrayList<>(projectionFields.size());
        for (DynamicLookupProjectionField projectionField : projectionFields) {
            CatalogTable sourceTable =
                    projectionField.getInputSide() == DynamicLookupProjectionField.InputSide.FACT
                            ? factCatalogTable
                            : dimensionCatalogTable;
            Column sourceColumn = resolveColumn(sourceTable, projectionField.getSourceFieldName());
            boolean nullable =
                    projectionField.getInputSide() == DynamicLookupProjectionField.InputSide.FACT
                            ? sourceColumn.isNullable()
                            : joinType == DynamicLookupDescriptor.JoinType.LEFT
                                    || sourceColumn.isNullable();
            columns.add(copyColumn(sourceColumn, projectionField.getOutputFieldName(), nullable));
        }
        TableIdentifier factTableId = factCatalogTable.getTableId();
        TableIdentifier outputTableId =
                TableIdentifier.of(
                        factTableId.getCatalogName(),
                        factTableId.getDatabaseName(),
                        factTableId.getSchemaName(),
                        outputId);
        return CatalogTable.of(
                outputTableId,
                TableSchema.builder().columns(columns).build(),
                new HashMap<>(),
                Collections.emptyList(),
                "Dynamic lookup output",
                factCatalogTable.getCatalogName());
    }

    private static Column copyColumn(
            Column sourceColumn, String outputFieldName, boolean nullable) {
        Map<String, Object> columnOptions =
                sourceColumn.getOptions() == null
                        ? new HashMap<>()
                        : new HashMap<>(sourceColumn.getOptions());
        return PhysicalColumn.builder()
                .name(outputFieldName)
                .dataType(sourceColumn.getDataType())
                .columnLength(sourceColumn.getColumnLength())
                .scale(sourceColumn.getScale())
                .nullable(nullable)
                .defaultValue(sourceColumn.getDefaultValue())
                .comment(sourceColumn.getComment())
                .sourceType(sourceColumn.getSourceType())
                .options(columnOptions)
                .build();
    }

    private static List<DynamicLookupProjectionField> parseProjectionFields(
            List<String> fieldSpecs,
            CatalogTable factCatalogTable,
            CatalogTable dimensionCatalogTable) {
        List<DynamicLookupProjectionField> fields = new ArrayList<>(fieldSpecs.size());
        for (String fieldSpec : fieldSpecs) {
            String[] aliasSplit = fieldSpec.trim().split("(?i)\\s+as\\s+", 2);
            String sourceSpec = aliasSplit[0].trim();
            String outputFieldName;
            int dotIndex = sourceSpec.indexOf('.');
            if (dotIndex < 0 || dotIndex == sourceSpec.length() - 1) {
                throw new JobDefineCheckException(
                        "Dynamic lookup field projection must use '<side>.<field>' syntax, but got"
                                + " '"
                                + fieldSpec
                                + "'");
            }
            String side = sourceSpec.substring(0, dotIndex).trim();
            String sourceFieldName = sourceSpec.substring(dotIndex + 1).trim();
            outputFieldName = aliasSplit.length == 2 ? aliasSplit[1].trim() : sourceFieldName;
            DynamicLookupProjectionField.InputSide inputSide;
            if ("fact".equalsIgnoreCase(side)) {
                inputSide = DynamicLookupProjectionField.InputSide.FACT;
            } else if ("dimension".equalsIgnoreCase(side)) {
                inputSide = DynamicLookupProjectionField.InputSide.DIMENSION;
            } else {
                throw new JobDefineCheckException(
                        "Dynamic lookup field projection side must be fact or dimension, but got '"
                                + side
                                + "'");
            }
            CatalogTable sourceTable =
                    inputSide == DynamicLookupProjectionField.InputSide.FACT
                            ? factCatalogTable
                            : dimensionCatalogTable;
            fields.add(
                    new DynamicLookupProjectionField(
                            inputSide,
                            sourceFieldName,
                            resolveFieldIndex(sourceTable, sourceFieldName),
                            outputFieldName));
        }
        return fields;
    }

    private static DynamicLookupDescriptor.JoinType parseJoinType(String joinType) {
        if ("LEFT".equalsIgnoreCase(joinType)) {
            return DynamicLookupDescriptor.JoinType.LEFT;
        }
        if ("INNER".equalsIgnoreCase(joinType)) {
            return DynamicLookupDescriptor.JoinType.INNER;
        }
        throw new JobDefineCheckException(
                "Dynamic lookup join.type must be LEFT or INNER, but got '" + joinType + "'");
    }

    private static void validateDynamicLookupModeConfig(
            Config lookupConfig, Config factConfig, Config dimensionConfig) {
        requireDynamicLookupPaths(factConfig, "changelog-mode", "required-capability");
        requireDynamicLookupPaths(dimensionConfig, "primary-key-update", "required-capability");
        requireDynamicLookupPaths(lookupConfig, "schema-change.behavior");
        if (!"APPEND_ONLY".equalsIgnoreCase(factConfig.getString("changelog-mode"))) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 requires fact.changelog-mode=APPEND_ONLY");
        }
        if (!"FAIL".equalsIgnoreCase(dimensionConfig.getString("primary-key-update"))) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 requires dimension.primary-key-update=FAIL");
        }
        if (!"FAIL".equalsIgnoreCase(lookupConfig.getString("schema-change.behavior"))) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 requires schema-change.behavior=FAIL");
        }
    }

    private static void validateDynamicLookupResourceConfig(Config lookupConfig) {
        requireDynamicLookupPaths(
                lookupConfig,
                "state.max-concurrent-snapshots",
                "resource.max-concurrent-snapshots",
                "state.backend",
                "state.ttl",
                "resource.max-logical-state-bytes-per-subtask",
                "resource.max-resident-state-bytes-per-subtask");
        if (lookupConfig.getInt("state.max-concurrent-snapshots") != 1) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 requires state.max-concurrent-snapshots=1");
        }
        if (lookupConfig.getInt("resource.max-concurrent-snapshots") != 1) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 requires resource.max-concurrent-snapshots=1");
        }
        if (!"IN_MEMORY".equalsIgnoreCase(lookupConfig.getString("state.backend"))) {
            throw new JobDefineCheckException("Dynamic lookup M0 requires state.backend=IN_MEMORY");
        }
        if (!"NONE".equalsIgnoreCase(lookupConfig.getString("state.ttl"))) {
            throw new JobDefineCheckException("Dynamic lookup M0 requires state.ttl=NONE");
        }
        long logicalStateBytes =
                getDynamicLookupBytes(lookupConfig, "resource.max-logical-state-bytes-per-subtask");
        if (logicalStateBytes > DYNAMIC_LOOKUP_M0_MAX_LOGICAL_STATE_BYTES) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 in-memory runtime supports at most 512 MiB logical state"
                            + " per subtask");
        }
        long residentStateBytes =
                getDynamicLookupBytes(
                        lookupConfig, "resource.max-resident-state-bytes-per-subtask");
        if (residentStateBytes > DYNAMIC_LOOKUP_M0_MAX_RESIDENT_STATE_BYTES) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 supports at most 512 MiB resident state per subtask");
        }
        if (residentStateBytes < logicalStateBytes) {
            throw new JobDefineCheckException(
                    "Dynamic lookup M0 in-memory runtime requires resident state budget to cover"
                            + " logical state budget");
        }
    }

    private static void requireDynamicLookupPaths(Config config, String... paths) {
        for (String path : paths) {
            if (!config.hasPath(path)) {
                throw new JobDefineCheckException("Dynamic lookup M0 requires '" + path + "'");
            }
        }
    }

    private static long getDynamicLookupBytes(Config config, String path) {
        String value = getDynamicLookupScalar(config, path);
        return parseDynamicLookupBytes(value, path);
    }

    private static String getDynamicLookupScalar(Config config, String path) {
        return String.valueOf(config.getValue(path).unwrapped()).trim();
    }

    private static long parseDynamicLookupBytes(String value, String path) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        long multiplier;
        String number;
        if (normalized.endsWith("kb")) {
            multiplier = KIB;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("mb")) {
            multiplier = MIB;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("gb")) {
            multiplier = GIB;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("tb")) {
            multiplier = GIB * 1024L;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("b")) {
            multiplier = 1L;
            number = normalized.substring(0, normalized.length() - 1);
        } else {
            multiplier = 1L;
            number = normalized;
        }
        final double parsed;
        try {
            parsed = Double.parseDouble(number.trim());
        } catch (NumberFormatException e) {
            throw new JobDefineCheckException(
                    "Dynamic lookup byte value for '" + path + "' is invalid: '" + value + "'", e);
        }
        if (parsed < 0) {
            throw new JobDefineCheckException(
                    "Dynamic lookup byte value must be non-negative: " + path);
        }
        return (long) Math.ceil(parsed * multiplier);
    }

    private static List<Integer> resolveFieldIndexes(
            CatalogTable catalogTable, List<String> fields) {
        return fields.stream()
                .map(field -> resolveFieldIndex(catalogTable, field))
                .collect(Collectors.toList());
    }

    private static int resolveFieldIndex(CatalogTable catalogTable, String fieldName) {
        List<Column> columns = catalogTable.getTableSchema().getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(fieldName)) {
                return i;
            }
        }
        throw new JobDefineCheckException(
                "Dynamic lookup field '"
                        + fieldName
                        + "' is missing from schema "
                        + catalogTable.getTablePath().getFullName());
    }

    private static Column resolveColumn(CatalogTable catalogTable, String fieldName) {
        return catalogTable.getTableSchema().getColumns().stream()
                .filter(column -> column.getName().equals(fieldName))
                .findFirst()
                .orElseThrow(
                        () ->
                                new JobDefineCheckException(
                                        "Dynamic lookup field '"
                                                + fieldName
                                                + "' is missing from schema "
                                                + catalogTable.getTablePath().getFullName()));
    }

    private static void validateJoinKeyCompatibility(
            NamedDynamicLookupConfig namedConfig,
            CatalogTable factCatalogTable,
            List<String> factKeys,
            CatalogTable dimensionCatalogTable,
            List<String> dimensionKeys) {
        if (factKeys.size() != dimensionKeys.size()) {
            throw new JobDefineCheckException(
                    "Dynamic lookup '"
                            + namedConfig.name
                            + "' requires fact.key and dimension.key to contain the same number"
                            + " of fields, but got "
                            + factKeys.size()
                            + " and "
                            + dimensionKeys.size());
        }
        for (int i = 0; i < factKeys.size(); i++) {
            String factKey = factKeys.get(i);
            String dimensionKey = dimensionKeys.get(i);
            Column factColumn = resolveColumn(factCatalogTable, factKey);
            Column dimensionColumn = resolveColumn(dimensionCatalogTable, dimensionKey);
            if (!factColumn.getDataType().equals(dimensionColumn.getDataType())) {
                throw new JobDefineCheckException(
                        "Dynamic lookup '"
                                + namedConfig.name
                                + "' requires matching join-key types, but fact."
                                + factKey
                                + " is "
                                + factColumn.getDataType()
                                + " and dimension."
                                + dimensionKey
                                + " is "
                                + dimensionColumn.getDataType());
            }
        }
    }

    private static final class NamedDynamicLookupConfig {
        private final String name;
        private final Config config;

        private NamedDynamicLookupConfig(String name, Config config) {
            this.name = name;
            this.config = config;
        }
    }

    public static void checkProducedTypeEquals(Set<Action> inputActions) {
        SeaTunnelDataType<?> expectedType = getProducedType(new ArrayList<>(inputActions).get(0));
        for (Action action : inputActions) {
            SeaTunnelDataType<?> producedType = getProducedType(action);
            if (!expectedType.equals(producedType)) {
                throw new JobDefineCheckException(
                        "Transform/Sink don't support processing data with two different"
                                + " structures.");
            }
        }
    }

    @Deprecated
    private static <T> T findLast(LinkedHashMap<?, T> map) {
        int size = map.size();
        int i = 1;
        for (T value : map.values()) {
            if (i == size) {
                return value;
            }
            i++;
        }
        // never execution
        return null;
    }

    public List<SinkAction<?, ?, ?, ?>> parseSink(
            int configIndex,
            Config sinkConfig,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {

        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = getFactoryId(readonlyConfig);
        List<String> inputIds = getInputIds(readonlyConfig);

        List<List<Tuple2<CatalogTable, Action>>> inputVertices =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        if (inputVertices.isEmpty()) {
            // Tolerates incorrect configuration of simple graph
            inputVertices = Collections.singletonList(findLast(tableWithActionMap));
        } else if (inputVertices.size() > 1) {
            for (List<Tuple2<CatalogTable, Action>> inputVertex : inputVertices) {
                if (inputVertex.size() > 1) {
                    throw new JobDefineCheckException(
                            "Sink don't support simultaneous writing of data from multi-table"
                                    + " source and other sources.");
                }
            }
        }

        // get jar urls
        Set<URL> jarUrls = new HashSet<>();
        if (!DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())) {
            jarUrls.addAll(getSinkPluginJarPaths(sinkConfig));
        }
        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();
        int failedTableStartIndex = failedTables.size();

        // union
        if (inputVertices.size() > 1) {
            Set<Action> inputActions =
                    inputVertices.stream()
                            .flatMap(Collection::stream)
                            .map(Tuple2::_2)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            checkProducedTypeEquals(inputActions);
            Tuple2<CatalogTable, Action> inputActionSample = inputVertices.get(0).get(0);
            Optional<SinkAction<?, ?, ?, ?>> sinkAction =
                    createSinkAction(
                            inputActionSample._1(),
                            inputActions,
                            readonlyConfig,
                            classLoader,
                            jarUrls,
                            new HashSet<>(),
                            factoryId,
                            inputActionSample._2().getParallelism(),
                            configIndex);
            sinkAction.ifPresent(sinkActions::add);
            return sinkActions;
        }

        // TODO move it into tryGenerateMultiTableSink when we don't support sink template
        // sink template
        for (Tuple2<CatalogTable, Action> tuple : inputVertices.get(0)) {
            Optional<SinkAction<?, ?, ?, ?>> sinkAction =
                    createSinkAction(
                            tuple._1(),
                            Collections.singleton(tuple._2()),
                            readonlyConfig,
                            classLoader,
                            jarUrls,
                            new HashSet<>(),
                            factoryId,
                            tuple._2().getParallelism(),
                            configIndex);
            sinkAction.ifPresent(sinkActions::add);
        }
        if (DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())) {
            return sinkActions;
        }
        Optional<SinkAction<?, ?, ?, ?>> multiTableSink =
                tryGenerateMultiTableSink(
                        sinkActions,
                        readonlyConfig,
                        classLoader,
                        factoryId,
                        configIndex,
                        getInitialFailedTablesForSink(failedTableStartIndex));
        return multiTableSink
                .<List<SinkAction<?, ?, ?, ?>>>map(Collections::singletonList)
                .orElse(sinkActions);
    }

    private Optional<SinkAction<?, ?, ?, ?>> tryGenerateMultiTableSink(
            List<SinkAction<?, ?, ?, ?>> sinkActions,
            ReadonlyConfig options,
            ClassLoader classLoader,
            String factoryId,
            int configIndex,
            List<MultiTableFailedTable> skippedTables) {
        if (sinkActions.isEmpty()) {
            return Optional.empty();
        }
        if (sinkActions.stream()
                .anyMatch(action -> !(action.getSink() instanceof SupportMultiTableSink))) {
            log.info("Unsupported multi table sink api, rollback to sink template");
            return Optional.empty();
        }
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        Set<URL> jars =
                sinkActions.stream()
                        .flatMap(a -> a.getJarUrls().stream())
                        .collect(Collectors.toSet());
        sinkActions.forEach(
                action -> {
                    SeaTunnelSink sink = action.getSink();
                    TablePath tablePath = action.getConfig().getTablePath();
                    sinks.put(tablePath, sink);
                });
        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createMultiTableSink(
                        sinks,
                        MultiTableFailureHelper.withFailedTables(
                                MultiTableFailureHelper.mergeOptions(options, envOptions),
                                skippedTables),
                        classLoader);
        String actionName =
                JobConfigParser.createSinkActionName(configIndex, factoryId, "MultiTableSink");
        SinkAction<?, ?, ?, ?> multiTableAction =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        actionName,
                        sinkActions.get(0).getUpstream(),
                        sink,
                        jars,
                        new HashSet<>());
        multiTableAction.setParallelism(sinkActions.get(0).getParallelism());
        return Optional.of(multiTableAction);
    }

    private List<MultiTableFailedTable> getInitialFailedTablesForSink(int failedTableStartIndex) {
        Map<String, MultiTableFailedTable> initialFailedTables = new LinkedHashMap<>();
        sourceFailedTables.forEach(
                failedTable -> initialFailedTables.put(failedTable.getTablePath(), failedTable));
        failedTables
                .subList(failedTableStartIndex, failedTables.size())
                .forEach(
                        failedTable ->
                                initialFailedTables.put(failedTable.getTablePath(), failedTable));
        return new ArrayList<>(initialFailedTables.values());
    }

    protected Optional<SinkAction<?, ?, ?, ?>> createSinkAction(
            CatalogTable catalogTable,
            Set<Action> inputActions,
            ReadonlyConfig readonlyConfig,
            ClassLoader classLoader,
            Set<URL> factoryUrls,
            Set<ConnectorJarIdentifier> connectorJarIdentifiers,
            String factoryId,
            int parallelism,
            int configIndex) {

        if (DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions())) {
            SinkConfig actionConfig = new SinkConfig(catalogTable.getTableId().toTablePath());
            SinkAction<Object, Void, Void, Void> sinkAction =
                    new SinkAction<>(
                            idGenerator.getNextId(),
                            JobConfigParser.createSinkActionName(
                                    configIndex,
                                    "dry-run-sample",
                                    actionConfig.getTablePath().toString()),
                            new ArrayList<>(inputActions),
                            new DryRunSampleSink(),
                            Collections.emptySet(),
                            Collections.emptySet(),
                            actionConfig);
            sinkAction.setParallelism(1);
            log.info("Dry-run sample: sink DDL preview for {} is SKIPPED", factoryId);
            return Optional.of(sinkAction);
        }

        Function<PluginIdentifier, SeaTunnelSink> fallbackCreateSink =
                pluginIdentifier -> {
                    SeaTunnelSinkPluginDiscovery sinkPluginDiscovery =
                            new SeaTunnelSinkPluginDiscovery();
                    return sinkPluginDiscovery.createPluginInstance(pluginIdentifier);
                };

        SeaTunnelSink<?, ?, ?, ?> sink;
        try {
            sink =
                    FactoryUtil.createAndPrepareSink(
                            catalogTable,
                            readonlyConfig,
                            classLoader,
                            factoryId,
                            fallbackCreateSink,
                            null);
        } catch (Exception error) {
            return handleCreateSinkFailure(
                    catalogTable, factoryId, MultiTableFailurePhase.SINK_INIT, error);
        }
        sink.setJobContext(jobConfig.getJobContext());
        SinkConfig actionConfig = new SinkConfig(catalogTable.getTableId().toTablePath());
        long id = idGenerator.getNextId();
        String actionName =
                JobConfigParser.createSinkActionName(
                        configIndex, factoryId, actionConfig.getTablePath().toString());
        SinkAction<?, ?, ?, ?> sinkAction =
                new SinkAction<>(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        sink,
                        factoryUrls,
                        connectorJarIdentifiers,
                        actionConfig);
        try {
            if (!isStartWithSavePoint) {
                handleSaveMode(sink);
            } else {
                handleSchemaSaveModeWithRestore(sink);
            }
        } catch (Exception error) {
            return handleCreateSinkFailure(
                    catalogTable, factoryId, MultiTableFailurePhase.SAVE_MODE, error);
        }
        sinkAction.setParallelism(parallelism);
        return Optional.of(sinkAction);
    }

    public void handleSaveMode(SeaTunnelSink<?, ?, ?, ?> sink) {
        if (SupportSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportSaveMode saveModeSink = (SupportSaveMode) sink;
            if (envOptions
                    .get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION)
                    .equals(SaveModeExecuteLocation.CLIENT)) {
                log.warn(
                        "SaveMode execute location on CLIENT is deprecated, please use CLUSTER"
                                + " instead.");
                Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
                if (saveModeHandler.isPresent()) {
                    try (SaveModeHandler handler = saveModeHandler.get()) {
                        handler.open();
                        new SaveModeExecuteWrapper(handler).execute();
                    } catch (Exception e) {
                        throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                    }
                }
            }
        }
    }

    public void handleSchemaSaveModeWithRestore(SeaTunnelSink<?, ?, ?, ?> sink) {
        if (SupportSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportSaveMode saveModeSink = (SupportSaveMode) sink;
            if (envOptions
                    .get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION)
                    .equals(SaveModeExecuteLocation.CLIENT)) {
                Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
                if (saveModeHandler.isPresent()) {
                    try (SaveModeHandler handler = saveModeHandler.get()) {
                        handler.open();
                        handler.handleSchemaSaveModeWithRestore();
                    } catch (Exception e) {
                        throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                    }
                }
            }
        }
    }

    private Optional<SinkAction<?, ?, ?, ?>> handleCreateSinkFailure(
            CatalogTable catalogTable,
            String factoryId,
            MultiTableFailurePhase phase,
            Throwable error) {
        if (!MultiTableFailureHelper.shouldContinueOtherTables(envOptions)) {
            throw wrapThrowable(error);
        }
        MultiTableFailedTable failedTable =
                MultiTableFailureHelper.buildFailedTable(
                        catalogTable.getTablePath().getFullName(), phase, factoryId, error);
        failedTables.add(failedTable);
        log.warn(
                "Skip failed sink table during parsing: {}",
                MultiTableFailureHelper.formatFailedTableLine(failedTable),
                error);
        return Optional.empty();
    }

    private String buildFailureSummary(String title) {
        return MultiTableFailureHelper.formatFailedTableSummary(title, failedTables);
    }

    private RuntimeException wrapThrowable(Throwable error) {
        if (error instanceof RuntimeException) {
            return (RuntimeException) error;
        }
        return new RuntimeException(error);
    }

    private List<URL> getSourcePluginJarPaths(Config sourceConfig) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SOURCE_PLUGIN,
                        sourceConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sourcePluginDiscovery.getPluginJarAndDependencyPaths(
                        Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private List<URL> getTransformPluginJarPaths(Config transformConfig) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.TRANSFORM_PLUGIN,
                        transformConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                transformPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private List<URL> getSinkPluginJarPaths(Config sinkConfig) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        PluginIdentifier pluginIdentifier =
                PluginIdentifier.of(
                        CollectionConstants.SEATUNNEL_PLUGIN,
                        CollectionConstants.SINK_PLUGIN,
                        sinkConfig.getString(CollectionConstants.PLUGIN_NAME));
        List<URL> pluginJarPaths =
                sinkPluginDiscovery.getPluginJarAndDependencyPaths(
                        Lists.newArrayList(pluginIdentifier));
        return pluginJarPaths;
    }

    private ChangeStreamTableSourceCheckpoint getSourceCheckpoint(
            int sourceConfigIndex, String sourceFactoryId) {
        String sourceActionName =
                JobConfigParser.createSourceActionName(sourceConfigIndex, sourceFactoryId);
        JobPipelineCheckpointData pipelineCheckpointData =
                pipelineCheckpoints.get(sourceConfigIndex);
        Preconditions.checkArgument(
                pipelineCheckpointData.getPipelineId() == sourceConfigIndex + 1,
                String.format(
                        "The pipeline id in the checkpoint data is %d, but the config index is %d.",
                        pipelineCheckpointData.getPipelineId(), sourceConfigIndex + 1));

        List<JobPipelineCheckpointData.ActionState> sourceCheckpointData =
                pipelineCheckpointData.getTaskStates().entrySet().stream()
                        .filter(entry -> entry.getKey().contains(sourceActionName))
                        .map(e -> e.getValue())
                        .collect(Collectors.toList());
        Preconditions.checkArgument(
                sourceCheckpointData.size() == 1,
                String.format(
                        "The source action name %s is not found in the checkpoint keys %s.",
                        sourceActionName, pipelineCheckpointData.getTaskStates().keySet()));

        byte[] coordinatorState = sourceCheckpointData.get(0).getCoordinatorState().get(0);
        List<List<byte[]>> subtaskState =
                sourceCheckpointData.get(0).getSubtaskState().stream()
                        .flatMap(
                                (Function<
                                                JobPipelineCheckpointData.ActionSubtaskState,
                                                Stream<List<byte[]>>>)
                                        state ->
                                                state == null
                                                        ? Stream.of(Collections.emptyList())
                                                        : Stream.of(state.getState()))
                        .collect(Collectors.toList());
        return new ChangeStreamTableSourceCheckpoint(coordinatorState, subtaskState);
    }

    private Config handleDataSource(Config seaTunnelJobConfig, MetadataConfig metaDataConfig) {
        Config tempconfig = seaTunnelJobConfig;
        // Only resolve MetaData configs when:
        // 1. MetaData is enabled
        // 2. The job config contains metadata_datasource_id in any connector
        if (metaDataConfig != null
                && metaDataConfig.isEnabled()
                && hasDatasourceId(seaTunnelJobConfig)) {
            tempconfig =
                    MetadataProviderManager.resolveDataSourceConfigs(
                            seaTunnelJobConfig, metaDataConfig);
        }
        // Compatible with old code
        tempconfig = MetalakeConfigUtils.getMetalakeConfig(tempconfig);
        return tempconfig;
    }

    /**
     * Checks if the job config contains metadata_datasource_id in any connector configuration.
     *
     * @param config the SeaTunnel job configuration
     * @return true if any connector (source or sink) contains metadata_datasource_id, false
     *     otherwise
     */
    private boolean hasDatasourceId(Config config) {
        List<? extends Config> sourceConfigs =
                TypesafeConfigUtils.getConfigList(
                        config, PluginType.SOURCE.getType(), Collections.emptyList());
        for (Config sourceConfig : sourceConfigs) {
            if (hasDatasourceIdInConnector(sourceConfig)) {
                return true;
            }
        }

        List<? extends Config> sinkConfigs =
                TypesafeConfigUtils.getConfigList(
                        config, PluginType.SINK.getType(), Collections.emptyList());
        for (Config sinkConfig : sinkConfigs) {
            if (hasDatasourceIdInConnector(sinkConfig)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if a single connector config contains metadata_datasource_id.
     *
     * @param connectorConfig the connector configuration
     * @return true if metadata_datasource_id is present, false otherwise
     */
    private boolean hasDatasourceIdInConnector(Config connectorConfig) {
        try {
            // Check at root level
            if (connectorConfig.hasPath(ConnectorCommonOptions.METADATA_DATASOURCE_ID.key())) {
                return true;
            }

            // Check inside the nested connector config
            String connectorIdentifier = getConnectorIdentifier(connectorConfig);
            if (!"unknown".equals(connectorIdentifier)) {
                Config nestedConfig = connectorConfig.getConfig(connectorIdentifier);
                if (nestedConfig.hasPath(ConnectorCommonOptions.METADATA_DATASOURCE_ID.key())) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to check metadata_datasource_id in connector config", e);
        }
        return false;
    }

    /**
     * Gets the connector identifier (plugin name) from a connector config.
     *
     * @param config the connector configuration
     * @return the connector identifier or \”unknown\” if not found
     */
    private String getConnectorIdentifier(Config config) {
        try {
            if (config.hasPath(ConnectorCommonOptions.PLUGIN_NAME.key())) {
                return config.getString(ConnectorCommonOptions.PLUGIN_NAME.key());
            }
        } catch (Exception e) {
            // Ignore, try the nested structure approach
        }
        // Fallback: look for nested object structure
        for (Map.Entry<String, ConfigValue> entry : config.root().entrySet()) {
            if (entry.getValue().valueType() == ConfigValueType.OBJECT) {
                return entry.getKey();
            }
        }
        return "unknown";
    }
}
