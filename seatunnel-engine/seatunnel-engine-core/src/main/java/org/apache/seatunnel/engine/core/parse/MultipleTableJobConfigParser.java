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
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.CollectionConstants;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getFactoryId;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getFactoryUrls;
import static org.apache.seatunnel.engine.core.parse.ConfigParserUtil.getInputIds;

@Slf4j
public class MultipleTableJobConfigParser {

    private final IdGenerator idGenerator;
    private final JobConfig jobConfig;

    private final List<URL> commonPluginJars;
    private final Config seaTunnelJobConfig;

    private final ReadonlyConfig envOptions;

    private final JobConfigParser fallbackParser;
    private final boolean isStartWithSavePoint;

    public MultipleTableJobConfigParser(
            String jobDefineFilePath, IdGenerator idGenerator, JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList(), false);
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig, IdGenerator idGenerator, JobConfig jobConfig) {
        this(seaTunnelJobConfig, idGenerator, jobConfig, Collections.emptyList(), false);
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = ConfigBuilder.of(Paths.get(jobDefineFilePath));
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.fallbackParser =
                new JobConfigParser(idGenerator, commonPluginJars, isStartWithSavePoint);
    }

    public MultipleTableJobConfigParser(
            Config seaTunnelJobConfig,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars,
            boolean isStartWithSavePoint) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.isStartWithSavePoint = isStartWithSavePoint;
        this.seaTunnelJobConfig = seaTunnelJobConfig;
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.fallbackParser =
                new JobConfigParser(idGenerator, commonPluginJars, isStartWithSavePoint);
    }

    public ImmutablePair<List<Action>, Set<URL>> parse() {
        List<? extends Config> sourceConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "source", Collections.emptyList());
        List<? extends Config> transformConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "transform", Collections.emptyList());
        List<? extends Config> sinkConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, "sink", Collections.emptyList());

        List<URL> connectorJars = getConnectorJarList(sourceConfigs, sinkConfigs);
        if (!commonPluginJars.isEmpty()) {
            connectorJars.addAll(commonPluginJars);
        }
        ClassLoader classLoader =
                new SeaTunnelChildFirstClassLoader(
                        connectorJars, Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(classLoader);

        ConfigParserUtil.checkGraph(sourceConfigs, transformConfigs, sinkConfigs);

        this.fillJobConfig();

        LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap =
                new LinkedHashMap<>();

        log.info("start generating all sources.");
        for (Config sourceConfig : sourceConfigs) {
            Tuple2<String, List<Tuple2<CatalogTable, Action>>> tuple2 =
                    parseSource(sourceConfig, classLoader);
            tableWithActionMap.put(tuple2._1(), tuple2._2());
        }

        log.info("start generating all transforms.");
        parseTransforms(transformConfigs, classLoader, tableWithActionMap);

        log.info("start generating all sinks.");
        List<Action> sinkActions = new ArrayList<>();
        for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
            Config sinkConfig = sinkConfigs.get(configIndex);
            sinkActions.addAll(parseSink(configIndex, sinkConfig, classLoader, tableWithActionMap));
        }
        Set<URL> factoryUrls = getUsedFactoryUrls(sinkActions);
        factoryUrls.addAll(commonPluginJars);
        sinkActions.forEach(this::addCommonPluginJarsToAction);
        return new ImmutablePair<>(sinkActions, factoryUrls);
    }

    public Set<URL> getUsedFactoryUrls(List<Action> sinkActions) {
        Set<URL> urls = new HashSet<>();
        fillUsedFactoryUrls(sinkActions, urls);
        return urls;
    }

    private List<URL> getConnectorJarList(
            List<? extends Config> sourceConfigs, List<? extends Config> sinkConfigs) {
        List<PluginIdentifier> factoryIds =
                Stream.concat(
                                sourceConfigs.stream()
                                        .map(ConfigParserUtil::getFactoryId)
                                        .map(
                                                factory ->
                                                        PluginIdentifier.of(
                                                                CollectionConstants
                                                                        .SEATUNNEL_PLUGIN,
                                                                CollectionConstants.SOURCE_PLUGIN,
                                                                factory)),
                                sinkConfigs.stream()
                                        .map(ConfigParserUtil::getFactoryId)
                                        .map(
                                                factory ->
                                                        PluginIdentifier.of(
                                                                CollectionConstants
                                                                        .SEATUNNEL_PLUGIN,
                                                                CollectionConstants.SINK_PLUGIN,
                                                                factory)))
                        .collect(Collectors.toList());
        return new SeaTunnelSinkPluginDiscovery().getPluginJarPaths(factoryIds);
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

    void addCommonPluginJarsToAction(Action action) {
        action.getJarUrls().addAll(commonPluginJars);
        if (!action.getUpstream().isEmpty()) {
            action.getUpstream().forEach(this::addCommonPluginJarsToAction);
        }
    }

    private void fillJobConfig() {
        jobConfig.getJobContext().setJobMode(envOptions.get(EnvCommonOptions.JOB_MODE));
        if (StringUtils.isEmpty(jobConfig.getName())
                || jobConfig.getName().equals(Constants.LOGO)) {
            jobConfig.setName(envOptions.get(EnvCommonOptions.JOB_NAME));
        }
        envOptions
                .toMap()
                .forEach(
                        (k, v) -> {
                            jobConfig.getEnvOptions().put(k, v);
                        });
    }

    private static <T extends Factory> boolean isFallback(
            ClassLoader classLoader,
            Class<T> factoryClass,
            String factoryId,
            Consumer<T> virtualCreator) {
        Optional<T> factory =
                FactoryUtil.discoverOptionalFactory(classLoader, factoryClass, factoryId);
        if (!factory.isPresent()) {
            return true;
        }
        try {
            virtualCreator.accept(factory.get());
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException
                    && "The Factory has not been implemented and the deprecated Plugin will be used."
                            .equals(e.getMessage())) {
                return true;
            }
        }
        return false;
    }

    private int getParallelism(ReadonlyConfig config) {
        return Math.max(
                1,
                config.getOptional(CommonOptions.PARALLELISM)
                        .orElse(envOptions.get(CommonOptions.PARALLELISM)));
    }

    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
            Config sourceConfig, ClassLoader classLoader) {
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        final String factoryId = getFactoryId(readonlyConfig);
        final String tableId =
                readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME).orElse(DEFAULT_ID);

        final int parallelism = getParallelism(readonlyConfig);

        boolean fallback =
                isFallback(
                        classLoader,
                        TableSourceFactory.class,
                        factoryId,
                        (factory) -> factory.createSource(null));

        if (fallback) {
            Tuple2<CatalogTable, Action> tuple =
                    fallbackParser.parseSource(sourceConfig, jobConfig, tableId, parallelism);
            return new Tuple2<>(tableId, Collections.singletonList(tuple));
        }

        List<Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>>>
                sources =
                        FactoryUtil.createAndPrepareSource(readonlyConfig, classLoader, factoryId);

        Set<URL> factoryUrls =
                getFactoryUrls(readonlyConfig, classLoader, TableSourceFactory.class, factoryId);

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        for (int configIndex = 0; configIndex < sources.size(); configIndex++) {
            Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> tuple2 =
                    sources.get(configIndex);
            long id = idGenerator.getNextId();
            String actionName =
                    JobConfigParser.createSourceActionName(configIndex, factoryId, tableId);
            SeaTunnelSource<Object, SourceSplit, Serializable> source = tuple2._1();
            source.setJobContext(jobConfig.getJobContext());
            PluginUtil.ensureJobModeMatch(jobConfig.getJobContext(), source);
            SourceAction<Object, SourceSplit, Serializable> action =
                    new SourceAction<>(id, actionName, tuple2._1(), factoryUrls);
            action.setParallelism(parallelism);
            for (CatalogTable catalogTable : tuple2._2()) {
                actions.add(new Tuple2<>(catalogTable, action));
            }
        }
        return new Tuple2<>(tableId, actions);
    }

    public void parseTransforms(
            List<? extends Config> transformConfigs,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        if (CollectionUtils.isEmpty(transformConfigs) || transformConfigs.size() == 0) {
            return;
        }
        Queue<Config> configList = new LinkedList<>(transformConfigs);
        while (!configList.isEmpty()) {
            parseTransform(configList, classLoader, tableWithActionMap);
        }
    }

    private void parseTransform(
            Queue<Config> transforms,
            ClassLoader classLoader,
            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        Config config = transforms.poll();
        final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        final String factoryId = getFactoryId(readonlyConfig);
        // get factory urls
        Set<URL> factoryUrls =
                getFactoryUrls(readonlyConfig, classLoader, TableTransformFactory.class, factoryId);
        final List<String> inputIds = getInputIds(readonlyConfig);

        List<Tuple2<CatalogTable, Action>> inputs =
                inputIds.stream()
                        .map(tableWithActionMap::get)
                        .filter(Objects::nonNull)
                        .peek(
                                input -> {
                                    if (input.size() > 1) {
                                        throw new JobDefineCheckException(
                                                "Adding transform to multi-table source is not supported.");
                                    }
                                })
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
                readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME).orElse(DEFAULT_ID);

        boolean fallback =
                isFallback(
                        classLoader,
                        TableTransformFactory.class,
                        factoryId,
                        (factory) -> factory.createTransform(null));

        Set<Action> inputActions =
                inputs.stream()
                        .map(Tuple2::_2)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        SeaTunnelDataType<?> expectedType = getProducedType(inputs.get(0)._2());
        checkProducedTypeEquals(inputActions);
        int spareParallelism = inputs.get(0)._2().getParallelism();
        int parallelism =
                readonlyConfig.getOptional(CommonOptions.PARALLELISM).orElse(spareParallelism);
        if (fallback) {
            Tuple2<CatalogTable, Action> tuple =
                    fallbackParser.parseTransform(
                            config,
                            jobConfig,
                            tableId,
                            parallelism,
                            (SeaTunnelRowType) expectedType,
                            inputActions);
            tableWithActionMap.put(tableId, Collections.singletonList(tuple));
            return;
        }

        CatalogTable catalogTable = inputs.get(0)._1();
        SeaTunnelTransform<?> transform =
                FactoryUtil.createAndPrepareTransform(
                        catalogTable, readonlyConfig, classLoader, factoryId);
        transform.setJobContext(jobConfig.getJobContext());
        long id = idGenerator.getNextId();
        String actionName =
                JobConfigParser.createTransformActionName(
                        0, factoryId, JobConfigParser.getTableName(config));

        TransformAction transformAction =
                new TransformAction(
                        id, actionName, new ArrayList<>(inputActions), transform, factoryUrls);
        transformAction.setParallelism(parallelism);
        tableWithActionMap.put(
                tableId,
                Collections.singletonList(
                        new Tuple2<>(transform.getProducedCatalogTable(), transformAction)));
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
            try {
                return ((TransformAction) action)
                        .getTransform()
                        .getProducedCatalogTable()
                        .getSeaTunnelRowType();
            } catch (UnsupportedOperationException e) {
                // TODO remove it when all connector use `getProducedCatalogTables`
                return ((TransformAction) action).getTransform().getProducedType();
            }
        }
        throw new UnsupportedOperationException();
    }

    public static void checkProducedTypeEquals(Set<Action> inputActions) {
        SeaTunnelDataType<?> expectedType = getProducedType(new ArrayList<>(inputActions).get(0));
        for (Action action : inputActions) {
            SeaTunnelDataType<?> producedType = getProducedType(action);
            if (!expectedType.equals(producedType)) {
                throw new JobDefineCheckException(
                        "Transform/Sink don't support processing data with two different structures.");
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
                            "Sink don't support simultaneous writing of data from multi-table source and other sources.");
                }
            }
        }

        boolean fallback =
                isFallback(
                        classLoader,
                        TableSinkFactory.class,
                        factoryId,
                        (factory) -> factory.createSink(null));
        if (fallback) {
            return fallbackParser.parseSinks(configIndex, inputVertices, sinkConfig, jobConfig);
        }

        // get factory urls
        Set<URL> factoryUrls =
                getFactoryUrls(readonlyConfig, classLoader, TableSinkFactory.class, factoryId);
        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();

        // union
        if (inputVertices.size() > 1) {
            Set<Action> inputActions =
                    inputVertices.stream()
                            .flatMap(Collection::stream)
                            .map(Tuple2::_2)
                            .collect(Collectors.toCollection(LinkedHashSet::new));
            checkProducedTypeEquals(inputActions);
            Tuple2<CatalogTable, Action> inputActionSample = inputVertices.get(0).get(0);
            SinkAction<?, ?, ?, ?> sinkAction =
                    createSinkAction(
                            inputActionSample._1(),
                            inputActions,
                            readonlyConfig,
                            classLoader,
                            factoryUrls,
                            factoryId,
                            inputActionSample._2().getParallelism(),
                            configIndex);
            sinkActions.add(sinkAction);
            return sinkActions;
        }

        // TODO move it into tryGenerateMultiTableSink when we don't support sink template
        // sink template
        for (Tuple2<CatalogTable, Action> tuple : inputVertices.get(0)) {
            SinkAction<?, ?, ?, ?> sinkAction =
                    createSinkAction(
                            tuple._1(),
                            Collections.singleton(tuple._2()),
                            readonlyConfig,
                            classLoader,
                            factoryUrls,
                            factoryId,
                            tuple._2().getParallelism(),
                            configIndex);
            sinkActions.add(sinkAction);
        }
        Optional<SinkAction<?, ?, ?, ?>> multiTableSink =
                tryGenerateMultiTableSink(sinkActions, readonlyConfig, classLoader);
        return multiTableSink
                .<List<SinkAction<?, ?, ?, ?>>>map(Collections::singletonList)
                .orElse(sinkActions);
    }

    private Optional<SinkAction<?, ?, ?, ?>> tryGenerateMultiTableSink(
            List<SinkAction<?, ?, ?, ?>> sinkActions,
            ReadonlyConfig options,
            ClassLoader classLoader) {
        if (sinkActions.stream()
                .anyMatch(action -> !(action.getSink() instanceof SupportMultiTableSink))) {
            log.info("Unsupported multi table sink api, rollback to sink template");
            return Optional.empty();
        }
        Map<String, SeaTunnelSink> sinks = new HashMap<>();
        Set<URL> jars =
                sinkActions.stream()
                        .flatMap(a -> a.getJarUrls().stream())
                        .collect(Collectors.toSet());
        sinkActions.forEach(
                action -> {
                    SeaTunnelSink sink = action.getSink();
                    String tableId = action.getConfig().getMultipleRowTableId();
                    sinks.put(tableId, sink);
                });
        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createMultiTableSink(sinks, options, classLoader);
        SinkAction<?, ?, ?, ?> multiTableAction =
                new SinkAction<>(
                        idGenerator.getNextId(),
                        "MultiTableSink-" + sinkActions.get(0).getSink().getPluginName(),
                        sinkActions.get(0).getUpstream(),
                        sink,
                        jars);
        multiTableAction.setParallelism(sinkActions.get(0).getParallelism());
        return Optional.of(multiTableAction);
    }

    private SinkAction<?, ?, ?, ?> createSinkAction(
            CatalogTable catalogTable,
            Set<Action> inputActions,
            ReadonlyConfig readonlyConfig,
            ClassLoader classLoader,
            Set<URL> factoryUrls,
            String factoryId,
            int parallelism,
            int configIndex) {
        SeaTunnelSink<?, ?, ?, ?> sink =
                FactoryUtil.createAndPrepareSink(
                        catalogTable, readonlyConfig, classLoader, factoryId);
        sink.setJobContext(jobConfig.getJobContext());
        SinkConfig actionConfig =
                new SinkConfig(catalogTable.getTableId().toTablePath().toString());
        long id = idGenerator.getNextId();
        String actionName =
                JobConfigParser.createSinkActionName(
                        configIndex, factoryId, actionConfig.getMultipleRowTableId());
        SinkAction<?, ?, ?, ?> sinkAction =
                new SinkAction<>(
                        id,
                        actionName,
                        new ArrayList<>(inputActions),
                        sink,
                        factoryUrls,
                        actionConfig);
        if (!isStartWithSavePoint) {
            handleSaveMode(sink);
        }
        sinkAction.setParallelism(parallelism);
        return sinkAction;
    }

    public static void handleSaveMode(SeaTunnelSink<?, ?, ?, ?> sink) {
        if (SupportDataSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportDataSaveMode saveModeSink = (SupportDataSaveMode) sink;
            DataSaveMode dataSaveMode = saveModeSink.getUserConfigSaveMode();
            saveModeSink.handleSaveMode(dataSaveMode);
        }
    }
}
