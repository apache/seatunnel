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
import org.apache.seatunnel.api.env.ParsingMode;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportDataSaveMode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SinkConfig;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MultipleTableJobConfigParser {

    private static final ILogger LOGGER = Logger.getLogger(JobConfigParser.class);

    private final IdGenerator idGenerator;
    private final JobConfig jobConfig;

    private final List<URL> commonPluginJars;
    private final Config seaTunnelJobConfig;

    private final ReadonlyConfig envOptions;

    private final JobConfigParser fallbackParser;

    private final ParsingMode parsingMode;

    public MultipleTableJobConfigParser(
            String jobDefineFilePath, IdGenerator idGenerator, JobConfig jobConfig) {
        this(jobDefineFilePath, idGenerator, jobConfig, Collections.emptyList());
    }

    public MultipleTableJobConfigParser(
            String jobDefineFilePath,
            IdGenerator idGenerator,
            JobConfig jobConfig,
            List<URL> commonPluginJars) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.seaTunnelJobConfig = ConfigBuilder.of(Paths.get(jobDefineFilePath));
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.fallbackParser =
                new JobConfigParser(jobDefineFilePath, idGenerator, jobConfig, commonPluginJars);
        this.parsingMode = envOptions.get(EnvCommonOptions.DAG_PARSING_MODE);
    }

    public ImmutablePair<List<Action>, Set<URL>> parse() {
        if (parsingMode == ParsingMode.SINGLENESS) {
            return fallbackParser.parse();
        }
        List<URL> connectorJars = new ArrayList<>();
        try {
            connectorJars = FileUtils.searchJarFiles(Common.connectorJarDir("seatunnel"));
        } catch (IOException e) {
            LOGGER.info(e);
        }
        ClassLoader classLoader = new SeaTunnelChildFirstClassLoader(connectorJars);
        Thread.currentThread().setContextClassLoader(classLoader);
        // TODO: Support configuration transform
        List<? extends Config> sourceConfigs = seaTunnelJobConfig.getConfigList("source");
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        if (CollectionUtils.isEmpty(sourceConfigs) || CollectionUtils.isEmpty(sinkConfigs)) {
            throw new JobDefineCheckException("Source And Sink can not be null");
        }
        this.fillJobConfig();
        Map<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap = new HashMap<>();
        for (Config sourceConfig : sourceConfigs) {
            Tuple2<String, List<Tuple2<CatalogTable, Action>>> tuple2 =
                    parserSource(sourceConfig, classLoader);
            tableWithActionMap.put(tuple2._1(), tuple2._2());
        }
        List<Action> sinkActions = new ArrayList<>();
        for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
            Config sinkConfig = sinkConfigs.get(configIndex);
            sinkActions.addAll(
                    parserSink(configIndex, sinkConfig, classLoader, tableWithActionMap));
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
                .getOptional(EnvCommonOptions.CHECKPOINT_INTERVAL)
                .ifPresent(
                        interval ->
                                jobConfig
                                        .getEnvOptions()
                                        .put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), interval));
    }

    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parserSource(
            Config sourceConfig, ClassLoader classLoader) {
        List<CatalogTable> catalogTables =
                CatalogTableUtil.getCatalogTables(sourceConfig, classLoader);
        if (catalogTables.isEmpty()) {
            throw new JobDefineCheckException(
                    "The source needs catalog table, please configure `catalog` or `schema` options.");
        }
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        String factoryId = getFactoryId(readonlyConfig);
        String tableId =
                readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME).orElse("default");

        if (parsingMode == ParsingMode.SHARDING) {
            catalogTables = Collections.singletonList(catalogTables.get(0));
        }
        List<Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>>>
                sources =
                        FactoryUtil.createAndPrepareSource(
                                catalogTables, readonlyConfig, classLoader, factoryId);

        // get factory urls
        Set<URL> factoryUrls = new HashSet<>();
        URL factoryUrl =
                FactoryUtil.getFactoryUrl(
                        FactoryUtil.discoverFactory(
                                classLoader, TableSourceFactory.class, factoryId));
        factoryUrls.add(factoryUrl);
        getCatalogFactoryUrl(sourceConfig, classLoader).ifPresent(factoryUrls::add);

        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        int parallelism = getParallelism(readonlyConfig);
        for (int configIndex = 0; configIndex < sources.size(); configIndex++) {
            Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> tuple2 =
                    sources.get(configIndex);
            long id = idGenerator.getNextId();
            String actionName =
                    JobConfigParser.createSourceActionName(configIndex, factoryId, tableId);
            SourceAction<Object, SourceSplit, Serializable> action =
                    new SourceAction<>(id, actionName, tuple2._1(), factoryUrls);
            action.setParallelism(parallelism);
            for (CatalogTable catalogTable : tuple2._2()) {
                actions.add(new Tuple2<>(catalogTable, action));
            }
        }
        return new Tuple2<>(tableId, actions);
    }

    public static Optional<URL> getCatalogFactoryUrl(Config config, ClassLoader classLoader) {
        // catalog url
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(config);
        Map<String, String> catalogOptions =
                readonlyConfig.getOptional(CatalogOptions.CATALOG_OPTIONS).orElse(new HashMap<>());
        // TODO: fallback key
        String factoryId =
                catalogOptions.getOrDefault(
                        CommonOptions.FACTORY_ID.key(),
                        readonlyConfig.get(CommonOptions.PLUGIN_NAME));
        Optional<CatalogFactory> optionalFactory =
                FactoryUtil.discoverOptionalFactory(classLoader, CatalogFactory.class, factoryId);
        return optionalFactory.map(FactoryUtil::getFactoryUrl);
    }

    private int getParallelism(ReadonlyConfig config) {
        return Math.max(
                1,
                config.getOptional(CommonOptions.PARALLELISM)
                        .orElse(envOptions.get(CommonOptions.PARALLELISM)));
    }

    public List<SinkAction<?, ?, ?, ?>> parserSink(
            int configIndex,
            Config sinkConfig,
            ClassLoader classLoader,
            Map<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
        Map<TablePath, CatalogTable> tableMap =
                CatalogTableUtil.getCatalogTables(sinkConfig, classLoader).stream()
                        .collect(
                                Collectors.toMap(
                                        catalogTable -> catalogTable.getTableId().toTablePath(),
                                        catalogTable -> catalogTable));
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = getFactoryId(readonlyConfig);
        String leftTableId =
                readonlyConfig.getOptional(CommonOptions.SOURCE_TABLE_NAME).orElse("default");
        List<Tuple2<CatalogTable, Action>> tableTuples = tableWithActionMap.get(leftTableId);

        // get factory urls
        Set<URL> factoryUrls = new HashSet<>();
        URL factoryUrl =
                FactoryUtil.getFactoryUrl(
                        FactoryUtil.discoverFactory(
                                classLoader, TableSinkFactory.class, factoryId));
        factoryUrls.add(factoryUrl);
        getCatalogFactoryUrl(sinkConfig, classLoader).ifPresent(factoryUrls::add);

        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();
        for (Tuple2<CatalogTable, Action> tableTuple : tableTuples) {
            CatalogTable catalogTable = tableTuple._1();
            Action leftAction = tableTuple._2();
            Optional<CatalogTable> insteadTable;
            if (parsingMode == ParsingMode.SHARDING) {
                insteadTable = tableMap.values().stream().findFirst();
            } else {
                // TODO: another table full name map
                insteadTable =
                        Optional.ofNullable(tableMap.get(catalogTable.getTableId().toTablePath()));
            }
            if (insteadTable.isPresent()) {
                catalogTable = insteadTable.get();
            }
            SeaTunnelSink<?, ?, ?, ?> sink =
                    FactoryUtil.createAndPrepareSink(
                            catalogTable, readonlyConfig, classLoader, factoryId);
            SinkConfig actionConfig =
                    new SinkConfig(catalogTable.getTableId().toTablePath().toString());
            long id = idGenerator.getNextId();
            String actionName =
                    JobConfigParser.createSinkActionName(
                            configIndex,
                            factoryId,
                            String.format(
                                    "%s(%s)", leftTableId, actionConfig.getMultipleRowTableId()));
            SinkAction<?, ?, ?, ?> sinkAction =
                    new SinkAction<>(
                            id,
                            actionName,
                            Collections.singletonList(leftAction),
                            sink,
                            factoryUrls,
                            actionConfig);
            handleSaveMode(sink);
            sinkAction.setParallelism(leftAction.getParallelism());
            sinkActions.add(sinkAction);
        }
        return sinkActions;
    }

    private static void handleSaveMode(SeaTunnelSink<?, ?, ?, ?> sink) {
        if (SupportDataSaveMode.class.isAssignableFrom(sink.getClass())) {
            SupportDataSaveMode saveModeSink = (SupportDataSaveMode) sink;
            DataSaveMode dataSaveMode = saveModeSink.getDataSaveMode();
            saveModeSink.handleSaveMode(dataSaveMode);
        }
    }

    private static String getFactoryId(ReadonlyConfig readonlyConfig) {
        return readonlyConfig
                .getOptional(CommonOptions.FACTORY_ID)
                .orElse(readonlyConfig.get(CommonOptions.PLUGIN_NAME));
    }
}
