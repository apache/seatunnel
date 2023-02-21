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

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.env.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.Serializable;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

public class MultipleTableJobConfigParser {

    private static final ILogger LOGGER = Logger.getLogger(JobConfigParser.class);

    private final IdGenerator idGenerator;
    private final JobConfig jobConfig;

    private final List<URL> commonPluginJars;
    private final Config seaTunnelJobConfig;

    private final ReadonlyConfig envOptions;

    private final Map<String, List<Tuple2<CatalogTable, Action>>> graph;

    private final Set<URL> jarUrls;

    private final JobConfigParser fallbackParser;

    public MultipleTableJobConfigParser(String jobDefineFilePath,
                                        IdGenerator idGenerator,
                                        JobConfig jobConfig) {
        this(jobDefineFilePath,
            idGenerator,
            jobConfig,
            Collections.emptyList());
    }

    public MultipleTableJobConfigParser(String jobDefineFilePath,
                                        IdGenerator idGenerator,
                                        JobConfig jobConfig,
                                        List<URL> commonPluginJars) {
        this.idGenerator = idGenerator;
        this.jobConfig = jobConfig;
        this.commonPluginJars = commonPluginJars;
        this.seaTunnelJobConfig = ConfigBuilder.of(Paths.get(jobDefineFilePath));
        this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
        this.graph = new HashMap<>();
        this.jarUrls = new HashSet<>();
        this.fallbackParser = new JobConfigParser(jobDefineFilePath, idGenerator, jobConfig, commonPluginJars);
    }

    public ImmutablePair<List<Action>, Set<URL>> parse() {
        if (!envOptions.get(EnvCommonOptions.MULTIPLE_TABLE_ENABLE)) {
            return fallbackParser.parse();
        }
        ClassLoader classLoader = new SeaTunnelChildFirstClassLoader(new ArrayList<>());
        Thread.currentThread().setContextClassLoader(classLoader);
        // TODO: Support configuration transform
        List<? extends Config> sourceConfigs = seaTunnelJobConfig.getConfigList("source");
        List<? extends Config> sinkConfigs = seaTunnelJobConfig.getConfigList("sink");
        if (CollectionUtils.isEmpty(sourceConfigs) || CollectionUtils.isEmpty(sinkConfigs)) {
            throw new JobDefineCheckException("Source And Sink can not be null");
        }
        this.fillJobConfig();
        for (Config sourceConfig : sourceConfigs) {
            parserSource(sourceConfig, classLoader);
        }
        List<Action> sinkActions = new ArrayList<>();
        for (Config sinkConfig : sinkConfigs) {
            sinkActions.addAll(parserSink(sinkConfig, classLoader));
        }
        sinkActions.forEach(this::addCommonPluginJarsToAction);
        jarUrls.addAll(commonPluginJars);
        return new ImmutablePair<>(sinkActions, null);
    }

    void addCommonPluginJarsToAction(Action action) {
        action.getJarUrls().addAll(commonPluginJars);
        if (!action.getUpstream().isEmpty()) {
            action.getUpstream().forEach(this::addCommonPluginJarsToAction);
        }
    }

    private void fillJobConfig() {
        jobConfig.getJobContext().setJobMode(envOptions.get(EnvCommonOptions.JOB_MODE));
        if (StringUtils.isEmpty(jobConfig.getName())) {
            jobConfig.setName(envOptions.get(EnvCommonOptions.JOB_NAME));
        }
        envOptions.getOptional(EnvCommonOptions.CHECKPOINT_INTERVAL)
            .ifPresent(interval -> jobConfig.getEnvOptions()
                .put(EnvCommonOptions.CHECKPOINT_INTERVAL.key(), interval));
    }

    public void parserSource(Config sourceConfig, ClassLoader classLoader) {
        List<CatalogTable> catalogTables = CatalogTableUtil.getCatalogTables(sourceConfig, classLoader);
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        String factoryId = readonlyConfig.getOptional(CommonOptions.FACTORY_ID).orElse(readonlyConfig.get(CommonOptions.PLUGIN_NAME));
        String tableId = readonlyConfig.getOptional(CommonOptions.RESULT_TABLE_NAME).orElse("default");

        List<Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>>> sources =
            FactoryUtil.createAndPrepareSource(catalogTables, readonlyConfig, classLoader, factoryId);
        // TODO: get factory jar
        Set<URL> factoryUrls = new HashSet<>();
        List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
        int parallelism = getParallelism(readonlyConfig);
        for (Tuple2<SeaTunnelSource<Object, SourceSplit, Serializable>, List<CatalogTable>> tuple2 : sources) {
            long id = idGenerator.getNextId();
            SourceAction<Object, SourceSplit, Serializable> action = new SourceAction<>(id, factoryId, tuple2._1(), factoryUrls);
            action.setParallelism(parallelism);
            for (CatalogTable catalogTable : tuple2._2()) {
                actions.add(new Tuple2<>(catalogTable, action));
            }
        }
        graph.put(tableId, actions);
    }

    private int getParallelism(ReadonlyConfig config) {
        return Math.max(1,
            config.getOptional(CommonOptions.PARALLELISM)
                .orElse(envOptions.get(CommonOptions.PARALLELISM)));
    }

    public List<SinkAction<?, ?, ?, ?>> parserSink(Config sinkConfig, ClassLoader classLoader) {
        Map<TablePath, CatalogTable> tableMap = CatalogTableUtil.getCatalogTables(sinkConfig, classLoader)
            .stream()
            .collect(Collectors.toMap(catalogTable -> catalogTable.getTableId().toTablePath(), catalogTable -> catalogTable));
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = readonlyConfig.getOptional(CommonOptions.FACTORY_ID).orElse(readonlyConfig.get(CommonOptions.PLUGIN_NAME));
        String leftTableId = readonlyConfig.getOptional(CommonOptions.SOURCE_TABLE_NAME).orElse("default");
        List<Tuple2<CatalogTable, Action>> tableTuples = graph.get(leftTableId);
        // TODO: get factory jar
        Set<URL> factoryUrls = new HashSet<>();
        List<SinkAction<?, ?, ?, ?>> sinkActions = new ArrayList<>();
        for (Tuple2<CatalogTable, Action> tableTuple : tableTuples) {
            CatalogTable catalogTable = tableTuple._1();
            Action leftAction = tableTuple._2();
            // TODO: another table full name map
            CatalogTable insteadTable = tableMap.get(catalogTable.getTableId().toTablePath());
            if (insteadTable != null) {
                catalogTable = insteadTable;
            }
            SeaTunnelSink<?, ?, ?, ?> sink = FactoryUtil.createAndPrepareSink(catalogTable, readonlyConfig, classLoader, factoryId);
            long id = idGenerator.getNextId();
            SinkAction<?, ?, ?, ?> sinkAction = new SinkAction<>(id, factoryId, Collections.singletonList(leftAction), sink, factoryUrls);
            sinkAction.setParallelism(leftAction.getParallelism());
            sinkActions.add(sinkAction);
        }
        return sinkActions;
    }
}
