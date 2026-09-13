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

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.core.starter.utils.ConfigBuilder;
import org.apache.seatunnel.engine.common.config.DryRunSampleConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.exception.JobDefineCheckException;
import org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.parse.MultipleTableJobConfigParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class MultipleTableJobConfigParserTest {

    @Test
    public void testRejectsCyclicTransformDependenciesWithoutRetryingIndefinitely() {
        Common.setDeployMode(DeployMode.CLIENT);
        Config config =
                ConfigFactory.parseString(
                        "env { execution.parallelism = 1, job.mode = BATCH }\n"
                                + "source { FakeSource { plugin_output = src, schema = { fields { name = string } } } }\n"
                                + "transform {\n"
                                + "  sql { plugin_input = [t2], plugin_output = t1, query = \"select 1 from dual\" }\n"
                                + "  sql { plugin_input = [t1], plugin_output = t2, query = \"select 1 from dual\" }\n"
                                + "}\n"
                                + "sink { console { plugin_input = [src] } }");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser parser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);

        JobDefineCheckException exception =
                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(5),
                        () ->
                                Assertions.assertThrows(
                                        JobDefineCheckException.class, () -> parser.parse(null)));

        Assertions.assertTrue(exception.getMessage().contains("Transform dependency cycle"));
        Assertions.assertTrue(exception.getMessage().contains("t1 -> t2 -> t1"));
    }

    @Test
    public void testParseTransformsFailsWhenNoDependencyCanBeResolved() {
        Config config =
                ConfigFactory.parseString(
                        "env { execution.parallelism = 1, job.mode = BATCH }\n"
                                + "transform {\n"
                                + "  sql { plugin_input = [t2], plugin_output = t1, query = \"select 1 from dual\" }\n"
                                + "  sql { plugin_input = [t1], plugin_output = t2, query = \"select 1 from dual\" }\n"
                                + "}");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser parser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);

        JobDefineCheckException exception =
                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(5),
                        () ->
                                Assertions.assertThrows(
                                        JobDefineCheckException.class,
                                        () ->
                                                parser.parseTransforms(
                                                        config.getConfigList("transform"),
                                                        Thread.currentThread()
                                                                .getContextClassLoader(),
                                                        new LinkedHashMap<>())));

        Assertions.assertTrue(
                exception.getMessage().contains("Unable to resolve transform dependencies"));
        Assertions.assertTrue(exception.getMessage().contains("t1 <- [t2]"));
        Assertions.assertTrue(exception.getMessage().contains("t2 <- [t1]"));
    }

    @Test
    public void testParsesTransformsDeclaredOutOfDependencyOrder() {
        Common.setDeployMode(DeployMode.CLIENT);
        Config config =
                ConfigFactory.parseString(
                        "env { execution.parallelism = 1, job.mode = BATCH }\n"
                                + "source { FakeSource { plugin_output = src, schema = { fields { name = string } } } }\n"
                                + "transform {\n"
                                + "  sql { plugin_input = [src,t1,t1], plugin_output = t2, query = \"select * from dual\" }\n"
                                + "  sql { plugin_input = [src], plugin_output = t1, query = \"select * from dual\" }\n"
                                + "}\n"
                                + "sink { console { plugin_input = [t2] } }");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());

        ImmutablePair<List<Action>, Set<URL>> parsed =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig).parse(null);

        Assertions.assertEquals(1, parsed.getLeft().size());
        Action sink = parsed.getLeft().get(0);
        Assertions.assertEquals(1, sink.getUpstream().size());
        Action terminalTransform = sink.getUpstream().get(0);
        Assertions.assertEquals("Transform[2]-sql", terminalTransform.getName());
        Assertions.assertEquals(2, terminalTransform.getUpstream().size());
        Assertions.assertEquals(
                "Transform[1]-sql", terminalTransform.getUpstream().get(1).getName());
    }

    @Test
    public void testParseTransformsRejectsPartiallyResolvedInputsBeforePluginDiscovery() {
        Config config =
                ConfigFactory.parseString(
                        "env { execution.parallelism = 1, job.mode = BATCH }\n"
                                + "transform {\n"
                                + "  NonExistentTransform { plugin_input = [src,missing], plugin_output = t1 }\n"
                                + "}");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser parser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActions =
                sourceActionMap();

        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class,
                        () ->
                                parser.parseTransforms(
                                        config.getConfigList("transform"),
                                        Thread.currentThread().getContextClassLoader(),
                                        tableWithActions));

        Assertions.assertTrue(
                exception.getMessage().contains("Unable to resolve transform dependencies"));
        Assertions.assertTrue(exception.getMessage().contains("t1 <- [src, missing]"));
    }

    @Test
    public void testTransformEvaluationOrderKeepsLegacySinkFallbackOnTerminalTransform() {
        Config config =
                ConfigFactory.parseString(
                        "env { execution.parallelism = 1, job.mode = BATCH }\n"
                                + "transform {\n"
                                + "  sql { plugin_input = [t1], plugin_output = t2, query = \"select * from dual\" }\n"
                                + "  sql { plugin_input = [src], plugin_output = t1, query = \"select * from dual\" }\n"
                                + "}\n"
                                + "sink { console {} }");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser parser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActions =
                sourceActionMap();

        parser.parseTransforms(
                config.getConfigList("transform"),
                Thread.currentThread().getContextClassLoader(),
                tableWithActions);

        Assertions.assertEquals(
                Arrays.asList("src", "t1", "t2"), new ArrayList<>(tableWithActions.keySet()));
        List<SinkAction<?, ?, ?, ?>> sinks =
                parser.parseSink(
                        0,
                        config.getConfigList("sink").get(0),
                        Thread.currentThread().getContextClassLoader(),
                        tableWithActions);
        Assertions.assertEquals(1, sinks.size());
        Assertions.assertEquals("Transform[2]-sql", sinks.get(0).getUpstream().get(0).getName());
    }

    @Test
    public void testExplicitEmptyInputTransformUsesResolvedTerminalUpstream() {
        Common.setDeployMode(DeployMode.CLIENT);
        Config config =
                ConfigFactory.parseString(
                        "env { execution.parallelism = 1, job.mode = BATCH }\n"
                                + "source { FakeSource { plugin_output = src, schema = { fields { name = string } } } }\n"
                                + "transform {\n"
                                + "  sql { plugin_input = [src], plugin_output = t1, query = \"select * from dual\" }\n"
                                + "  sql { plugin_input = [], plugin_output = t2, query = \"select * from dual\" }\n"
                                + "}\n"
                                + "sink {\n"
                                + "  console { plugin_input = [t1] }\n"
                                + "  console { plugin_input = [t2] }\n"
                                + "}");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());

        ImmutablePair<List<Action>, Set<URL>> parsed =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig).parse(null);

        Assertions.assertEquals(2, parsed.getLeft().size());
        Action terminalTransform = parsed.getLeft().get(1).getUpstream().get(0);
        Assertions.assertEquals("Transform[1]-sql", terminalTransform.getName());
        Assertions.assertEquals(1, terminalTransform.getUpstream().size());
        Assertions.assertEquals(
                "Transform[0]-sql", terminalTransform.getUpstream().get(0).getName());
    }

    @Test
    public void testSampleDryRunReplacesConfiguredSink() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fake_to_console_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        DryRunSampleConfig.configure(jobConfig, 10, false);

        ImmutablePair<List<Action>, Set<URL>> parsed =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig)
                        .parse(null);

        Assertions.assertFalse(parsed.getLeft().isEmpty());
        parsed.getLeft()
                .forEach(
                        action -> {
                            Assertions.assertInstanceOf(SinkAction.class, action);
                            Assertions.assertEquals(
                                    "dry-run-sample",
                                    ((SinkAction<?, ?, ?, ?>) action).getSink().getPluginName());
                            Assertions.assertEquals(1, action.getParallelism());
                        });
        Assertions.assertFalse(jobConfig.getJobContext().isEnableCheckpoint());
    }

    @Test
    public void testSampleDryRunUsesSingleParallelismForConfiguredSources() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        DryRunSampleConfig.configure(jobConfig, 10, false);

        ImmutablePair<List<Action>, Set<URL>> parsed =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig)
                        .parse(null);

        Assertions.assertEquals(1, parsed.getLeft().size());
        Action sinkAction = parsed.getLeft().get(0);
        Assertions.assertEquals(1, sinkAction.getParallelism());
        Assertions.assertEquals(2, sinkAction.getUpstream().size());
        sinkAction
                .getUpstream()
                .forEach(sourceAction -> Assertions.assertEquals(1, sourceAction.getParallelism()));
    }

    @Test
    public void testSampleDryRunUsesSingleParallelismForConfiguredTransform() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource(
                        "/batch_fake_to_console_with_transform_name.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        DryRunSampleConfig.configure(jobConfig, 10, false);

        ImmutablePair<List<Action>, Set<URL>> parsed =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig)
                        .parse(null);

        Assertions.assertEquals(1, parsed.getLeft().size());
        Action transformAction = parsed.getLeft().get(0).getUpstream().get(0);
        Assertions.assertEquals("t_sql_named", transformAction.getName());
        Assertions.assertEquals(1, transformAction.getParallelism());
    }

    @Test
    public void testUserEnvCannotEnableSampleDryRun() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fake_to_console_multi_table.conf");
        Config baseConfig = ConfigBuilder.of(Paths.get(filePath));
        Config config =
                ConfigFactory.parseString("env { __seatunnel_dry_run_sample = true }")
                        .withFallback(baseConfig);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());

        ImmutablePair<List<Action>, Set<URL>> parsed =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig).parse(null);

        Assertions.assertFalse(
                DryRunSampleConfig.isEnabled(jobConfig.getEnvOptions()),
                "User env options must not activate sample mode");
        parsed.getLeft()
                .forEach(
                        action ->
                                Assertions.assertNotEquals(
                                        "dry-run-sample",
                                        ((SinkAction<?, ?, ?, ?>) action)
                                                .getSink()
                                                .getPluginName()));
    }

    @Test
    public void testSimpleJobParse() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = ContentFormatUtilTest.getResource("/batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals("Sink[0]-LocalFile-MultiTableSink", actions.get(0).getName());
        Assertions.assertEquals(1, actions.get(0).getUpstream().size());
        Assertions.assertEquals(
                "Source[0]-FakeSource", actions.get(0).getUpstream().get(0).getName());

        Assertions.assertFalse(jobConfig.getJobContext().isEnableCheckpoint());
        Assertions.assertEquals(3, actions.get(0).getUpstream().get(0).getParallelism());
        Assertions.assertEquals(3, actions.get(0).getParallelism());
    }

    @Test
    public void testComplexJobParse() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fakesource_to_file_complex.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());

        Assertions.assertTrue(jobConfig.getJobContext().isEnableCheckpoint());
        Assertions.assertEquals("Sink[0]-LocalFile-fake", actions.get(0).getName());
        Assertions.assertEquals(2, actions.get(0).getUpstream().size());

        String[] expected = {"Source[0]-FakeSource", "Source[1]-FakeSource"};
        String[] actual = {
            actions.get(0).getUpstream().get(0).getName(),
            actions.get(0).getUpstream().get(1).getName()
        };

        Arrays.sort(expected);
        Arrays.sort(actual);

        Assertions.assertArrayEquals(expected, actual);

        Assertions.assertEquals(3, actions.get(0).getUpstream().get(0).getParallelism());
        Assertions.assertEquals(3, actions.get(0).getUpstream().get(1).getParallelism());
        Assertions.assertEquals(3, actions.get(0).getParallelism());
    }

    @Test
    public void testMultipleSinkName() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = ContentFormatUtilTest.getResource("/batch_fakesource_to_two_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(2, actions.size());

        // This is union sink
        Assertions.assertEquals("Sink[0]-LocalFile-fake", actions.get(0).getName());

        // This is multiple table sink
        Assertions.assertEquals("Sink[1]-LocalFile-MultiTableSink", actions.get(1).getName());
    }

    @Test
    public void testMultipleTableSourceWithMultiTableSinkParse() throws IOException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fake_to_console_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config config = ConfigBuilder.of(Paths.get(filePath));
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals("Sink[0]-console-MultiTableSink", actions.get(0).getName());
        Assertions.assertFalse(
                ((SinkAction) actions.get(0)).getSink().createCommitter().isPresent());
        Assertions.assertFalse(
                ((SinkAction) actions.get(0)).getSink().createAggregatedCommitter().isPresent());
    }

    @Test
    public void testDuplicatedTransformInOnePipeline() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource(
                        "/batch_fake_to_console_with_duplicated_transform.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config config = ConfigBuilder.of(Paths.get(filePath));
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals("Transform[0]-sql", actions.get(0).getUpstream().get(0).getName());
        Assertions.assertEquals("Transform[1]-sql", actions.get(1).getUpstream().get(0).getName());
    }

    @Test
    public void testTransformNameOverride() throws IOException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource(
                        "/batch_fake_to_console_with_transform_name.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config config = ConfigBuilder.of(Paths.get(filePath));
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertEquals("t_sql_named", actions.get(0).getUpstream().get(0).getName());
        Assertions.assertEquals(4, actions.get(0).getUpstream().get(0).getParallelism());
    }

    @Test
    public void testCreateDifferentClassLoader() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = ContentFormatUtilTest.getResource("/batch_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext(System.currentTimeMillis()));
        final ClassLoader[] classLoaders = new ClassLoader[3];
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(filePath, new IdGenerator(), jobConfig) {
                    @Override
                    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
                            int configIndex, Config sourceConfig, ClassLoader classLoader) {
                        classLoaders[0] = classLoader;
                        return super.parseSource(configIndex, sourceConfig, classLoader);
                    }

                    @Override
                    public void parseTransforms(
                            List<? extends Config> transformConfigs,
                            ClassLoader classLoader,
                            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>>
                                    tableWithActionMap) {
                        classLoaders[1] = classLoader;
                        super.parseTransforms(transformConfigs, classLoader, tableWithActionMap);
                    }

                    @Override
                    public List<SinkAction<?, ?, ?, ?>> parseSink(
                            int configIndex,
                            Config sinkConfig,
                            ClassLoader classLoader,
                            LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>>
                                    tableWithActionMap) {
                        classLoaders[2] = classLoader;
                        return super.parseSink(
                                configIndex, sinkConfig, classLoader, tableWithActionMap);
                    }
                };
        AtomicInteger getClassLoaderTimes = new AtomicInteger();
        AtomicInteger releaseClassLoaderTimes = new AtomicInteger();
        jobConfigParser.parse(
                new ClassLoaderService() {
                    @Override
                    public ClassLoader getClassLoader(long jobId, Collection<URL> jars) {
                        getClassLoaderTimes.getAndIncrement();
                        return new SeaTunnelChildFirstClassLoader(jars);
                    }

                    @Override
                    public void releaseClassLoader(long jobId, Collection<URL> jars) {
                        releaseClassLoaderTimes.getAndIncrement();
                    }

                    @Override
                    public void close() {}
                });
        Assertions.assertEquals(2, getClassLoaderTimes.get());
        Assertions.assertEquals(2, releaseClassLoaderTimes.get());
        Assertions.assertEquals(classLoaders[0], classLoaders[1]);
        Assertions.assertNotEquals(classLoaders[0], classLoaders[2]);
        Assertions.assertNotEquals(classLoaders[1], classLoaders[2]);
    }

    @Test
    public void testMultipleTableJobConfigWithEnvOptionCheck() {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource(
                        "/batch_fake_to_console_with_error_env_option.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config config = ConfigBuilder.of(Paths.get(filePath));

        Exception checkExp = null;
        try {
            new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig);
        } catch (Exception e) {
            checkExp = e;
        }
        Assertions.assertInstanceOf(IllegalArgumentException.class, checkExp);
    }

    @Test
    public void testSkipFailedTableWhenContinueOtherTablesEnabled() throws IOException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fake_to_console_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config baseConfig = ConfigBuilder.of(Paths.get(filePath));
        Config config =
                ConfigFactory.parseString(
                                "env { multi_table { failure_policy = \"CONTINUE_OTHER_TABLES\" } }")
                        .withFallback(baseConfig);
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig) {
                    @Override
                    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
                            int configIndex, Config sourceConfig, ClassLoader classLoader) {
                        Tuple2<String, List<Tuple2<CatalogTable, Action>>> parsedSource =
                                super.parseSource(configIndex, sourceConfig, classLoader);
                        Action action = parsedSource._2().get(0)._2();
                        return new Tuple2<>(
                                parsedSource._1(),
                                Arrays.asList(
                                        new Tuple2<>(mockCatalogTable("table1"), action),
                                        new Tuple2<>(mockCatalogTable("table2"), action),
                                        new Tuple2<>(mockCatalogTable("table3"), action)));
                    }

                    @Override
                    protected Optional<SinkAction<?, ?, ?, ?>> createSinkAction(
                            CatalogTable catalogTable,
                            Set<Action> inputActions,
                            org.apache.seatunnel.api.configuration.ReadonlyConfig readonlyConfig,
                            ClassLoader classLoader,
                            Set<URL> factoryUrls,
                            Set<org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier>
                                    connectorJarIdentifiers,
                            String factoryId,
                            int parallelism,
                            int configIndex) {
                        if (catalogTable.getTablePath().getTableName().equals("table2")) {
                            return Optional.empty();
                        }
                        return super.createSinkAction(
                                catalogTable,
                                inputActions,
                                readonlyConfig,
                                classLoader,
                                factoryUrls,
                                connectorJarIdentifiers,
                                factoryId,
                                parallelism,
                                configIndex);
                    }
                };

        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertInstanceOf(SinkAction.class, actions.get(0));
        Assertions.assertInstanceOf(MultiTableSink.class, ((SinkAction) actions.get(0)).getSink());
        MultiTableSink multiTableSink = (MultiTableSink) ((SinkAction) actions.get(0)).getSink();
        Assertions.assertEquals(
                2, multiTableSink.getSinks().size(), multiTableSink.getSinks().keySet().toString());
        Assertions.assertFalse(
                multiTableSink.getSinks().keySet().stream()
                        .anyMatch(tablePath -> tablePath.getTableName().equals("table2")));
    }

    @Test
    public void testFailWhenNoSourceTablesRemainAfterDiscovery() throws IOException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fake_to_console_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config baseConfig = ConfigBuilder.of(Paths.get(filePath));
        Config config =
                ConfigFactory.parseString(
                                "env { multi_table { failure_policy = \"CONTINUE_OTHER_TABLES\" } }")
                        .withFallback(baseConfig);
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig) {
                    @Override
                    public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
                            int configIndex, Config sourceConfig, ClassLoader classLoader) {
                        return new Tuple2<>("fake", Collections.emptyList());
                    }
                };

        JobDefineCheckException exception =
                Assertions.assertThrows(
                        JobDefineCheckException.class, () -> jobConfigParser.parse(null));
        Assertions.assertTrue(exception.getMessage().contains("No source tables were available"));
    }

    @Test
    public void testSourceDiscoveryFailedTableIsPropagatedToMultiTableSink() throws IOException {
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath =
                ContentFormatUtilTest.getResource("/batch_fake_to_console_multi_table.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext());
        Config baseConfig = ConfigBuilder.of(Paths.get(filePath));
        Config config =
                ConfigFactory.parseString(
                                "env { multi_table { failure_policy = \"CONTINUE_OTHER_TABLES\" } }")
                        .withFallback(baseConfig);
        MultipleTableJobConfigParser jobConfigParser =
                new MultipleTableJobConfigParser(config, new IdGenerator(), jobConfig) {
                    @Override
                    protected Tuple2<
                                    SeaTunnelSource<Object, SourceSplit, Serializable>,
                                    List<CatalogTable>>
                            createAndPrepareSource(
                                    int configIndex,
                                    ReadonlyConfig readonlyConfig,
                                    ClassLoader classLoader,
                                    String factoryId,
                                    Function<PluginIdentifier, SeaTunnelSource>
                                            fallbackCreateSource) {
                        MultiTableFailureHelper.recordFailedTable(
                                MultiTableFailureHelper.buildFailedTable(
                                        "test_db.test_schema.table2",
                                        MultiTableFailurePhase.DISCOVERY,
                                        factoryId,
                                        new RuntimeException("metadata read failed")));
                        return new Tuple2<>(
                                new TestSeaTunnelSource(),
                                Collections.singletonList(mockCatalogTable("table1")));
                    }
                };

        ImmutablePair<List<Action>, Set<URL>> parse = jobConfigParser.parse(null);
        List<Action> actions = parse.getLeft();
        Assertions.assertEquals(1, actions.size());
        Assertions.assertInstanceOf(SinkAction.class, actions.get(0));
        Assertions.assertInstanceOf(MultiTableSink.class, ((SinkAction) actions.get(0)).getSink());
        MultiTableSink multiTableSink = (MultiTableSink) ((SinkAction) actions.get(0)).getSink();
        Assertions.assertEquals(1, multiTableSink.getSinks().size());
        Assertions.assertEquals(1, multiTableSink.getInitialFailedTables().size());
        MultiTableFailedTable failedTable = multiTableSink.getInitialFailedTables().get(0);
        Assertions.assertEquals("test_db.test_schema.table2", failedTable.getTablePath());
        Assertions.assertEquals(MultiTableFailurePhase.DISCOVERY, failedTable.getPhase());
        Assertions.assertTrue(failedTable.getMessageSummary().contains("metadata read failed"));
    }

    private LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> sourceActionMap() {
        CatalogTable catalogTable = mockCatalogTable("source_table");
        SourceAction<Object, SourceSplit, Serializable> sourceAction =
                new SourceAction<>(
                        1L,
                        "source",
                        new TestSeaTunnelSource(catalogTable),
                        Collections.emptySet(),
                        Collections.emptySet());
        LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActions =
                new LinkedHashMap<>();
        tableWithActions.put(
                "src", Collections.singletonList(new Tuple2<>(catalogTable, sourceAction)));
        return tableWithActions;
    }

    private static CatalogTable mockCatalogTable(String tableName) {
        return CatalogTable.of(
                TableIdentifier.of(
                        "test_catalog", TablePath.of("test_db", "test_schema", tableName)),
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 0L, true, null, null))
                        .build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static class TestSeaTunnelSource
            implements SeaTunnelSource<Object, SourceSplit, Serializable> {

        private final CatalogTable catalogTable;

        private TestSeaTunnelSource() {
            this(mockCatalogTable("test_table"));
        }

        private TestSeaTunnelSource(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<Object, SourceSplit> createReader(SourceReader.Context readerContext) {
            return null;
        }

        @Override
        public SourceSplitEnumerator<SourceSplit, Serializable> createEnumerator(
                SourceSplitEnumerator.Context<SourceSplit> enumeratorContext) {
            return null;
        }

        @Override
        public SourceSplitEnumerator<SourceSplit, Serializable> restoreEnumerator(
                SourceSplitEnumerator.Context<SourceSplit> enumeratorContext,
                Serializable checkpointState) {
            return null;
        }

        @Override
        public String getPluginName() {
            return "TestSource";
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(catalogTable);
        }
    }
}
