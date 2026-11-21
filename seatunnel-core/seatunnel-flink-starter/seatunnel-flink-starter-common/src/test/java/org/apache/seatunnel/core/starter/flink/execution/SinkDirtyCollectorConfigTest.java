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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.NoOpDirtyRecordCollector;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.streaming.api.datastream.DataStreamSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Guards the real Flink starter's explicit dirty-config fail-fast and absent-config behavior.
 *
 * <p>The inherited configuration entry point is exercised without launching Flink tasks, so invalid
 * plugin selection cannot silently revert to the no-feature path.
 */
public class SinkDirtyCollectorConfigTest {
    @TempDir Path temporaryDirectory;

    @Test
    void noDirtyConfigUsesNoOp() {
        TestProcessor processor = new TestProcessor();
        Assertions.assertSame(
                NoOpDirtyRecordCollector.INSTANCE,
                processor.createDirtyRecordCollector(ConfigFactory.empty(), ConfigFactory.empty()));
    }

    @Test
    void explicitInvalidCollectorDoesNotFallBackToNoOp() {
        TestProcessor processor = new TestProcessor();
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                processor.createDirtyRecordCollector(
                                        ConfigFactory.parseString(
                                                "dirty.collector.type = missing_collector"),
                                        ConfigFactory.empty()));
        Assertions.assertTrue(error.getMessage().contains("Unknown dirty.collector type"));
    }

    @Test
    void explicitInvalidValidatorDoesNotFallBackToNoOp() {
        TestProcessor processor = new TestProcessor();
        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                processor.createDirtyRecordCollector(
                                        ConfigFactory.empty(),
                                        ConfigFactory.parseString(
                                                "dirty.collector.type = log\ndirty.validator.type = missing_validator")));
        Assertions.assertTrue(error.getMessage().contains("dirty.validator type"));
    }

    @Test
    void invalidDirtyPolicyFailsBeforeSinkPreparation() {
        TestProcessor processor =
                new TestProcessor(
                        ConfigFactory.parseString("dirty.collector.type=missing_collector"),
                        ConfigFactory.parseString("plugin_name=DefinitelyMissingSink"));
        DataStreamTableInfo input =
                new DataStreamTableInfo(
                        null, Collections.singletonList(catalogTable("first")), "input");
        IllegalArgumentException failure =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> processor.execute(Collections.singletonList(input)));
        Assertions.assertTrue(failure.getMessage().contains("Unknown dirty.collector type"));
    }

    @Test
    void activeDirtyPolicyRejectsMultiTableInputBeforeTemplatePreparation() {
        TestProcessor processor =
                new TestProcessor(
                        ConfigFactory.parseString("dirty.collector.type=log"),
                        ConfigFactory.parseString("plugin_name=DefinitelyMissingSink"));
        DataStreamTableInfo input =
                new DataStreamTableInfo(
                        null,
                        Arrays.asList(catalogTable("first"), catalogTable("second")),
                        "input");
        IllegalArgumentException failure =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> processor.execute(Collections.singletonList(input)));
        Assertions.assertTrue(failure.getMessage().contains("single input catalog table"));
    }

    @Test
    void activeDirtyPolicyRejectsMultiTableCapableSinkBeforeSaveMode() throws Exception {
        withRecordingFactory(
                () -> {
                    for (String failurePolicy :
                            new String[] {"FAIL_FAST", "CONTINUE_OTHER_TABLES"}) {
                        TestProcessor processor =
                                new TestProcessor(
                                        ConfigFactory.parseString(
                                                "dirty.collector.type=log\nmulti_table.failure_policy="
                                                        + failurePolicy),
                                        ConfigFactory.parseString("plugin_name=SaveModeRecording"));
                        IllegalArgumentException failure =
                                Assertions.assertThrows(
                                        IllegalArgumentException.class,
                                        () ->
                                                processor.execute(
                                                        Collections.singletonList(
                                                                new DataStreamTableInfo(
                                                                        null,
                                                                        Collections.singletonList(
                                                                                catalogTable(
                                                                                        "first")),
                                                                        "input"))));
                        Assertions.assertTrue(
                                failure.getMessage().contains("synchronous single-table writer"));
                        Assertions.assertEquals(
                                Collections.singletonList("existing-target-row"),
                                processor.targetRows());
                    }
                });
    }

    @Test
    void disabledDirtyPoliciesPreserveMultiTableSinkSaveMode() throws Exception {
        withRecordingFactory(
                () -> {
                    for (String policy :
                            new String[] {
                                "",
                                "dirty.collector.type=noop",
                                "dirty.collector.type=none",
                                "dirty.collector=null"
                            }) {
                        TestProcessor processor =
                                new TestProcessor(
                                        policy.isEmpty()
                                                ? ConfigFactory.empty()
                                                : ConfigFactory.parseString(
                                                        "dirty.collector.type=log"),
                                        ConfigFactory.parseString(
                                                "plugin_name=SaveModeRecording\n" + policy));
                        UnsupportedOperationException failure =
                                Assertions.assertThrows(
                                        UnsupportedOperationException.class,
                                        () ->
                                                processor.execute(
                                                        Collections.singletonList(
                                                                new DataStreamTableInfo(
                                                                        null,
                                                                        Collections.singletonList(
                                                                                catalogTable(
                                                                                        "first")),
                                                                        "input"))));
                        Assertions.assertEquals(
                                "Stream execution is outside this config test",
                                failure.getMessage());
                        Assertions.assertTrue(
                                processor.targetRows().isEmpty(),
                                "Default/no-op planning must still execute save mode");
                    }
                });
    }

    /**
     * Makes the real factory-discovery path see a test-only provider without a production SPI file.
     * The context loader and descriptor stay scoped to this planner-entry regression.
     */
    private void withRecordingFactory(Executable action) throws Exception {
        Path service = temporaryDirectory.resolve("META-INF/services/" + Factory.class.getName());
        Files.createDirectories(service.getParent());
        Files.write(
                service,
                Collections.singletonList(RecordingSinkFactory.class.getName()),
                StandardCharsets.UTF_8);
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader =
                new URLClassLoader(new URL[] {temporaryDirectory.toUri().toURL()}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            try {
                action.execute();
            } catch (Exception | Error failure) {
                throw failure;
            } catch (Throwable failure) {
                throw new AssertionError(failure);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Supplies a real multi-table-capable sink to the inherited planner, keeping target data local.
     * Factory discovery probes do not alter that data; only the actual save-mode entry does.
     */
    public static final class RecordingSinkFactory
            implements TableSinkFactory<SeaTunnelRow, Void, Void, Void> {
        private final List<String> targetRows =
                new ArrayList<>(Collections.singletonList("existing-target-row"));

        @Override
        public String factoryIdentifier() {
            return "SaveModeRecording";
        }

        @Override
        public OptionRule optionRule() {
            return OptionRule.builder().build();
        }

        @Override
        public TableSink<SeaTunnelRow, Void, Void, Void> createSink(
                TableSinkFactoryContext context) {
            return () -> new RecordingSaveModeSink(targetRows);
        }
    }

    /**
     * Models destructive target preparation at the actual save-mode entry of a prepared sink.
     * Runtime execution remains forbidden so successful writer activity cannot mask late rejection.
     */
    private static final class RecordingSaveModeSink
            implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void>,
                    SupportMultiTableSink,
                    SupportSaveMode {
        private final List<String> targetRows;

        private RecordingSaveModeSink(List<String> targetRows) {
            this.targetRows = targetRows;
        }

        @Override
        public Optional<SaveModeHandler> getSaveModeHandler() {
            targetRows.clear();
            return Optional.empty();
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            throw new UnsupportedOperationException("Runtime writing is outside this config test");
        }

        @Override
        public String getPluginName() {
            return "SaveModeRecording";
        }
    }

    private static CatalogTable catalogTable(String name) {
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", name),
                TableSchema.builder().build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "test");
    }

    /**
     * Executes the inherited configuration path without launching Flink streams.
     *
     * <p>Both sink extension hooks reject execution so these tests remain focused on startup
     * configuration rather than simulating successful runtime writes.
     */
    private static final class TestProcessor extends AbstractSinkExecuteProcessor {
        private TestProcessor() {
            super(
                    new ArrayList<>(),
                    ConfigFactory.empty(),
                    Collections.emptyList(),
                    new JobContext());
        }

        private TestProcessor(Config envConfig, Config sinkConfig) {
            super(
                    new ArrayList<>(),
                    envConfig,
                    Collections.singletonList(sinkConfig),
                    new JobContext());
        }

        @Override
        protected List<Optional<? extends Factory>> initializePlugins(
                List<URL> jarPaths, List<? extends Config> pluginConfigs) {
            if (!pluginConfigs.isEmpty()
                    && "SaveModeRecording".equals(pluginConfigs.get(0).getString("plugin_name"))) {
                return Collections.singletonList(Optional.of(new RecordingSinkFactory()));
            }
            return Collections.nCopies(pluginConfigs.size(), Optional.empty());
        }

        private List<String> targetRows() {
            return ((RecordingSinkFactory) plugins.get(0).get()).targetRows;
        }

        @Override
        protected DataStreamSink<SeaTunnelRow> createVersionSpecificDataStreamSink(
                DataStreamTableInfo stream,
                SeaTunnelSink sink,
                int parallelism,
                Config sinkConfig) {
            throw new UnsupportedOperationException("Stream execution is outside this config test");
        }

        @Override
        protected DataStreamSink<SeaTunnelRow> createVersionSpecificDataStreamSink(
                DataStreamTableInfo stream,
                SeaTunnelSink sink,
                int parallelism,
                Config sinkConfig,
                DirtyRecordCollector dirtyCollector) {
            throw new UnsupportedOperationException("Stream execution is outside this config test");
        }
    }
}
