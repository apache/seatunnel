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
import org.apache.seatunnel.api.sink.DirtyRecordCollector;
import org.apache.seatunnel.api.sink.NoOpDirtyRecordCollector;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.flink.streaming.api.datastream.DataStreamSink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
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
class SinkDirtyCollectorConfigTest {
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
            return Collections.nCopies(pluginConfigs.size(), Optional.empty());
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
