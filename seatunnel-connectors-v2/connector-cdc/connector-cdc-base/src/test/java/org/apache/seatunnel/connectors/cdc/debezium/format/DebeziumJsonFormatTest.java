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

package org.apache.seatunnel.connectors.cdc.debezium.format;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class DebeziumJsonFormatTest {

    public static final SingleChoiceOption STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class,
                            Arrays.asList(
                                    StartupMode.INITIAL,
                                    StartupMode.EARLIEST,
                                    StartupMode.LATEST,
                                    StartupMode.SPECIFIC,
                                    StartupMode.SNAPSHOT_ONLY))
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "Optional startup mode for CDC source, valid enumerations are "
                                    + "\"initial\", \"earliest\", \"latest\" or \"specific\"");

    public static final SingleChoiceOption STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .singleChoice(
                            StopMode.class,
                            Arrays.asList(StopMode.LATEST, StopMode.SPECIFIC, StopMode.NEVER))
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Optional stop mode for CDC source, valid enumerations are "
                                    + "\"never\", \"latest\" or \"specific\"");

    static class TestIncrementalSource extends IncrementalSource<Object, SourceConfig> {
        public TestIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
            super(options, catalogTables);
        }

        @Override
        public Option<StartupMode> getStartupModeOption() {
            return STARTUP_MODE;
        }

        @Override
        public Option<StopMode> getStopModeOption() {
            return STOP_MODE;
        }

        @Override
        public SourceConfig.Factory<SourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
            return null;
        }

        @Override
        public DebeziumDeserializationSchema<Object> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            return null;
        }

        @Override
        public DataSourceDialect<SourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            return null;
        }

        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return null;
        }

        @Override
        public String getPluginName() {
            return "";
        }

        @Override
        public Optional<String> driverName() {
            return Optional.empty();
        }
    }

    @Test
    void testGetProducedCatalogTablesWithCompatibleDebeziumJson() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                JdbcSourceOptions.FORMAT.key(), "compatible_debezium_json"));
        TestIncrementalSource source = new TestIncrementalSource(config, Collections.emptyList());
        List<CatalogTable> tables = source.getProducedCatalogTables();
        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals(
                "default.default.default", tables.get(0).getTableId().toTablePath().getFullName());
    }

    @Test
    void testSnapshotOnlyRejectsStartupTimestampBeforeSourceInitialization() {
        Map<String, Object> options = new HashMap<>();
        options.put(STARTUP_MODE.key(), "snapshot-only");
        options.put(SourceOptions.STARTUP_TIMESTAMP.key(), 1L);

        assertSnapshotOnlyConfigRejected(options, "startup.timestamp");
    }

    @Test
    void testSnapshotOnlyRejectsStopModeBeforeSourceInitialization() {
        Map<String, Object> options = new HashMap<>();
        options.put(STARTUP_MODE.key(), "snapshot-only");
        options.put(STOP_MODE.key(), "latest");

        assertSnapshotOnlyConfigRejected(options, "stop.mode");
    }

    @Test
    void testSnapshotOnlyRejectsSpecificOffsetsBeforeSourceInitialization() {
        for (Option<?> offsetOption :
                Arrays.asList(
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)) {
            Map<String, Object> options = new HashMap<>();
            options.put(STARTUP_MODE.key(), "snapshot-only");
            options.put(
                    offsetOption.key(),
                    offsetOption == SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE
                            ? "mysql-bin.000001"
                            : 4L);

            assertSnapshotOnlyConfigRejected(options, offsetOption.key());
        }
    }

    private void assertSnapshotOnlyConfigRejected(
            Map<String, Object> options, String expectedOption) {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                new FailingSource(
                                        ReadonlyConfig.fromMap(options), Collections.emptyList()));

        Assertions.assertTrue(exception.getMessage().contains(expectedOption));
    }

    static class FailingSource extends TestIncrementalSource {
        FailingSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
            super(options, catalogTables);
        }

        @Override
        public SourceConfig.Factory<SourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
            throw new AssertionError("source initialization must follow option validation");
        }
    }
}
