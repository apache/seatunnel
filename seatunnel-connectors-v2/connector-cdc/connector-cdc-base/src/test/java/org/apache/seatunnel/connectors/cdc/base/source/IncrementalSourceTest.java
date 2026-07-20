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

package org.apache.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.HybridPendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class IncrementalSourceTest {

    private static final Option<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription("Test startup mode");

    private static final Option<StopMode> STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .enumType(StopMode.class)
                    .defaultValue(StopMode.NEVER)
                    .withDescription("Test stop mode");

    @Test
    void testSnapshotOnlyRestoreKeepsCheckpointedTables() {
        TableId processedTable = TableId.parse("db.processed");
        TableId remainingTable = TableId.parse("db.remaining");
        TableId remainingSplitTable = TableId.parse("db.remaining_split");
        TableId assignedSplitTable = TableId.parse("db.assigned_split");
        TableId newlyDiscoveredTable = TableId.parse("db.new");
        Set<TableId> checkpointedTables =
                new HashSet<>(
                        Arrays.asList(
                                processedTable,
                                remainingTable,
                                remainingSplitTable,
                                assignedSplitTable));
        SnapshotSplit remainingSplit =
                new SnapshotSplit("db.remaining_split.0", remainingSplitTable, null, null, null);
        SnapshotSplit assignedSplit =
                new SnapshotSplit("db.assigned_split.0", assignedSplitTable, null, null, null);
        HybridPendingSplitsState checkpointState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.singletonList(processedTable),
                                Collections.singletonList(remainingSplit),
                                Collections.singletonMap(assignedSplit.splitId(), assignedSplit),
                                Collections.emptyMap(),
                                false,
                                Collections.singletonList(remainingTable),
                                false,
                                true),
                        null);

        Set<TableId> discoveredTables =
                new HashSet<>(
                        Arrays.asList(remainingTable, remainingSplitTable, newlyDiscoveredTable));

        Assertions.assertEquals(
                checkpointedTables,
                IncrementalSource.getCapturedTablesForRestore(
                        StartupMode.SNAPSHOT, discoveredTables, checkpointState));
        Assertions.assertEquals(
                discoveredTables,
                IncrementalSource.getCapturedTablesForRestore(
                        StartupMode.INITIAL, discoveredTables, checkpointState));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotOnlyRestoreEnumeratorDoesNotDiscoverCatalog() throws Exception {
        TableId checkpointTable = TableId.parse("db.checkpoint_table");
        HybridPendingSplitsState checkpointState =
                new HybridPendingSplitsState(
                        new SnapshotPhaseState(
                                Collections.emptyList(),
                                Collections.emptyList(),
                                Collections.emptyMap(),
                                Collections.emptyMap(),
                                false,
                                Collections.singletonList(checkpointTable),
                                false,
                                false),
                        null);
        TestIncrementalSource source =
                new TestIncrementalSource(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(STARTUP_MODE.key(), "snapshot")),
                        Collections.emptyList());
        SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(2);

        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.restoreEnumerator(enumeratorContext, checkpointState);
        enumerator.open();

        Mockito.verify(source.getDialect(), Mockito.never())
                .discoverDataCollections(source.getSourceConfig());
        HybridPendingSplitsState restoredState =
                (HybridPendingSplitsState) enumerator.snapshotState(1L);
        Assertions.assertTrue(
                restoredState.getSnapshotPhaseState().isRemainingTablesCheckpointed());
        Assertions.assertEquals(
                Collections.singletonList(checkpointTable),
                restoredState.getSnapshotPhaseState().getRemainingTables());
    }

    private static class TestIncrementalSource extends IncrementalSource<Object, SourceConfig> {
        private SourceConfig sourceConfig;
        private DataSourceDialect<SourceConfig> dialect;

        private TestIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
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
            sourceConfig = Mockito.mock(SourceConfig.class);
            StartupConfig startupConfig =
                    new StartupConfig(
                            config.get(STARTUP_MODE),
                            config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE),
                            config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS),
                            config.get(SourceOptions.STARTUP_TIMESTAMP));
            StopConfig stopConfig = new StopConfig(config.get(STOP_MODE), null, null, null);
            Mockito.when(sourceConfig.getStartupConfig()).thenReturn(startupConfig);
            Mockito.when(sourceConfig.getStopConfig()).thenReturn(stopConfig);
            return ignored -> sourceConfig;
        }

        @Override
        public DebeziumDeserializationSchema<Object> createDebeziumDeserializationSchema(
                ReadonlyConfig config) {
            return null;
        }

        @Override
        public DataSourceDialect<SourceConfig> createDataSourceDialect(ReadonlyConfig config) {
            dialect = Mockito.mock(DataSourceDialect.class);
            return dialect;
        }

        @Override
        public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
            return Mockito.mock(OffsetFactory.class);
        }

        @Override
        public String getPluginName() {
            return "test-cdc";
        }

        @Override
        public Optional<String> driverName() {
            return Optional.empty();
        }

        private SourceConfig getSourceConfig() {
            return sourceConfig;
        }

        private DataSourceDialect<SourceConfig> getDialect() {
            return dialect;
        }
    }
}
