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
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.IncrementalPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.PendingSplitsState;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.event.SnapshotSplitWatermark;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
    @SuppressWarnings("unchecked")
    void testSnapshotOnlyRestoreUsesCheckpointedTablesWithoutCatalogDiscovery() throws Exception {
        TableId processedTable = TableId.parse("db.processed");
        TableId remainingTable = TableId.parse("db.remaining");
        TableId remainingSplitTable = TableId.parse("db.remaining_split");
        TableId assignedSplitTable = TableId.parse("db.assigned_split");
        TableId newlyDiscoveredTable = TableId.parse("db.new");
        SnapshotSplit remainingSplit =
                new SnapshotSplit("db.remaining_split.0", remainingSplitTable, null, null, null);
        SnapshotSplit assignedSplit =
                new SnapshotSplit("db.assigned_split.0", assignedSplitTable, null, null, null);
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.singletonList(processedTable),
                        Collections.singletonList(remainingSplit),
                        Collections.singletonMap(assignedSplit.splitId(), assignedSplit),
                        Collections.emptyMap(),
                        false,
                        Collections.singletonList(remainingTable),
                        false,
                        false);
        TestIncrementalSource source =
                new TestIncrementalSource(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(STARTUP_MODE.key(), "snapshot-only")),
                        Collections.emptyList());
        Mockito.when(source.getDialect().discoverDataCollections(source.getSourceConfig()))
                .thenReturn(Collections.singletonList(newlyDiscoveredTable));
        SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(2);

        SourceSplitEnumerator<SourceSplitBase, PendingSplitsState> enumerator =
                source.restoreEnumerator(enumeratorContext, checkpointState);
        enumerator.open();

        Mockito.verify(source.getDialect(), Mockito.never())
                .discoverDataCollections(source.getSourceConfig());
        SnapshotPhaseState restoredState = (SnapshotPhaseState) enumerator.snapshotState(1L);
        Assertions.assertTrue(restoredState.isRemainingTablesCheckpointed());
        Assertions.assertEquals(
                Collections.singletonList(remainingTable), restoredState.getRemainingTables());
        Assertions.assertFalse(restoredState.getRemainingTables().contains(newlyDiscoveredTable));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotOnlyRestoreRejectsTableRemovedFromConfig() {
        // db.b still has a split in the checkpoint, but table-names no longer lists it: the
        // readers would not capture it, so the restore must fail with the table name.
        SnapshotSplit removedTableSplit =
                new SnapshotSplit("db.b.0", TableId.parse("db.b"), null, null, null);
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.singletonList(TableId.parse("db.a")),
                        Collections.singletonList(removedTableSplit),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Collections.emptyList(),
                        false,
                        false);
        TestIncrementalSource source =
                new TestIncrementalSource(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(STARTUP_MODE.key(), "snapshot-only")),
                        Collections.singletonList(catalogTable(TablePath.of("db", "a"))));
        SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> source.restoreEnumerator(enumeratorContext, checkpointState));
        Assertions.assertTrue(exception.getMessage().contains("[db.b]"), exception.getMessage());
    }

    @Test
    void testSnapshotOnlyRestoreRejectsUnfinishedTableOfAnotherSchema() {
        // A table that is not split yet, configured only in another schema
        SnapshotPhaseState checkpointState =
                snapshotOnlyState(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        TableId.parse("db.public.c"));
        TestIncrementalSource source =
                snapshotOnlySource(catalogTable(TablePath.of("db", "other", "c")));

        IllegalStateException exception =
                Assertions.assertThrows(
                        IllegalStateException.class,
                        () -> source.restoreEnumerator(enumeratorContext(), checkpointState));
        Assertions.assertTrue(
                exception.getMessage().contains("[db.public.c]"), exception.getMessage());
    }

    @Test
    void testSnapshotOnlyRestoreAcceptsConfiguredTables() throws Exception {
        // Configured names may differ in case, carry a schema the checkpointed id lacks, or have
        // the same three parts as the id of a PostgreSQL table
        SnapshotPhaseState checkpointState =
                snapshotOnlyState(
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        TableId.parse("db.a"),
                        TableId.parse("db.b"),
                        TableId.parse("db.public.c"));
        TestIncrementalSource source =
                snapshotOnlySource(
                        catalogTable(TablePath.of("DB", "A")),
                        catalogTable(TablePath.of("db", "public", "b")),
                        catalogTable(TablePath.of("db", "public", "c")));

        Assertions.assertNotNull(source.restoreEnumerator(enumeratorContext(), checkpointState));
    }

    @Test
    void testSnapshotOnlyRestoreAcceptsFinishedTableThatIsNoLongerConfigured() throws Exception {
        // db.done has only finished splits, so it is not read again and may have been removed
        // from the table list or dropped; db.a still has an unfinished split and is configured
        SnapshotSplit finishedSplit =
                new SnapshotSplit("db.done.0", TableId.parse("db.done"), null, null, null);
        SnapshotSplit unfinishedSplit =
                new SnapshotSplit("db.a.0", TableId.parse("db.a"), null, null, null);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(finishedSplit.splitId(), finishedSplit);
        assignedSplits.put(unfinishedSplit.splitId(), unfinishedSplit);
        SnapshotPhaseState checkpointState =
                snapshotOnlyState(
                        assignedSplits,
                        Collections.singletonMap(
                                finishedSplit.splitId(),
                                new SnapshotSplitWatermark(finishedSplit.splitId(), null, null)));
        TestIncrementalSource source = snapshotOnlySource(catalogTable(TablePath.of("db", "a")));

        Assertions.assertNotNull(source.restoreEnumerator(enumeratorContext(), checkpointState));
    }

    /**
     * A checkpoint of a snapshot-only job: the tables of the assigned splits are already split, the
     * remaining tables are not split yet.
     */
    private static SnapshotPhaseState snapshotOnlyState(
            Map<String, SnapshotSplit> assignedSplits,
            Map<String, SnapshotSplitWatermark> splitCompletedOffsets,
            TableId... remainingTables) {
        List<TableId> processedTables =
                assignedSplits.values().stream()
                        .map(SnapshotSplit::getTableId)
                        .distinct()
                        .collect(Collectors.toList());
        return new SnapshotPhaseState(
                processedTables,
                Collections.emptyList(),
                assignedSplits,
                splitCompletedOffsets,
                false,
                Arrays.asList(remainingTables),
                false,
                true);
    }

    private static TestIncrementalSource snapshotOnlySource(CatalogTable... catalogTables) {
        return new TestIncrementalSource(
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(STARTUP_MODE.key(), "snapshot-only")),
                Arrays.asList(catalogTables));
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext() {
        SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(enumeratorContext.currentParallelism()).thenReturn(1);
        return enumeratorContext;
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSnapshotOnlyRestoreRejectsIncrementalCheckpoint() {
        // A job that already entered the binlog phase checkpoints an IncrementalPhaseState.
        // Restoring it with startup.mode changed to snapshot-only must fail fast rather than resume
        // binlog streaming, which would break the bounded snapshot-only contract.
        TestIncrementalSource source =
                new TestIncrementalSource(
                        ReadonlyConfig.fromMap(
                                Collections.singletonMap(STARTUP_MODE.key(), "snapshot-only")),
                        Collections.emptyList());
        SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> source.restoreEnumerator(enumeratorContext, new IncrementalPhaseState()),
                "snapshot-only restore must reject an incremental (binlog) checkpoint");
    }

    @Test
    @SuppressWarnings("unchecked")
    void testInitialRestoreRejectsSnapshotOnlyCheckpoint() {
        TestIncrementalSource source =
                new TestIncrementalSource(
                        ReadonlyConfig.fromMap(Collections.emptyMap()), Collections.emptyList());
        SourceSplitEnumerator.Context<SourceSplitBase> enumeratorContext =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        SnapshotPhaseState checkpointState =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Collections.emptyList(),
                        false,
                        true);

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> source.restoreEnumerator(enumeratorContext, checkpointState),
                "initial restore must reject a snapshot-only checkpoint");
    }

    private static CatalogTable catalogTable(TablePath tablePath) {
        return CatalogTable.of(
                TableIdentifier.of("test", tablePath),
                TableSchema.builder().build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
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
