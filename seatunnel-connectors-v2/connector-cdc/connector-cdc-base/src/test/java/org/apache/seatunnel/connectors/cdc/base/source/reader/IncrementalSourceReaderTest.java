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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.SourceSplitStateBase;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordEmitter;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.SourceReaderOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

class IncrementalSourceReaderTest {

    private static final TableId KEPT_TABLE =
            new TableId("alpha_online", null, "account_histories");
    private static final TableId REMOVED_TABLE =
            new TableId("alpha_online", null, "account_interests");

    @Test
    void testAddSplitsEnqueuesPrunedRestoredIncrementalSplit() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenReturn(Collections.singletonList(KEPT_TABLE));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(Collections.singletonList(restoredIncrementalSplit()));

            Assertions.assertEquals(1, reader.getNumberOfCurrentlyAssignedSplits());
            List<SourceSplitBase> state = reader.snapshotState(1L);
            Assertions.assertEquals(1, state.size());
            Assertions.assertEquals(
                    Collections.singletonList(KEPT_TABLE),
                    state.get(0).asIncrementalSplit().getTableIds());
        } finally {
            reader.close();
        }
    }

    @Test
    void testAddSplitsKeepsRestoredSplitWhenDiscoveryReturnsEmpty() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenReturn(Collections.emptyList());
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(Collections.singletonList(restoredIncrementalSplit()));

            Assertions.assertEquals(1, reader.getNumberOfCurrentlyAssignedSplits());
            List<SourceSplitBase> state = reader.snapshotState(1L);
            Assertions.assertEquals(
                    Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                    state.get(0).asIncrementalSplit().getTableIds());
        } finally {
            reader.close();
        }
    }

    @Test
    void testAddSplitsKeepsRestoredSplitWhenDiscoveryFails() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenThrow(new RuntimeException("database unavailable"));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(Collections.singletonList(restoredIncrementalSplit()));

            Assertions.assertEquals(1, reader.getNumberOfCurrentlyAssignedSplits());
            List<SourceSplitBase> state = reader.snapshotState(1L);
            Assertions.assertEquals(
                    Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                    state.get(0).asIncrementalSplit().getTableIds());
        } finally {
            reader.close();
        }
    }

    @Test
    void testAddSplitsDiscoversCapturedTablesOnlyOncePerBatch() {
        SourceConfig sourceConfig = Mockito.mock(SourceConfig.class);
        DataSourceDialect<SourceConfig> dialect =
                Mockito.mock(DataSourceDialect.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(dialect.discoverDataCollections(sourceConfig))
                .thenReturn(Collections.singletonList(KEPT_TABLE));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        IncrementalSourceReader<Object, SourceConfig> reader =
                createReader(dialect, sourceConfig, context);

        try {
            reader.addSplits(
                    Arrays.asList(
                            restoredIncrementalSplit(),
                            restoredIncrementalSplit("incremental-split-1")));

            Mockito.verify(dialect, Mockito.times(1)).discoverDataCollections(sourceConfig);
        } finally {
            reader.close();
        }
    }

    private static IncrementalSourceReader<Object, SourceConfig> createReader(
            DataSourceDialect<SourceConfig> dialect,
            SourceConfig sourceConfig,
            SourceReader.Context context) {
        Mockito.when(dialect.getName()).thenReturn("TestCDC");
        @SuppressWarnings("unchecked")
        IncrementalSourceSplitReader<SourceConfig> splitReader =
                Mockito.mock(IncrementalSourceSplitReader.class);
        CountDownLatch wakeUp = new CountDownLatch(1);
        try {
            Mockito.when(splitReader.fetch())
                    .thenAnswer(
                            invocation -> {
                                wakeUp.await();
                                return null;
                            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Mockito.doAnswer(
                        invocation -> {
                            wakeUp.countDown();
                            return null;
                        })
                .when(splitReader)
                .wakeUp();

        @SuppressWarnings("unchecked")
        RecordEmitter<SourceRecords, Object, SourceSplitStateBase> recordEmitter =
                Mockito.mock(RecordEmitter.class);
        @SuppressWarnings("unchecked")
        DebeziumDeserializationSchema<Object> deserializationSchema =
                Mockito.mock(DebeziumDeserializationSchema.class);

        return new IncrementalSourceReader<>(
                dialect,
                new ArrayBlockingQueue<RecordsWithSplitIds<SourceRecords>>(2),
                () -> splitReader,
                recordEmitter,
                new SourceReaderOptions(ReadonlyConfig.fromMap(Collections.emptyMap())),
                context,
                sourceConfig,
                deserializationSchema);
    }

    private static IncrementalSplit restoredIncrementalSplit() {
        return restoredIncrementalSplit("incremental-split-0");
    }

    private static IncrementalSplit restoredIncrementalSplit(String splitId) {
        Map<TableId, byte[]> historyTableChanges = new HashMap<>();
        historyTableChanges.put(KEPT_TABLE, new byte[] {1});
        historyTableChanges.put(REMOVED_TABLE, new byte[] {2});
        return new IncrementalSplit(
                splitId,
                Arrays.asList(KEPT_TABLE, REMOVED_TABLE),
                null,
                null,
                Collections.emptyList(),
                Arrays.asList(catalogTable(KEPT_TABLE), catalogTable(REMOVED_TABLE)),
                historyTableChanges);
    }

    private static CatalogTable catalogTable(TableId tableId) {
        TablePath tablePath = TablePath.of(tableId.catalog(), tableId.table());
        return CatalogTable.of(
                TableIdentifier.of("test", tablePath),
                TableSchema.builder().build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }
}
