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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.MultiTableSinkReplicaMode;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.state.IcebergSinkState;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcebergSinkWriterTest {

    @Mock private IcebergTableLoader tableLoader;
    @Mock private IcebergSinkConfig config;

    private TableSchema minimalSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.INT_TYPE, (Long) null, true, null, null))
                .build();
    }

    /**
     * Restoring from state must only recover commitUser — no commit or I/O against the table should
     * happen. The actual re-commit is the sole responsibility of
     * IcebergAggregatedCommitter.restoreCommit().
     */
    @Test
    void testRestoreFromStateDoesNotCommit() {
        IcebergSinkState state = new IcebergSinkState("restored-user", 5L);

        new IcebergSinkWriter(
                tableLoader, config, minimalSchema(), Collections.singletonList(state));

        verifyNoInteractions(tableLoader);
    }

    @Test
    void testRestoreFromStateRecoversPreviousCommitUser() throws IOException {
        String originalUser = "original-commit-user";
        IcebergSinkState state = new IcebergSinkState(originalUser, 3L);

        IcebergSinkWriter writer =
                new IcebergSinkWriter(
                        tableLoader, config, minimalSchema(), Collections.singletonList(state));

        List<IcebergSinkState> snapped = writer.snapshotState(4L);
        assertEquals(originalUser, snapped.get(0).getCommitUser());
    }

    @Test
    void testNewWriterWithoutStateGeneratesRandomCommitUser() throws IOException {
        IcebergSinkWriter w1 = new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);
        IcebergSinkWriter w2 = new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);

        List<IcebergSinkState> s1 = w1.snapshotState(1L);
        List<IcebergSinkState> s2 = w2.snapshotState(1L);
        assertNotEquals(
                s1.get(0).getCommitUser(),
                s2.get(0).getCommitUser(),
                "Each new writer must get a unique commitUser");
    }

    @Test
    void testPrepareCommitPassesCheckpointIdToCommitInfo() throws IOException {
        IcebergSinkWriter writer =
                new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);

        Optional<IcebergCommitInfo> info = writer.prepareCommit(42L);
        assertEquals(42L, info.get().getCheckpointId());
    }

    @Test
    void testSnapshotStatePersistsCheckpointId() throws IOException {
        IcebergSinkWriter writer =
                new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);

        List<IcebergSinkState> states = writer.snapshotState(99L);
        assertEquals(1, states.size());
        assertEquals(99L, states.get(0).getCheckpointId());
    }

    /**
     * A record writer that fails to close must not keep the table loader — and the catalog and
     * Hadoop resources it owns — alive for the rest of the task.
     */
    @Test
    void testCloseReleasesTableLoaderWhenRecordWriterFails() throws IOException {
        IcebergSinkWriter writer =
                new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);
        RecordWriter recordWriter = mock(RecordWriter.class);
        doThrow(new IllegalStateException("simulated record writer close failure"))
                .when(recordWriter)
                .close();
        ReflectionUtils.setField(writer, IcebergSinkWriter.class, "writer", recordWriter);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, writer::close);

        assertEquals("simulated record writer close failure", thrown.getMessage());
        verify(tableLoader).close();
    }

    /** The record writer is released even when the table loader fails to close. */
    @Test
    void testCloseReleasesRecordWriterWhenTableLoaderFails() throws IOException {
        IcebergSinkWriter writer =
                new IcebergSinkWriter(tableLoader, config, minimalSchema(), null);
        RecordWriter recordWriter = mock(RecordWriter.class);
        ReflectionUtils.setField(writer, IcebergSinkWriter.class, "writer", recordWriter);
        doThrow(new IOException("simulated table loader close failure")).when(tableLoader).close();

        IOException thrown = assertThrows(IOException.class, writer::close);

        assertEquals("simulated table loader close failure", thrown.getMessage());
        verify(recordWriter).close();
    }

    /**
     * A table with identifier fields is written by a delta writer, which applies deletes and
     * upserts by key, so its writer is KEYED and routes by the identifier column, found by name in
     * the row even when the row orders its columns differently from the table.
     */
    @Test
    void testTableWithIdentifierFieldsIsKeyedByThatColumn() {
        Table table = createTable(Collections.singleton(1));
        IcebergSinkWriter writer =
                new IcebergSinkWriter(loaderOf(table), sinkConfig(null), nameThenId(), null);

        assertEquals(MultiTableSinkReplicaMode.KEYED, writer.replicaMode());
        assertEquals(Optional.of(1), writer.primaryKey());
    }

    /** {@code iceberg.table.primary-keys} replaces the identifier fields of the table. */
    @Test
    void testConfiguredPrimaryKeysAreTheRoutingKey() {
        Table table = createTable(Collections.emptySet());
        IcebergSinkWriter writer =
                new IcebergSinkWriter(loaderOf(table), sinkConfig("name"), nameThenId(), null);

        assertEquals(MultiTableSinkReplicaMode.KEYED, writer.replicaMode());
        assertEquals(Optional.of(0), writer.primaryKey());
    }

    /**
     * Without identifier fields, a configured key or upsert mode, the table is written by an append
     * writer, which only adds rows, so the order across writers does not matter.
     */
    @Test
    void testAppendOnlyTableIsRandomWithoutAKey() {
        Table table = createTable(Collections.emptySet());
        IcebergSinkWriter writer =
                new IcebergSinkWriter(loaderOf(table), sinkConfig(null), nameThenId(), null);

        assertEquals(MultiTableSinkReplicaMode.RANDOM, writer.replicaMode());
        assertEquals(Optional.empty(), writer.primaryKey());
    }

    /**
     * When the identifier column is not in the rows, the writer stays KEYED but has no key, so a
     * job with several replicas is rejected instead of routing the changes of one key at random.
     */
    @Test
    void testKeyColumnMissingFromTheRowLeavesNoKey() {
        Table table = createTable(Collections.singleton(1));
        TableSchema onlyName =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .build();
        IcebergSinkWriter writer =
                new IcebergSinkWriter(loaderOf(table), sinkConfig(null), onlyName, null);

        assertEquals(MultiTableSinkReplicaMode.KEYED, writer.replicaMode());
        assertEquals(Optional.empty(), writer.primaryKey());
    }

    private static Table createTable(Set<Integer> identifierFieldIds) {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db"));
        Schema schema =
                new Schema(
                        Arrays.asList(
                                Types.NestedField.required(1, "id", Types.LongType.get()),
                                Types.NestedField.optional(2, "name", Types.StringType.get())),
                        identifierFieldIds);
        return catalog.createTable(
                TableIdentifier.of("db", "events"),
                schema,
                PartitionSpec.unpartitioned(),
                Collections.singletonMap("format-version", "2"));
    }

    private static IcebergTableLoader loaderOf(Table table) {
        IcebergTableLoader loader = mock(IcebergTableLoader.class);
        when(loader.loadTable()).thenReturn(table);
        return loader;
    }

    private static IcebergSinkConfig sinkConfig(String primaryKeys) {
        Map<String, Object> options = new HashMap<>();
        options.put(IcebergCommonOptions.KEY_CATALOG_NAME.key(), "test");
        options.put(IcebergCommonOptions.CATALOG_PROPS.key(), Collections.emptyMap());
        if (primaryKeys != null) {
            options.put(IcebergSinkOptions.TABLE_PRIMARY_KEYS.key(), primaryKeys);
        }
        return new IcebergSinkConfig(ReadonlyConfig.fromMap(options));
    }

    /** The rows carry {@code name} first, while the table has {@code id} first. */
    private static TableSchema nameThenId() {
        return TableSchema.builder()
                .column(
                        PhysicalColumn.of(
                                "name", BasicType.STRING_TYPE, (Long) null, true, null, null))
                .column(
                        PhysicalColumn.of(
                                "id", BasicType.LONG_TYPE, (Long) null, false, null, null))
                .build();
    }
}
