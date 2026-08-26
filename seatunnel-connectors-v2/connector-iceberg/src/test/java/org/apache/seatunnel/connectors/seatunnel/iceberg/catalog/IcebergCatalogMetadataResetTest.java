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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergDropDataStrategy;

import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class IcebergCatalogMetadataResetTest {

    private static final TablePath TABLE_PATH = TablePath.of("database.table");

    @Test
    void shouldResetMetadataWhenTruncatingTable() throws Exception {
        org.apache.iceberg.catalog.Catalog icebergNativeCatalog =
                mock(org.apache.iceberg.catalog.Catalog.class);
        Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
        TableOperations operations = mock(TableOperations.class);
        TableMetadata base = tableMetadataWithMainBranch();
        TableIdentifier tableIdentifier = TableIdentifier.of("database", "table");

        when(icebergNativeCatalog.tableExists(tableIdentifier)).thenReturn(true);
        when(icebergNativeCatalog.loadTable(tableIdentifier)).thenReturn(table);
        when(((HasTableOperations) table).operations()).thenReturn(operations);
        when(operations.current()).thenReturn(base);

        IcebergCatalog catalog = icebergCatalogWith(icebergNativeCatalog);
        catalog.truncateTable(TABLE_PATH, true, IcebergDropDataStrategy.HARD_METADATA_RESET, null);

        ArgumentCaptor<TableMetadata> metadataCaptor = ArgumentCaptor.forClass(TableMetadata.class);
        verify(operations).commit(same(base), metadataCaptor.capture());

        TableMetadata resetMetadata = metadataCaptor.getValue();
        assertNull(resetMetadata.currentSnapshot());
        assertEquals(0, resetMetadata.snapshots().size());
        assertFalse(resetMetadata.refs().containsKey(SnapshotRef.MAIN_BRANCH));
        assertEquals(base.schema().asStruct(), resetMetadata.schema().asStruct());
        assertEquals(base.spec(), resetMetadata.spec());
        assertEquals(base.sortOrder(), resetMetadata.sortOrder());
        assertEquals(base.location(), resetMetadata.location());
        assertEquals(base.properties(), resetMetadata.properties());
        assertEquals(base.uuid(), resetMetadata.uuid());
    }

    @Test
    void shouldResetAllRefsAndRecreateCommitBranchWhenConfigured() throws Exception {
        org.apache.iceberg.catalog.Catalog icebergNativeCatalog =
                mock(org.apache.iceberg.catalog.Catalog.class);
        Table table = mock(Table.class, withSettings().extraInterfaces(HasTableOperations.class));
        Table resetTable = mock(Table.class);
        ManageSnapshots manageSnapshots = mock(ManageSnapshots.class);
        TableOperations operations = mock(TableOperations.class);
        TableMetadata base = tableMetadataWithBranches();
        TableIdentifier tableIdentifier = TableIdentifier.of("database", "table");

        when(icebergNativeCatalog.tableExists(tableIdentifier)).thenReturn(true);
        when(icebergNativeCatalog.loadTable(tableIdentifier)).thenReturn(table, resetTable);
        when(((HasTableOperations) table).operations()).thenReturn(operations);
        when(operations.current()).thenReturn(base);
        when(resetTable.manageSnapshots()).thenReturn(manageSnapshots);
        when(manageSnapshots.createBranch("st_branch")).thenReturn(manageSnapshots);

        IcebergCatalog catalog = icebergCatalogWith(icebergNativeCatalog);
        catalog.truncateTable(
                TABLE_PATH, true, IcebergDropDataStrategy.HARD_METADATA_RESET, "st_branch");

        ArgumentCaptor<TableMetadata> metadataCaptor = ArgumentCaptor.forClass(TableMetadata.class);
        verify(operations).commit(same(base), metadataCaptor.capture());
        TableMetadata resetMetadata = metadataCaptor.getValue();
        assertEquals(0, resetMetadata.snapshots().size());
        assertEquals(Collections.emptyMap(), resetMetadata.refs());
        verify(resetTable).manageSnapshots();
        verify(manageSnapshots).createBranch("st_branch");
        verify(manageSnapshots).commit();
    }

    @Test
    void shouldDeleteFromConfiguredBranchWhenUsingDeleteCommitStrategy() throws Exception {
        org.apache.iceberg.catalog.Catalog icebergNativeCatalog =
                mock(org.apache.iceberg.catalog.Catalog.class);
        Table table = mock(Table.class);
        org.apache.iceberg.DeleteFiles deleteFiles = mock(org.apache.iceberg.DeleteFiles.class);
        TableIdentifier tableIdentifier = TableIdentifier.of("database", "table");

        when(icebergNativeCatalog.tableExists(tableIdentifier)).thenReturn(true);
        when(icebergNativeCatalog.loadTable(tableIdentifier)).thenReturn(table);
        when(table.newDelete()).thenReturn(deleteFiles);
        when(deleteFiles.toBranch("st_branch")).thenReturn(deleteFiles);
        when(deleteFiles.deleteFromRowFilter(any())).thenReturn(deleteFiles);

        IcebergCatalog catalog = icebergCatalogWith(icebergNativeCatalog);
        catalog.truncateTable(TABLE_PATH, true, IcebergDropDataStrategy.DELETE_COMMIT, "st_branch");

        verify(deleteFiles).toBranch("st_branch");
        verify(deleteFiles).deleteFromRowFilter(any());
        verify(deleteFiles).commit();
    }

    private static IcebergCatalog icebergCatalogWith(
            org.apache.iceberg.catalog.Catalog nativeCatalog) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("catalog_name", "catalog");
        configMap.put("iceberg.catalog.config", Collections.singletonMap("type", "hadoop"));
        IcebergCatalog catalog = new IcebergCatalog("catalog", ReadonlyConfig.fromMap(configMap));
        Field field = IcebergCatalog.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(catalog, nativeCatalog);
        return catalog;
    }

    private static TableMetadata tableMetadataWithMainBranch() {
        TableMetadata base = emptyTableMetadata();
        Snapshot snapshot = snapshot(1L);
        return TableMetadata.buildFrom(base)
                .addSnapshot(snapshot)
                .setRef(
                        SnapshotRef.MAIN_BRANCH,
                        SnapshotRef.branchBuilder(snapshot.snapshotId()).build())
                .build();
    }

    private static TableMetadata tableMetadataWithBranches() {
        TableMetadata base = emptyTableMetadata();
        Snapshot snapshot = snapshot(1L);
        return TableMetadata.buildFrom(base)
                .addSnapshot(snapshot)
                .setRef(
                        SnapshotRef.MAIN_BRANCH,
                        SnapshotRef.branchBuilder(snapshot.snapshotId()).build())
                .setRef("st_branch", SnapshotRef.branchBuilder(snapshot.snapshotId()).build())
                .setRef("other_branch", SnapshotRef.branchBuilder(snapshot.snapshotId()).build())
                .build();
    }

    private static TableMetadata emptyTableMetadata() {
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
        return TableMetadata.newTableMetadata(
                schema,
                PartitionSpec.unpartitioned(),
                "file:///tmp/iceberg-table",
                Collections.emptyMap());
    }

    private static Snapshot snapshot(long snapshotId) {
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.snapshotId()).thenReturn(snapshotId);
        when(snapshot.sequenceNumber()).thenReturn(snapshotId);
        when(snapshot.parentId()).thenReturn(null);
        when(snapshot.timestampMillis()).thenReturn(1_000L + snapshotId);
        return snapshot;
    }
}
