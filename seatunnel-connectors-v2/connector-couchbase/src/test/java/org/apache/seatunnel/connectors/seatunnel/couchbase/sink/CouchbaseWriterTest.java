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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.couchbase.exception.CouchbaseConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.kv.MutationResult;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CouchbaseWriterTest {

    // ---- shared helpers -------------------------------------------------

    private static CouchbaseWriterOptions minimalOptions() {
        return CouchbaseWriterOptions.builder()
                .withConnectionString("couchbase://localhost")
                .withUsername("user")
                .withPassword("pass")
                .withBucket("test-bucket")
                .withScope("_default")
                .withCollection("test-collection")
                .withUpsertEnable(true)
                .withFlushSize(-1) // disable size-based flush unless a test needs it
                .build();
    }

    private static CatalogTable minimalCatalogTable() {
        TableSchema schema = TableSchema.builder().build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "table"),
                schema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private static Collection buildMockCollectionChain(Cluster cluster) {
        Collection collection = mock(Collection.class);
        Scope scope = mock(Scope.class);
        when(scope.collection(anyString())).thenReturn(collection);
        Bucket bucket = mock(Bucket.class);
        when(bucket.scope(anyString())).thenReturn(scope);
        when(cluster.bucket(anyString())).thenReturn(bucket);
        return collection;
    }

    private static SeaTunnelRow insertRow() {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {"v1"});
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    // ---- 1. registration + flush on signal -------------------------------

    @Test
    void shouldRegisterFlushActionAndFlushBufferedRecordsOnSignal() throws Exception {
        Cluster cluster = mock(Cluster.class);
        Collection collection = buildMockCollectionChain(cluster);
        when(collection.upsert(anyString(), any())).thenReturn(mock(MutationResult.class));

        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), context);
            writer.write(insertRow());

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            // Buffered only: nothing written until the engine delivers the flush signal.
            verify(collection, never()).upsert(anyString(), any());

            actionCaptor.getValue().run();

            verify(collection, times(1)).upsert(anyString(), any());
        }
    }

    // ---- 2. flush failure propagation -------------------------------------

    @Test
    void shouldPropagateFlushFailure() throws Exception {
        Cluster cluster = mock(Cluster.class);
        Collection collection = buildMockCollectionChain(cluster);
        doThrow(new RuntimeException("boom")).when(collection).upsert(anyString(), any());

        SinkWriter.Context context = mock(SinkWriter.Context.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), context);
            writer.write(insertRow());

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            Assertions.assertThrows(
                    CouchbaseConnectorException.class, () -> actionCaptor.getValue().run());
        }
    }

    // ---- 3. fallback flush on close (Spark/Flink; no registerFlushAction) --

    @Test
    void shouldFlushOnCloseWhenEngineNeverInvokesFlushAction() throws Exception {
        Cluster cluster = mock(Cluster.class);
        Collection collection = buildMockCollectionChain(cluster);
        when(collection.upsert(anyString(), any())).thenReturn(mock(MutationResult.class));

        SinkWriter.Context context = mock(SinkWriter.Context.class);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), context);
            writer.write(insertRow());

            // Simulate Spark/Flink: registered action is never invoked.
            verify(collection, never()).upsert(anyString(), any());

            writer.close();

            verify(collection, times(1)).upsert(anyString(), any());
            verify(cluster, times(1)).disconnect();
        }
    }

    // ---- 4. checkpoint flush (prepareCommit) -------------------------------

    @Test
    void shouldFlushOnPrepareCommitWhenEngineNeverInvokesFlushAction() throws Exception {
        Cluster cluster = mock(Cluster.class);
        Collection collection = buildMockCollectionChain(cluster);
        when(collection.upsert(anyString(), any())).thenReturn(mock(MutationResult.class));

        SinkWriter.Context context = mock(SinkWriter.Context.class);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), context);
            writer.write(insertRow());

            verify(collection, never()).upsert(anyString(), any());

            Assertions.assertEquals(java.util.Optional.empty(), writer.prepareCommit());

            verify(collection, times(1)).upsert(anyString(), any());

            // Buffer must be cleared: a second checkpoint with no new rows sends nothing more.
            Assertions.assertEquals(java.util.Optional.empty(), writer.prepareCommit());
            verify(collection, times(1)).upsert(anyString(), any());
        }
    }

    // ---- 5. suppressed disconnect exception on close when flush fails ------

    @Test
    void closeShouldKeepFlushExceptionWhenDisconnectAlsoThrows() throws Exception {
        Cluster cluster = mock(Cluster.class);
        Collection collection = buildMockCollectionChain(cluster);
        doThrow(new RuntimeException("boom")).when(collection).upsert(anyString(), any());
        doThrow(new RuntimeException("disconnect failed")).when(cluster).disconnect();

        SinkWriter.Context context = mock(SinkWriter.Context.class);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), context);
            writer.write(insertRow());

            CouchbaseConnectorException thrown =
                    Assertions.assertThrows(CouchbaseConnectorException.class, writer::close);

            boolean disconnectSuppressed = false;
            for (Throwable suppressed : thrown.getSuppressed()) {
                if (suppressed.getMessage() != null
                        && suppressed.getMessage().contains("disconnect failed")) {
                    disconnectSuppressed = true;
                }
            }
            Assertions.assertTrue(
                    disconnectSuppressed,
                    "The disconnect failure should be suppressed on the primary flush exception");

            verify(cluster, times(1)).disconnect();
        }
    }
}
