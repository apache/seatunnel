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

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link CouchbaseWriter}'s constructor closes the Couchbase {@link Cluster} on every
 * failure path that can occur after the cluster is connected, preventing SDK thread/resource leaks
 * on repeated task restarts.
 *
 * <p>Two failure points are exercised:
 *
 * <ol>
 *   <li>{@code bucket.waitUntilReady()} throws — cluster must be disconnected.
 *   <li>Collection resolution throws — cluster must be disconnected.
 * </ol>
 *
 * <p>Uses {@code mockito-inline} to intercept the static {@link Cluster#connect} factory so the
 * tests remain independent of a live Couchbase installation.
 */
class CouchbaseWriterConstructorLeakTest {

    /** Minimal {@link CouchbaseWriterOptions} pointing at a fake cluster. */
    private static CouchbaseWriterOptions minimalOptions() {
        return CouchbaseWriterOptions.builder()
                .withConnectionString("couchbase://localhost")
                .withUsername("user")
                .withPassword("pass")
                .withBucket("test-bucket")
                .withScope("_default")
                .withCollection("test-collection")
                .build();
    }

    /**
     * Builds a minimal {@link CatalogTable} from {@code ROW_TYPE}. The table-path values are
     * irrelevant for the constructor-failure scenarios.
     */
    private static CatalogTable minimalCatalogTable() {
        TableSchema schema = TableSchema.builder().build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "table"),
                schema,
                java.util.Collections.emptyMap(),
                java.util.Collections.emptyList(),
                "");
    }

    /**
     * Returns a no-op {@link SinkWriter.Context}. The context field is unused by the constructor
     * code paths under test.
     */
    private static SinkWriter.Context noopContext() {
        return mock(SinkWriter.Context.class);
    }

    // -------------------------------------------------------------------------
    // Helper: build the cluster + bucket mock chain
    // -------------------------------------------------------------------------

    private static Cluster mockCluster(Bucket bucket) {
        Cluster cluster = mock(Cluster.class);
        when(cluster.bucket(anyString())).thenReturn(bucket);
        return cluster;
    }

    private static Bucket mockBucket(Scope scope) {
        Bucket bucket = mock(Bucket.class);
        when(bucket.scope(anyString())).thenReturn(scope);
        return bucket;
    }

    private static Scope mockScope(Collection collection) {
        Scope scope = mock(Scope.class);
        when(scope.collection(anyString())).thenReturn(collection);
        return scope;
    }

    // -------------------------------------------------------------------------
    // Failure scenario 1: waitUntilReady throws
    // -------------------------------------------------------------------------

    @Test
    void constructor_waitUntilReadyThrows_disconnectsClusterAndRethrows() {
        Bucket bucket = mock(Bucket.class);
        // waitUntilReady will throw; collection resolution must never be reached.
        doThrow(new RuntimeException("simulated readiness timeout"))
                .when(bucket)
                .waitUntilReady(any(Duration.class));

        Cluster cluster = mockCluster(bucket);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            assertThrows(
                    RuntimeException.class,
                    () ->
                            new CouchbaseWriter(
                                    minimalOptions(), minimalCatalogTable(), noopContext()),
                    "Constructor must rethrow the readiness exception");
        }

        // The cluster was connected; it must be disconnected despite the failure.
        verify(cluster).disconnect();
    }

    // -------------------------------------------------------------------------
    // Failure scenario 2: collection resolution throws
    // -------------------------------------------------------------------------

    @Test
    void constructor_collectionResolutionThrows_disconnectsClusterAndRethrows() {
        Scope scope = mock(Scope.class);
        when(scope.collection(anyString()))
                .thenThrow(new RuntimeException("simulated collection not found"));

        Bucket bucket = mockBucket(scope);
        // waitUntilReady succeeds (no-op by default for mocks).

        Cluster cluster = mockCluster(bucket);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            assertThrows(
                    RuntimeException.class,
                    () ->
                            new CouchbaseWriter(
                                    minimalOptions(), minimalCatalogTable(), noopContext()),
                    "Constructor must rethrow the collection-resolution exception");
        }

        // The cluster was connected; it must be disconnected despite the failure.
        verify(cluster).disconnect();
    }

    // -------------------------------------------------------------------------
    // Happy path: no failure → disconnect must NOT be called during construction
    // -------------------------------------------------------------------------

    @Test
    void constructor_successfulInit_doesNotDisconnectDuringConstruction() {
        Collection collection = mock(Collection.class);
        Scope scope = mockScope(collection);
        Bucket bucket = mockBucket(scope);
        Cluster cluster = mockCluster(bucket);

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), noopContext());

            // Cluster must not have been disconnected during successful construction.
            verify(cluster, never()).disconnect();

            // Clean up: close the writer so background resources are released.
            writer.close();
        }
    }
}
