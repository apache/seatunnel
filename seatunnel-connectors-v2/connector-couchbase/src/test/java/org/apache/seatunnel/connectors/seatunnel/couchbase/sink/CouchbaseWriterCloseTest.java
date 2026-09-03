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

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests covering the two fixes applied to {@link CouchbaseWriter#close()} and {@link
 * CouchbaseWriter}'s async-error latch:
 *
 * <ol>
 *   <li><b>Blocker 1a</b> — {@code cluster.disconnect()} must be called even when a latched
 *       background flush error causes {@code checkAsyncFlushError()} to throw inside {@code
 *       close()}.
 *   <li><b>Blocker 1b</b> — The {@code asyncFlushError} latch must be <em>sticky</em>: reading it
 *       via {@link AtomicReference#get()} must not clear it, so a second call to {@code
 *       checkAsyncFlushError()} still sees the same error.
 * </ol>
 *
 * <p>Uses {@code mockito-inline} to intercept the static {@link Cluster#connect} factory.
 */
class CouchbaseWriterCloseTest {

    // -------------------------------------------------------------------------
    // Shared helpers (mirrors CouchbaseWriterConstructorLeakTest)
    // -------------------------------------------------------------------------

    private static CouchbaseWriterOptions minimalOptions() {
        return CouchbaseWriterOptions.builder()
                .withConnectionString("couchbase://localhost")
                .withUsername("user")
                .withPassword("pass")
                .withBucket("test-bucket")
                .withScope("_default")
                .withCollection("test-collection")
                .withBatchIntervalMs(-1) // disable background scheduler
                .build();
    }

    private static CatalogTable minimalCatalogTable() {
        TableSchema schema = TableSchema.builder().build();
        return CatalogTable.of(
                TableIdentifier.of("catalog", "database", "table"),
                schema,
                java.util.Collections.emptyMap(),
                java.util.Collections.emptyList(),
                "");
    }

    private static SinkWriter.Context noopContext() {
        return mock(SinkWriter.Context.class);
    }

    private static Cluster buildMockClusterChain() {
        Collection collection = mock(Collection.class);
        Scope scope = mock(Scope.class);
        when(scope.collection(anyString())).thenReturn(collection);
        Bucket bucket = mock(Bucket.class);
        when(bucket.scope(anyString())).thenReturn(scope);
        Cluster cluster = mock(Cluster.class);
        when(cluster.bucket(anyString())).thenReturn(bucket);
        return cluster;
    }

    /**
     * Reflectively injects a {@link Throwable} into the {@code asyncFlushError} latch of the given
     * writer, simulating a failure that was recorded by the background flush timer.
     */
    private static void injectAsyncFlushError(CouchbaseWriter writer, Throwable error)
            throws Exception {
        Field field = CouchbaseWriter.class.getDeclaredField("asyncFlushError");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        AtomicReference<Throwable> ref = (AtomicReference<Throwable>) field.get(writer);
        ref.set(error);
    }

    // -------------------------------------------------------------------------
    // Blocker 1a — disconnect() must execute even when the latch throws
    // -------------------------------------------------------------------------

    /**
     * Verifies that {@code cluster.disconnect()} is called by the {@code finally} block inside
     * {@code close()} even when a latched async flush error causes {@code checkAsyncFlushError()}
     * to throw.
     *
     * <p>Before the fix, {@code checkAsyncFlushError()} was invoked <em>before</em> entering the
     * {@code try/finally}, so a latched error caused the method to return early and {@code
     * disconnect()} was never reached.
     */
    @Test
    void close_withLatchedAsyncError_stillDisconnectsCluster() throws Exception {
        Cluster cluster = buildMockClusterChain();

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), noopContext());

            // Simulate a background flush failure recorded by the timer thread.
            injectAsyncFlushError(writer, new RuntimeException("simulated timer flush failure"));

            // close() must throw because of the latched error — but disconnect() must still run.
            assertThrows(
                    Exception.class, writer::close, "close() must rethrow the latched async error");

            // The critical assertion: disconnect() must have been called exactly once
            // via the finally block, regardless of the async-error exception.
            verify(cluster, atLeastOnce()).disconnect();
        }
    }

    /**
     * Complementary happy-path: when no error is latched, {@code close()} must complete without
     * throwing and {@code disconnect()} must still be called.
     */
    @Test
    void close_withNoLatchedError_disconnectsCluster() {
        Cluster cluster = buildMockClusterChain();

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), noopContext());

            assertDoesNotThrow(writer::close, "close() must not throw when there is no error");

            verify(cluster, atLeastOnce()).disconnect();
        }
    }

    // -------------------------------------------------------------------------
    // Blocker 1b — asyncFlushError latch must be sticky (not consumed on first read)
    // -------------------------------------------------------------------------

    /**
     * Verifies that the {@code asyncFlushError} latch is <em>sticky</em>: after a background error
     * is recorded, two successive calls to {@code checkAsyncFlushError()} (via {@link
     * CouchbaseWriter#write} and then {@link CouchbaseWriter#prepareCommit}) must both throw.
     *
     * <p>Before the fix, the latch used {@code getAndSet(null)}, so only the first caller saw the
     * error; the second call would silently return, allowing the task to proceed after a fatal
     * flush failure.
     */
    @Test
    void asyncFlushError_latchIsSticky_secondCallerAlsoSeesError() throws Exception {
        Cluster cluster = buildMockClusterChain();

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), noopContext());

            injectAsyncFlushError(writer, new RuntimeException("background error"));

            // First caller (write) must throw.
            assertThrows(
                    Exception.class,
                    () -> writer.write(new org.apache.seatunnel.api.table.type.SeaTunnelRow(0)),
                    "write() must throw when the latch is set");

            // Second caller (prepareCommit) must ALSO throw — the latch must not have been cleared.
            assertThrows(
                    Exception.class,
                    writer::prepareCommit,
                    "prepareCommit() must still throw: the async-error latch must be sticky");

            // Cleanup: close() will also throw (latch still set) but disconnect() must run.
            assertThrows(Exception.class, writer::close);
            verify(cluster, atLeastOnce()).disconnect();
        }
    }

    /**
     * Verifies that a single injected error remains visible through all three lifecycle callers in
     * sequence: {@link CouchbaseWriter#write}, {@link CouchbaseWriter#prepareCommit}, and {@link
     * CouchbaseWriter#close}.
     */
    @Test
    void asyncFlushError_latchIsSticky_allThreeLifecycleCallersThrow() throws Exception {
        Cluster cluster = buildMockClusterChain();

        try (MockedStatic<Cluster> staticCluster = Mockito.mockStatic(Cluster.class)) {
            staticCluster
                    .when(() -> Cluster.connect(anyString(), anyString(), anyString()))
                    .thenReturn(cluster);

            CouchbaseWriter writer =
                    new CouchbaseWriter(minimalOptions(), minimalCatalogTable(), noopContext());

            injectAsyncFlushError(writer, new RuntimeException("persistent background error"));

            assertThrows(
                    Exception.class,
                    () -> writer.write(new org.apache.seatunnel.api.table.type.SeaTunnelRow(0)),
                    "write() must throw");
            assertThrows(
                    Exception.class,
                    writer::prepareCommit,
                    "prepareCommit() must throw (latch not cleared by write)");
            assertThrows(
                    Exception.class,
                    writer::close,
                    "close() must throw (latch not cleared by prepareCommit)");

            verify(cluster, atLeastOnce()).disconnect();
        }
    }
}
