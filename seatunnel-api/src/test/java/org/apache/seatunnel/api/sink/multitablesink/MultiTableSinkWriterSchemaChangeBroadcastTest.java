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

package org.apache.seatunnel.api.sink.multitablesink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for shared-physical-sink schema evolution. When several source tables resolve
 * to the same physical sink table, the coordinator must broadcast schema changes to every sibling
 * sub-writer and freeze every affected queue before any one writer mutates the external schema.
 */
public class MultiTableSinkWriterSchemaChangeBroadcastTest {

    /** Shared physical sink used by the fan-out cases. */
    private static final String PHYSICAL_SINK_SHARED = "catalog.shared.users";

    /** Unrelated physical sink used to verify strict isolation. */
    private static final String PHYSICAL_SINK_OTHER = "catalog.other.users";

    /**
     * Verifies the classic multi-table template case: the source-matched writer receives the event
     * directly and the sibling that targets the same physical table receives it by broadcast.
     */
    @Test
    void schemaChangeForOneSourceFansOutToSiblingsSharingThePhysicalSink() throws IOException {
        RecordingSinkWriter sinkForA = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkForB = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkForC = new RecordingSinkWriter(PHYSICAL_SINK_OTHER);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkForA);
        writers.put(SinkIdentifier.of("dbB.users", 0), sinkForB);
        writers.put(SinkIdentifier.of("dbC.users", 0), sinkForC);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(1, sinkForA.getInvocationCount());
        assertEquals(1, sinkForB.getInvocationCount());
        assertEquals(0, sinkForC.getInvocationCount());
    }

    /**
     * Verifies that writers targeting different physical tables keep the legacy isolated routing
     * behavior and do not receive sibling broadcasts.
     */
    @Test
    void singlePhysicalSinkDoesNotProduceAnyBroadcast() throws IOException {
        RecordingSinkWriter sinkA = new RecordingSinkWriter("catalog.dbA.users");
        RecordingSinkWriter sinkB = new RecordingSinkWriter("catalog.dbB.users");

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 0), sinkB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(1, sinkA.getInvocationCount());
        assertEquals(0, sinkB.getInvocationCount());
    }

    /**
     * Verifies that the fan-out reaches every sibling when three source tables collapse into one
     * physical sink table.
     */
    @Test
    void schemaChangeReachesAllSiblingsWhenThreeSourcesShareOnePhysicalSink() throws IOException {
        RecordingSinkWriter sinkA = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkB = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkC = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 0), sinkB);
        writers.put(SinkIdentifier.of("dbC.users", 0), sinkC);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbB", null, "users")));

        assertEquals(1, sinkA.getInvocationCount());
        assertEquals(1, sinkB.getInvocationCount());
        assertEquals(1, sinkC.getInvocationCount());
    }

    /**
     * Verifies that the coordinator freezes every affected queue before any sub-writer applies the
     * schema change. Without this, one queue can mutate the external schema while another queue is
     * still writing rows with the stale in-memory schema.
     */
    @Test
    void schemaChangeWaitsUntilSiblingQueueLeavesWriteCriticalSection() throws Exception {
        RecordingSinkWriter sinkA = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);
        RecordingSinkWriter sinkB = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 1), sinkB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 2, buildContextMap(writers));
        MultiTableWriterRunnable queueOneRunnable = getRunnable(coordinator, 1);

        CountDownLatch siblingWriteEntered = new CountDownLatch(1);
        CountDownLatch releaseSiblingWrite = new CountDownLatch(1);
        Thread siblingWriteThread =
                new Thread(
                        () -> {
                            synchronized (queueOneRunnable) {
                                siblingWriteEntered.countDown();
                                awaitLatch(releaseSiblingWrite);
                            }
                        });
        siblingWriteThread.start();
        assertTrue(siblingWriteEntered.await(5, TimeUnit.SECONDS));

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        Thread schemaChangeThread =
                new Thread(
                        () -> {
                            schemaChangeSubmitted.countDown();
                            try {
                                coordinator.applySchemaChange(
                                        new TestSchemaChangeEvent(
                                                TablePath.of("dbA", null, "users")));
                            } catch (IOException error) {
                                throw new RuntimeException(error);
                            }
                        });
        schemaChangeThread.start();
        assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));

        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(0, sinkA.getInvocationCount());
        assertEquals(0, sinkB.getInvocationCount());

        releaseSiblingWrite.countDown();
        siblingWriteThread.join(TimeUnit.SECONDS.toMillis(5));
        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(siblingWriteThread.isAlive());
        assertFalse(schemaChangeThread.isAlive());
        assertEquals(1, sinkA.getInvocationCount());
        assertEquals(1, sinkB.getInvocationCount());
    }

    /**
     * Verifies that a buggy connector returning a null Optional does not crash schema-change
     * routing. The coordinator should simply keep the legacy source-only behavior.
     */
    @Test
    void nullPhysicalSinkIdentifierFallsBackToLegacyRouting() throws IOException {
        NullReturningPhysicalSinkWriter sinkA = new NullReturningPhysicalSinkWriter();
        RecordingSinkWriter sinkB = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sinkA);
        writers.put(SinkIdentifier.of("dbB.users", 0), sinkB);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));

        coordinator.applySchemaChange(
                new TestSchemaChangeEvent(TablePath.of("dbA", null, "users")));

        assertEquals(1, sinkA.getInvocationCount());
        assertEquals(0, sinkB.getInvocationCount());
    }

    /** Builds the writer-context map required by the multi-table coordinator constructor. */
    private Map<SinkIdentifier, SinkWriter.Context> buildContextMap(
            Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers) {
        Map<SinkIdentifier, SinkWriter.Context> contextMap = new LinkedHashMap<>();
        for (SinkIdentifier sinkIdentifier : writers.keySet()) {
            contextMap.put(sinkIdentifier, new TestSinkWriterContext());
        }
        return contextMap;
    }

    /**
     * Reads the private runnable list so the concurrency test can lock one queue exactly the same
     * way the production write path does.
     */
    @SuppressWarnings("unchecked")
    private MultiTableWriterRunnable getRunnable(MultiTableSinkWriter coordinator, int queueIndex)
            throws Exception {
        Field field = MultiTableSinkWriter.class.getDeclaredField("runnable");
        field.setAccessible(true);
        List<MultiTableWriterRunnable> runnables =
                (List<MultiTableWriterRunnable>) field.get(coordinator);
        return runnables.get(queueIndex);
    }

    /**
     * Waits for a latch inside helper threads without leaking checked exceptions into the test
     * harness.
     */
    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(error);
        }
    }

    /**
     * Minimal sink-writer implementation that records schema-change invocations and advertises a
     * shared physical sink identifier.
     */
    private static class RecordingSinkWriter
            implements SinkWriter<SeaTunnelRow, Void, Object>,
                    SupportMultiTableSinkWriter<Void>,
                    SupportSchemaEvolutionSinkWriter {

        /** Physical sink identifier returned to the coordinator. */
        private final String physicalSinkIdentifier;

        /** Counts how many schema-change events this writer observed. */
        private final AtomicInteger invocationCount = new AtomicInteger();

        private RecordingSinkWriter(String physicalSinkIdentifier) {
            this.physicalSinkIdentifier = physicalSinkIdentifier;
        }

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            invocationCount.incrementAndGet();
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}

        @Override
        public Optional<String> getPhysicalSinkTableIdentifier() {
            return Optional.of(physicalSinkIdentifier);
        }

        /** Returns the number of schema-change invocations received by this writer. */
        int getInvocationCount() {
            return invocationCount.get();
        }
    }

    /** Simulates a connector that incorrectly returns a null Optional from the new SPI method. */
    @SuppressWarnings("DataFlowIssue")
    private static class NullReturningPhysicalSinkWriter extends RecordingSinkWriter {

        private NullReturningPhysicalSinkWriter() {
            super(PHYSICAL_SINK_SHARED);
        }

        @Override
        public Optional<String> getPhysicalSinkTableIdentifier() {
            return null;
        }
    }

    /** Minimal writer context used by the coordinator constructor inside these tests. */
    private static class TestSinkWriterContext implements SinkWriter.Context {

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return new DefaultEventProcessor();
        }
    }

    /** Minimal schema-change event implementation for the coordinator regression tests. */
    private static class TestSchemaChangeEvent implements SchemaChangeEvent {

        /** Logical source table path carried by the event. */
        private final TableIdentifier tableIdentifier;

        /** Creation time exposed through the generic event contract. */
        private final long createdTime = System.currentTimeMillis();

        /** Mutable post-change catalog table reference required by the event contract. */
        private CatalogTable changeAfter;

        /** Job identifier exposed through the generic event contract. */
        private String jobId;

        private TestSchemaChangeEvent(TablePath tablePath) {
            this.tableIdentifier = TableIdentifier.of("test", tablePath);
        }

        @Override
        public TableIdentifier tableIdentifier() {
            return tableIdentifier;
        }

        @Override
        public long getCreatedTime() {
            return createdTime;
        }

        @Override
        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public EventType getEventType() {
            return EventType.SCHEMA_CHANGE_ADD_COLUMN;
        }

        @Override
        public CatalogTable getChangeAfter() {
            return changeAfter;
        }

        @Override
        public void setChangeAfter(CatalogTable table) {
            this.changeAfter = table;
        }
    }
}
