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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for shared-physical-sink schema evolution. When several source tables resolve
 * to the same physical sink table, the coordinator must broadcast schema changes to every sibling
 * sub-writer and route the DDL through the same in-band queue workers as data rows.
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

    /**
     * Once row writes and schema changes share one ordered queue, older queued rows must be
     * consumed before the schema change mutates the writer.
     */
    @Test
    void queuedRowsDrainBeforeSchemaChangeMutatesTheWriter() throws Exception {
        OrderedRecordingSinkWriter sink = new OrderedRecordingSinkWriter(PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        Thread schemaChangeThread;
        synchronized (queueRunnable) {
            coordinator.write(row);
            schemaChangeThread =
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
            assertTrue(
                    sink.callOrder.isEmpty(),
                    "the queue worker must stay blocked until the runnable monitor is released");
        }

        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(schemaChangeThread.isAlive());
        assertEquals(
                Arrays.asList("row", "schema"),
                sink.callOrder,
                "older queued rows must be consumed before the schema change runs");
    }

    /**
     * The first schema change after startup must also enter the in-band queue barrier path.
     * Otherwise a concurrent bootstrap write could still let the DDL bypass queued row ordering.
     */
    @Test
    void firstSchemaChangeAfterStartupStillWaitsForTheQueueBarrier() throws Exception {
        RecordingSinkWriter sink = new RecordingSinkWriter(PHYSICAL_SINK_SHARED);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
        Thread schemaChangeThread;
        synchronized (queueRunnable) {
            schemaChangeThread =
                    new Thread(
                            () -> {
                                schemaChangeSubmitted.countDown();
                                try {
                                    coordinator.applySchemaChange(
                                            new TestSchemaChangeEvent(
                                                    TablePath.of("dbA", null, "users")));
                                } catch (Throwable throwable) {
                                    schemaChangeFailure.set(throwable);
                                }
                            });
            schemaChangeThread.start();
            assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));
            TimeUnit.MILLISECONDS.sleep(200);
            assertEquals(
                    0,
                    sink.getInvocationCount(),
                    "the first schema change must stay queued behind the runnable monitor");
            assertTrue(
                    schemaChangeFailure.get() == null,
                    "the startup schema change should block on the shared barrier instead of failing");
        }

        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(schemaChangeThread.isAlive());
        assertTrue(schemaChangeFailure.get() == null);
        assertEquals(1, sink.getInvocationCount());
    }

    /**
     * If a queued old-schema row fails before the barrier is reached, applySchemaChange must fail
     * fast with the original write error instead of waiting forever for a dead queue worker.
     */
    @Test
    void schemaChangeFailsFastWhenQueuedRowWriteFailsBeforeBarrier() throws Exception {
        IOException rowWriteFailure = new IOException("boom-before-barrier");
        FailingRowSinkWriter sink = new FailingRowSinkWriter(PHYSICAL_SINK_SHARED, rowWriteFailure);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");

        CountDownLatch schemaChangeSubmitted = new CountDownLatch(1);
        AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
        Thread schemaChangeThread;
        synchronized (queueRunnable) {
            coordinator.write(row);
            schemaChangeThread =
                    new Thread(
                            () -> {
                                schemaChangeSubmitted.countDown();
                                try {
                                    coordinator.applySchemaChange(
                                            new TestSchemaChangeEvent(
                                                    TablePath.of("dbA", null, "users")));
                                } catch (Throwable throwable) {
                                    schemaChangeFailure.set(throwable);
                                }
                            });
            schemaChangeThread.start();
            assertTrue(schemaChangeSubmitted.await(5, TimeUnit.SECONDS));
            TimeUnit.MILLISECONDS.sleep(200);
            assertTrue(
                    schemaChangeFailure.get() == null,
                    "schema change should still be waiting on the shared barrier before release");
        }

        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(
                schemaChangeThread.isAlive(),
                "schema change thread must fail fast instead of hanging behind a dead worker");
        assertTrue(schemaChangeFailure.get() instanceof IOException);
        assertEquals("boom-before-barrier", schemaChangeFailure.get().getMessage());
        assertEquals(0, sink.getInvocationCount());
    }

    /**
     * Once a queue worker has already failed, the schema-change entry point must still honor its
     * declared {@link IOException} contract instead of letting a raw runtime failure escape.
     */
    @Test
    void schemaChangeKeepsIOExceptionContractWhenWorkerAlreadyFailed() throws Exception {
        IOException rowWriteFailure = new IOException("boom-before-schema-entry");
        FailingRowSinkWriter sink = new FailingRowSinkWriter(PHYSICAL_SINK_SHARED, rowWriteFailure);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");
        coordinator.write(row);

        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (queueRunnable.getThrowable() == null && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        assertTrue(
                queueRunnable.getThrowable() != null,
                "the worker must observe the row failure before schema change retries the contract");

        IOException schemaChangeFailure =
                org.junit.jupiter.api.Assertions.assertThrows(
                        IOException.class,
                        () ->
                                coordinator.applySchemaChange(
                                        new TestSchemaChangeEvent(
                                                TablePath.of("dbA", null, "users"))));
        assertEquals("boom-before-schema-entry", schemaChangeFailure.getMessage());
        // checkQueueRemain() (invoked by close()) only re-checks subSinkErrorCheck() while a
        // queue element still looks pending, and MultiTableWriterRunnable clears that pending
        // flag in a separate volatile write issued after the worker's throwable field is already
        // stored (MultiTableWriterRunnable.run()). Whether close() observes that narrow window
        // and re-surfaces the same row failure is a timing race in existing close() behavior, not
        // a regression and not what this test verifies above. Tolerate either outcome, but fail
        // if close() surfaces anything other than the exact failure already asserted.
        try {
            coordinator.close();
        } catch (RuntimeException e) {
            org.junit.jupiter.api.Assertions.assertSame(
                    rowWriteFailure,
                    e.getCause(),
                    "close() surfaced an unexpected failure instead of the known row failure: "
                            + e);
        }
    }

    /**
     * Models the worker-exit window after applySchemaChange's final pre-enqueue error check. A
     * barrier published to an already-dead queue must observe the terminal worker failure instead
     * of waiting forever for a notification that can no longer arrive.
     */
    @Test
    void schemaChangeBarrierDetectsWorkerFailureAfterPreEnqueueCheck() throws Exception {
        IOException rowWriteFailure = new IOException("boom-during-barrier-enqueue");
        FailingRowSinkWriter sink = new FailingRowSinkWriter(PHYSICAL_SINK_SHARED, rowWriteFailure);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> writers = new HashMap<>();
        writers.put(SinkIdentifier.of("dbA.users", 0), sink);

        MultiTableSinkWriter coordinator =
                new MultiTableSinkWriter(writers, 1, buildContextMap(writers));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.setTableId("dbA.users");
        coordinator.write(row);

        MultiTableWriterRunnable queueRunnable = getRunnable(coordinator, 0);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (queueRunnable.getThrowable() == null && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        assertTrue(queueRunnable.getThrowable() != null);

        AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
        Thread schemaChangeThread =
                new Thread(
                        () -> {
                            try {
                                invokeSchemaChangeBarrier(
                                        coordinator,
                                        new TestSchemaChangeEvent(
                                                TablePath.of("dbA", null, "users")));
                            } catch (Throwable throwable) {
                                schemaChangeFailure.set(throwable);
                            }
                        });
        schemaChangeThread.start();
        schemaChangeThread.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(
                schemaChangeThread.isAlive(),
                "a barrier enqueued after worker exit must not wait forever");
        assertTrue(schemaChangeFailure.get() instanceof IOException);
        assertEquals("boom-during-barrier-enqueue", schemaChangeFailure.get().getMessage());
        RuntimeException closeFailure =
                org.junit.jupiter.api.Assertions.assertThrows(
                        RuntimeException.class, coordinator::close);
        assertTrue(closeFailure.getCause() instanceof IOException);
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
     * Reads the private runnable list so the concurrency tests can lock one queue exactly the same
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

    private void invokeSchemaChangeBarrier(
            MultiTableSinkWriter coordinator, SchemaChangeEvent schemaChangeEvent)
            throws Throwable {
        Method method =
                MultiTableSinkWriter.class.getDeclaredMethod(
                        "enqueueSchemaChangeBarrier", SchemaChangeEvent.class);
        method.setAccessible(true);
        try {
            method.invoke(coordinator, schemaChangeEvent);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
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
        public void write(SeaTunnelRow element) throws IOException {}

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

    /**
     * Captures the visible order between row writes and schema changes so backlog-ordering
     * regressions fail deterministically.
     */
    private static class OrderedRecordingSinkWriter extends RecordingSinkWriter {

        private final List<String> callOrder = new java.util.ArrayList<>();

        private OrderedRecordingSinkWriter(String physicalSinkIdentifier) {
            super(physicalSinkIdentifier);
        }

        @Override
        public synchronized void write(SeaTunnelRow element) throws IOException {
            callOrder.add("row");
        }

        @Override
        public synchronized void applySchemaChange(SchemaChangeEvent event) {
            callOrder.add("schema");
            super.applySchemaChange(event);
        }
    }

    /**
     * Writer used to prove a row failure ahead of the barrier fails schema change instead of
     * hanging.
     */
    private static class FailingRowSinkWriter extends RecordingSinkWriter {

        private final IOException writeFailure;

        private FailingRowSinkWriter(String physicalSinkIdentifier, IOException writeFailure) {
            super(physicalSinkIdentifier);
            this.writeFailure = writeFailure;
        }

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            throw writeFailure;
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
