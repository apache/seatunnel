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
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiTableSinkWriterTest {

    @Test
    public void testPrepareCommitState() throws IOException {
        int threads = 50;
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        for (int i = 0; i < threads; i++) {
            sinkWriters.put(
                    SinkIdentifier.of(TablePath.DEFAULT.toString(), i), new TestSinkWriter());
            sinkWritersContext.put(
                    SinkIdentifier.of(TablePath.DEFAULT.toString(), i),
                    new TestSinkWriterContext());
        }
        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, threads, sinkWritersContext);
        DefaultSerializer<Serializable> defaultSerializer = new DefaultSerializer<>();

        for (int i = 0; i < 100; i++) {
            byte[] bytes = defaultSerializer.serialize(multiTableSinkWriter.prepareCommit(i).get());
            defaultSerializer.deserialize(bytes);
        }
    }

    @Test
    public void testContinueOtherTablesKeepsHealthyTableRunning() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter failedWriter = new RecordingSinkWriter(true);
        RecordingSinkWriter healthyWriter = new RecordingSinkWriter(false);
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 1);
        sinkWriters.put(failedIdentifier, failedWriter);
        sinkWriters.put(healthyIdentifier, healthyWriter);
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH);

        multiTableSinkWriter.write(buildRow("test.failed", 0));
        multiTableSinkWriter.write(buildRow("test.healthy", 1));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, commitInfo.get().getCommitInfo().size());
        Assertions.assertTrue(
                commitInfo.get().getCommitInfo().keySet().stream()
                        .allMatch(
                                identifier ->
                                        "test.healthy".equals(identifier.getTableIdentifier())));
        Assertions.assertEquals(1, healthyWriter.getWriteCount());
        Assertions.assertEquals(1, failedWriter.getWriteCount());

        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed"));
    }

    @Test
    public void testInitialFailedTableIsSkippedAndReported() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter skippedWriter = new RecordingSinkWriter(false);
        RecordingSinkWriter healthyWriter = new RecordingSinkWriter(false);
        SinkIdentifier skippedIdentifier = SinkIdentifier.of("test.skipped", 0);
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 1);
        sinkWriters.put(skippedIdentifier, skippedWriter);
        sinkWriters.put(healthyIdentifier, healthyWriter);
        sinkWritersContext.put(skippedIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        MultiTableFailedTable initialFailedTable =
                MultiTableFailureHelper.buildFailedTable(
                        "test.skipped",
                        MultiTableFailurePhase.SINK_INIT,
                        "console",
                        new RuntimeException("startup failure"));
        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH,
                        Collections.singletonList(initialFailedTable));

        multiTableSinkWriter.write(buildRow("test.skipped", 0));
        multiTableSinkWriter.write(buildRow("test.healthy", 1));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, commitInfo.get().getCommitInfo().size());
        Assertions.assertEquals(0, skippedWriter.getWriteCount());
        Assertions.assertEquals(1, healthyWriter.getWriteCount());

        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.skipped"));
        Assertions.assertTrue(closeException.getMessage().contains("startup failure"));
    }

    @Test
    public void testRuntimeWriteRetriesFailedTableBeforeIsolation() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter failedThenRecoveredWriter = new RetryableWriteSinkWriter(2);
        RecordingSinkWriter healthyWriter = new RecordingSinkWriter(false);
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 1);
        sinkWriters.put(failedIdentifier, failedThenRecoveredWriter);
        sinkWriters.put(healthyIdentifier, healthyWriter);
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH,
                        2,
                        0);

        multiTableSinkWriter.write(buildRow("test.failed", 0));
        multiTableSinkWriter.write(buildRow("test.healthy", 1));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(2, commitInfo.get().getCommitInfo().size());
        Assertions.assertEquals(3, failedThenRecoveredWriter.getWriteCount());
        Assertions.assertEquals(1, healthyWriter.getWriteCount());
        Assertions.assertDoesNotThrow(multiTableSinkWriter::close);
    }

    @Test
    public void testRuntimeWriteIsolatesFailedTableAfterRetryExhausted() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter failedWriter = new RecordingSinkWriter(true);
        RecordingSinkWriter healthyWriter = new RecordingSinkWriter(false);
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 1);
        sinkWriters.put(failedIdentifier, failedWriter);
        sinkWriters.put(healthyIdentifier, healthyWriter);
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH,
                        2,
                        0);

        multiTableSinkWriter.write(buildRow("test.failed", 0));
        multiTableSinkWriter.write(buildRow("test.healthy", 1));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, commitInfo.get().getCommitInfo().size());
        Assertions.assertEquals(3, failedWriter.getWriteCount());
        Assertions.assertEquals(1, healthyWriter.getWriteCount());

        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed"));
        Assertions.assertTrue(
                MultiTableFailureHelper.isIsolatedFailure(closeException.getMessage()));
    }

    @Test
    public void testRuntimeWriteZeroRetryKeepsImmediateIsolation() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter failedWriter = new RecordingSinkWriter(true);
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        sinkWriters.put(failedIdentifier, failedWriter);
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH,
                        0,
                        0);

        multiTableSinkWriter.write(buildRow("test.failed", 0));

        RuntimeException prepareCommitException =
                Assertions.assertThrows(
                        RuntimeException.class, () -> multiTableSinkWriter.prepareCommit(1L));
        Assertions.assertTrue(
                MultiTableFailureHelper.isIsolatedFailure(prepareCommitException.getMessage()));
        Assertions.assertEquals(1, failedWriter.getWriteCount());
        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(
                MultiTableFailureHelper.isIsolatedFailure(closeException.getMessage()));
    }

    @Test
    public void testFailFastDoesNotUseTableRetry() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter failedWriter = new RecordingSinkWriter(true);
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        sinkWriters.put(failedIdentifier, failedWriter);
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.FAIL_FAST,
                        JobMode.BATCH,
                        2,
                        0);

        multiTableSinkWriter.write(buildRow("test.failed", 0));

        Assertions.assertThrows(
                RuntimeException.class, () -> multiTableSinkWriter.prepareCommit(1L));
        Assertions.assertEquals(1, failedWriter.getWriteCount());
        multiTableSinkWriter.close();
    }

    @Test
    public void testPrepareCommitRetriesBeforeIsolation() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        PrepareCommitRetrySinkWriter retryWriter = new PrepareCommitRetrySinkWriter(1);
        SinkIdentifier sinkIdentifier = SinkIdentifier.of("test.retry", 0);
        sinkWriters.put(sinkIdentifier, retryWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH,
                        1,
                        0);

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, commitInfo.get().getCommitInfo().size());
        Assertions.assertEquals(2, retryWriter.getPrepareCommitCount());
        Assertions.assertDoesNotThrow(multiTableSinkWriter::close);
    }

    @Test
    public void testSnapshotStateRetriesBeforeIsolation() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        SnapshotRetrySinkWriter retryWriter = new SnapshotRetrySinkWriter(1);
        SinkIdentifier sinkIdentifier = SinkIdentifier.of("test.retry", 0);
        sinkWriters.put(sinkIdentifier, retryWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH,
                        1,
                        0);

        List<MultiTableState> states = multiTableSinkWriter.snapshotState(1L);

        Assertions.assertEquals(1, states.size());
        Assertions.assertEquals(1, retryWriter.getSnapshotCount());
        Assertions.assertEquals(2, retryWriter.getSnapshotAttemptCount());
        Assertions.assertDoesNotThrow(multiTableSinkWriter::close);
    }

    @Test
    public void testIsolatedFailureMarkerRecognition() {
        String message =
                MultiTableFailureHelper.withIsolatedFailureMarker(
                        "Failed tables were isolated in multi-table sink.");

        Assertions.assertTrue(MultiTableFailureHelper.isIsolatedFailure(message));
        Assertions.assertFalse(MultiTableFailureHelper.isIsolatedFailure(null));
        Assertions.assertFalse(MultiTableFailureHelper.isIsolatedFailure("regular failure"));
    }

    @Test
    public void testSingleWriterFallbackAcceptsExplicitTableId() {
        Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap = new HashMap<>();
        RecordingSinkWriter onlyWriter = new RecordingSinkWriter(false, true);
        BlockingQueue<SeaTunnelRow> queue = new LinkedBlockingQueue<>(1);
        tableIdWriterMap.put("http", onlyWriter);
        queue.add(buildRow("Optional[http]", 1));

        MultiTableWriterRunnable runnable = new MultiTableWriterRunnable(tableIdWriterMap, queue);
        runnable.run();

        Assertions.assertNull(runnable.getThrowable());
        Assertions.assertEquals(1, onlyWriter.getWriteCount());
        Assertions.assertEquals("http", runnable.getCurrentTableId());
    }

    @Test
    public void testRunnableSelectsWriterUnderLock() {
        GuardedWriterMap tableIdWriterMap = new GuardedWriterMap();
        RecordingSinkWriter writer = new RecordingSinkWriter(false, true);
        BlockingQueue<SeaTunnelRow> queue = new LinkedBlockingQueue<>(1);
        tableIdWriterMap.put("test.table", writer);
        queue.add(buildRow("test.table", 1));

        MultiTableWriterRunnable runnable = new MultiTableWriterRunnable(tableIdWriterMap, queue);
        tableIdWriterMap.setRequiredLock(runnable);
        runnable.run();

        Assertions.assertNull(runnable.getThrowable());
        Assertions.assertEquals(1, writer.getWriteCount());
    }

    @Test
    public void testRunnableDoesNotHoldLockWhileWaitingForRows() throws Exception {
        BlockingPollQueue queue = new BlockingPollQueue();
        Map<String, SinkWriter<SeaTunnelRow, ?, ?>> tableIdWriterMap = new HashMap<>();
        tableIdWriterMap.put("test.table", new RecordingSinkWriter(false));
        MultiTableWriterRunnable runnable = new MultiTableWriterRunnable(tableIdWriterMap, queue);
        Thread worker = new Thread(runnable);
        worker.start();

        Assertions.assertTrue(queue.awaitPollStarted());
        FutureTask<Boolean> lockProbe =
                new FutureTask<>(
                        () -> {
                            synchronized (runnable) {
                                return true;
                            }
                        });
        Thread lockProbeThread = new Thread(lockProbe);
        lockProbeThread.start();

        try {
            Assertions.assertTrue(lockProbe.get(200, TimeUnit.MILLISECONDS));
        } finally {
            queue.releasePoll();
            worker.interrupt();
            worker.join(1000);
            lockProbeThread.join(1000);
        }
    }

    @Test
    public void testSingleWriterAcceptsNullTableId() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter onlyWriter = new RecordingSinkWriter(false);
        SinkIdentifier sinkIdentifier = SinkIdentifier.of(TablePath.DEFAULT.toString(), 0);
        sinkWriters.put(sinkIdentifier, onlyWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 1, sinkWritersContext);

        multiTableSinkWriter.write(buildRow(null, 1));
        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, onlyWriter.getWriteCount());
    }

    @Test
    public void testFailedTableMetadataIsSerializable() throws IOException {
        MultiTableFailedTable failedTable =
                MultiTableFailureHelper.buildFailedTable(
                        "test.skipped",
                        MultiTableFailurePhase.SINK_INIT,
                        "console",
                        new RuntimeException("startup failure"));
        DefaultSerializer<MultiTableFailedTable> serializer = new DefaultSerializer<>();

        byte[] bytes = serializer.serialize(failedTable);
        MultiTableFailedTable restored = serializer.deserialize(bytes);

        Assertions.assertEquals("test.skipped", restored.getTablePath());
        Assertions.assertEquals(MultiTableFailurePhase.SINK_INIT, restored.getPhase());
        Assertions.assertEquals("console", restored.getPluginName());
        Assertions.assertEquals("RuntimeException", restored.getExceptionClass());
        Assertions.assertEquals("startup failure", restored.getMessageSummary());
        Assertions.assertEquals(failedTable.getFirstFailureTime(), restored.getFirstFailureTime());
        Assertions.assertNull(restored.getCause());
    }

    private SeaTunnelRow buildRow(String tableId, int value) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {value});
        row.setTableId(tableId);
        return row;
    }

    static class TestSinkWriter
            implements SinkWriter<SeaTunnelRow, TestSinkState, Object>,
                    SupportMultiTableSinkWriter {
        @Override
        public void write(SeaTunnelRow seaTunnelRow) {}

        @Override
        public Optional<TestSinkState> prepareCommit() throws IOException {
            return Optional.of(new TestSinkState("test"));
        }

        @Override
        public List<Object> snapshotState(long checkpointId) throws IOException {
            return SinkWriter.super.snapshotState(checkpointId);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}

        @Override
        public Optional<Integer> primaryKey() {
            return Optional.of(0);
        }
    }

    static class RecordingSinkWriter extends TestSinkWriter {
        private final boolean failOnWrite;
        private final boolean interruptAfterWrite;
        private final AtomicInteger writeCount = new AtomicInteger();

        RecordingSinkWriter(boolean failOnWrite) {
            this(failOnWrite, false);
        }

        RecordingSinkWriter(boolean failOnWrite, boolean interruptAfterWrite) {
            this.failOnWrite = failOnWrite;
            this.interruptAfterWrite = interruptAfterWrite;
        }

        @Override
        public void write(SeaTunnelRow seaTunnelRow) {
            writeCount.incrementAndGet();
            if (failOnWrite) {
                throw new RuntimeException("intentional sink failure");
            }
            if (interruptAfterWrite) {
                Thread.currentThread().interrupt();
            }
        }

        int getWriteCount() {
            return writeCount.get();
        }
    }

    static class RetryableWriteSinkWriter extends RecordingSinkWriter {
        private final int failuresBeforeSuccess;

        RetryableWriteSinkWriter(int failuresBeforeSuccess) {
            super(false);
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public void write(SeaTunnelRow seaTunnelRow) {
            super.write(seaTunnelRow);
            if (getWriteCount() <= failuresBeforeSuccess) {
                throw new RuntimeException("temporary sink failure");
            }
        }
    }

    static class PrepareCommitRetrySinkWriter extends RecordingSinkWriter {
        private final int failuresBeforeSuccess;
        private final AtomicInteger prepareCommitCount = new AtomicInteger();

        PrepareCommitRetrySinkWriter(int failuresBeforeSuccess) {
            super(false);
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public Optional<TestSinkState> prepareCommit(long checkpointId) throws IOException {
            int count = prepareCommitCount.incrementAndGet();
            if (count <= failuresBeforeSuccess) {
                throw new IOException("temporary prepare commit failure");
            }
            return Optional.of(new TestSinkState("retry"));
        }

        int getPrepareCommitCount() {
            return prepareCommitCount.get();
        }
    }

    static class SnapshotRetrySinkWriter extends RecordingSinkWriter {
        private final int failuresBeforeSuccess;
        private final AtomicInteger snapshotAttemptCount = new AtomicInteger();
        private final AtomicInteger snapshotCount = new AtomicInteger();

        SnapshotRetrySinkWriter(int failuresBeforeSuccess) {
            super(false);
            this.failuresBeforeSuccess = failuresBeforeSuccess;
        }

        @Override
        public List<Object> snapshotState(long checkpointId) throws IOException {
            int count = snapshotAttemptCount.incrementAndGet();
            if (count <= failuresBeforeSuccess) {
                throw new IOException("temporary snapshot failure");
            }
            snapshotCount.incrementAndGet();
            return Collections.singletonList(new TestSinkState("snapshot"));
        }

        int getSnapshotAttemptCount() {
            return snapshotAttemptCount.get();
        }

        int getSnapshotCount() {
            return snapshotCount.get();
        }
    }

    static class GuardedWriterMap extends HashMap<String, SinkWriter<SeaTunnelRow, ?, ?>> {
        private static final long serialVersionUID = 1L;

        private Object requiredLock;

        void setRequiredLock(Object requiredLock) {
            this.requiredLock = requiredLock;
        }

        @Override
        public SinkWriter<SeaTunnelRow, ?, ?> get(Object key) {
            assertLocked();
            return super.get(key);
        }

        private void assertLocked() {
            if (requiredLock != null && !Thread.holdsLock(requiredLock)) {
                throw new AssertionError("table writer map must be read under runnable lock");
            }
        }
    }

    static class BlockingPollQueue extends LinkedBlockingQueue<SeaTunnelRow> {
        private static final long serialVersionUID = 1L;

        private final CountDownLatch pollStarted = new CountDownLatch(1);
        private final CountDownLatch releasePoll = new CountDownLatch(1);

        @Override
        public SeaTunnelRow poll(long timeout, TimeUnit unit) throws InterruptedException {
            pollStarted.countDown();
            releasePoll.await();
            return null;
        }

        boolean awaitPollStarted() throws InterruptedException {
            return pollStarted.await(1, TimeUnit.SECONDS);
        }

        void releasePoll() {
            releasePoll.countDown();
        }
    }

    static class TestSinkWriterContext implements SinkWriter.Context {

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

    @Data
    @AllArgsConstructor
    static class TestSinkState implements Serializable {
        private String state;
    }
}
