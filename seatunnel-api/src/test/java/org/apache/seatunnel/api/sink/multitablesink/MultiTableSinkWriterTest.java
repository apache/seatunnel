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

import org.apache.seatunnel.api.common.error.RowErrorHandlingFatalException;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.DefaultEventProcessor;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.MultiTableCommonOptions;
import org.apache.seatunnel.api.options.MultiTableFailurePolicy;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.HashUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

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
    public void testRuntimeWriteIsolationIgnoresQuarantinedWriterCloseFailure() throws IOException {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        CloseFailingSinkWriter failedWriter = new CloseFailingSinkWriter(true);
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
                        0,
                        0);

        multiTableSinkWriter.write(buildRow("test.failed", 0));
        multiTableSinkWriter.write(buildRow("test.healthy", 1));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, commitInfo.get().getCommitInfo().size());
        Assertions.assertEquals(1, healthyWriter.getWriteCount());
        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed"));
        Assertions.assertTrue(
                MultiTableFailureHelper.isIsolatedFailure(closeException.getMessage()));
    }

    @Test
    public void testRestoreWriterSkipsRuntimeFailedTablesFromCheckpointState() throws IOException {
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
                        0,
                        0);

        multiTableSinkWriter.write(buildRow("test.failed", 0));
        multiTableSinkWriter.write(buildRow("test.healthy", 1));

        List<MultiTableState> states = multiTableSinkWriter.snapshotState(1L);

        Assertions.assertEquals(1, states.size());
        Assertions.assertEquals(1, states.get(0).getFailedTables().size());
        Assertions.assertEquals(
                "test.failed", states.get(0).getFailedTables().get(0).getTablePath());
        Assertions.assertEquals(1, failedWriter.getWriteCount());
        Assertions.assertEquals(1, healthyWriter.getWriteCount());
        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed"));

        RecordingSinkWriter restoredFailedWriter = new RecordingSinkWriter(false);
        RecordingSinkWriter restoredHealthyWriter = new RecordingSinkWriter(false);
        Map<TablePath, SeaTunnelSink> restoredSinks = new HashMap<>();
        restoredSinks.put(TablePath.of("test.failed"), new TestSeaTunnelSink(restoredFailedWriter));
        restoredSinks.put(
                TablePath.of("test.healthy"), new TestSeaTunnelSink(restoredHealthyWriter));
        Map<String, Object> options = new HashMap<>();
        options.put(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(), 1);
        options.put(
                MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key(),
                MultiTableFailurePolicy.CONTINUE_OTHER_TABLES.name());
        options.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        options.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);
        MultiTableSink multiTableSink =
                new MultiTableSink(
                        new MultiTableFactoryContext(
                                ReadonlyConfig.fromMap(options),
                                Thread.currentThread().getContextClassLoader(),
                                restoredSinks));
        SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> restoredWriter =
                multiTableSink.restoreWriter(new TestSinkWriterContext(), states);

        restoredWriter.write(buildRow("test.failed", 2));
        restoredWriter.write(buildRow("test.healthy", 3));
        Optional<MultiTableCommitInfo> commitInfo = restoredWriter.prepareCommit(2L);

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, commitInfo.get().getCommitInfo().size());
        Assertions.assertTrue(
                commitInfo.get().getCommitInfo().keySet().stream()
                        .allMatch(
                                identifier ->
                                        "test.healthy".equals(identifier.getTableIdentifier())));
        Assertions.assertEquals(0, restoredFailedWriter.getWriteCount());
        Assertions.assertEquals(1, restoredHealthyWriter.getWriteCount());
        IOException restoredCloseException =
                Assertions.assertThrows(IOException.class, restoredWriter::close);
        Assertions.assertTrue(restoredCloseException.getMessage().contains("test.failed"));
    }

    @Test
    public void testRestoreWriterDoesNotRestorePreviouslyFailedTable() throws IOException {
        MultiTableFailedTable failedTable =
                MultiTableFailureHelper.buildFailedTable(
                        "test.failed",
                        MultiTableFailurePhase.RUNTIME_WRITE,
                        "test",
                        new RuntimeException("restore should skip"));
        MultiTableState state =
                new MultiTableState(new HashMap<>(), Collections.singletonList(failedTable));
        RecordingSinkWriter healthyWriter = new RecordingSinkWriter(false);
        Map<TablePath, SeaTunnelSink> restoredSinks = new HashMap<>();
        restoredSinks.put(TablePath.of("test.failed"), new ThrowingRestoreSink());
        restoredSinks.put(TablePath.of("test.healthy"), new TestSeaTunnelSink(healthyWriter));
        Map<String, Object> options = new HashMap<>();
        options.put(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(), 1);
        options.put(
                MultiTableCommonOptions.MULTI_TABLE_FAILURE_POLICY.key(),
                MultiTableFailurePolicy.CONTINUE_OTHER_TABLES.name());
        options.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        options.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);
        MultiTableSink multiTableSink =
                new MultiTableSink(
                        new MultiTableFactoryContext(
                                ReadonlyConfig.fromMap(options),
                                Thread.currentThread().getContextClassLoader(),
                                restoredSinks));

        SinkWriter<SeaTunnelRow, MultiTableCommitInfo, MultiTableState> restoredWriter =
                multiTableSink.restoreWriter(
                        new TestSinkWriterContext(), Collections.singletonList(state));
        restoredWriter.write(buildRow("test.healthy", 1));

        Optional<MultiTableCommitInfo> commitInfo = restoredWriter.prepareCommit(1L);

        Assertions.assertTrue(commitInfo.isPresent());
        Assertions.assertEquals(1, healthyWriter.getWriteCount());
        IOException closeException =
                Assertions.assertThrows(IOException.class, restoredWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed"));
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

        IOException prepareCommitException =
                Assertions.assertThrows(
                        IOException.class, () -> multiTableSinkWriter.prepareCommit(1L));
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

        Assertions.assertThrows(IOException.class, () -> multiTableSinkWriter.prepareCommit(1L));
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
    public void testPrepareCommitIsolatesConcurrentFailures() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 2);
        sinkWriters.put(healthyIdentifier, new TestSinkWriter());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        CountDownLatch preparesStarted = new CountDownLatch(2);
        AtomicInteger failedPrepares = new AtomicInteger();
        for (int queueIndex = 0; queueIndex < 2; queueIndex++) {
            SinkIdentifier failedIdentifier =
                    SinkIdentifier.of("test.failed" + queueIndex, queueIndex);
            sinkWriters.put(
                    failedIdentifier,
                    new TestSinkWriter() {
                        @Override
                        public Optional<TestSinkState> prepareCommit(long checkpointId)
                                throws IOException {
                            preparesStarted.countDown();
                            try {
                                Assertions.assertTrue(preparesStarted.await(5, TimeUnit.SECONDS));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException(e);
                            }
                            failedPrepares.incrementAndGet();
                            throw new IOException("intentional prepare failure");
                        }
                    });
            sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
        }

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH);
        MultiTableCommitInfo commitInfo =
                Assertions.assertTimeoutPreemptively(
                        Duration.ofSeconds(10),
                        () -> multiTableSinkWriter.prepareCommit(1L).orElseThrow());
        Assertions.assertEquals(2, failedPrepares.get());
        Assertions.assertEquals(
                Collections.singleton(healthyIdentifier), commitInfo.getCommitInfo().keySet());

        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed0"));
        Assertions.assertTrue(closeException.getMessage().contains("test.failed1"));
        Assertions.assertTrue(closeException.getMessage().contains("phase=checkpoint"));
    }

    @Test
    public void testPrepareCommitFatalRowErrorHandlingFailureFailsFast() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        FatalPrepareCommitSinkWriter fatalWriter = new FatalPrepareCommitSinkWriter();
        SinkIdentifier sinkIdentifier = SinkIdentifier.of("test.fatal", 0);
        sinkWriters.put(sinkIdentifier, fatalWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.STREAMING,
                        2,
                        0);

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class, () -> multiTableSinkWriter.prepareCommit(1L));

        Assertions.assertFalse(MultiTableFailureHelper.isIsolatedFailure(exception.getMessage()));
        Assertions.assertEquals(1, fatalWriter.getPrepareCommitCount());
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
    public void testSnapshotStateFatalRowErrorHandlingFailureFailsFast() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        FatalSnapshotSinkWriter fatalWriter = new FatalSnapshotSinkWriter();
        SinkIdentifier sinkIdentifier = SinkIdentifier.of("test.fatal", 0);
        sinkWriters.put(sinkIdentifier, fatalWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.STREAMING,
                        2,
                        0);

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class, () -> multiTableSinkWriter.snapshotState(1L));

        Assertions.assertFalse(MultiTableFailureHelper.isIsolatedFailure(exception.getMessage()));
        Assertions.assertEquals(1, fatalWriter.getSnapshotAttemptCount());
    }

    @Test
    public void testAggregatedFlushIsolatesConcurrentFailuresAndFlushesHealthyTable() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 2);
        sinkWriters.put(healthyIdentifier, new TestSinkWriter());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        AtomicInteger failedFlushCount = new AtomicInteger();
        CountDownLatch failedFlushesStarted = new CountDownLatch(2);
        for (int queueIndex = 0; queueIndex < 2; queueIndex++) {
            SinkIdentifier failedIdentifier =
                    SinkIdentifier.of("test.failed" + queueIndex, queueIndex);
            sinkWriters.put(failedIdentifier, new TestSinkWriter());
            sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
            SinkContextProxy failedProxy =
                    new SinkContextProxy(queueIndex, 2, new TestSinkWriterContext());
            failedProxy.registerFlushAction(
                    () -> {
                        failedFlushCount.incrementAndGet();
                        failedFlushesStarted.countDown();
                        Assertions.assertTrue(failedFlushesStarted.await(5, TimeUnit.SECONDS));
                        throw new IOException("intentional flush failure");
                    });
            proxyContexts.put(failedIdentifier, failedProxy);
        }
        AtomicInteger healthyFlushCount = new AtomicInteger();
        SinkContextProxy healthyProxy = new SinkContextProxy(2, 2, new TestSinkWriterContext());
        healthyProxy.registerFlushAction(healthyFlushCount::incrementAndGet);
        proxyContexts.put(healthyIdentifier, healthyProxy);

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH);

        Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(10), () -> multiTableSinkWriter.aggregatedFlush(proxyContexts));
        Assertions.assertEquals(2, failedFlushCount.get());
        Assertions.assertEquals(1, healthyFlushCount.get());

        Assertions.assertDoesNotThrow(() -> multiTableSinkWriter.aggregatedFlush(proxyContexts));
        Assertions.assertEquals(2, failedFlushCount.get());
        Assertions.assertEquals(2, healthyFlushCount.get());

        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed0"));
        Assertions.assertTrue(closeException.getMessage().contains("test.failed1"));
        Assertions.assertTrue(closeException.getMessage().contains("phase=timer_flush"));
    }

    @Test
    public void testAggregatedFlushReportsFailureAndClosesQueuesIndependently() throws Exception {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        SinkIdentifier slowIdentifier = SinkIdentifier.of("test.slow", 1);
        SinkIdentifier siblingIdentifier = SinkIdentifier.of("test.sibling", 3);
        CountDownLatch failedWriterClosed = new CountDownLatch(1);
        AtomicBoolean slowFlushFinished = new AtomicBoolean();
        AtomicBoolean slowWriterClosed = new AtomicBoolean();
        sinkWriters.put(
                failedIdentifier,
                new TestSinkWriter() {
                    @Override
                    public void close() {
                        failedWriterClosed.countDown();
                    }
                });
        sinkWriters.put(
                slowIdentifier,
                new TestSinkWriter() {
                    @Override
                    public void close() {
                        Assertions.assertTrue(slowFlushFinished.get());
                        slowWriterClosed.set(true);
                    }
                });
        sinkWriters.put(siblingIdentifier, new TestSinkWriter());
        sinkWritersContext.put(slowIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(siblingIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());

        AtomicInteger slowQueueFlushCount = new AtomicInteger();
        CountDownLatch slowFlushStarted = new CountDownLatch(1);
        CompletableFuture<Void> releaseSlowFlush = new CompletableFuture<>();
        IOException expectedFailure = new IOException("intentional flush failure");
        SinkContextProxy slowProxy = new SinkContextProxy(1, 2, new TestSinkWriterContext());
        slowProxy.registerFlushAction(
                () -> {
                    // Block whichever child is visited first on this queue.
                    if (slowQueueFlushCount.incrementAndGet() == 1) {
                        slowFlushStarted.countDown();
                        releaseSlowFlush.join();
                        slowFlushFinished.set(true);
                    }
                });
        SinkContextProxy siblingProxy = new SinkContextProxy(3, 2, new TestSinkWriterContext());
        siblingProxy.registerFlushAction(slowProxy.getFlushAction());
        SinkContextProxy failedProxy = new SinkContextProxy(0, 2, new TestSinkWriterContext());
        failedProxy.registerFlushAction(
                () -> {
                    Assertions.assertTrue(slowFlushStarted.await(5, TimeUnit.SECONDS));
                    throw expectedFailure;
                });
        proxyContexts.put(slowIdentifier, slowProxy);
        proxyContexts.put(siblingIdentifier, siblingProxy);
        proxyContexts.put(failedIdentifier, failedProxy);

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 2, sinkWritersContext);
        FutureTask<Void> flush =
                new FutureTask<>(
                        () -> {
                            multiTableSinkWriter.aggregatedFlush(proxyContexts);
                            return null;
                        });
        Thread dispatcher = new Thread(flush);
        dispatcher.setDaemon(true);
        FutureTask<Void> close =
                new FutureTask<>(
                        () -> {
                            multiTableSinkWriter.close();
                            return null;
                        });
        Thread closer = new Thread(close);
        closer.setDaemon(true);
        try {
            multiTableSinkWriter.write(buildRow("test.failed", 0));
            dispatcher.start();
            ExecutionException error =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> flush.get(5, TimeUnit.SECONDS));
            Assertions.assertSame(expectedFailure, error.getCause());

            closer.start();
            Assertions.assertTrue(failedWriterClosed.await(5, TimeUnit.SECONDS));
            Assertions.assertFalse(slowWriterClosed.get());
        } finally {
            releaseSlowFlush.complete(null);
            dispatcher.join(5000);
            if (closer.getState() == Thread.State.NEW) {
                closer.start();
            }
            close.get(5, TimeUnit.SECONDS);
        }
        Assertions.assertTrue(slowWriterClosed.get());
        Assertions.assertEquals(1, slowQueueFlushCount.get());
    }

    @Test
    public void testInterruptedAggregatedFlushDoesNotStartAnotherChildFlush() throws Exception {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        AtomicInteger startedFlushes = new AtomicInteger();
        CountDownLatch firstFlushStarted = new CountDownLatch(1);
        CompletableFuture<Void> releaseFirstFlush = new CompletableFuture<>();
        for (int index : new int[] {0, 2}) {
            SinkIdentifier identifier = SinkIdentifier.of("test.table" + index, index);
            SinkWriter.Context context = new TestSinkWriterContext();
            sinkWriters.put(identifier, new TestSinkWriter());
            sinkWritersContext.put(identifier, context);
            SinkContextProxy proxy = new SinkContextProxy(index, 2, context);
            proxy.registerFlushAction(
                    () -> {
                        if (startedFlushes.incrementAndGet() == 1) {
                            firstFlushStarted.countDown();
                            releaseFirstFlush.join();
                            // Simulate a connector that consumes the interrupt during I/O.
                            Thread.interrupted();
                        }
                    });
            proxyContexts.put(identifier, proxy);
        }

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 2, sinkWritersContext);
        AtomicBoolean dispatcherInterrupted = new AtomicBoolean();
        FutureTask<Void> flush =
                new FutureTask<>(
                        () -> {
                            try {
                                multiTableSinkWriter.aggregatedFlush(proxyContexts);
                                return null;
                            } finally {
                                dispatcherInterrupted.set(Thread.currentThread().isInterrupted());
                            }
                        });
        Thread dispatcher = new Thread(flush);
        dispatcher.setDaemon(true);
        dispatcher.start();
        try {
            Assertions.assertTrue(firstFlushStarted.await(5, TimeUnit.SECONDS));
            dispatcher.interrupt();
            ExecutionException error =
                    Assertions.assertThrows(
                            ExecutionException.class, () -> flush.get(5, TimeUnit.SECONDS));
            Assertions.assertTrue(error.getCause() instanceof InterruptedException);
            Assertions.assertTrue(dispatcherInterrupted.get());
        } finally {
            releaseFirstFlush.complete(null);
            dispatcher.join(5000);
            multiTableSinkWriter.close();
        }
        Assertions.assertEquals(1, startedFlushes.get());
    }

    @Test
    public void testAggregatedFlushFatalRowErrorHandlingFailureFailsFast() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        SinkIdentifier sinkIdentifier = SinkIdentifier.of("test.fatal", 0);
        sinkWriters.put(sinkIdentifier, new TestSinkWriter());
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        AtomicInteger flushCount = new AtomicInteger();
        SinkContextProxy fatalProxy = new SinkContextProxy(0, 1, new TestSinkWriterContext());
        fatalProxy.registerFlushAction(
                () -> {
                    flushCount.incrementAndGet();
                    throw new RowErrorHandlingFatalException(
                            "fatal collector failure",
                            new RuntimeException("fatal collector failure"));
                });
        proxyContexts.put(sinkIdentifier, fatalProxy);

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        1,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.STREAMING,
                        2,
                        0);

        RuntimeException exception =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> multiTableSinkWriter.aggregatedFlush(proxyContexts));

        Assertions.assertFalse(MultiTableFailureHelper.isIsolatedFailure(exception.getMessage()));
        Assertions.assertEquals(1, flushCount.get());
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
        BlockingQueue<MultiTableWriterRunnable.QueueElement> queue = new LinkedBlockingQueue<>(1);
        tableIdWriterMap.put("http", onlyWriter);
        queue.add(MultiTableWriterRunnable.rowRequest(buildRow("Optional[http]", 1)));

        MultiTableWriterRunnable runnable = new MultiTableWriterRunnable(tableIdWriterMap, queue);
        runnable.run();

        Assertions.assertTrue(runnable.getThrowable() instanceof InterruptedException);
        Assertions.assertEquals(1, onlyWriter.getWriteCount());
        Assertions.assertEquals("http", runnable.getCurrentTableId());
    }

    @Test
    public void testRunnableSelectsWriterUnderLock() {
        GuardedWriterMap tableIdWriterMap = new GuardedWriterMap();
        RecordingSinkWriter writer = new RecordingSinkWriter(false, true);
        BlockingQueue<MultiTableWriterRunnable.QueueElement> queue = new LinkedBlockingQueue<>(1);
        tableIdWriterMap.put("test.table", writer);
        queue.add(MultiTableWriterRunnable.rowRequest(buildRow("test.table", 1)));

        MultiTableWriterRunnable runnable = new MultiTableWriterRunnable(tableIdWriterMap, queue);
        tableIdWriterMap.setRequiredLock(runnable);
        runnable.run();

        Assertions.assertTrue(runnable.getThrowable() instanceof InterruptedException);
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
            await().atMost(1, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(lockProbe.isDone()));
            Assertions.assertTrue(lockProbe.get());
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
    public void testCloseWaitsForExecutorTermination() throws Exception {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter onlyWriter = new RecordingSinkWriter(false);
        SinkIdentifier sinkIdentifier = SinkIdentifier.of(TablePath.DEFAULT.toString(), 0);
        sinkWriters.put(sinkIdentifier, onlyWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 1, sinkWritersContext);
        ExecutorService executorService = getExecutorService(multiTableSinkWriter);
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskInterrupted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        AtomicReference<Throwable> releaserFailure = new AtomicReference<>();
        executorService.submit(
                () -> {
                    taskStarted.countDown();
                    try {
                        releaseTask.await();
                    } catch (InterruptedException e) {
                        taskInterrupted.countDown();
                        try {
                            releaseTask.await();
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
        Assertions.assertTrue(taskStarted.await(1, TimeUnit.SECONDS));

        Thread delayedReleaser =
                new Thread(
                        () -> {
                            try {
                                if (!taskInterrupted.await(1, TimeUnit.SECONDS)) {
                                    releaserFailure.set(
                                            new AssertionError(
                                                    "executor task was not interrupted by close"));
                                    return;
                                }
                                Thread.sleep(500L);
                                releaseTask.countDown();
                            } catch (Throwable e) {
                                if (e instanceof InterruptedException) {
                                    Thread.currentThread().interrupt();
                                }
                                releaserFailure.set(e);
                            }
                        });
        delayedReleaser.start();

        try {
            long startedAt = System.nanoTime();
            multiTableSinkWriter.close();
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            Assertions.assertTrue(
                    elapsedMillis >= 400L, "close should wait for executor tasks to stop");
            Assertions.assertTrue(executorService.isTerminated());
        } finally {
            releaseTask.countDown();
            delayedReleaser.join(1000L);
        }
        Assertions.assertNull(releaserFailure.get());
    }

    @Test
    public void testWriteSuccessHandlerReportsAfterAsyncWriteCompletes() throws Exception {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        BlockingSuccessSinkWriter onlyWriter = new BlockingSuccessSinkWriter();
        SinkIdentifier sinkIdentifier = SinkIdentifier.of(TablePath.DEFAULT.toString(), 0);
        sinkWriters.put(sinkIdentifier, onlyWriter);
        sinkWritersContext.put(sinkIdentifier, new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 1, sinkWritersContext);
        CountDownLatch successReported = new CountDownLatch(1);
        AtomicReference<SeaTunnelRow> reportedRow = new AtomicReference<>();
        multiTableSinkWriter.setWriteSuccessHandler(
                row -> {
                    reportedRow.set(row);
                    successReported.countDown();
                });
        SeaTunnelRow row = buildRow(TablePath.DEFAULT.toString(), 1);

        multiTableSinkWriter.write(row);
        Assertions.assertTrue(onlyWriter.awaitWriteStarted());
        Assertions.assertEquals(1L, successReported.getCount());

        onlyWriter.releaseWrite();
        Assertions.assertTrue(successReported.await(1, TimeUnit.SECONDS));
        Assertions.assertSame(row, reportedRow.get());
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

    @Test
    public void testMultiTableStateDeserializesPreFailedTablesCheckpoint() throws IOException {
        // Captured from the pre-PR MultiTableState shape that only had the states field.
        // The class UID for that shape is 5992121739651030596L by serialver.
        String preFailedTablesState =
                "rO0ABXNyADxvcmcuYXBhY2hlLnNlYXR1bm5lbC5hcGkuc2luay5tdWx0aXRhYmxlc2luay5NdWx0aVRhYmxlU3RhdGVTKEr5fsueRAIAAUwABnN0YXRlc3QAD0xqYXZhL3V0aWwvTWFwO3hwc3IAEWphdmEudXRpbC5IYXNoTWFwBQfawcMWYNEDAAJGAApsb2FkRmFjdG9ySQAJdGhyZXNob2xkeHA/QAAAAAAAAHcIAAAAEAAAAAB4";
        DefaultSerializer<MultiTableState> serializer = new DefaultSerializer<>();

        MultiTableState restored =
                serializer.deserialize(Base64.getDecoder().decode(preFailedTablesState));

        Assertions.assertNotNull(restored.getStates());
        Assertions.assertTrue(restored.getStates().isEmpty());
        Assertions.assertTrue(restored.getFailedTables().isEmpty());
    }

    /**
     * A primary key whose {@code hashCode()} is {@link Integer#MIN_VALUE} must still route to a
     * valid queue. {@code Math.abs(Integer.MIN_VALUE)} is itself negative, so the old routing
     * produced a negative index and the write failed with {@link IndexOutOfBoundsException}
     * whenever the replica count was not a power of two.
     */
    @Test
    public void testMinValuePrimaryKeyHashRoutesToValidQueue() throws IOException {
        // 3 is not a power of two, so Integer.MIN_VALUE % 3 == -2 under the old routing
        int replicaNum = 3;
        // Under the fix both rows route to (Integer.MIN_VALUE & Integer.MAX_VALUE) % 3 == 0.
        int expectedIndex = (Integer.MIN_VALUE & Integer.MAX_VALUE) % replicaNum;
        Assertions.assertEquals(0, expectedIndex);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter[] writersByIndex = new RecordingSinkWriter[replicaNum];
        for (int i = 0; i < replicaNum; i++) {
            SinkIdentifier identifier = SinkIdentifier.of("test.table", i);
            RecordingSinkWriter writer = new RecordingSinkWriter(false);
            writersByIndex[i] = writer;
            sinkWriters.put(identifier, writer);
            sinkWritersContext.put(identifier, new TestSinkWriterContext());
        }

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, replicaNum, sinkWritersContext);

        // Integer.valueOf(Integer.MIN_VALUE).hashCode() and Long.valueOf(Long.MIN_VALUE)
        // .hashCode() are both Integer.MIN_VALUE
        SeaTunnelRow longKeyRow = new SeaTunnelRow(new Object[] {Long.MIN_VALUE});
        longKeyRow.setTableId("test.table");

        Assertions.assertDoesNotThrow(
                () -> multiTableSinkWriter.write(buildRow("test.table", Integer.MIN_VALUE)));
        Assertions.assertDoesNotThrow(() -> multiTableSinkWriter.write(longKeyRow));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(commitInfo.isPresent());

        int totalWrites =
                sinkWriters.values().stream()
                        .mapToInt(writer -> ((RecordingSinkWriter) writer).getWriteCount())
                        .sum();
        Assertions.assertEquals(2, totalWrites);

        // Pin the target queue exactly rather than only asserting no-throw: both rows must land
        // in queue 0 and nowhere else, so this fails if the routing regresses, not merely if it
        // throws.
        Assertions.assertEquals(2, writersByIndex[expectedIndex].getWriteCount());
        for (int i = 0; i < replicaNum; i++) {
            if (i != expectedIndex) {
                Assertions.assertEquals(0, writersByIndex[i].getWriteCount());
            }
        }
    }

    @Test
    public void testOrdinaryNegativeHashRoutesToMaskedIndex() throws IOException {
        // The fix is a general bitmask, not a special case for Integer.MIN_VALUE: it changes the
        // index for every negative hash, since Math.abs(h) is -h while h & Integer.MAX_VALUE is
        // h + 2^31. An INT primary key hashes to its own value, so this key's hash is -5, and:
        //   old routing: Math.abs(-5) % 3               == 2
        //   new routing: (-5 & Integer.MAX_VALUE) % 3   == 0
        // The resulting index is asserted exactly, not merely checked for being in range.
        int replicaNum = 3;
        int primaryKey = -5;
        int expectedIndex = (primaryKey & Integer.MAX_VALUE) % replicaNum;
        Assertions.assertEquals(0, expectedIndex);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter[] writersByIndex = new RecordingSinkWriter[replicaNum];
        for (int i = 0; i < replicaNum; i++) {
            SinkIdentifier identifier = SinkIdentifier.of("test.table", i);
            RecordingSinkWriter writer = new RecordingSinkWriter(false);
            writersByIndex[i] = writer;
            sinkWriters.put(identifier, writer);
            sinkWritersContext.put(identifier, new TestSinkWriterContext());
        }

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, replicaNum, sinkWritersContext);

        multiTableSinkWriter.write(buildRow("test.table", primaryKey));

        Optional<MultiTableCommitInfo> commitInfo = multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(commitInfo.isPresent());

        Assertions.assertEquals(1, writersByIndex[expectedIndex].getWriteCount());
        // Index 2 is where the old Math.abs routing would have sent this row.
        Assertions.assertEquals(0, writersByIndex[2].getWriteCount());
    }

    @Test
    public void testReplicaNumBelowOneIsRejectedWithTheOptionNamed() {
        // multi_table_sink_replica has no lower bound at the option layer, so 0 and negative
        // values reach MultiTableSink. replicaNum becomes the queue count and the thread-pool
        // size in MultiTableSinkWriter, where Executors.newFixedThreadPool(queueSize * 2)
        // rejects them with an IllegalArgumentException carrying no message at all. The
        // exception type is unchanged; what is asserted here is that it now names the option
        // and the offending value, and that it is raised before any writer is created.
        for (int replicaNum : new int[] {0, -1}) {
            IllegalArgumentException exception =
                    Assertions.assertThrows(
                            IllegalArgumentException.class, () -> buildMultiTableSink(replicaNum));
            Assertions.assertNotNull(exception.getMessage());
            Assertions.assertTrue(
                    exception
                            .getMessage()
                            .contains(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key()),
                    "message must name the option, but was: " + exception.getMessage());
            Assertions.assertTrue(
                    exception.getMessage().contains(String.valueOf(replicaNum)),
                    "message must name the rejected value, but was: " + exception.getMessage());
        }
    }

    @Test
    public void testReplicaNumOfOneAndAboveStillBuilds() {
        // Guards the lower bound against being set too high: the smallest legal value and a
        // larger one must both still construct, so the check above cannot regress into
        // rejecting valid configuration.
        Assertions.assertDoesNotThrow(() -> buildMultiTableSink(1));
        Assertions.assertDoesNotThrow(() -> buildMultiTableSink(4));
    }

    @Test
    public void testReplicaNumDefaultsToOneWhenUnset() {
        // The default must remain a legal value, otherwise the new check would reject every
        // job that does not set the option explicitly.
        Map<String, Object> options = new HashMap<>();
        options.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        options.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(
                TablePath.of("test.table"), new TestSeaTunnelSink(new RecordingSinkWriter(false)));
        Assertions.assertDoesNotThrow(
                () ->
                        new MultiTableSink(
                                new MultiTableFactoryContext(
                                        ReadonlyConfig.fromMap(options),
                                        Thread.currentThread().getContextClassLoader(),
                                        sinks)));
    }

    private MultiTableSink buildMultiTableSink(int replicaNum) {
        Map<String, Object> options = new HashMap<>();
        options.put(SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(), replicaNum);
        options.put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 0);
        options.put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), 0);
        Map<TablePath, SeaTunnelSink> sinks = new HashMap<>();
        sinks.put(
                TablePath.of("test.table"), new TestSeaTunnelSink(new RecordingSinkWriter(false)));
        return new MultiTableSink(
                new MultiTableFactoryContext(
                        ReadonlyConfig.fromMap(options),
                        Thread.currentThread().getContextClassLoader(),
                        sinks));
    }

    @Test
    public void testByteArrayPrimaryKeyRoutesEqualKeysToOneWriter() throws IOException {
        // BINARY/VARBINARY primary keys reach the sink as byte[], whose hashCode() is identity
        // based. Every row below carries a fresh array with the same content, as a CDC stream
        // does for one BINARY(16) key, and all of them must land on the same queue so that
        // UPDATE_BEFORE/UPDATE_AFTER/DELETE for that key stay ordered.
        int replicaNum = 4;
        int rows = 32;
        byte[] key = new byte[] {1, 2, 3};
        int expectedIndex = HashUtils.bucketIndex(Arrays.hashCode(key), replicaNum);

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        RecordingSinkWriter[] writersByIndex = new RecordingSinkWriter[replicaNum];
        for (int i = 0; i < replicaNum; i++) {
            SinkIdentifier identifier = SinkIdentifier.of("test.table", i);
            RecordingSinkWriter writer = new RecordingSinkWriter(false);
            writersByIndex[i] = writer;
            sinkWriters.put(identifier, writer);
            sinkWritersContext.put(identifier, new TestSinkWriterContext());
        }
        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, replicaNum, sinkWritersContext);

        for (int i = 0; i < rows; i++) {
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {key.clone()});
            row.setTableId("test.table");
            multiTableSinkWriter.write(row);
        }
        Assertions.assertTrue(multiTableSinkWriter.prepareCommit(1L).isPresent());

        for (int i = 0; i < replicaNum; i++) {
            Assertions.assertEquals(
                    i == expectedIndex ? rows : 0,
                    writersByIndex[i].getWriteCount(),
                    "rows with the same binary key must all reach writer " + expectedIndex);
        }
        multiTableSinkWriter.close();
    }

    @Test
    public void testPrimaryKeyIndexIsRefreshedAfterSchemaChange() throws IOException {
        // A sub-writer that keys on a column position (the JDBC sink does) moves that position
        // when a column is added in front of the key. The coordinator must re-read primaryKey()
        // after the schema-change barrier, otherwise rows of one key are hashed on the new
        // leading column and spread across workers.
        int replicaNum = 3;
        int key = 7;
        int keyIndex = HashUtils.bucketIndex(key, replicaNum);
        int fillerValue = 0;
        Assertions.assertNotEquals(keyIndex, HashUtils.bucketIndex(fillerValue, replicaNum));

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        KeyIndexShiftingSinkWriter[] writersByIndex = new KeyIndexShiftingSinkWriter[replicaNum];
        for (int i = 0; i < replicaNum; i++) {
            SinkIdentifier identifier = SinkIdentifier.of("test.table", i);
            KeyIndexShiftingSinkWriter writer = new KeyIndexShiftingSinkWriter();
            writersByIndex[i] = writer;
            sinkWriters.put(identifier, writer);
            sinkWritersContext.put(identifier, new TestSinkWriterContext());
        }
        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, replicaNum, sinkWritersContext);

        multiTableSinkWriter.write(buildRow("test.table", key));
        Assertions.assertTrue(multiTableSinkWriter.prepareCommit(1L).isPresent());
        Assertions.assertEquals(1, writersByIndex[keyIndex].getWriteCount());

        multiTableSinkWriter.applySchemaChange(
                AlterTableAddColumnEvent.addFirst(
                        TableIdentifier.of("test", TablePath.of("test.table")),
                        PhysicalColumn.builder()
                                .name("filler")
                                .dataType(BasicType.INT_TYPE)
                                .build()));
        for (KeyIndexShiftingSinkWriter writer : writersByIndex) {
            Assertions.assertEquals(Optional.of(1), writer.primaryKey());
        }

        SeaTunnelRow shiftedRow = new SeaTunnelRow(new Object[] {fillerValue, key});
        shiftedRow.setTableId("test.table");
        multiTableSinkWriter.write(shiftedRow);
        Assertions.assertTrue(multiTableSinkWriter.prepareCommit(2L).isPresent());

        for (int i = 0; i < replicaNum; i++) {
            Assertions.assertEquals(
                    i == keyIndex ? 2 : 0,
                    writersByIndex[i].getWriteCount(),
                    "both rows of key " + key + " must reach writer " + keyIndex);
        }
        multiTableSinkWriter.close();
    }

    private SeaTunnelRow buildRow(String tableId, int value) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {value});
        row.setTableId(tableId);
        return row;
    }

    private ExecutorService getExecutorService(MultiTableSinkWriter multiTableSinkWriter)
            throws Exception {
        Field executorServiceField = MultiTableSinkWriter.class.getDeclaredField("executorService");
        executorServiceField.setAccessible(true);
        return (ExecutorService) executorServiceField.get(multiTableSinkWriter);
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

    /** Keys on column 0 until a schema change puts a new column in front of the key. */
    static class KeyIndexShiftingSinkWriter extends RecordingSinkWriter
            implements SupportSchemaEvolutionSinkWriter {
        private volatile int keyIndex = 0;

        KeyIndexShiftingSinkWriter() {
            super(false);
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent event) {
            keyIndex = 1;
        }

        @Override
        public Optional<Integer> primaryKey() {
            return Optional.of(keyIndex);
        }
    }

    static class CloseFailingSinkWriter extends RecordingSinkWriter {
        CloseFailingSinkWriter(boolean failOnWrite) {
            super(failOnWrite);
        }

        @Override
        public void close() throws IOException {
            throw new IOException("intentional close failure");
        }
    }

    static class BlockingSuccessSinkWriter extends RecordingSinkWriter {
        private final CountDownLatch writeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseWrite = new CountDownLatch(1);

        BlockingSuccessSinkWriter() {
            super(false);
        }

        @Override
        public void write(SeaTunnelRow seaTunnelRow) {
            writeStarted.countDown();
            try {
                Assertions.assertTrue(releaseWrite.await(1, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            super.write(seaTunnelRow);
        }

        boolean awaitWriteStarted() throws InterruptedException {
            return writeStarted.await(1, TimeUnit.SECONDS);
        }

        void releaseWrite() {
            releaseWrite.countDown();
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

    static class FatalPrepareCommitSinkWriter extends RecordingSinkWriter {
        private final AtomicInteger prepareCommitCount = new AtomicInteger();

        FatalPrepareCommitSinkWriter() {
            super(false);
        }

        @Override
        public Optional<TestSinkState> prepareCommit(long checkpointId) {
            prepareCommitCount.incrementAndGet();
            throw new RowErrorHandlingFatalException(
                    "fatal collector failure", new RuntimeException("fatal collector failure"));
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

    static class FatalSnapshotSinkWriter extends RecordingSinkWriter {
        private final AtomicInteger snapshotAttemptCount = new AtomicInteger();

        FatalSnapshotSinkWriter() {
            super(false);
        }

        @Override
        public List<Object> snapshotState(long checkpointId) {
            snapshotAttemptCount.incrementAndGet();
            throw new RowErrorHandlingFatalException(
                    "fatal collector failure", new RuntimeException("fatal collector failure"));
        }

        int getSnapshotAttemptCount() {
            return snapshotAttemptCount.get();
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

    static class BlockingPollQueue
            extends LinkedBlockingQueue<MultiTableWriterRunnable.QueueElement> {
        private static final long serialVersionUID = 1L;

        private final CountDownLatch pollStarted = new CountDownLatch(1);
        private final CountDownLatch releasePoll = new CountDownLatch(1);

        @Override
        public MultiTableWriterRunnable.QueueElement poll(long timeout, TimeUnit unit)
                throws InterruptedException {
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

    static class TestSeaTunnelSink
            implements SeaTunnelSink<SeaTunnelRow, Object, TestSinkState, Object> {
        private final SinkWriter<SeaTunnelRow, TestSinkState, Object> writer;

        TestSeaTunnelSink(SinkWriter<SeaTunnelRow, TestSinkState, Object> writer) {
            this.writer = writer;
        }

        @Override
        public String getPluginName() {
            return "test";
        }

        @Override
        public SinkWriter<SeaTunnelRow, TestSinkState, Object> createWriter(
                SinkWriter.Context context) {
            return writer;
        }

        @Override
        public SinkWriter<SeaTunnelRow, TestSinkState, Object> restoreWriter(
                SinkWriter.Context context, List<Object> states) {
            return writer;
        }
    }

    static class ThrowingRestoreSink extends TestSeaTunnelSink {
        ThrowingRestoreSink() {
            super(new RecordingSinkWriter(false));
        }

        @Override
        public SinkWriter<SeaTunnelRow, TestSinkState, Object> createWriter(
                SinkWriter.Context context) {
            throw new AssertionError("failed table writer should not be created");
        }

        @Override
        public SinkWriter<SeaTunnelRow, TestSinkState, Object> restoreWriter(
                SinkWriter.Context context, List<Object> states) {
            throw new AssertionError("failed table writer should not be restored");
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
