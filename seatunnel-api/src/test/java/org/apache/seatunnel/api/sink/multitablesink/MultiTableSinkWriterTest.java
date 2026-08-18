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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.CloseTableEvent;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
    public void testCloseTableEventAllowsInFlightRowsUntilFinalSnapshot() throws IOException {
        String table1 = TablePath.of("db", "schema", "table1").getFullName();
        TrackingSinkWriter table1Writer0 = new TrackingSinkWriter("table1-0");

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        sinkWriters.put(SinkIdentifier.of(table1, 0), table1Writer0);
        sinkWritersContext.put(SinkIdentifier.of(table1, 0), new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 1, sinkWritersContext);

        multiTableSinkWriter.handleCloseTableEvent(
                new CloseTableEvent(TablePath.of("db", "schema", "table1"), 0, 2));
        multiTableSinkWriter.handleCloseTableEvent(
                new CloseTableEvent(TablePath.of("db", "schema", "table1"), 1, 2));

        Assertions.assertEquals(0, table1Writer0.closeCount.get());
        SeaTunnelRow lateRow = new SeaTunnelRow(new Object[] {1});
        lateRow.setTableId(table1);
        multiTableSinkWriter.write(lateRow);

        multiTableSinkWriter.snapshotState(1L);

        Assertions.assertEquals(1, table1Writer0.writeCount.get());
        Assertions.assertEquals(1, table1Writer0.closeCount.get());
        IOException exception =
                Assertions.assertThrows(
                        IOException.class, () -> multiTableSinkWriter.write(lateRow));
        Assertions.assertTrue(exception.getMessage().contains(table1));
    }

    @Test
    public void testCloseTableEventDefersCloseUntilFinalSnapshot() throws IOException {
        String table1 = TablePath.of("db", "schema", "table1").getFullName();
        String table2 = TablePath.of("db", "schema", "table2").getFullName();
        TrackingSinkWriter table1Writer0 = new TrackingSinkWriter("table1-0");
        TrackingSinkWriter table1Writer1 = new TrackingSinkWriter("table1-1");
        TrackingSinkWriter table2Writer0 = new TrackingSinkWriter("table2-0");

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        sinkWriters.put(SinkIdentifier.of(table1, 0), table1Writer0);
        sinkWriters.put(SinkIdentifier.of(table1, 1), table1Writer1);
        sinkWriters.put(SinkIdentifier.of(table2, 0), table2Writer0);
        sinkWritersContext.put(SinkIdentifier.of(table1, 0), new TestSinkWriterContext());
        sinkWritersContext.put(SinkIdentifier.of(table1, 1), new TestSinkWriterContext());
        sinkWritersContext.put(SinkIdentifier.of(table2, 0), new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 2, sinkWritersContext);

        multiTableSinkWriter.handleCloseTableEvent(
                new CloseTableEvent(TablePath.of("db", "schema", "table1"), 0, 2));
        multiTableSinkWriter.handleCloseTableEvent(
                new CloseTableEvent(TablePath.of("db", "schema", "table1"), 1, 2));

        Assertions.assertEquals(0, table1Writer0.closeCount.get());
        Assertions.assertEquals(0, table1Writer1.closeCount.get());
        Assertions.assertEquals(0, table2Writer0.closeCount.get());

        Optional<MultiTableCommitInfo> beforeSnapshotCommitInfo =
                multiTableSinkWriter.prepareCommit(1L);
        Assertions.assertTrue(beforeSnapshotCommitInfo.isPresent());
        Assertions.assertEquals(3, beforeSnapshotCommitInfo.get().getCommitInfo().size());

        List<MultiTableState> checkpointStates = multiTableSinkWriter.snapshotState(1L);
        Assertions.assertEquals(1, checkpointStates.size());
        Assertions.assertTrue(
                checkpointStates.get(0).getStates().containsKey(SinkIdentifier.of(table1, 0)));
        Assertions.assertTrue(
                checkpointStates.get(0).getStates().containsKey(SinkIdentifier.of(table1, 1)));

        Assertions.assertEquals(1, table1Writer0.closeCount.get());
        Assertions.assertEquals(1, table1Writer1.closeCount.get());
        Assertions.assertEquals(0, table2Writer0.closeCount.get());

        Optional<MultiTableCommitInfo> afterSnapshotCommitInfo =
                multiTableSinkWriter.prepareCommit(2L);
        Assertions.assertTrue(afterSnapshotCommitInfo.isPresent());
        Assertions.assertEquals(1, afterSnapshotCommitInfo.get().getCommitInfo().size());
        Assertions.assertTrue(
                afterSnapshotCommitInfo
                        .get()
                        .getCommitInfo()
                        .containsKey(SinkIdentifier.of(table2, 0)));
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
    public void testAggregatedFlushIsolatesFailedTableAndFlushesHealthyTable() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 1);
        sinkWriters.put(failedIdentifier, new TestSinkWriter());
        sinkWriters.put(healthyIdentifier, new TestSinkWriter());
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        AtomicInteger failedFlushCount = new AtomicInteger();
        SinkContextProxy failedProxy = new SinkContextProxy(0, 2, new TestSinkWriterContext());
        failedProxy.registerFlushAction(
                () -> {
                    failedFlushCount.incrementAndGet();
                    throw new IOException("intentional flush failure");
                });
        AtomicInteger healthyFlushCount = new AtomicInteger();
        SinkContextProxy healthyProxy = new SinkContextProxy(1, 2, new TestSinkWriterContext());
        healthyProxy.registerFlushAction(healthyFlushCount::incrementAndGet);
        proxyContexts.put(failedIdentifier, failedProxy);
        proxyContexts.put(healthyIdentifier, healthyProxy);

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.CONTINUE_OTHER_TABLES,
                        JobMode.BATCH);

        Assertions.assertDoesNotThrow(() -> multiTableSinkWriter.aggregatedFlush(proxyContexts));
        Assertions.assertEquals(1, failedFlushCount.get());
        Assertions.assertEquals(1, healthyFlushCount.get());

        Assertions.assertDoesNotThrow(() -> multiTableSinkWriter.aggregatedFlush(proxyContexts));
        Assertions.assertEquals(1, failedFlushCount.get());
        Assertions.assertEquals(2, healthyFlushCount.get());

        IOException closeException =
                Assertions.assertThrows(IOException.class, multiTableSinkWriter::close);
        Assertions.assertTrue(closeException.getMessage().contains("test.failed"));
        Assertions.assertTrue(closeException.getMessage().contains("phase=timer_flush"));
    }

    @Test
    public void testAggregatedFlushFailFastStopsAtFirstFailure() {
        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        Map<SinkIdentifier, SinkContextProxy> proxyContexts = new HashMap<>();
        SinkIdentifier failedIdentifier = SinkIdentifier.of("test.failed", 0);
        SinkIdentifier healthyIdentifier = SinkIdentifier.of("test.healthy", 1);
        sinkWriters.put(failedIdentifier, new TestSinkWriter());
        sinkWriters.put(healthyIdentifier, new TestSinkWriter());
        sinkWritersContext.put(failedIdentifier, new TestSinkWriterContext());
        sinkWritersContext.put(healthyIdentifier, new TestSinkWriterContext());

        AtomicInteger failedFlushCount = new AtomicInteger();
        SinkContextProxy failedProxy = new SinkContextProxy(0, 2, new TestSinkWriterContext());
        failedProxy.registerFlushAction(
                () -> {
                    failedFlushCount.incrementAndGet();
                    throw new IOException("intentional flush failure");
                });
        AtomicInteger healthyFlushCount = new AtomicInteger();
        SinkContextProxy healthyProxy = new SinkContextProxy(1, 2, new TestSinkWriterContext());
        healthyProxy.registerFlushAction(healthyFlushCount::incrementAndGet);
        proxyContexts.put(failedIdentifier, failedProxy);
        proxyContexts.put(healthyIdentifier, healthyProxy);

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(
                        sinkWriters,
                        2,
                        sinkWritersContext,
                        MultiTableFailurePolicy.FAIL_FAST,
                        JobMode.BATCH);

        IOException flushException =
                Assertions.assertThrows(
                        IOException.class,
                        () -> multiTableSinkWriter.aggregatedFlush(proxyContexts));
        Assertions.assertEquals("intentional flush failure", flushException.getMessage());
        Assertions.assertEquals(1, failedFlushCount.get());
        Assertions.assertEquals(0, healthyFlushCount.get());
        Assertions.assertDoesNotThrow(multiTableSinkWriter::close);
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

    static class TrackingSinkWriter extends TestSinkWriter {
        private final AtomicInteger writeCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();
        private final String stateValue;

        TrackingSinkWriter(String stateValue) {
            this.stateValue = stateValue;
        }

        @Override
        public void write(SeaTunnelRow element) {
            writeCount.incrementAndGet();
        }

        @Override
        public Optional<TestSinkState> prepareCommit() {
            return Optional.of(new TestSinkState(stateValue));
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
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
