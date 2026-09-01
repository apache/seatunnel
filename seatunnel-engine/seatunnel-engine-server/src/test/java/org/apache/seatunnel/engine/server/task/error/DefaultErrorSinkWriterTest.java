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

package org.apache.seatunnel.engine.server.task.error;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultErrorSinkWriterTest {

    @Test
    public void testValidateSupportedErrorSinkLifecycleAllowsSimpleSink() {
        assertDoesNotThrow(
                () ->
                        DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                "simple", new TestSink(false, false, false)));
    }

    @Test
    public void testValidateSupportedErrorSinkLifecycleRejectsStatefulSink() {
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                        "stateful", new TestSink(true, false, false)));

        assertTrue(ex.getMessage().contains("writer state serializer"));
    }

    @Test
    public void testValidateSupportedErrorSinkLifecycleAllowsOptionalWriterState() {
        SeaTunnelSink<SeaTunnelRow, String, String, String> sink =
                new TestSink(true, false, false) {
                    @Override
                    public boolean requiresWriterState() {
                        return false;
                    }
                };

        assertDoesNotThrow(
                () ->
                        DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                "optional-state", sink));
    }

    @Test
    public void testValidateSupportedErrorSinkLifecycleRejectsCommittingSink() {
        RuntimeException ex =
                assertThrows(
                        RuntimeException.class,
                        () ->
                                DefaultErrorSinkWriter.validateSupportedErrorSinkLifecycle(
                                        "committing", new TestSink(false, true, true)));

        assertTrue(ex.getMessage().contains("commit info serializer"));
        assertTrue(ex.getMessage().contains("committer"));
        assertTrue(ex.getMessage().contains("aggregated commit info serializer"));
        assertTrue(ex.getMessage().contains("aggregated committer"));
    }

    @Test
    public void testErrorSinkWriterContextRetainsRegisteredFlushAction() throws Exception {
        Class<?> contextClass =
                Class.forName(DefaultErrorSinkWriter.class.getName() + "$SimpleWriterContext");
        Constructor<?> constructor =
                Arrays.stream(contextClass.getDeclaredConstructors())
                        .filter(candidate -> candidate.getParameterCount() == 3)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("SimpleWriterContext constructor"));
        constructor.setAccessible(true);
        SinkWriter.Context context = (SinkWriter.Context) constructor.newInstance(null, null, 0);
        AtomicInteger flushes = new AtomicInteger();
        RunnableWithException flushAction = flushes::incrementAndGet;

        context.registerFlushAction(flushAction);

        assertTrue(context.getFlushAction() == flushAction);
        context.getFlushAction().run();
        assertTrue(flushes.get() == 1);
    }

    @Test
    public void testFlushWithCheckpointUsesCheckpointAwarePrepareCommit() throws Exception {
        Class<?> contextClass =
                Class.forName(DefaultErrorSinkWriter.class.getName() + "$SimpleWriterContext");
        Constructor<?> constructor =
                Arrays.stream(contextClass.getDeclaredConstructors())
                        .filter(candidate -> candidate.getParameterCount() == 3)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("SimpleWriterContext constructor"));
        constructor.setAccessible(true);
        SinkWriter.Context context = (SinkWriter.Context) constructor.newInstance(null, null, 0);
        AtomicInteger flushes = new AtomicInteger();
        context.registerFlushAction(flushes::incrementAndGet);

        CountingWriter sinkWriter = new CountingWriter();
        DefaultErrorSinkWriter<SeaTunnelRow> writer =
                new DefaultErrorSinkWriter<>(
                        StageErrorConfig.builder().mode(ErrorHandlerMode.ROUTE).build(),
                        ErrorSinkConfig.empty(),
                        1L,
                        0,
                        null);
        setField(writer, "initialized", true);
        setField(writer, "writer", sinkWriter);
        setField(writer, "writerLock", new Object());
        setField(writer, "writerContext", context);
        setField(writer, "pendingRows", new AtomicInteger(0));

        writer.flush(37L);

        assertEquals(1, flushes.get());
        assertEquals(0, sinkWriter.noArgPrepareCommits.get());
        assertEquals(1, sinkWriter.checkpointPrepareCommits.get());
        assertEquals(37L, sinkWriter.lastCheckpointId);
    }

    @Test
    @Timeout(3)
    public void testBlockPolicyFailsWhenWorkerFailedWhileQueueIsFull() throws Exception {
        StageErrorConfig config =
                StageErrorConfig.builder()
                        .mode(ErrorHandlerMode.ROUTE)
                        .queueOverflowPolicy(QueueOverflowPolicy.BLOCK)
                        .queueCapacity(1)
                        .build();
        DefaultErrorSinkWriter<SeaTunnelRow> writer =
                new DefaultErrorSinkWriter<>(config, ErrorSinkConfig.empty(), 1L, 0, null);
        ArrayBlockingQueue<SeaTunnelRow> queue = new ArrayBlockingQueue<>(1);
        queue.put(new SeaTunnelRow(new Object[] {"queued"}));

        setField(writer, "initialized", true);
        setField(writer, "queue", queue);
        setField(writer, "pendingRows", new AtomicInteger(0));
        setField(writer, "errorRowType", errorRowType());
        Thread workerFailurePublisher =
                new Thread(
                        () -> {
                            try {
                                Thread.sleep(200L);
                                setField(writer, "workerFailure", new IOException("worker failed"));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        workerFailurePublisher.start();

        Exception ex =
                assertThrows(
                        Exception.class,
                        () ->
                                writer.write(
                                        new RowErrorContext("SINK", "SINK", "Jdbc", "table"),
                                        new SeaTunnelRow(new Object[] {1}),
                                        new RuntimeException("row error")));
        workerFailurePublisher.join(1000L);

        assertTrue(ex.getMessage().contains("worker failed"));
    }

    private static SeaTunnelRowType errorRowType() {
        return new SeaTunnelRowType(
                new String[] {
                    "error_stage",
                    "plugin_type",
                    "plugin_name",
                    "source_table_path",
                    "job_id",
                    "error_message",
                    "exception_class",
                    "stacktrace",
                    "original_data",
                    "occur_time"
                },
                new SeaTunnelDataType[] {
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.LONG_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    BasicType.STRING_TYPE,
                    LocalTimeType.LOCAL_DATE_TIME_TYPE
                });
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class TestSink implements SeaTunnelSink<SeaTunnelRow, String, String, String> {

        private final boolean writerStateful;
        private final boolean committing;
        private final boolean aggregatedCommitting;

        private TestSink(boolean writerStateful, boolean committing, boolean aggregatedCommitting) {
            this.writerStateful = writerStateful;
            this.committing = committing;
            this.aggregatedCommitting = aggregatedCommitting;
        }

        @Override
        public String getPluginName() {
            return "Test";
        }

        @Override
        public SinkWriter<SeaTunnelRow, String, String> createWriter(SinkWriter.Context context) {
            return new NoopWriter();
        }

        @Override
        public Optional<Serializer<String>> getWriterStateSerializer() {
            return writerStateful ? Optional.of(new NoopSerializer()) : Optional.empty();
        }

        @Override
        public Optional<SinkCommitter<String>> createCommitter() {
            return committing ? Optional.of(new NoopCommitter()) : Optional.empty();
        }

        @Override
        public Optional<Serializer<String>> getCommitInfoSerializer() {
            return committing ? Optional.of(new NoopSerializer()) : Optional.empty();
        }

        @Override
        public Optional<SinkAggregatedCommitter<String, String>> createAggregatedCommitter() {
            return aggregatedCommitting
                    ? Optional.of(new NoopAggregatedCommitter())
                    : Optional.empty();
        }

        @Override
        public Optional<Serializer<String>> getAggregatedCommitInfoSerializer() {
            return aggregatedCommitting ? Optional.of(new NoopSerializer()) : Optional.empty();
        }
    }

    private static class NoopSerializer implements Serializer<String> {

        @Override
        public byte[] serialize(String obj) {
            return new byte[0];
        }

        @Override
        public String deserialize(byte[] serialized) {
            return "";
        }
    }

    private static class NoopWriter implements SinkWriter<SeaTunnelRow, String, String> {

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }

    private static class CountingWriter extends NoopWriter {

        private final AtomicInteger noArgPrepareCommits = new AtomicInteger();
        private final AtomicInteger checkpointPrepareCommits = new AtomicInteger();
        private long lastCheckpointId = -1L;

        @Override
        public Optional<String> prepareCommit() {
            noArgPrepareCommits.incrementAndGet();
            return Optional.empty();
        }

        @Override
        public Optional<String> prepareCommit(long checkpointId) {
            checkpointPrepareCommits.incrementAndGet();
            lastCheckpointId = checkpointId;
            return Optional.empty();
        }
    }

    private static class NoopCommitter implements SinkCommitter<String> {

        @Override
        public List<String> commit(List<String> commitInfos) {
            return Collections.emptyList();
        }

        @Override
        public void abort(List<String> commitInfos) {}
    }

    private static class NoopAggregatedCommitter
            implements SinkAggregatedCommitter<String, String> {

        @Override
        public List<String> commit(List<String> aggregatedCommitInfo) {
            return Collections.emptyList();
        }

        @Override
        public String combine(List<String> commitInfos) {
            return "";
        }

        @Override
        public void abort(List<String> aggregatedCommitInfo) {}

        @Override
        public void close() throws IOException {}
    }
}
