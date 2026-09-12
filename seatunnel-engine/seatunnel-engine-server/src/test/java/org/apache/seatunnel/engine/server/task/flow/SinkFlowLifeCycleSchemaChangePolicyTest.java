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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.Meter;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.NonRetryableException;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class SinkFlowLifeCycleSchemaChangePolicyTest {

    @Test
    void testSupportedSchemaChangeIsApplied() throws Exception {
        RecordingSchemaEvolutionWriter writer = new RecordingSchemaEvolutionWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new SchemaEvolutionSink(writer, SchemaChangeType.ADD_COLUMN));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        sinkFlow.received(new Record<>(createAddColumnEvent()));

        Assertions.assertEquals(1, writer.appliedCount.get());
    }

    @Test
    void testUnsupportedSinkSchemaChangeTypeFailsBeforeWriterApply() throws Exception {
        RecordingSchemaEvolutionWriter writer = new RecordingSchemaEvolutionWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new SchemaEvolutionSink(writer, SchemaChangeType.DROP_COLUMN));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        RuntimeException error =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> sinkFlow.received(new Record<>(createAddColumnEvent())));

        Assertions.assertTrue(error.getMessage().contains("not supported end to end"));
        Assertions.assertEquals(0, writer.appliedCount.get());
    }

    @Test
    void testUnsupportedCommentEventIsDroppedForSchemaEvolutionSink() throws Exception {
        RecordingSchemaEvolutionWriter writer = new RecordingSchemaEvolutionWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new SchemaEvolutionSink(writer, SchemaChangeType.ADD_COLUMN));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        sinkFlow.received(new Record<>(createCommentEvent()));

        Assertions.assertEquals(0, writer.appliedCount.get());
    }

    @Test
    void testCommentEventIsDroppedForSinkWithoutSchemaEvolutionSupport() throws Exception {
        RecordingSchemaEvolutionWriter writer = new RecordingSchemaEvolutionWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new PlainSink(writer));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        sinkFlow.received(new Record<>(createCommentEvent()));

        Assertions.assertEquals(0, writer.appliedCount.get());
    }

    /**
     * Regression test for the composite {@link AlterTableColumnsEvent} capability check.
     *
     * <p>A sink that only advertises {@link SchemaChangeType#ADD_COLUMN} must be rejected at the
     * policy gate when the incoming composite event also contains a DROP_COLUMN sub-event. Before
     * the fix, the OR-based check returned {@code true} as long as ANY one capability matched,
     * allowing mixed DDL to slip through and fail later inside the sink writer.
     */
    @Test
    void testMixedCompositeEventFailsAtPolicyGateForPartiallySupportedSink() throws Exception {
        RecordingSchemaEvolutionWriter writer = new RecordingSchemaEvolutionWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new SchemaEvolutionSink(writer, SchemaChangeType.ADD_COLUMN));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        TableIdentifier tableId = TableIdentifier.of("catalog", "database", "table");
        AlterTableColumnsEvent mixedEvent = new AlterTableColumnsEvent(tableId);
        mixedEvent.addEvent(
                AlterTableAddColumnEvent.add(
                        tableId,
                        PhysicalColumn.of(
                                "new_col", BasicType.STRING_TYPE, 64L, true, null, null)));
        mixedEvent.addEvent(new AlterTableDropColumnEvent(tableId, "old_col"));
        mixedEvent.setJobId("job-under-test");

        RuntimeException error =
                Assertions.assertThrows(
                        RuntimeException.class, () -> sinkFlow.received(new Record<>(mixedEvent)));

        Assertions.assertTrue(error.getMessage().contains("not supported end to end"));
        Assertions.assertEquals(0, writer.appliedCount.get());
    }

    @Test
    void testSinkWithoutSchemaEvolutionSupportFailsBeforeWriterApply() throws Exception {
        RecordingSchemaEvolutionWriter writer = new RecordingSchemaEvolutionWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new PlainSink(writer));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        RuntimeException error =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> sinkFlow.received(new Record<>(createAddColumnEvent())));

        Assertions.assertTrue(error.getMessage().contains("does not advertise schema evolution"));
        Assertions.assertTrue(hasCause(error, NonRetryableException.class));
        Assertions.assertEquals(0, writer.appliedCount.get());
    }

    private static boolean hasCause(Throwable error, Class<?> causeType) {
        Throwable current = error;
        while (current != null) {
            if (causeType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    @Test
    void testExplicitDeprecatedSchemaChangeOverrideRemainsSupported() throws Exception {
        LegacySchemaChangeWriter writer = new LegacySchemaChangeWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new SchemaEvolutionSink(writer, SchemaChangeType.ADD_COLUMN));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        sinkFlow.received(new Record<>(createAddColumnEvent()));

        Assertions.assertEquals(1, writer.appliedCount.get());
    }

    @Test
    void testInheritedNoOpSchemaChangeMethodIsDroppedDuringCompatibilityWindow() throws Exception {
        NoOpSchemaChangeWriter writer = new NoOpSchemaChangeWriter();
        SinkFlowLifeCycle<SeaTunnelRow, String, String, String> sinkFlow =
                createSinkFlow(new SchemaEvolutionSink(writer, SchemaChangeType.ADD_COLUMN));
        sinkFlow.init();
        sinkFlow.restoreState(Collections.emptyList());

        sinkFlow.received(new Record<>(createAddColumnEvent()));

        Assertions.assertEquals(0, writer.writtenCount.get());
    }

    private static SinkFlowLifeCycle<SeaTunnelRow, String, String, String> createSinkFlow(
            SeaTunnelSink<SeaTunnelRow, String, String, String> sink) {
        SinkAction<SeaTunnelRow, String, String, String> sinkAction =
                new SinkAction<>(1L, "sink", sink, Collections.emptySet(), Collections.emptySet());
        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 0);
        SeaTunnelTask runningTask = Mockito.mock(SeaTunnelTask.class);
        return new SinkFlowLifeCycle<>(
                sinkAction,
                taskLocation,
                0,
                runningTask,
                new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 2L, 0),
                false,
                new CompletableFuture<>(),
                new TestMetricsContext());
    }

    private static AlterTableAddColumnEvent createAddColumnEvent() {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("catalog", "database", "table"),
                        PhysicalColumn.of(
                                "added_col", BasicType.STRING_TYPE, 64L, true, null, null));
        event.setJobId("job-under-test");
        return event;
    }

    private static AlterTableCommentEvent createCommentEvent() {
        AlterTableCommentEvent event =
                AlterTableCommentEvent.of(
                        TableIdentifier.of("catalog", "database", "table"),
                        "old comment",
                        "new comment");
        event.setJobId("job-under-test");
        return event;
    }

    private static class PlainSink implements SeaTunnelSink<SeaTunnelRow, String, String, String> {
        private final SinkWriter<SeaTunnelRow, String, String> writer;

        private PlainSink(SinkWriter<SeaTunnelRow, String, String> writer) {
            this.writer = writer;
        }

        @Override
        public String getPluginName() {
            return "plain";
        }

        @Override
        public SinkWriter<SeaTunnelRow, String, String> createWriter(SinkWriter.Context context) {
            return writer;
        }
    }

    private static class SchemaEvolutionSink extends PlainSink
            implements SupportSchemaEvolutionSink {
        private final List<SchemaChangeType> supportedTypes;

        private SchemaEvolutionSink(
                SinkWriter<SeaTunnelRow, String, String> writer, SchemaChangeType supportedType) {
            super(writer);
            this.supportedTypes = Collections.singletonList(supportedType);
        }

        @Override
        public List<SchemaChangeType> supports() {
            return supportedTypes;
        }
    }

    private static class RecordingSchemaEvolutionWriter
            implements SinkWriter<SeaTunnelRow, String, String>, SupportSchemaEvolutionSinkWriter {
        private final AtomicInteger appliedCount = new AtomicInteger();

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void applySchemaChange(
                org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent event) {
            appliedCount.incrementAndGet();
        }

        @Override
        public void close() throws IOException {}
    }

    private static class LegacySchemaChangeWriter
            implements SinkWriter<SeaTunnelRow, String, String> {
        private final AtomicInteger appliedCount = new AtomicInteger();

        @Override
        public void write(SeaTunnelRow element) {}

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void applySchemaChange(
                org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent event) {
            appliedCount.incrementAndGet();
        }

        @Override
        public void close() throws IOException {}
    }

    private static class NoOpSchemaChangeWriter
            implements SinkWriter<SeaTunnelRow, String, String> {
        private final AtomicInteger writtenCount = new AtomicInteger();

        @Override
        public void write(SeaTunnelRow element) {
            writtenCount.incrementAndGet();
        }

        @Override
        public Optional<String> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}
    }

    private static class TestMetricsContext implements MetricsContext {
        @Override
        public Counter counter(String name) {
            return new ThreadSafeCounter(name);
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public Meter meter(String name) {
            return null;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }
    }
}
