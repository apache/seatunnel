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

package org.apache.seatunnel.translation.flink.sink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class FlinkSinkWriterTest {

    @Test
    void testPrepareCommitSnapshotsStateAndAdvancesCheckpoint() throws Exception {
        RecordingSinkWriter delegate = new RecordingSinkWriter();
        RecordingContext context = new RecordingContext();

        FlinkSinkWriter<SeaTunnelRow, String, String> flinkSinkWriter =
                new FlinkSinkWriter<>(delegate, 1L, context);

        // first checkpoint
        List<CommitWrapper<String>> commits = flinkSinkWriter.prepareCommit(false);
        List<FlinkWriterState<String>> states = flinkSinkWriter.snapshotState();

        // prepareCommit should call delegate.prepareCommit with checkpointId 1
        Assertions.assertEquals(Collections.singletonList(1L), delegate.prepareCommitCalls);
        Assertions.assertEquals("commit-1", commits.get(0).getCommit());

        // snapshotState should have been called exactly once for checkpointId 1
        Assertions.assertEquals(Collections.singletonList(1L), delegate.snapshotCalls);
        Assertions.assertEquals(1, states.size());
        Assertions.assertEquals(1L, states.get(0).getCheckpointId());
        Assertions.assertEquals("state-1", states.get(0).getState());

        // internal checkpointId should have advanced to 2 for next round
        commits = flinkSinkWriter.prepareCommit(false);
        states = flinkSinkWriter.snapshotState();

        Assertions.assertEquals(2, delegate.prepareCommitCalls.size());
        Assertions.assertEquals(2, delegate.snapshotCalls.size());
        Assertions.assertEquals("commit-2", commits.get(0).getCommit());
        Assertions.assertEquals(2L, states.get(0).getCheckpointId());
        Assertions.assertEquals("state-2", states.get(0).getState());
    }

    @Test
    void testSnapshotStateWithoutPrepareCommitFallsBack() throws Exception {
        RecordingSinkWriter delegate = new RecordingSinkWriter();
        RecordingContext context = new RecordingContext();

        FlinkSinkWriter<SeaTunnelRow, String, String> flinkSinkWriter =
                new FlinkSinkWriter<>(delegate, 3L, context);

        // Direct snapshotState should call delegate.snapshotState with checkpointId 3
        List<FlinkWriterState<String>> states = flinkSinkWriter.snapshotState();

        Assertions.assertEquals(Collections.singletonList(3L), delegate.snapshotCalls);
        Assertions.assertEquals(1, states.size());
        Assertions.assertEquals(3L, states.get(0).getCheckpointId());
        Assertions.assertEquals("state-3", states.get(0).getState());
    }

    @Test
    void testSchemaChangeEventDoesNotForceCommit() throws Exception {
        SchemaAwareRecordingSinkWriter delegate = new SchemaAwareRecordingSinkWriter();
        RecordingContext context = new RecordingContext();

        FlinkSinkWriter<SeaTunnelRow, String, String> flinkSinkWriter =
                new FlinkSinkWriter<>(delegate, 7L, context);

        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("catalog", "database", "table"),
                        PhysicalColumn.of(
                                "added_col",
                                org.apache.seatunnel.api.table.type.BasicType.STRING_TYPE,
                                64L,
                                true,
                                null,
                                null));
        event.setJobId("job-under-test");
        SeaTunnelRow schemaEvent = new SeaTunnelRow(0);
        Map<String, Object> options = new LinkedHashMap<>();
        options.put("schema_change_event", event);
        options.put("schema_subtask_id", 0L);
        schemaEvent.setOptions(options);
        flinkSinkWriter.write(schemaEvent, null);

        SeaTunnelRow row = new SeaTunnelRow(1);
        row.setField(0, "value");
        flinkSinkWriter.write(row, null);

        // Schema change should apply without forcing commit - commits happen via normal Flink
        // lifecycle
        Assertions.assertEquals(1, delegate.writtenRows.size());
        Assertions.assertEquals(Collections.emptyList(), delegate.prepareCommitCalls);
        Assertions.assertEquals(1, delegate.appliedSchemaChanges.size());
        Assertions.assertEquals(event, delegate.appliedSchemaChanges.get(0));
    }

    private static class RecordingSinkWriter implements SinkWriter<SeaTunnelRow, String, String> {

        protected final List<Long> prepareCommitCalls = new ArrayList<>();
        protected final List<Long> snapshotCalls = new ArrayList<>();
        protected final List<SeaTunnelRow> writtenRows = new ArrayList<>();

        @Override
        public void write(SeaTunnelRow element) throws IOException {
            writtenRows.add(element);
        }

        @Override
        public Optional<String> prepareCommit() {
            // not used in these tests
            return Optional.empty();
        }

        @Override
        public Optional<String> prepareCommit(long checkpointId) {
            prepareCommitCalls.add(checkpointId);
            return Optional.of("commit-" + checkpointId);
        }

        @Override
        public List<String> snapshotState(long checkpointId) {
            snapshotCalls.add(checkpointId);
            return Collections.singletonList("state-" + checkpointId);
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {}
    }

    private static class SchemaAwareRecordingSinkWriter extends RecordingSinkWriter
            implements SupportSchemaEvolutionSinkWriter {

        private final List<org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent>
                appliedSchemaChanges = new ArrayList<>();

        @Override
        public void applySchemaChange(
                org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent event) {
            appliedSchemaChanges.add(event);
        }
    }

    private static class RecordingContext implements SinkWriter.Context {

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return new NoopMetricsContext();
        }

        @Override
        public EventListener getEventListener() {
            return event -> {};
        }
    }

    private static class NoopMetricsContext implements MetricsContext {

        @Override
        public org.apache.seatunnel.api.common.metrics.Counter counter(String name) {
            return new org.apache.seatunnel.api.common.metrics.Counter() {
                @Override
                public void inc() {}

                @Override
                public void inc(long n) {}

                @Override
                public void dec() {}

                @Override
                public void dec(long n) {}

                @Override
                public void set(long n) {}

                @Override
                public long getCount() {
                    return 0;
                }

                @Override
                public String name() {
                    return name;
                }

                @Override
                public org.apache.seatunnel.api.common.metrics.Unit unit() {
                    return org.apache.seatunnel.api.common.metrics.Unit.COUNT;
                }
            };
        }

        @Override
        public <C extends org.apache.seatunnel.api.common.metrics.Counter> C counter(
                String name, C counter) {
            return counter;
        }

        @Override
        public org.apache.seatunnel.api.common.metrics.Meter meter(String name) {
            return new org.apache.seatunnel.api.common.metrics.Meter() {
                @Override
                public void markEvent() {}

                @Override
                public void markEvent(long n) {}

                @Override
                public double getRate() {
                    return 0;
                }

                @Override
                public long getCount() {
                    return 0;
                }

                @Override
                public String name() {
                    return name;
                }

                @Override
                public org.apache.seatunnel.api.common.metrics.Unit unit() {
                    return org.apache.seatunnel.api.common.metrics.Unit.COUNT;
                }
            };
        }

        @Override
        public <M extends org.apache.seatunnel.api.common.metrics.Meter> M meter(
                String name, M meter) {
            return meter;
        }
    }
}
