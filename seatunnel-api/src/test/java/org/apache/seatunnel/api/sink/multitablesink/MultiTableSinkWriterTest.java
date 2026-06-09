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
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.CloseTableEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    public void testCloseTableEventRejectsNewRowsImmediately() throws IOException {
        String table1 = TablePath.of("db", "schema", "table1").getFullName();
        TrackingSinkWriter table1Writer0 = new TrackingSinkWriter("table1-0");

        Map<SinkIdentifier, SinkWriter<SeaTunnelRow, ?, ?>> sinkWriters = new HashMap<>();
        Map<SinkIdentifier, SinkWriter.Context> sinkWritersContext = new HashMap<>();
        sinkWriters.put(SinkIdentifier.of(table1, 0), table1Writer0);
        sinkWritersContext.put(SinkIdentifier.of(table1, 0), new TestSinkWriterContext());

        MultiTableSinkWriter multiTableSinkWriter =
                new MultiTableSinkWriter(sinkWriters, 1, sinkWritersContext);

        multiTableSinkWriter.handleCloseTableEvent(
                new CloseTableEvent(TablePath.of("db", "schema", "table1"), 0, 1));

        Assertions.assertEquals(0, table1Writer0.closeCount.get());
        SeaTunnelRow lateRow = new SeaTunnelRow(new Object[] {1});
        lateRow.setTableId(table1);
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
    }

    static class TrackingSinkWriter extends TestSinkWriter {
        private final AtomicInteger closeCount = new AtomicInteger();
        private final String stateValue;

        TrackingSinkWriter(String stateValue) {
            this.stateValue = stateValue;
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
