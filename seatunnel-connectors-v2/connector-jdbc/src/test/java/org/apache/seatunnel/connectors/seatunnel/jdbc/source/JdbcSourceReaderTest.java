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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.CloseTableEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcInputFormat;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.event.JdbcTableFinishedEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class JdbcSourceReaderTest {

    @Test
    void testPollNextEmitsPendingCloseTableEventBeforeReadingNextSplit() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat inputFormat = Mockito.mock(JdbcInputFormat.class);
        JdbcSourceReader reader = new JdbcSourceReader(context, inputFormat);
        TablePath tablePath = TablePath.of("db", "schema", "table");
        TestCollector collector = new TestCollector();

        reader.handleSourceEvent(new JdbcTableFinishedEvent(tablePath, 1));
        reader.pollNext(collector);

        Assertions.assertEquals(1, collector.closeTableEvents.size());
        Assertions.assertEquals(tablePath, collector.closeTableEvents.get(0).getTablePath());
        Assertions.assertEquals(0, collector.closeTableEvents.get(0).getSourceSubtaskId());
        Assertions.assertEquals(1, collector.closeTableEvents.get(0).getExpectedSourceEventCount());
        Mockito.verifyNoInteractions(inputFormat);
    }

    @Test
    void testPollNextReportsSplitFinishedEventAfterSplitConsumed() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat inputFormat = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(inputFormat.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, inputFormat);
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceSplit split =
                new JdbcSourceSplit(tablePath, "split-0", "select 1", null, null, null, null);

        reader.addSplits(Collections.singletonList(split));
        reader.pollNext(new TestCollector());

        Assertions.assertEquals(1, context.sourceEvents.size());
        Assertions.assertInstanceOf(JdbcSplitFinishedEvent.class, context.sourceEvents.get(0));
        Assertions.assertEquals(
                tablePath, ((JdbcSplitFinishedEvent) context.sourceEvents.get(0)).getTablePath());
        Mockito.verify(inputFormat).open(split);
        Mockito.verify(inputFormat).close();
    }

    @Test
    void testSnapshotStateRestoresPendingCloseTableEvent() throws Exception {
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceReader reader =
                new JdbcSourceReader(
                        new TestSourceReaderContext(), Mockito.mock(JdbcInputFormat.class));
        reader.handleSourceEvent(new JdbcTableFinishedEvent(tablePath, 2));

        List<JdbcSourceSplit> snapshot = reader.snapshotState(1L);

        Assertions.assertEquals(1, snapshot.size());
        Assertions.assertTrue(snapshot.get(0).isPendingCloseTableEvent());

        JdbcSourceReader restoredReader =
                new JdbcSourceReader(
                        new TestSourceReaderContext(), Mockito.mock(JdbcInputFormat.class));
        restoredReader.addSplits(snapshot);
        TestCollector collector = new TestCollector();

        restoredReader.pollNext(collector);

        Assertions.assertEquals(1, collector.closeTableEvents.size());
        Assertions.assertEquals(tablePath, collector.closeTableEvents.get(0).getTablePath());
        Assertions.assertEquals(0, collector.closeTableEvents.get(0).getSourceSubtaskId());
        Assertions.assertEquals(2, collector.closeTableEvents.get(0).getExpectedSourceEventCount());
    }

    @Test
    void testSnapshotStateRoundTripsWaitingGlobalCloseTable() throws Exception {
        TestSourceReaderContext context = new TestSourceReaderContext();
        JdbcInputFormat inputFormat = Mockito.mock(JdbcInputFormat.class);
        Mockito.when(inputFormat.reachedEnd()).thenReturn(true);
        JdbcSourceReader reader = new JdbcSourceReader(context, inputFormat);
        TablePath tablePath = TablePath.of("db", "schema", "table");
        JdbcSourceSplit split =
                new JdbcSourceSplit(tablePath, "split-0", "select 1", null, null, null, null);

        reader.addSplits(Collections.singletonList(split));
        reader.pollNext(new TestCollector());

        List<JdbcSourceSplit> snapshot = reader.snapshotState(1L);
        Assertions.assertEquals(1, snapshot.size());
        Assertions.assertTrue(snapshot.get(0).isCloseTableMarker());
        Assertions.assertFalse(snapshot.get(0).isPendingCloseTableEvent());

        JdbcSourceReader restoredReader =
                new JdbcSourceReader(
                        new TestSourceReaderContext(), Mockito.mock(JdbcInputFormat.class));
        restoredReader.addSplits(snapshot);
        List<JdbcSourceSplit> restoredSnapshot = restoredReader.snapshotState(2L);

        Assertions.assertEquals(1, restoredSnapshot.size());
        Assertions.assertTrue(restoredSnapshot.get(0).isCloseTableMarker());
        Assertions.assertFalse(restoredSnapshot.get(0).isPendingCloseTableEvent());
    }

    private static final class TestSourceReaderContext
            implements org.apache.seatunnel.api.source.SourceReader.Context {
        private final List<SourceEvent> sourceEvents = new ArrayList<>();

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public org.apache.seatunnel.api.source.Boundedness getBoundedness() {
            return org.apache.seatunnel.api.source.Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {
            sourceEvents.add(sourceEvent);
        }

        @Override
        public org.apache.seatunnel.api.common.metrics.MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public org.apache.seatunnel.api.event.EventListener getEventListener() {
            return null;
        }
    }

    private static final class TestCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private final List<CloseTableEvent> closeTableEvents = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {}

        @Override
        public void collect(CloseTableEvent event) {
            closeTableEvents.add(event);
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }
}
