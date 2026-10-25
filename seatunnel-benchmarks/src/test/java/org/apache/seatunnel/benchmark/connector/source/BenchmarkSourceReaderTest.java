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

package org.apache.seatunnel.benchmark.connector.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BenchmarkSourceReaderTest {

    @Test
    void shouldEmitItsInterleavedSequenceAndFinish() throws Exception {
        TestReaderContext context = new TestReaderContext();
        BenchmarkSourceReader reader = new BenchmarkSourceReader(context);
        BenchmarkSourceSplit split = new BenchmarkSourceSplit(1, 2, 6, 0L, 0L, 8, 16, 1L);
        TestCollector collector = new TestCollector();

        reader.addSplits(Collections.singletonList(split));
        reader.handleNoMoreSplits();
        reader.pollNext(collector);

        assertEquals(3, collector.rows.size());
        assertEquals(1L, collector.rows.get(0).getField(0));
        assertEquals(3L, collector.rows.get(1).getField(0));
        assertEquals(5L, collector.rows.get(2).getField(0));
        assertEquals(8, ((String) collector.rows.get(0).getField(2)).length());
        assertTrue(context.finished);
    }

    @Test
    void shouldCalculateAbsoluteScheduleWithoutAccumulatingRoundingError() {
        BenchmarkSourceSplit split = new BenchmarkSourceSplit(0, 1, 10, 1_000L, 3L, 0, 1, 0L);

        assertEquals(1_000L, BenchmarkSourceReader.scheduledMillis(split, 0L));
        assertEquals(1_333L, BenchmarkSourceReader.scheduledMillis(split, 1L));
        assertEquals(2_000L, BenchmarkSourceReader.scheduledMillis(split, 3L));
    }

    private static final class TestCollector implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }

    private static final class TestReaderContext implements SourceReader.Context {
        private boolean finished;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            finished = true;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
