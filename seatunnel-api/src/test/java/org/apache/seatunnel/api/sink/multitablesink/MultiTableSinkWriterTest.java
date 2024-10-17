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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
