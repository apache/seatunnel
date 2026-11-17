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

package org.apache.seatunnel.benchmark.multitablesink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.sink.multitablesink.SinkContextProxy;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.function.RunnableWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Exercises the production timer flush callback with two simulated buffered batches per queue. */
public final class MultiTableTimerFlushFixture implements AutoCloseable {

    private final List<FlushWriter> writers = new ArrayList<>();
    private final SinkWriter<SeaTunnelRow, ?, ?> coordinator;
    private final RunnableWithException flushAction;
    private long rounds;

    public MultiTableTimerFlushFixture(int queueCount, long writerFlushMillis) throws IOException {
        if (writerFlushMillis < 0) {
            throw new IllegalArgumentException("writerFlushMillis must be non-negative");
        }
        FlushSink childSink = new FlushSink(writerFlushMillis);
        Map<TablePath, SeaTunnelSink> sinks =
                Map.of(
                        TablePath.of("benchmark", "table_0"), childSink,
                        TablePath.of("benchmark", "table_1"), childSink);
        ReadonlyConfig options =
                ReadonlyConfig.fromMap(
                        Map.of(
                                SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(),
                                queueCount));
        MultiTableSink sink =
                new MultiTableSink(
                        new MultiTableFactoryContext(options, getClass().getClassLoader(), sinks));
        SinkWriter.Context context = new SinkContextProxy(0, 1, new DefaultSinkWriterContext(0, 1));
        coordinator = sink.createWriter(context);
        flushAction = context.getFlushAction();
        if (flushAction == null) {
            coordinator.close();
            throw new IllegalStateException("MultiTableSink did not register a timer flush action");
        }
    }

    public void flush() throws Exception {
        flushAction.run();
    }

    /** Confirms that the callback waited for one flush from every child writer. */
    public void verifyRound() {
        rounds++;
        for (int i = 0; i < writers.size(); i++) {
            long flushedBatches = writers.get(i).flushedBatches;
            if (flushedBatches != rounds) {
                throw new IllegalStateException(
                        String.format(
                                "Writer %d flushed %d batches after %d timer rounds",
                                i, flushedBatches, rounds));
            }
        }
    }

    @Override
    public void close() throws IOException {
        coordinator.close();
    }

    private final class FlushSink
            implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void>, SupportMultiTableSink {

        private final long writerFlushMillis;

        private FlushSink(long writerFlushMillis) {
            this.writerFlushMillis = writerFlushMillis;
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            FlushWriter writer = new FlushWriter(writerFlushMillis);
            context.registerFlushAction(writer::flush);
            writers.add(writer);
            return writer;
        }

        @Override
        public String getPluginName() {
            return "MultiTableTimerFlushBenchmarkSink";
        }
    }

    private static final class FlushWriter
            implements SinkWriter<SeaTunnelRow, Void, Void>, SupportMultiTableSinkWriter<Void> {

        private final long writerFlushMillis;
        private long flushedBatches;

        private FlushWriter(long writerFlushMillis) {
            this.writerFlushMillis = writerFlushMillis;
        }

        private void flush() throws InterruptedException {
            TimeUnit.MILLISECONDS.sleep(writerFlushMillis);
            flushedBatches++;
        }

        @Override
        public void write(SeaTunnelRow row) {
            throw new UnsupportedOperationException(
                    "Only timer flush is supported by this fixture");
        }

        @Override
        public Optional<Void> prepareCommit() {
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() {}
    }
}
