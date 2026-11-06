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

package org.apache.seatunnel.benchmark.sink;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.MultiTableFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Exercises production MultiTableSink creation, routing, bounded queues and consumer threads with
 * in-memory sinks. Records are read-only after setup, and each writer owns its counters: there is
 * no shared atomic counter on the measured consumer path.
 */
public final class MultiTableSinkFixture implements AutoCloseable {

    /** HASH spreads keys across replicas; SINGLE_QUEUE keeps all keys on replica zero. */
    public enum Routing {
        HASH,
        SINGLE_QUEUE
    }

    private static final int PRODUCTION_QUEUE_CAPACITY = 1024;
    private static final long CHECKSUM_MULTIPLIER = 31L;

    private final int replicaNum;
    private final int tableCount;
    private final int recordPoolSize;
    private final Routing routing;
    private final int consumerBatchSize;
    private final long batchDelayNanos;
    private final FixtureWriter[] writers;
    private final MultiTableResourceManager<Object> resourceManager =
            new MultiTableResourceManager<Object>() {
                @Override
                public void close() {
                    resourceClosed = true;
                }
            };

    private SeaTunnelRow[] records;
    private int[] recordOwners;
    private long[] cycleCounts;
    private long[] cycleChecksums;
    private long[] cycleMultipliers;
    private MultiTableSinkWriter coordinator;
    private long publishedRows;
    private long checkpointId;
    private boolean closed;
    private boolean resourceClosed;

    public MultiTableSinkFixture(
            int replicaNum,
            int tableCount,
            int recordPoolSize,
            Routing routing,
            int consumerBatchSize,
            long batchDelayMicros) {
        if (replicaNum < 1 || tableCount < 1) {
            throw new IllegalArgumentException("replicaNum and tableCount must be positive");
        }
        // Include one dequeued row per consumer. A wrapped record must not still be in flight.
        if (recordPoolSize < (long) replicaNum * (PRODUCTION_QUEUE_CAPACITY + 1)
                || recordPoolSize < tableCount
                || Integer.bitCount(recordPoolSize) != 1) {
            throw new IllegalArgumentException(
                    "recordPoolSize must be a power of two covering all queues and tables");
        }
        if (consumerBatchSize < 1 || batchDelayMicros < 0) {
            throw new IllegalArgumentException(
                    "consumerBatchSize must be positive and batchDelayMicros non-negative");
        }
        this.replicaNum = replicaNum;
        this.tableCount = tableCount;
        this.recordPoolSize = recordPoolSize;
        this.routing = Objects.requireNonNull(routing, "routing");
        this.consumerBatchSize = consumerBatchSize;
        this.batchDelayNanos = TimeUnit.MICROSECONDS.toNanos(batchDelayMicros);
        this.writers = new FixtureWriter[Math.multiplyExact(tableCount, replicaNum)];
    }

    /** Creates the real composite sink and its replica writers without an engine or plugin scan. */
    public void setUp() throws IOException {
        Map<TablePath, SeaTunnelSink> sinks = new LinkedHashMap<>();
        String[] tableIds = new String[tableCount];
        for (int table = 0; table < tableCount; table++) {
            TablePath path = TablePath.of("benchmark", "table_" + table);
            tableIds[table] = path.toString();
            sinks.put(path, new FixtureSink(table));
        }
        createRecords(tableIds);
        ReadonlyConfig options =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA.key(),
                                replicaNum));
        MultiTableSink sink =
                new MultiTableSink(
                        new MultiTableFactoryContext(options, getClass().getClassLoader(), sinks));
        coordinator = (MultiTableSinkWriter) sink.createWriter(new FixtureContext());
    }

    /** Publishes one preallocated row; the production bounded queues supply backpressure. */
    public long publish() throws IOException {
        coordinator.write(records[(int) (publishedRows & (recordPoolSize - 1))]);
        return ++publishedRows;
    }

    /**
     * Completes queued and in-flight writes, flushes simulated consumer batches and validates every
     * writer. JMH invokes this outside each timed iteration, including warmup iterations.
     */
    public void drainAndVerify() throws IOException {
        coordinator.prepareCommit(++checkpointId);
        long cycles = publishedRows / recordPoolSize;
        int remainder = (int) (publishedRows & (recordPoolSize - 1));
        long[] counts = new long[writers.length];
        long[] checksums = new long[writers.length];
        for (int writer = 0; writer < writers.length; writer++) {
            counts[writer] = cycles * cycleCounts[writer];
            // Java long overflow intentionally defines a deterministic rolling checksum.
            for (long cycle = 0; cycle < cycles; cycle++) {
                checksums[writer] =
                        checksums[writer] * cycleMultipliers[writer] + cycleChecksums[writer];
            }
        }
        for (int record = 0; record < remainder; record++) {
            int owner = recordOwners[record];
            counts[owner]++;
            checksums[owner] = checksums[owner] * CHECKSUM_MULTIPLIER + record;
        }
        for (int writer = 0; writer < writers.length; writer++) {
            FixtureWriter actual = writers[writer];
            if (actual.rowCount != counts[writer]
                    || actual.flushedRows != counts[writer]
                    || actual.checksum != checksums[writer]) {
                throw new IllegalStateException(
                        String.format(
                                "MultiTableSink writer %d mismatch: published=%d, expected=%d, "
                                        + "consumed=%d, flushed=%d, expectedChecksum=%d, checksum=%d",
                                writer,
                                publishedRows,
                                counts[writer],
                                actual.rowCount,
                                actual.flushedRows,
                                checksums[writer],
                                actual.checksum));
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (coordinator != null) {
            try {
                drainAndVerify();
            } finally {
                coordinator.close();
            }
        }
    }

    long getPublishedRows() {
        return publishedRows;
    }

    long getConsumedRows() {
        long total = 0;
        for (FixtureWriter writer : writers) {
            total += writer.rowCount;
        }
        return total;
    }

    long getConsumedRows(int table, int replica) {
        return writers[table * replicaNum + replica].rowCount;
    }

    long getBatchCount() {
        long total = 0;
        for (FixtureWriter writer : writers) {
            total += writer.batches;
        }
        return total;
    }

    boolean isFullyClosed() {
        if (!resourceClosed) {
            return false;
        }
        for (FixtureWriter writer : writers) {
            if (!writer.closed) {
                return false;
            }
        }
        return true;
    }

    private void createRecords(String[] tableIds) {
        records = new SeaTunnelRow[recordPoolSize];
        recordOwners = new int[recordPoolSize];
        cycleCounts = new long[writers.length];
        cycleChecksums = new long[writers.length];
        cycleMultipliers = new long[writers.length];
        java.util.Arrays.fill(cycleMultipliers, 1L);
        for (int i = 0; i < recordPoolSize; i++) {
            int table = i % tableCount;
            long key = routing == Routing.SINGLE_QUEUE ? 0L : i / tableCount;
            SeaTunnelRow row = new SeaTunnelRow(new Object[] {key, (long) i});
            row.setTableId(tableIds[table]);
            // getOptions lazily allocates. Complete it before the row is shared across threads.
            row.getOptions();
            records[i] = row;
            int owner = table * replicaNum + (int) (key % replicaNum);
            recordOwners[i] = owner;
            cycleCounts[owner]++;
            cycleChecksums[owner] = cycleChecksums[owner] * CHECKSUM_MULTIPLIER + i;
            cycleMultipliers[owner] *= CHECKSUM_MULTIPLIER;
        }
    }

    private final class FixtureSink
            implements SeaTunnelSink<SeaTunnelRow, Void, Void, Void>, SupportMultiTableSink {

        private final int table;

        private FixtureSink(int table) {
            this.table = table;
        }

        @Override
        public SinkWriter<SeaTunnelRow, Void, Void> createWriter(SinkWriter.Context context) {
            int replica = context.getIndexOfSubtask();
            FixtureWriter writer =
                    new FixtureWriter(replica, consumerBatchSize, batchDelayNanos, resourceManager);
            writers[table * replicaNum + replica] = writer;
            return writer;
        }

        @Override
        public String getPluginName() {
            return "MultiTableBenchmarkSink";
        }
    }

    private static final class FixtureWriter
            implements SinkWriter<SeaTunnelRow, Void, Void>, SupportMultiTableSinkWriter<Object> {

        private final int replica;
        private final int consumerBatchSize;
        private final long batchDelayNanos;
        private final MultiTableResourceManager<Object> resourceManager;
        private long rowCount;
        private long checksum;
        private long flushedRows;
        private long batches;
        private int pendingRows;
        private boolean closed;

        private FixtureWriter(
                int replica,
                int consumerBatchSize,
                long batchDelayNanos,
                MultiTableResourceManager<Object> resourceManager) {
            this.replica = replica;
            this.consumerBatchSize = consumerBatchSize;
            this.batchDelayNanos = batchDelayNanos;
            this.resourceManager = resourceManager;
        }

        @Override
        public Optional<Integer> primaryKey() {
            return Optional.of(0);
        }

        @Override
        public MultiTableResourceManager<Object> initMultiTableResourceManager(
                int tableSize, int queueSize) {
            return resourceManager;
        }

        @Override
        public void setMultiTableResourceManager(
                MultiTableResourceManager<Object> manager, int queueIndex) {
            if (manager != resourceManager || queueIndex != replica) {
                throw new IllegalStateException("Unexpected MultiTableSink resource assignment");
            }
        }

        @Override
        public void write(SeaTunnelRow row) throws IOException {
            checksum = checksum * CHECKSUM_MULTIPLIER + (Long) row.getField(1);
            rowCount++;
            if (++pendingRows == consumerBatchSize) {
                flush();
            }
        }

        @Override
        public Optional<Void> prepareCommit() throws IOException {
            flush();
            return Optional.empty();
        }

        @Override
        public void abortPrepare() {}

        @Override
        public void close() throws IOException {
            flush();
            closed = true;
        }

        private void flush() throws IOException {
            if (pendingRows == 0) {
                return;
            }
            long deadline = System.nanoTime() + batchDelayNanos;
            long remaining;
            while ((remaining = deadline - System.nanoTime()) > 0) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedIOException("Benchmark batch delay interrupted");
                }
                LockSupport.parkNanos(remaining);
            }
            flushedRows += pendingRows;
            pendingRows = 0;
            batches++;
        }
    }

    private static final class FixtureContext implements SinkWriter.Context {

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
            return null;
        }
    }
}
