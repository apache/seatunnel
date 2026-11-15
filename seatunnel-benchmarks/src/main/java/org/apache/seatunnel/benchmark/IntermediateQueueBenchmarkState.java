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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.common.metrics.ThreadSafeCounter;
import org.apache.seatunnel.api.table.type.Record;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.engine.common.config.server.QueueType;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.task.flow.IntermediateQueueFlowLifeCycle;
import org.apache.seatunnel.engine.server.task.group.queue.AbstractIntermediateQueue;
import org.apache.seatunnel.engine.server.task.group.queue.IntermediateBlockingQueue;
import org.apache.seatunnel.engine.server.task.group.queue.IntermediateDisruptor;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEvent;
import org.apache.seatunnel.engine.server.task.group.queue.disruptor.RecordEventFactory;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/** Owns the queue lifecycle and reusable records shared by the queue comparison benchmarks. */
final class IntermediateQueueBenchmarkState {

    private static final int PAYLOAD_SIZE = 256;
    private static final long CONSUMER_DRAIN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final QueueType queueType;
    private final int capacity;
    private final int recordPoolSize;
    private final AtomicLong consumedRecords = new AtomicLong();
    private final AtomicLong consumedChecksum = new AtomicLong();
    private final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();

    private Record<?>[] records;
    private AbstractIntermediateQueue<?> queue;
    private IntermediateQueueFlowLifeCycle<?> flowLifeCycle;
    private Thread blockingQueueConsumer;
    private long publishedRecords;

    IntermediateQueueBenchmarkState(QueueType queueType, int capacity, int recordPoolSize) {
        this.queueType = queueType;
        this.capacity = capacity;
        this.recordPoolSize = recordPoolSize;
    }

    void setUp() throws Exception {
        validateParameters();
        records = createRecords(recordPoolSize);
        queue = createQueue();
        flowLifeCycle =
                new IntermediateQueueFlowLifeCycle<>(null, new CompletableFuture<>(), queue);

        if (queueType == QueueType.BLOCKINGQUEUE) {
            startBlockingQueueConsumer();
        } else {
            flowLifeCycle.collect(new BenchmarkCollector());
        }
    }

    long publish() {
        checkConsumerFailure();
        Record<?> record = records[(int) (publishedRecords & (recordPoolSize - 1))];
        flowLifeCycle.received(record);
        return ++publishedRecords;
    }

    void tearDown() throws Exception {
        Throwable failure = null;
        try {
            awaitConsumerDrain();
            checkConsumerFailure();
        } catch (Throwable throwable) {
            failure = throwable;
        }

        if (blockingQueueConsumer != null) {
            blockingQueueConsumer.interrupt();
            try {
                blockingQueueConsumer.join(TimeUnit.SECONDS.toMillis(1));
                if (blockingQueueConsumer.isAlive()) {
                    throw new IllegalStateException("Blocking queue consumer did not stop");
                }
            } catch (Throwable throwable) {
                failure = combineFailures(failure, throwable);
            }
        }

        if (flowLifeCycle != null) {
            try {
                flowLifeCycle.close();
            } catch (Throwable throwable) {
                failure = combineFailures(failure, throwable);
            }
        }

        if (failure != null) {
            rethrow(failure);
        }
    }

    long getPublishedRecords() {
        return publishedRecords;
    }

    long getConsumedRecords() {
        return consumedRecords.get();
    }

    long getConsumedChecksum() {
        return consumedChecksum.get();
    }

    private void validateParameters() {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("capacity must be a positive power of two");
        }
        // Keep at least one reusable record per queue slot so a saturated queue does not contain
        // duplicate record instances. Records remain read-only after trial setup.
        if (recordPoolSize < capacity || Integer.bitCount(recordPoolSize) != 1) {
            throw new IllegalArgumentException(
                    "recordPoolSize must be a power of two and at least capacity");
        }
    }

    private AbstractIntermediateQueue<?> createQueue() {
        Counter totalQueueSize = new ThreadSafeCounter("totalQueueSize");
        Counter queueSize = new ThreadSafeCounter("queueSize");
        Counter putBlockedNs = new ThreadSafeCounter("putBlockedNs");
        Counter flushSuccess = new ThreadSafeCounter("flushSuccess");
        Counter flushFailure = new ThreadSafeCounter("flushFailure");

        if (queueType == QueueType.BLOCKINGQUEUE) {
            return new IntermediateBlockingQueue(
                    new ArrayBlockingQueue<>(capacity),
                    totalQueueSize,
                    queueSize,
                    putBlockedNs,
                    flushSuccess,
                    flushFailure);
        }

        Disruptor<RecordEvent> disruptor =
                new Disruptor<>(
                        new RecordEventFactory(),
                        capacity,
                        DaemonThreadFactory.INSTANCE,
                        ProducerType.SINGLE,
                        new YieldingWaitStrategy());
        return new IntermediateDisruptor(
                disruptor, totalQueueSize, queueSize, putBlockedNs, flushSuccess, flushFailure);
    }

    private static Record<?>[] createRecords(int size) {
        Record<?>[] recordPool = new Record<?>[size];
        for (int i = 0; i < size; i++) {
            byte[] payload = new byte[PAYLOAD_SIZE];
            for (int j = 0; j < payload.length; j++) {
                payload[j] = (byte) (i + j);
            }
            recordPool[i] =
                    new Record<>(
                            new SeaTunnelRow(
                                    new Object[] {
                                        (long) i, "queue-benchmark-" + i, payload, i % 128
                                    }));
        }
        return recordPool;
    }

    private void startBlockingQueueConsumer() {
        blockingQueueConsumer =
                new Thread(
                        () -> {
                            try {
                                while (!Thread.currentThread().isInterrupted()) {
                                    flowLifeCycle.collect(new BenchmarkCollector());
                                }
                            } catch (InterruptedException ignored) {
                                Thread.currentThread().interrupt();
                            } catch (Throwable throwable) {
                                consumerFailure.compareAndSet(null, throwable);
                            }
                        },
                        "intermediate-blocking-queue-benchmark-consumer");
        blockingQueueConsumer.setDaemon(true);
        blockingQueueConsumer.start();
    }

    private void consume(Record<?> record) {
        SeaTunnelRow row = (SeaTunnelRow) record.getData();
        byte[] payload = (byte[]) row.getField(2);
        long checksum =
                (Long) row.getField(0)
                        + ((String) row.getField(1)).length()
                        + Byte.toUnsignedInt(payload[0])
                        + Byte.toUnsignedInt(payload[payload.length - 1])
                        + (Integer) row.getField(3);
        consumedChecksum.addAndGet(checksum);
        consumedRecords.incrementAndGet();
    }

    private void awaitConsumerDrain() {
        long deadline = System.nanoTime() + CONSUMER_DRAIN_TIMEOUT_NANOS;
        while (consumedRecords.get() != publishedRecords && System.nanoTime() < deadline) {
            checkConsumerFailure();
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        if (consumedRecords.get() != publishedRecords) {
            throw new IllegalStateException(
                    String.format(
                            "Timed out waiting for %s: published=%d, consumed=%d",
                            queueType, publishedRecords, consumedRecords.get()));
        }
    }

    private void checkConsumerFailure() {
        Throwable failure = consumerFailure.get();
        if (failure != null) {
            throw new IllegalStateException("Queue consumer failed", failure);
        }
    }

    private static Throwable combineFailures(Throwable first, Throwable second) {
        if (first == null) {
            return second;
        }
        first.addSuppressed(second);
        return first;
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        throw (Error) failure;
    }

    private final class BenchmarkCollector implements Collector<Record<?>> {

        @Override
        public void collect(Record<?> record) {
            consume(record);
        }

        @Override
        public void close() {
            // Nothing to close.
        }
    }
}
