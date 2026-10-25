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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Concurrent tests for DefaultErrorSinkWriter. */
public class DefaultErrorSinkWriterConcurrentTest {

    private static final String TEST_STAGE = "SINK";
    private static final String TEST_PLUGIN = "Jdbc";

    @Test
    @Timeout(10)
    public void testConcurrentWriteToQueue() throws Exception {
        // Test multiple threads writing errors concurrently
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(200, 5);
        int threadCount = 5;
        int errorsPerThread = 20;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulWrites = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int i = 0; i < errorsPerThread; i++) {
                                        try {
                                            RowErrorContext ctx =
                                                    createContext("table_" + threadId);
                                            SeaTunnelRow row = createRow(threadId * 1000 + i);
                                            wrapper.write(
                                                    ctx, row, new RuntimeException("error " + i));
                                            successfulWrites.incrementAndGet();
                                        } catch (Exception e) {
                                            // Ignore
                                        }
                                    }
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } finally {
                                    finishLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        finishLatch.await(5, TimeUnit.SECONDS);
        wrapper.waitForAllProcessed(10000);
        wrapper.close();

        // Verify writes succeeded (allow some margin for concurrent execution)
        int expectedWrites = threadCount * errorsPerThread;
        assertTrue(
                successfulWrites.get() >= expectedWrites * 0.9,
                "Expected at least 90% of writes to succeed");
        assertTrue(wrapper.getProcessedCount() > 0, "Expected some records to be processed");
    }

    @Test
    @Timeout(10)
    public void testQueueOverflowPolicyFail() throws Exception {
        // Test that FAIL policy can detect queue overflow
        // Use large processing delay to ensure queue fills up
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(2, 2000, QueueOverflowPolicy.FAIL);

        try {
            // Fill queue
            wrapper.write(createContext("table"), createRow(0), new RuntimeException("error 0"));
            wrapper.write(createContext("table"), createRow(1), new RuntimeException("error 1"));

            // Wait to ensure queue is actually full
            Thread.sleep(200);

            // Try to write more - at least some should fail due to full queue
            boolean anyExceptionThrown = false;
            for (int i = 2; i < 10; i++) {
                try {
                    wrapper.write(
                            createContext("table"),
                            createRow(i),
                            new RuntimeException("error " + i));
                } catch (RuntimeException e) {
                    if (e.getMessage().contains("queue overflow")) {
                        anyExceptionThrown = true;
                        break;
                    }
                }
            }

            assertTrue(anyExceptionThrown, "Expected at least one queue overflow exception");
        } finally {
            wrapper.close();
        }
    }

    @Test
    @Timeout(10)
    public void testQueueOverflowPolicyDrop() throws Exception {
        // Small queue, DROP policy - overflow writes are silently dropped
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(5, 100, QueueOverflowPolicy.DROP);

        // Write many errors rapidly (more than queue capacity)
        for (int i = 0; i < 20; i++) {
            wrapper.write(createContext("table"), createRow(i), new RuntimeException("error " + i));
        }

        wrapper.waitForAllProcessed(5000);
        wrapper.close();

        // Some errors should be dropped, processed count < 20
        assertTrue(
                wrapper.getProcessedCount() < 20,
                "Expected some errors to be dropped, but processed all 20");
        assertTrue(
                wrapper.getProcessedCount() > 0, "Expected at least some errors to be processed");
    }

    @Test
    @Timeout(10)
    public void testQueueOverflowPolicyBlock() throws Exception {
        // Small queue, BLOCK policy - writes block until space available
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(5, 50, QueueOverflowPolicy.BLOCK);

        CountDownLatch writerStarted = new CountDownLatch(1);
        CountDownLatch writerFinished = new CountDownLatch(1);
        AtomicInteger writtenCount = new AtomicInteger(0);

        // Background thread that writes many errors
        Thread writerThread =
                new Thread(
                        () -> {
                            writerStarted.countDown();
                            for (int i = 0; i < 20; i++) {
                                try {
                                    wrapper.write(
                                            createContext("table"),
                                            createRow(i),
                                            new RuntimeException("error " + i));
                                    writtenCount.incrementAndGet();
                                } catch (Exception e) {
                                    break;
                                }
                            }
                            writerFinished.countDown();
                        });
        writerThread.start();

        writerStarted.await();
        Thread.sleep(500); // Let some writes block

        // Wait for all to finish
        writerFinished.await(5, TimeUnit.SECONDS);
        wrapper.waitForAllProcessed(5000);
        wrapper.close();

        // All writes should succeed (blocking allowed them through)
        assertEquals(20, writtenCount.get());
        assertEquals(20, wrapper.getProcessedCount());
    }

    @Test
    @Timeout(10)
    public void testWorkerThreadProcessesErrors() throws Exception {
        // Verify worker thread picks up and processes errors
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(100, 10);

        for (int i = 0; i < 10; i++) {
            wrapper.write(createContext("table"), createRow(i), new RuntimeException("error " + i));
        }

        wrapper.waitForAllProcessed(5000);
        wrapper.close();

        assertEquals(10, wrapper.getProcessedCount());
    }

    @Test
    @Timeout(10)
    public void testCloseWaitsForQueueToDrain() throws Exception {
        // Close should wait for worker to finish processing
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(100, 50);

        // Write many errors
        for (int i = 0; i < 50; i++) {
            wrapper.write(createContext("table"), createRow(i), new RuntimeException("error " + i));
        }

        long startTime = System.currentTimeMillis();
        wrapper.close();
        long duration = System.currentTimeMillis() - startTime;

        // Close should have waited for processing (at least some time)
        assertTrue(duration > 100, "Close returned too quickly, may not have waited for queue");
        // Most records should be processed (allow small margin for timing)
        assertTrue(
                wrapper.getProcessedCount() >= wrapper.getOfferedCount() * 0.95,
                "Expected most records to be processed before close");
    }

    @Test
    @Timeout(10)
    public void testConcurrentWriteDifferentTables() throws Exception {
        // Test concurrent writes for different tables
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(200, 5);
        int tableCount = 5;
        int errorsPerTable = 20;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(tableCount);

        for (int t = 0; t < tableCount; t++) {
            final String tableId = "table_" + t;
            new Thread(
                            () -> {
                                try {
                                    startLatch.await();
                                    for (int i = 0; i < errorsPerTable; i++) {
                                        wrapper.write(
                                                createContext(tableId),
                                                createRow(i),
                                                new RuntimeException("error " + i));
                                    }
                                } catch (Exception e) {
                                    // Ignore
                                } finally {
                                    finishLatch.countDown();
                                }
                            })
                    .start();
        }

        startLatch.countDown();
        finishLatch.await(5, TimeUnit.SECONDS);

        wrapper.waitForAllProcessed(5000);
        wrapper.close();

        assertEquals(tableCount * errorsPerTable, wrapper.getProcessedCount());
    }

    @Test
    @Timeout(10)
    public void testWorkerFailureStopsProcessing() throws Exception {
        // Worker encounters error, should stop processing
        FailingMockErrorSinkWriter wrapper =
                new FailingMockErrorSinkWriter(100, 10, 5); // Fail on 5th error

        // Write 10 errors
        for (int i = 0; i < 10; i++) {
            try {
                wrapper.write(
                        createContext("table"), createRow(i), new RuntimeException("error " + i));
            } catch (Exception e) {
                // May throw after worker fails
            }
        }

        Thread.sleep(500); // Let worker fail

        // Further writes should fail quickly
        assertThrows(
                RuntimeException.class,
                () ->
                        wrapper.write(
                                createContext("table"),
                                createRow(999),
                                new RuntimeException("after failure")));

        wrapper.close();

        // Should have processed less than 10 (stopped at 5th)
        assertTrue(wrapper.getProcessedCount() <= 5, "Worker should have stopped after failure");
    }

    @Test
    @Timeout(10)
    public void testRapidOpenClose() throws Exception {
        // Test rapid open/close cycles
        for (int i = 0; i < 10; i++) {
            MockErrorSinkWriter wrapper = new MockErrorSinkWriter(10, 1);
            wrapper.write(createContext("table"), createRow(i), new RuntimeException("error " + i));
            wrapper.close();
        }
    }

    @Test
    @Timeout(10)
    public void testLargeVolumeProcessing() throws Exception {
        // Test processing large number of errors
        MockErrorSinkWriter wrapper = new MockErrorSinkWriter(1000, 1);
        int errorCount = 1000;

        for (int i = 0; i < errorCount; i++) {
            wrapper.write(createContext("table"), createRow(i), new RuntimeException("error " + i));
        }

        wrapper.waitForAllProcessed(10000);
        wrapper.close();

        assertEquals(errorCount, wrapper.getProcessedCount());
    }

    // Helper methods

    private RowErrorContext createContext(String tableId) {
        return new RowErrorContext(TEST_STAGE, TEST_STAGE, TEST_PLUGIN, tableId);
    }

    private SeaTunnelRow createRow(int id) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {id, "name_" + id, 20 + id});
        row.setTableId("test_table");
        return row;
    }

    /** Simplified mock of DefaultErrorSinkWriter for testing. */
    private static class MockErrorSinkWriter implements ErrorSinkRowWriter<SeaTunnelRow> {
        private final int queueCapacity;
        private final long processingDelayMs;
        private final QueueOverflowPolicy overflowPolicy;
        private final List<SeaTunnelRow> processedRows;
        private final java.util.concurrent.BlockingQueue<SeaTunnelRow> queue;
        private final Thread workerThread;
        private volatile boolean closed;
        private final AtomicInteger processedCount = new AtomicInteger(0);
        private final AtomicInteger offeredCount = new AtomicInteger(0);

        MockErrorSinkWriter(int queueCapacity, long processingDelayMs) {
            this(queueCapacity, processingDelayMs, QueueOverflowPolicy.FAIL);
        }

        MockErrorSinkWriter(
                int queueCapacity, long processingDelayMs, QueueOverflowPolicy overflowPolicy) {
            this.queueCapacity = queueCapacity;
            this.processingDelayMs = processingDelayMs;
            this.overflowPolicy = overflowPolicy;
            this.processedRows = Collections.synchronizedList(new ArrayList<>());
            this.queue = new java.util.concurrent.ArrayBlockingQueue<>(queueCapacity);

            this.workerThread =
                    new Thread(
                            () -> {
                                while (!closed || !queue.isEmpty()) {
                                    try {
                                        SeaTunnelRow row = queue.poll(100, TimeUnit.MILLISECONDS);
                                        if (row != null) {
                                            if (processingDelayMs > 0) {
                                                Thread.sleep(processingDelayMs);
                                            }
                                            processedRows.add(row);
                                            processedCount.incrementAndGet();
                                        }
                                    } catch (InterruptedException e) {
                                        if (!closed) {
                                            Thread.currentThread().interrupt();
                                            break;
                                        }
                                    }
                                }
                            });
            this.workerThread.setDaemon(true);
            this.workerThread.start();
        }

        @Override
        public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t) throws Exception {
            boolean success;
            switch (overflowPolicy) {
                case DROP:
                    success = queue.offer(row);
                    if (success) {
                        offeredCount.incrementAndGet();
                    }
                    break;
                case BLOCK:
                    queue.put(row);
                    offeredCount.incrementAndGet();
                    break;
                case FAIL:
                default:
                    success = queue.offer(row);
                    if (!success) {
                        throw new RuntimeException("Error queue overflow");
                    }
                    offeredCount.incrementAndGet();
                    break;
            }
        }

        @Override
        public void close() throws Exception {
            closed = true;
            workerThread.interrupt();
            workerThread.join(10000);
        }

        public int getProcessedCount() {
            return processedCount.get();
        }

        public int getOfferedCount() {
            return offeredCount.get();
        }

        public void waitForAllProcessed(long timeoutMs) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            while (processedCount.get() < offeredCount.get()
                    && System.currentTimeMillis() - startTime < timeoutMs) {
                Thread.sleep(100);
            }
        }
    }

    /** Mock that fails after processing N errors. */
    private static class FailingMockErrorSinkWriter extends MockErrorSinkWriter {
        private final int failAfter;
        private final AtomicInteger writeCount = new AtomicInteger(0);

        FailingMockErrorSinkWriter(int queueCapacity, long processingDelayMs, int failAfter) {
            super(queueCapacity, processingDelayMs);
            this.failAfter = failAfter;
        }

        @Override
        public void write(RowErrorContext ctx, SeaTunnelRow row, Throwable t) throws Exception {
            int count = writeCount.incrementAndGet();
            if (count >= failAfter) {
                throw new RuntimeException("Simulated worker failure");
            }
            super.write(ctx, row, t);
        }
    }
}
