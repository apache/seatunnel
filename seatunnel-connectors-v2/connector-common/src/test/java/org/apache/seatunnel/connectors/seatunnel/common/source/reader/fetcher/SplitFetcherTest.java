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

package org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitsAddition;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies cancellation, ordinary split wakeups, blocked output and cleanup races through the
 * actual fetch loop.
 */
class SplitFetcherTest {

    /**
     * A blocked reader must reach cleanup through the manager's public close entry point without
     * leaving its child thread running.
     */
    @Test
    void managerCloseUnblocksFetchAndAllowsCleanup() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        CountDownLatch fetching = new CountDownLatch(1);
        CountDownLatch releaseFetch = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        when(reader.fetch())
                .thenAnswer(
                        invocation -> {
                            fetching.countDown();
                            try {
                                releaseFetch.await();
                                throw new AssertionError("Unexpected latch release");
                            } catch (InterruptedException e) {
                                throw new IOException(e);
                            }
                        });
        doAnswer(
                        invocation -> {
                            assertFalse(Thread.currentThread().isInterrupted());
                            new CountDownLatch(1).await(1, TimeUnit.MILLISECONDS);
                            closed.countDown();
                            return null;
                        })
                .when(reader)
                .close();
        SingleThreadFetcherManager<String, SourceSplit> manager =
                new SingleThreadFetcherManager<>(new LinkedBlockingQueue<>(), () -> reader);
        try {
            manager.addSplits(Collections.singletonList(split("snapshot")));
            assertTrue(fetching.await(5, TimeUnit.SECONDS));
            manager.close(5000);
            assertTrue(closed.await(1, TimeUnit.SECONDS));
            manager.checkErrors();
            assertTrue(manager.fetchers.isEmpty());
            verify(reader, times(1)).close();
        } finally {
            // A regressed implementation must fail the assertions above without leaking the
            // manager's non-daemon thread into subsequent tests.
            releaseFetch.countDown();
            manager.close(5000);
        }
    }

    /**
     * An ordinary add-splits wakeup must retain the fetched batch and keep the reader running until
     * an explicit shutdown.
     */
    @Test
    void addingSplitsDoesNotInterruptOrLoseRecords() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        BlockingQueue<RecordsWithSplitIds<String>> input = new LinkedBlockingQueue<>();
        BlockingQueue<RecordsWithSplitIds<String>> output = new LinkedBlockingQueue<>();
        CountDownLatch fetching = new CountDownLatch(1);
        when(reader.fetch())
                .thenAnswer(
                        invocation -> {
                            fetching.countDown();
                            try {
                                return input.take();
                            } catch (InterruptedException e) {
                                throw new IOException(e);
                            }
                        });
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher = fetcher(reader, output, errors);
        fetcher.addSplits(Collections.singletonList(split("first")));
        Thread thread = start(fetcher);
        try {
            assertTrue(fetching.await(5, TimeUnit.SECONDS));
            fetcher.addSplits(Collections.singletonList(split("second")));
            RecordsWithSplitIds<String> records = records();
            input.put(records);
            assertSame(records, output.poll(5, TimeUnit.SECONDS));
            verify(reader, times(2)).handleSplitsChanges(any(SplitsAddition.class));
            verify(reader, never()).close();
            assertTrue(errors.isEmpty());
        } finally {
            stop(fetcher, thread);
        }
        assertTrue(errors.isEmpty());
    }

    /**
     * Cancellation must also interrupt a full output queue without marking a batch finished before
     * that batch is delivered.
     */
    @Test
    void shutdownUnblocksFullOutputQueue() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        CountDownLatch offering = new CountDownLatch(1);
        BlockingQueue<RecordsWithSplitIds<String>> output =
                new ArrayBlockingQueue<RecordsWithSplitIds<String>>(1) {
                    @Override
                    public boolean offer(
                            RecordsWithSplitIds<String> records, long timeout, TimeUnit unit)
                            throws InterruptedException {
                        offering.countDown();
                        return super.offer(records, timeout, unit);
                    }
                };
        RecordsWithSplitIds<String> existing = records();
        output.put(existing);
        RecordsWithSplitIds<String> pending = records();
        when(pending.finishedSplits()).thenReturn(Collections.singleton("snapshot"));
        when(reader.fetch()).thenReturn(pending);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        AtomicBoolean finished = new AtomicBoolean();
        SplitFetcher<String, SourceSplit> fetcher =
                new SplitFetcher<>(
                        0, output, reader, errors::add, () -> {}, ignored -> finished.set(true));
        fetcher.addSplits(Collections.singletonList(split("snapshot")));
        Thread thread = start(fetcher);
        try {
            assertTrue(offering.await(5, TimeUnit.SECONDS));
        } finally {
            stop(fetcher, thread);
        }
        assertFalse(finished.get());
        assertSame(existing, output.take());
        assertTrue(output.isEmpty());
        assertTrue(errors.isEmpty());
        verify(reader).close();
    }

    /**
     * A reader may return normally with its interrupt flag still set; cleanup must still be able to
     * wait for its child tasks.
     */
    @Test
    void shutdownClearsResidualInterruptBeforeCleanup() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        CountDownLatch fetching = new CountDownLatch(1);
        AtomicBoolean cleanupCompleted = new AtomicBoolean();
        when(reader.fetch())
                .thenAnswer(
                        invocation -> {
                            fetching.countDown();
                            while (!Thread.currentThread().isInterrupted()) {
                                LockSupport.park();
                            }
                            return records();
                        });
        doAnswer(
                        invocation -> {
                            assertFalse(Thread.currentThread().isInterrupted());
                            new CountDownLatch(1).await(1, TimeUnit.MILLISECONDS);
                            cleanupCompleted.set(true);
                            return null;
                        })
                .when(reader)
                .close();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher =
                fetcher(reader, new LinkedBlockingQueue<>(), errors);
        fetcher.addSplits(Collections.singletonList(split("snapshot")));
        Thread thread = start(fetcher);
        try {
            assertTrue(fetching.await(5, TimeUnit.SECONDS));
        } finally {
            stop(fetcher, thread);
        }
        assertTrue(cleanupCompleted.get());
        assertTrue(errors.isEmpty());
    }

    /**
     * Shutdown arriving after a read failure must not interrupt the reader's close operation or
     * conceal the original failure.
     */
    @Test
    void lateShutdownDoesNotInterruptCleanupOrHideReadFailure() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        IOException failure = new IOException("read failure");
        when(reader.fetch()).thenThrow(failure);
        CountDownLatch closing = new CountDownLatch(1);
        CountDownLatch allowClose = new CountDownLatch(1);
        AtomicBoolean cleanupCompleted = new AtomicBoolean();
        doAnswer(
                        invocation -> {
                            closing.countDown();
                            assertTrue(allowClose.await(5, TimeUnit.SECONDS));
                            cleanupCompleted.set(true);
                            return null;
                        })
                .when(reader)
                .close();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher =
                fetcher(reader, new LinkedBlockingQueue<>(), errors);
        fetcher.addSplits(Collections.singletonList(split("snapshot")));
        Thread thread = start(fetcher);
        try {
            assertTrue(closing.await(5, TimeUnit.SECONDS));
            fetcher.shutdown();
            fetcher.shutdown();
        } finally {
            allowClose.countDown();
            stop(fetcher, thread);
        }
        assertTrue(cleanupCompleted.get());
        assertEquals(1, errors.size());
        assertSame(failure, errors.get(0).getCause().getCause());
    }

    /**
     * Closing before execution must neither fetch queued splits nor skip reader cleanup, including
     * when shutdown is repeated.
     */
    @Test
    void shutdownBeforeStartClosesReaderOnce() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher =
                fetcher(reader, new LinkedBlockingQueue<>(), errors);
        fetcher.addSplits(Collections.singletonList(split("snapshot")));
        fetcher.shutdown();
        fetcher.shutdown();
        Thread thread = start(fetcher);
        stop(fetcher, thread);
        verify(reader, never()).fetch();
        verify(reader, never()).handleSplitsChanges(any());
        verify(reader, times(1)).close();
        assertTrue(errors.isEmpty());
    }

    /**
     * An idle fetcher must be signalled without an interrupt, and failures in reader cleanup must
     * remain visible to the caller.
     */
    @Test
    void idleShutdownStillReportsCleanupFailure() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        IOException failure = new IOException("close failure");
        doThrow(failure).when(reader).close();
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher =
                fetcher(reader, new LinkedBlockingQueue<>(), errors);
        Thread thread = start(fetcher);
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals(Thread.State.WAITING, thread.getState());
        } finally {
            stop(fetcher, thread);
        }
        assertEquals(Collections.singletonList(failure), errors);
    }

    /**
     * A cyclic exception cause chain must terminate traversal, preserve the failure and still close
     * the reader.
     */
    @Test
    void cyclicFetchFailureDuringShutdownIsReported() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        IOException firstFailure = new IOException("first failure");
        IOException secondFailure = new IOException("second failure");
        firstFailure.initCause(secondFailure);
        secondFailure.initCause(firstFailure);
        CountDownLatch fetching = new CountDownLatch(1);
        when(reader.fetch())
                .thenAnswer(
                        invocation -> {
                            fetching.countDown();
                            try {
                                new CountDownLatch(1).await();
                                throw new AssertionError("Unexpected latch release");
                            } catch (InterruptedException ignored) {
                                throw firstFailure;
                            }
                        });
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher =
                fetcher(reader, new LinkedBlockingQueue<>(), errors);
        fetcher.addSplits(Collections.singletonList(split("snapshot")));
        Thread thread = start(fetcher);
        try {
            assertTrue(fetching.await(5, TimeUnit.SECONDS));
        } finally {
            stop(fetcher, thread);
        }

        assertEquals(1, errors.size());
        assertSame(firstFailure, errors.get(0).getCause().getCause());
        verify(reader).close();
    }

    /**
     * Interruptions unrelated to shutdown must remain visible as read failures instead of being
     * silently treated as cancellation.
     */
    @Test
    void unexpectedReadInterruptionIsReported() throws Exception {
        SplitReader<String, SourceSplit> reader = mock(SplitReader.class);
        IOException failure = new IOException(new InterruptedException("unexpected interrupt"));
        when(reader.fetch()).thenThrow(failure);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        SplitFetcher<String, SourceSplit> fetcher =
                fetcher(reader, new LinkedBlockingQueue<>(), errors);
        fetcher.addSplits(Collections.singletonList(split("snapshot")));
        Thread thread = start(fetcher);
        thread.join(5000);
        try {
            assertFalse(thread.isAlive());
            assertEquals(1, errors.size());
            assertSame(failure, errors.get(0).getCause().getCause());
        } finally {
            stop(fetcher, thread);
        }
    }

    /**
     * Builds a fetcher with the real task loop and an observable error handler so failures can be
     * checked after the thread exits.
     */
    private static SplitFetcher<String, SourceSplit> fetcher(
            SplitReader<String, SourceSplit> reader,
            BlockingQueue<RecordsWithSplitIds<String>> output,
            List<Throwable> errors) {
        return new SplitFetcher<>(0, output, reader, errors::add, () -> {}, ignored -> {});
    }

    /**
     * Creates a split with a stable identifier so assignment and completion use the same key
     * throughout the fetch loop.
     */
    private static SourceSplit split(String id) {
        SourceSplit split = mock(SourceSplit.class);
        when(split.splitId()).thenReturn(id);
        return split;
    }

    /**
     * Creates a batch whose identity can be checked across a wakeup, with no finished splits unless
     * the test explicitly sets them.
     */
    private static RecordsWithSplitIds<String> records() {
        RecordsWithSplitIds<String> records = mock(RecordsWithSplitIds.class);
        when(records.finishedSplits()).thenReturn(Collections.emptySet());
        return records;
    }

    /**
     * Starts the actual fetch loop on a daemon thread so a regressed shutdown cannot make a failed
     * assertion hang the test JVM.
     */
    private static Thread start(SplitFetcher<String, SourceSplit> fetcher) {
        Thread thread = new Thread(fetcher, "split-fetcher-shutdown-test");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Requires shutdown to terminate before the output offer timeout so an unchanged blocked queue
     * cannot make this test pass.
     */
    private static void stop(SplitFetcher<String, SourceSplit> fetcher, Thread thread)
            throws Exception {
        fetcher.shutdown();
        thread.join(5000);
        assertFalse(thread.isAlive(), "Fetcher did not terminate after shutdown");
    }
}
