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

package org.apache.seatunnel.edge.agent.starter.wal.mem;

import org.apache.seatunnel.edge.agent.connector.EdgeEvent;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePosition;
import org.apache.seatunnel.edge.agent.connector.EdgeSourcePositionStore;
import org.apache.seatunnel.edge.agent.starter.wal.WalRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MemWalStoreConcurrencyTest {

    private static final int THREAD_COUNT = 4;
    private static final int EVENTS_PER_THREAD = 200;

    /**
     * Multiple threads appending concurrently must produce unique, strictly positive IDs with no
     * duplicates or gaps.
     */
    @RepeatedTest(2)
    void concurrentAppendProducesUniqueMonotonicIds() throws Exception {
        MemWalStore store = new MemWalStore();
        CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<List<Long>>> futures = new ArrayList<>();

        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadIdx = t;
            futures.add(
                    executor.submit(
                            () -> {
                                barrier.await();
                                List<Long> ids = new ArrayList<>();
                                for (int i = 0; i < EVENTS_PER_THREAD; i++) {
                                    long id =
                                            store.append(
                                                    EdgeEvent.builder()
                                                            .sourceId("src-" + threadIdx)
                                                            .payload(
                                                                    new byte[] {
                                                                        (byte) threadIdx, (byte) i
                                                                    })
                                                            .eventTime(System.currentTimeMillis())
                                                            .build());
                                    ids.add(id);
                                }
                                return ids;
                            }));
        }

        Set<Long> allIds = new HashSet<>();
        for (Future<List<Long>> future : futures) {
            List<Long> ids = future.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(EVENTS_PER_THREAD, ids.size());
            allIds.addAll(ids);
        }

        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        int totalExpected = THREAD_COUNT * EVENTS_PER_THREAD;
        Assertions.assertEquals(totalExpected, allIds.size(), "All IDs must be unique");
        Assertions.assertEquals(1L, Collections.min(allIds), "IDs start at 1");
        Assertions.assertEquals(
                totalExpected, Collections.max(allIds), "IDs are contiguous up to N");
    }

    /**
     * Multiple producers appending concurrently while a consumer calls claimPending repeatedly.
     * Total claimed events must equal total appended — no events lost.
     */
    @RepeatedTest(2)
    void concurrentAppendAndClaimLosesNoEvents() throws Exception {
        MemWalStore store = new MemWalStore();
        int producerCount = 4;
        int eventsPerProducer = 300;
        int totalExpected = producerCount * eventsPerProducer;

        CountDownLatch producersDone = new CountDownLatch(producerCount);
        ExecutorService executor = Executors.newFixedThreadPool(producerCount + 1);
        CyclicBarrier barrier = new CyclicBarrier(producerCount + 1);

        for (int t = 0; t < producerCount; t++) {
            final int threadIdx = t;
            executor.submit(
                    () -> {
                        try {
                            barrier.await();
                            for (int i = 0; i < eventsPerProducer; i++) {
                                store.append(
                                        EdgeEvent.builder()
                                                .sourceId("producer-" + threadIdx)
                                                .payload(new byte[] {(byte) i})
                                                .eventTime(System.currentTimeMillis())
                                                .build());
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            producersDone.countDown();
                        }
                    });
        }

        // Consumer drains in a loop until all producers finish and queue is empty
        CopyOnWriteArrayList<WalRecord> allClaimed = new CopyOnWriteArrayList<>();
        Future<?> consumer =
                executor.submit(
                        () -> {
                            try {
                                barrier.await();
                                while (!producersDone.await(1, TimeUnit.MILLISECONDS)) {
                                    List<WalRecord> batch = store.claimPending(500, 10);
                                    allClaimed.addAll(batch);
                                }
                                // Final drain after all producers done
                                List<WalRecord> remaining =
                                        store.claimPending(Integer.MAX_VALUE, 10);
                                allClaimed.addAll(remaining);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        consumer.get(15, TimeUnit.SECONDS);
        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        Assertions.assertEquals(
                totalExpected,
                allClaimed.size(),
                "No events lost: total claimed must equal total appended");

        // Verify no duplicate IDs
        Set<Long> ids = new HashSet<>();
        for (WalRecord record : allClaimed) {
            Assertions.assertTrue(
                    ids.add(record.getId()), "Duplicate WAL record ID: " + record.getId());
        }
    }

    /**
     * claimPending is atomic: concurrent claims must never return the same record twice. Only one
     * thread wins the full drain; others get empty.
     */
    @RepeatedTest(2)
    void concurrentClaimPendingNeverReturnsDuplicates() throws Exception {
        MemWalStore store = new MemWalStore();
        int totalEvents = 800;

        for (int i = 0; i < totalEvents; i++) {
            store.append(
                    EdgeEvent.builder()
                            .sourceId("src")
                            .payload(new byte[] {(byte) (i & 0xFF)})
                            .eventTime(System.currentTimeMillis())
                            .build());
        }

        int claimerCount = 6;
        CyclicBarrier barrier = new CyclicBarrier(claimerCount);
        ExecutorService executor = Executors.newFixedThreadPool(claimerCount);
        List<Future<List<WalRecord>>> futures = new ArrayList<>();

        for (int t = 0; t < claimerCount; t++) {
            futures.add(
                    executor.submit(
                            () -> {
                                barrier.await();
                                return store.claimPending(Integer.MAX_VALUE, 10);
                            }));
        }

        List<WalRecord> allClaimed = new ArrayList<>();
        for (Future<List<WalRecord>> future : futures) {
            allClaimed.addAll(future.get(10, TimeUnit.SECONDS));
        }

        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        Assertions.assertEquals(
                totalEvents, allClaimed.size(), "All events must be claimed exactly once");

        Set<Long> ids = new HashSet<>();
        for (WalRecord record : allClaimed) {
            Assertions.assertTrue(
                    ids.add(record.getId()),
                    "Record claimed by multiple threads: id=" + record.getId());
        }
    }

    /**
     * After claimPending clears the list, subsequent claims return empty until new events are
     * appended — even under concurrent access.
     */
    @Test
    void claimPendingClearsPendingListAtomically() throws Exception {
        MemWalStore store = new MemWalStore();
        int batchSize = 100;

        for (int i = 0; i < batchSize; i++) {
            store.append(
                    EdgeEvent.builder()
                            .sourceId("src")
                            .payload(new byte[] {1})
                            .eventTime(1L)
                            .build());
        }

        List<WalRecord> first = store.claimPending(Integer.MAX_VALUE, 10);
        Assertions.assertEquals(batchSize, first.size());

        List<WalRecord> second = store.claimPending(Integer.MAX_VALUE, 10);
        Assertions.assertTrue(second.isEmpty(), "Second claim must be empty after full drain");
    }

    /**
     * sourcePositionStore() operations are safe under concurrent save/load from multiple threads.
     */
    @RepeatedTest(2)
    void concurrentPositionStoreAccess() throws Exception {
        MemWalStore store = new MemWalStore();
        EdgeSourcePositionStore posStore = store.sourcePositionStore();
        int writerCount = 4;
        int writesPerThread = 500;

        CyclicBarrier barrier = new CyclicBarrier(writerCount);
        ExecutorService executor = Executors.newFixedThreadPool(writerCount);
        AtomicInteger saveCount = new AtomicInteger();

        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < writerCount; t++) {
            final int threadIdx = t;
            futures.add(
                    executor.submit(
                            () -> {
                                try {
                                    barrier.await();
                                    for (int i = 0; i < writesPerThread; i++) {
                                        EdgeSourcePosition pos =
                                                EdgeSourcePosition.builder()
                                                        .sourceId("src-" + threadIdx)
                                                        .partition("file-" + i)
                                                        .offset(i * 100L)
                                                        .updatedAt(System.currentTimeMillis())
                                                        .build();
                                        posStore.save(pos);
                                        saveCount.incrementAndGet();
                                        // Interleave reads
                                        posStore.load("src-" + threadIdx, "file-" + i);
                                        posStore.loadBySource("src-" + threadIdx);
                                    }
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }));
        }

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        executor.shutdown();
        Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        Assertions.assertEquals(writerCount * writesPerThread, saveCount.get());
    }
}
