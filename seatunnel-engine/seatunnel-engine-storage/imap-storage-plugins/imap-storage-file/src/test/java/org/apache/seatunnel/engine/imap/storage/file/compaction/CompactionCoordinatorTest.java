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

package org.apache.seatunnel.engine.imap.storage.file.compaction;

import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.wal.reader.IFileReader.RecordConsumer;

import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

/** Exercises scheduler failure and cancellation independently of a filesystem's latency. */
class CompactionCoordinatorTest {
    private static final Path DIRECTORY = new Path("file:///compaction-coordinator-test");
    private static final Path INPUT = new Path(DIRECTORY, "segment-1-wal.txt");
    private static final long MAX_BYTES = 1024 * 1024;
    private static final long MAX_KEYS = 100;

    @Test
    void shouldBoundStopAndRetainInputsWhenReadIgnoresInterruption() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Planner planner = new Planner(plan(INPUT));
        Storage storage =
                new Storage() {
                    @Override
                    public void read(Path input, RecordConsumer<IMapFileData> consumer)
                            throws IOException {
                        entered.countDown();
                        awaitUninterruptibly(release);
                        super.read(input, consumer);
                    }
                };
        Closeable registration = register(planner, storage, 50);
        try {
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            long start = System.nanoTime();
            Assertions.assertThrows(IOException.class, registration::close);
            Assertions.assertTrue(
                    System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2),
                    "stopping must not wait indefinitely for filesystem I/O");
            Assertions.assertThrows(IOException.class, registration::close);
            Assertions.assertEquals(0, storage.published.get());
            Assertions.assertEquals(0, storage.deleted.get());
        } finally {
            release.countDown();
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertDoesNotThrow(registration::close));
        }
        Assertions.assertEquals(0, planner.committed.get());
        Assertions.assertEquals(0, storage.published.get());
        Assertions.assertEquals(0, storage.deleted.get());
    }

    @Test
    void shouldRetainInputsWhenCancellationRacesPublication() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Planner planner = new Planner(plan(INPUT));
        Storage storage =
                new Storage() {
                    @Override
                    public Path publish(CompactionPlan plan, Collection<IMapFileData> mutations) {
                        entered.countDown();
                        awaitUninterruptibly(release);
                        return super.publish(plan, mutations);
                    }
                };
        Closeable registration = register(planner, storage, 50);
        try {
            Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
            Assertions.assertThrows(IOException.class, registration::close);
        } finally {
            release.countDown();
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertDoesNotThrow(registration::close));
        }
        Assertions.assertEquals(1, storage.published.get());
        Assertions.assertEquals(0, planner.committed.get());
        Assertions.assertEquals(0, storage.deleted.get());
    }

    @Test
    void shouldRetryRecoverableReadFailure() throws Exception {
        Planner planner = new Planner(plan(INPUT));
        Storage storage =
                new Storage() {
                    @Override
                    public void read(Path input, RecordConsumer<IMapFileData> consumer)
                            throws IOException {
                        if (reads.getAndIncrement() == 0) {
                            throw new IOException("temporary read failure");
                        }
                        consumer.accept(mutation(1, null));
                    }
                };
        try (Closeable ignored = register(planner, storage, 1000)) {
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(1, storage.deleted.get()));
            Assertions.assertEquals(1, planner.committed.get());
            Assertions.assertEquals(2, storage.reads.get());
            Assertions.assertEquals(1, storage.published.get());
            Assertions.assertEquals(1, storage.deleted.get());
        }
    }

    @Test
    void shouldNotRereadDeferredPlanUntilSelectedInputsChange() throws Exception {
        Path second = new Path(DIRECTORY, "segment-2-wal.txt");
        Path third = new Path(DIRECTORY, "segment-3-wal.txt");
        Planner planner = new Planner(plan(INPUT, second));
        Storage storage =
                new Storage() {
                    @Override
                    public void read(Path input, RecordConsumer<IMapFileData> consumer)
                            throws IOException {
                        reads.incrementAndGet();
                        consumer.accept(
                                input.equals(third)
                                        ? mutation(2, null)
                                        : mutation(1, input.equals(INPUT) ? null : 100L));
                    }
                };
        try (Closeable ignored = register(planner, storage, 1000)) {
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(planner.selections.get() >= 2));
            Assertions.assertEquals(2, storage.reads.get());
            Assertions.assertEquals(0, storage.published.get());
            Assertions.assertEquals(0, storage.deleted.get());

            planner.selected.set(plan(INPUT, second, third));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(3, storage.deleted.get()));
            Assertions.assertEquals(1, planner.committed.get());
            Assertions.assertEquals(5, storage.reads.get());
            Assertions.assertEquals(1, storage.published.get());
            Assertions.assertEquals(3, storage.deleted.get());
        }
    }

    @Test
    void shouldDisableFatalTaskWhileOtherWritersContinue() throws Exception {
        AtomicInteger failedReads = new AtomicInteger();
        Planner failedPlanner = new Planner(plan(INPUT));
        Storage failedStorage =
                new Storage() {
                    @Override
                    public void read(Path input, RecordConsumer<IMapFileData> consumer) {
                        failedReads.incrementAndGet();
                        throw new AssertionError("synthetic fatal compaction failure");
                    }
                };
        AtomicInteger otherAttempts = new AtomicInteger();
        CompactionPlanner otherPlanner =
                new CompactionPlanner() {
                    @Override
                    public Optional<CompactionPlan> plan() {
                        otherAttempts.incrementAndGet();
                        return Optional.empty();
                    }

                    @Override
                    public void committed(CompactionPlan plan, Path snapshot) {
                        Assertions.fail("an empty plan cannot be committed");
                    }
                };
        try (Closeable failed = register(failedPlanner, failedStorage, 1000);
                Closeable other = register(otherPlanner, new Storage(), 1000)) {
            await().atMost(6, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(otherAttempts.get() >= 3));
            Assertions.assertEquals(1, failedReads.get());
            Assertions.assertEquals(0, failedStorage.published.get());
            Assertions.assertEquals(0, failedStorage.deleted.get());
        }
    }

    private static Closeable register(
            CompactionPlanner planner, CompactionStorage storage, long stopTimeoutMillis) {
        return CompactionCoordinator.register(
                planner, storage, 1, MAX_BYTES, MAX_KEYS, stopTimeoutMillis);
    }

    private static CompactionPlan plan(Path... inputs) {
        return new CompactionPlan(Arrays.asList(inputs), DIRECTORY);
    }

    private static IMapFileData mutation(long timestamp, Long expirationTime) {
        return IMapFileData.builder()
                .key(new byte[] {1})
                .keyClassName("key")
                .value(new byte[] {2})
                .valueClassName("value")
                .timestamp(timestamp)
                .expirationTime(expirationTime)
                .build();
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean released = false;
        while (!released) {
            try {
                latch.await();
                released = true;
            } catch (InterruptedException ignored) {
                // Simulate filesystem I/O that does not honor the cancellation interrupt.
            }
        }
    }

    private static final class Planner implements CompactionPlanner {
        private final AtomicReference<CompactionPlan> selected;
        private final AtomicInteger selections = new AtomicInteger();
        private final AtomicInteger committed = new AtomicInteger();

        private Planner(CompactionPlan plan) {
            selected = new AtomicReference<>(plan);
        }

        @Override
        public Optional<CompactionPlan> plan() {
            selections.incrementAndGet();
            return Optional.ofNullable(selected.get());
        }

        @Override
        public void committed(CompactionPlan plan, Path snapshot) {
            committed.incrementAndGet();
            selected.set(null);
        }
    }

    private static class Storage implements CompactionStorage {
        protected final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger published = new AtomicInteger();
        private final AtomicInteger deleted = new AtomicInteger();

        @Override
        public void read(Path input, RecordConsumer<IMapFileData> consumer) throws IOException {
            reads.incrementAndGet();
            consumer.accept(mutation(1, null));
        }

        @Override
        public Path publish(CompactionPlan plan, Collection<IMapFileData> mutations) {
            published.incrementAndGet();
            return new Path(DIRECTORY, "snapshot-wal.txt");
        }

        @Override
        public void delete(Path input) {
            deleted.incrementAndGet();
        }
    }
}
