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
package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcReaderProgressReportingTest {

    @Test
    void slowMasterDoesNotBlockMetricsOrQueueReports() throws Exception {
        ExecutorService metrics = Executors.newSingleThreadExecutor();
        CompletableFuture<Object> pending = new CompletableFuture<>();
        AtomicBoolean inFlight = new AtomicBoolean();
        AtomicInteger sent = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Runnable tick =
                () ->
                        TaskExecutionService.reportCdcProgressAsync(
                                inFlight,
                                () -> {
                                    sent.incrementAndGet();
                                    return pending;
                                },
                                failure::set);
        try {
            metrics.submit(tick).get(5, TimeUnit.SECONDS);
            metrics.submit(tick).get(5, TimeUnit.SECONDS);
            assertEquals(42, metrics.submit(() -> 42).get(5, TimeUnit.SECONDS));
            assertEquals(1, sent.get());
            assertTrue(inFlight.get());
            assertFalse(pending.isDone());
            pending.complete(null);
            metrics.submit(tick).get(5, TimeUnit.SECONDS);
            assertEquals(2, sent.get());
            assertFalse(inFlight.get());
            assertNull(failure.get());
        } finally {
            pending.complete(null);
            metrics.shutdownNow();
            assertTrue(metrics.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void failedMasterReleasesSlotForNextTick() throws Exception {
        AtomicBoolean inFlight = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CompletableFuture<Object> pending = new CompletableFuture<>();
        RuntimeException unavailable = new RuntimeException("master unavailable");
        TaskExecutionService.reportCdcProgressAsync(inFlight, () -> pending, failure::set);
        ExecutorService completion = Executors.newSingleThreadExecutor();
        try {
            completion
                    .submit(() -> pending.completeExceptionally(unavailable))
                    .get(5, TimeUnit.SECONDS);
            assertSame(unavailable, failure.get());
            assertFalse(inFlight.get());
            CompletableFuture<Object> next = new CompletableFuture<>();
            TaskExecutionService.reportCdcProgressAsync(inFlight, () -> next, failure::set);
            assertTrue(inFlight.get());
            next.complete(null);
            assertFalse(inFlight.get());
        } finally {
            completion.shutdownNow();
            assertTrue(completion.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void synchronousCollectionOrInvocationFailureAllowsRetry() {
        AtomicBoolean inFlight = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        RuntimeException rejected = new RuntimeException("invocation rejected");
        TaskExecutionService.reportCdcProgressAsync(
                inFlight,
                () -> {
                    throw rejected;
                },
                failure::set);
        assertSame(rejected, failure.get());
        assertFalse(inFlight.get());
        AtomicBoolean retried = new AtomicBoolean();
        TaskExecutionService.reportCdcProgressAsync(
                inFlight,
                () -> {
                    retried.set(true);
                    return CompletableFuture.completedFuture(null);
                },
                failure::set);
        assertTrue(retried.get());
        assertFalse(inFlight.get());
    }

    @Test
    void alreadyFailedOrCancelledInvocationReleasesSlot() {
        for (boolean cancel : new boolean[] {false, true}) {
            AtomicBoolean inFlight = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CompletableFuture<Object> pending = new CompletableFuture<>();
            if (cancel) {
                pending.cancel(false);
            } else {
                pending.completeExceptionally(new IllegalStateException("master unavailable"));
            }
            TaskExecutionService.reportCdcProgressAsync(inFlight, () -> pending, failure::set);
            assertTrue(failure.get() != null);
            assertFalse(inFlight.get());
        }
    }
}
