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

import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressEnvelope;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressOwner;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CdcReaderProgressReportingTest {

    @Test
    void uninitializedTasksDoNotExposeProgress() {
        SourceSeaTunnelTask reader =
                Mockito.mock(SourceSeaTunnelTask.class, Mockito.CALLS_REAL_METHODS);
        SourceSplitEnumeratorTask enumerator =
                Mockito.mock(SourceSplitEnumeratorTask.class, Mockito.CALLS_REAL_METHODS);
        assertNull(reader.getCdcProgressReport());
        assertNull(enumerator.getCdcProgressReport());
    }

    @Test
    void failingProviderDoesNotDiscardHealthyTaskReports() {
        SourceSeaTunnelTask uninitialized =
                Mockito.mock(SourceSeaTunnelTask.class, Mockito.CALLS_REAL_METHODS);
        SourceSeaTunnelTask failing = Mockito.mock(SourceSeaTunnelTask.class);
        SourceSeaTunnelTask healthy = Mockito.mock(SourceSeaTunnelTask.class);
        Mockito.when(failing.getCdcProgressOwner()).thenReturn(CdcProgressOwner.READER);
        Mockito.when(failing.getCdcProgressReport())
                .thenThrow(new IllegalStateException("provider unavailable"));
        Mockito.when(healthy.getCdcProgressOwner()).thenReturn(CdcProgressOwner.READER);
        Mockito.when(healthy.getTaskLocation())
                .thenReturn(new TaskLocation(new TaskGroupLocation(1L, 2, 3L), 4L, 0));
        Mockito.when(healthy.getCdcProgressSourceVertexId()).thenReturn(5L);
        Mockito.when(healthy.nextCdcProgressSequence()).thenReturn(1L);
        CdcReaderProgressReport report =
                new CdcReaderProgressReport(
                        "test",
                        CdcProgressLifecycle.INCREMENTAL,
                        "split",
                        CdcProgressValue.unavailable(),
                        CdcProgressValue.unsupported(),
                        CdcProgressValue.unsupported(),
                        0L,
                        null);
        Mockito.when(healthy.getCdcProgressReport()).thenReturn(report);
        List<CdcProgressEnvelope<?>> reports = new ArrayList<>();
        List<Exception> failures = new ArrayList<>();
        TaskExecutionService.collectCdcProgress(
                Arrays.asList(uninitialized, failing, healthy),
                CdcProgressOwner.READER,
                7L,
                8L,
                reports,
                failures::add);
        assertEquals(1, failures.size());
        assertEquals(1, reports.size());
        assertSame(report, reports.get(0).getReport());
        assertEquals(7L, reports.get(0).getExecutionAttemptId());
    }

    @Test
    void collectionFailureDoesNotKillSubsequentMonitorTicks() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();
        java.util.concurrent.ScheduledExecutorService monitor =
                Executors.newSingleThreadScheduledExecutor();
        java.util.concurrent.CountDownLatch recovered = new java.util.concurrent.CountDownLatch(1);
        try {
            monitor.scheduleAtFixedRate(
                    () ->
                            SeaTunnelServer.collectCdcProgressSafely(
                                    () -> {
                                        if (attempts.incrementAndGet() == 1) {
                                            throw new IllegalStateException(
                                                    "transient plan failure");
                                        }
                                        recovered.countDown();
                                    },
                                    ignored -> failures.incrementAndGet()),
                    0,
                    1,
                    TimeUnit.MILLISECONDS);
            assertTrue(recovered.await(5, TimeUnit.SECONDS));
            assertEquals(1, failures.get());
        } finally {
            monitor.shutdownNow();
            assertTrue(monitor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

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
    void synchronousFatalErrorReleasesSlotAndPropagates() {
        AtomicBoolean inFlight = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        OutOfMemoryError fatal = new OutOfMemoryError("synthetic collection failure");
        assertSame(
                fatal,
                assertThrows(
                        OutOfMemoryError.class,
                        () ->
                                TaskExecutionService.reportCdcProgressAsync(
                                        inFlight,
                                        () -> {
                                            throw fatal;
                                        },
                                        failure::set)));
        assertFalse(inFlight.get());
        assertNull(failure.get());
        CompletableFuture<Object> next = new CompletableFuture<>();
        TaskExecutionService.reportCdcProgressAsync(inFlight, () -> next, failure::set);
        assertTrue(inFlight.get());
        next.complete(null);
        assertFalse(inFlight.get());
    }

    @Test
    void callbackRegistrationErrorReleasesSlotAndPropagates() {
        AtomicBoolean inFlight = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CompletionStage<?> stage = Mockito.mock(CompletionStage.class);
        LinkageError fatal = new LinkageError("synthetic callback registration failure");
        Mockito.doThrow(fatal).when(stage).whenComplete(Mockito.any());
        assertSame(
                fatal,
                assertThrows(
                        LinkageError.class,
                        () ->
                                TaskExecutionService.reportCdcProgressAsync(
                                        inFlight, () -> stage, failure::set)));
        assertFalse(inFlight.get());
        assertNull(failure.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void synchronousFailureHandlerCanStartNextReport(boolean collectionFails) {
        AtomicBoolean inFlight = new AtomicBoolean();
        CompletableFuture<Object> next = new CompletableFuture<>();
        AtomicInteger sent = new AtomicInteger();
        TaskExecutionService.reportCdcProgressAsync(
                inFlight,
                () -> {
                    IllegalStateException failure =
                            new IllegalStateException("invocation rejected");
                    if (collectionFails) {
                        throw failure;
                    }
                    CompletableFuture<Object> failed = new CompletableFuture<>();
                    failed.completeExceptionally(failure);
                    return failed;
                },
                error ->
                        TaskExecutionService.reportCdcProgressAsync(
                                inFlight,
                                () -> {
                                    sent.incrementAndGet();
                                    return next;
                                },
                                ignored -> {}));
        assertEquals(1, sent.get());
        assertTrue(inFlight.get());
        next.complete(null);
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
