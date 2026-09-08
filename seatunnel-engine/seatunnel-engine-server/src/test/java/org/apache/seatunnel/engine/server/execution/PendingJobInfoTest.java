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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.diagnostic.PendingDiagnosticsCollector;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PendingJobInfoTest {
    @Test
    void metadataAndCompletionQueriesDoNotInitializeWaitingJobs() {
        AtomicInteger calls = new AtomicInteger();
        JobImmutableInformation information = mock(JobImmutableInformation.class);
        when(information.getJobName()).thenReturn("waiting");
        JobMaster master = mock(JobMaster.class);
        CompletableFuture<JobResult> actualCompletion = new CompletableFuture<>();
        when(master.getJobMasterCompleteFuture())
                .thenReturn(new PassiveCompletableFuture<>(actualCompletion));
        PendingJobInfo pending =
                new PendingJobInfo(
                        1L,
                        information,
                        () -> {
                            calls.incrementAndGet();
                            return master;
                        });
        assertEquals(1L, pending.getJobId());
        assertSame(information, pending.getJobImmutableInformation());
        assertNull(pending.getInitializedJobMaster());
        PassiveCompletableFuture<JobResult> completion = pending.getCompletionFuture();
        assertFalse(completion.isDone());
        assertEquals(
                JobStatus.PENDING,
                PendingDiagnosticsCollector.collectJobDiagnostic(
                                pending, Collections.emptyMap(), null)
                        .getJobStatus());
        assertEquals(0, calls.get());

        assertSame(master, pending.getJobMaster());
        JobResult result = new JobResult(JobStatus.FINISHED, null);
        actualCompletion.complete(result);
        assertSame(result, completion.join());
        assertSame(master, pending.getJobMaster());
        assertEquals(1, calls.get());
    }

    @Test
    void concurrentSchedulerAndCancellationInitializeOnlyOnce() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        JobMaster master = mock(JobMaster.class);
        when(master.getJobMasterCompleteFuture()).thenReturn(new PassiveCompletableFuture<>());
        PendingJobInfo pending =
                new PendingJobInfo(
                        1L,
                        mock(JobImmutableInformation.class),
                        () -> {
                            calls.incrementAndGet();
                            started.countDown();
                            awaitLatch(release);
                            return master;
                        });
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<JobMaster> scheduler = executor.submit(pending::getJobMaster);
            assertTrue(started.await(10, TimeUnit.SECONDS));
            Future<JobMaster> cancellation = executor.submit(pending::getJobMaster);
            release.countDown();
            assertSame(master, scheduler.get(10, TimeUnit.SECONDS));
            assertSame(master, cancellation.get(10, TimeUnit.SECONDS));
            assertEquals(1, calls.get());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void steppingDownDoesNotInitializeBacklog() {
        PendingJobInfo pending =
                new PendingJobInfo(
                        1L,
                        mock(JobImmutableInformation.class),
                        () -> {
                            throw new AssertionError("Step-down must not initialize waiting jobs");
                        });
        PassiveCompletableFuture<JobResult> completion = pending.getCompletionFuture();
        pending.interrupt();
        assertTrue(completion.isCompletedExceptionally());
        assertThrows(CancellationException.class, pending::getJobMaster);
    }

    @Test
    void steppingDownDuringInitializationInterruptsTheNewMaster() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        JobMaster master = mock(JobMaster.class);
        PendingJobInfo pending =
                new PendingJobInfo(
                        1L,
                        mock(JobImmutableInformation.class),
                        () -> {
                            started.countDown();
                            awaitLatch(release);
                            return master;
                        });
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> initialization =
                    executor.submit(
                            () -> assertThrows(CancellationException.class, pending::getJobMaster));
            assertTrue(started.await(10, TimeUnit.SECONDS));
            pending.interrupt();
            release.countDown();
            initialization.get(10, TimeUnit.SECONDS);
            verify(master).interrupt();
            assertTrue(pending.getCompletionFuture().isCompletedExceptionally());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failedInitializationCompletesWaitersAndIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("broken connector");
        PendingJobInfo pending =
                new PendingJobInfo(
                        1L,
                        mock(JobImmutableInformation.class),
                        () -> {
                            calls.incrementAndGet();
                            throw failure;
                        });
        PassiveCompletableFuture<JobResult> completion = pending.getCompletionFuture();
        assertSame(failure, assertThrows(IllegalStateException.class, pending::getJobMaster));
        assertSame(failure, assertThrows(IllegalStateException.class, pending::getJobMaster));
        assertTrue(completion.isCompletedExceptionally());
        assertEquals(1, calls.get());
    }

    @Test
    void connectorLinkageFailureAlsoCompletesWaiters() {
        NoClassDefFoundError failure = new NoClassDefFoundError("missing connector dependency");
        PendingJobInfo pending =
                new PendingJobInfo(
                        1L,
                        mock(JobImmutableInformation.class),
                        () -> {
                            throw failure;
                        });
        PassiveCompletableFuture<JobResult> completion = pending.getCompletionFuture();
        assertSame(failure, assertThrows(NoClassDefFoundError.class, pending::getJobMaster));
        assertTrue(completion.isCompletedExceptionally());
        assertSame(failure, assertThrows(NoClassDefFoundError.class, pending::getJobMaster));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for test initialization");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
