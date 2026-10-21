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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Test;

import com.hazelcast.map.IMap;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SubPlanHaRecoveryTest {
    @Test
    void waitsForReplacementBeforeDeployingAndDoesNotConsumePipelineRetries() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.plan.updatePipelineState(PipelineStatus.FAILED);
            assertTrue(fixture.firstAttempt.await(5, TimeUnit.SECONDS));
            assertEquals(
                    0, fixture.deployments.get(), "Unavailable slots must not reuse old futures");
            assertEquals(1, fixture.plan.getPipelineRestoreNum());
            fixture.available.set(true);
            await().atMost(5, TimeUnit.SECONDS).until(() -> fixture.deployments.get() == 1);
            assertEquals(1, fixture.plan.getPipelineRestoreNum());
            fixture.plan.getReSchedulerPipelineFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void cancellationStopsWaitingWithoutBlockingThePipelineMonitor() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.plan.updatePipelineState(PipelineStatus.FAILED);
            assertTrue(fixture.firstAttempt.await(5, TimeUnit.SECONDS));
            fixture.plan.forceStopPipeline();
            fixture.plan.getReSchedulerPipelineFuture().get(5, TimeUnit.SECONDS);
            int attempts = fixture.attempts.get();
            fixture.available.set(true);
            assertEquals(attempts, fixture.attempts.get());
            assertEquals(0, fixture.deployments.get());
        }
    }

    @Test
    void masterShutdownStopsAllocationRetries() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.plan.updatePipelineState(PipelineStatus.FAILED);
            assertTrue(fixture.firstAttempt.await(5, TimeUnit.SECONDS));
            fixture.completion.completeExceptionally(new InterruptedException("master stopped"));
            fixture.plan.getReSchedulerPipelineFuture().get(5, TimeUnit.SECONDS);
            assertEquals(0, fixture.deployments.get());
        }
    }

    @Test
    void cancellationIsNotBlockedByAnInFlightAllocation() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.blockAllocation.set(true);
            fixture.available.set(true);
            fixture.plan.updatePipelineState(PipelineStatus.FAILED);
            assertTrue(fixture.firstAttempt.await(5, TimeUnit.SECONDS));
            assertTimeout(Duration.ofSeconds(1), fixture.plan::forceStopPipeline);
            fixture.releaseAllocation.countDown();
            fixture.plan.getReSchedulerPipelineFuture().get(5, TimeUnit.SECONDS);
            assertEquals(0, fixture.deployments.get());
        }
    }

    @Test
    void cancellationIsNotBlockedByTheInitialRetryDelay() throws Exception {
        try (Fixture fixture = new Fixture(true, 2)) {
            assertTimeout(
                    Duration.ofSeconds(1),
                    () -> fixture.plan.updatePipelineState(PipelineStatus.FAILED));
            assertTimeout(Duration.ofSeconds(1), fixture.plan::forceStopPipeline);
            fixture.plan.getReSchedulerPipelineFuture().get(5, TimeUnit.SECONDS);
            assertEquals(0, fixture.attempts.get());
            assertEquals(0, fixture.deployments.get());
        }
    }

    @Test
    void existingModeKeepsItsSynchronousRecoveryPath() throws Exception {
        try (Fixture fixture = new Fixture(false)) {
            fixture.plan.updatePipelineState(PipelineStatus.FAILED);
            assertEquals(1, fixture.deployments.get());
            assertEquals(1, fixture.attempts.get());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final AtomicBoolean available = new AtomicBoolean();
        private final AtomicBoolean restore = new AtomicBoolean(true);
        private final AtomicInteger deployments = new AtomicInteger();
        private final AtomicInteger attempts = new AtomicInteger();
        private final CountDownLatch firstAttempt = new CountDownLatch(1);
        private final CountDownLatch releaseAllocation = new CountDownLatch(1);
        private final AtomicBoolean blockAllocation = new AtomicBoolean();
        private final CompletableFuture<JobResult> completion = new CompletableFuture<>();
        private final SubPlan plan;

        private Fixture(boolean ha) {
            this(ha, 0);
        }

        @SuppressWarnings("unchecked")
        private Fixture(boolean ha, int retryInterval) {
            JobConfig config = new JobConfig();
            config.getEnvOptions().put(EnvCommonOptions.JOB_RETRY_TIMES.key(), 3);
            config.getEnvOptions()
                    .put(EnvCommonOptions.JOB_RETRY_INTERVAL_SECONDS.key(), retryInterval);
            JobImmutableInformation info = mock(JobImmutableInformation.class);
            when(info.getJobConfig()).thenReturn(config);
            when(info.getJobId()).thenReturn(1L);
            Map<Object, Object> values = new HashMap<>();
            values.put(new PipelineLocation(1L, 1), PipelineStatus.FAILING);
            IMap<Object, Object> states = mock(IMap.class);
            when(states.get(any())).thenAnswer(call -> values.get(call.getArgument(0)));
            doAnswer(call -> values.put(call.getArgument(0), call.getArgument(1)))
                    .when(states)
                    .set(any(), any());
            IMap<Object, Long[]> timestamps = mock(IMap.class);
            when(timestamps.get(any())).thenReturn(new Long[PipelineStatus.values().length]);
            JobMaster master = mock(JobMaster.class);
            EngineConfig engine = new EngineConfig();
            engine.setApplicationHaEnabled(ha);
            when(master.getEngineConfig()).thenReturn(engine);
            when(master.isNeedRestore()).thenAnswer(call -> restore.get());
            when(master.getJobMasterCompleteFuture())
                    .thenReturn(new PassiveCompletableFuture<>(completion));
            when(master.getPhysicalPlan()).thenReturn(mock(PhysicalPlan.class));
            when(master.getCheckpointManager()).thenReturn(mock(CheckpointManager.class));
            doAnswer(
                            call -> {
                                restore.set(false);
                                return null;
                            })
                    .when(master)
                    .neverNeedRestore();
            when(master.preApplyResources(any(SubPlan.class)))
                    .thenAnswer(
                            call -> {
                                attempts.incrementAndGet();
                                firstAttempt.countDown();
                                return available.get();
                            });
            when(master.preApplyResources(any(SubPlan.class), any(BooleanSupplier.class)))
                    .thenAnswer(
                            call -> {
                                attempts.incrementAndGet();
                                firstAttempt.countDown();
                                if (blockAllocation.get()) {
                                    assertTrue(releaseAllocation.await(5, TimeUnit.SECONDS));
                                }
                                return available.get()
                                        && ((BooleanSupplier) call.getArgument(1)).getAsBoolean();
                            });
            plan =
                    new SubPlan(
                            1,
                            1,
                            0L,
                            Collections.emptyList(),
                            Collections.emptyList(),
                            info,
                            executor,
                            states,
                            timestamps,
                            Collections.emptyMap()) {
                        @Override
                        public void restorePipeline() {
                            deployments.incrementAndGet();
                        }
                    };
            plan.setJobMaster(master);
            plan.isRunning = true;
        }

        @Override
        public void close() throws InterruptedException {
            restore.set(false);
            releaseAllocation.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }
}
