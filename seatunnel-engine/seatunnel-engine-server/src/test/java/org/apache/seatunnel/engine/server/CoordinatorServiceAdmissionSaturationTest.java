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

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** Exercises real pipeline/checkpoint callbacks with every admission worker held by a latch. */
class CoordinatorServiceAdmissionSaturationTest extends AbstractSeaTunnelServerTest {
    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig config = super.loadSeaTunnelConfig();
        config.getEngineConfig().getCoordinatorServiceConfig().setCoreThreadNum(1);
        config.getEngineConfig().getCoordinatorServiceConfig().setMaxThreadNum(1);
        return config;
    }

    @Test
    void testCheckpointSavepointAndTerminalHistoryWhileAdmissionIsSaturated() throws Exception {
        CoordinatorService coordinator = server.getCoordinatorService();
        long jobId = instance.getFlakeIdGenerator("saturated-savepoint").newId();
        submitStreamingJob(jobId);
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> coordinator.getJobStatus(jobId) == JobStatus.RUNNING);
        JobMaster job = coordinator.getJobMaster(jobId);
        CheckpointCoordinator checkpoint = job.getCheckpointManager().getCheckpointCoordinator(1);
        try (AdmissionBlock ignored = blockAdmission(coordinator)) {
            CompletedCheckpoint previous = latestCheckpoint(checkpoint);
            await().atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                CompletedCheckpoint latest = latestCheckpoint(checkpoint);
                                return latest != null && latest != previous;
                            });
            coordinator.savePoint(jobId).get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.SAVEPOINT_DONE,
                    job.getJobMasterCompleteFuture().get(10, TimeUnit.SECONDS).getStatus());
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> coordinator.getJobMaster(jobId) == null);
            // This also exercises the history lookup after removal from the running-master map.
            Assertions.assertEquals(
                    JobStatus.SAVEPOINT_DONE,
                    coordinator.waitForJobComplete(jobId).get(10, TimeUnit.SECONDS).getStatus());
        } finally {
            coordinator.cancelJob(jobId).get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void testCancelWhileAdmissionIsSaturated() throws Exception {
        CoordinatorService coordinator = server.getCoordinatorService();
        long jobId = instance.getFlakeIdGenerator("saturated-cancel").newId();
        submitStreamingJob(jobId);
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> coordinator.getJobStatus(jobId) == JobStatus.RUNNING);
        JobMaster job = coordinator.getJobMaster(jobId);
        try (AdmissionBlock ignored = blockAdmission(coordinator)) {
            coordinator.cancelJob(jobId).get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    job.getJobMasterCompleteFuture().get(60, TimeUnit.SECONDS).getStatus());
            await().atMost(10, TimeUnit.SECONDS)
                    .until(() -> coordinator.getJobMaster(jobId) == null);
        } finally {
            coordinator.cancelJob(jobId).get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void testRestoreAndCompletionAfterReactivationWithSaturatedAdmission() throws Exception {
        CoordinatorService coordinator = server.getCoordinatorService();
        // Drive master transitions explicitly so the listener cannot race the saturation barrier.
        ScheduledExecutorService listener =
                (ScheduledExecutorService)
                        ReflectionUtils.getField(coordinator, "masterActiveListener").get();
        listener.shutdownNow();
        Assertions.assertTrue(listener.awaitTermination(10, TimeUnit.SECONDS));
        long jobId = instance.getFlakeIdGenerator("saturated-restore").newId();
        submitStreamingJob(jobId);
        await().atMost(60, TimeUnit.SECONDS)
                .until(() -> coordinator.getJobStatus(jobId) == JobStatus.RUNNING);
        JobMaster previous = coordinator.getJobMaster(jobId);
        CheckpointCoordinator checkpoint =
                previous.getCheckpointManager().getCheckpointCoordinator(1);
        await().atMost(60, TimeUnit.SECONDS).until(() -> latestCheckpoint(checkpoint) != null);
        coordinator.clearCoordinatorService();
        Assertions.assertTrue(previous.getJobMasterCompleteFuture().isCompletedExceptionally());
        // Install the production admission factory's pool before activation starts restore fan-out.
        Method factory = CoordinatorService.class.getDeclaredMethod("createCoordinatorExecutor");
        factory.setAccessible(true);
        ThreadPoolExecutor rebuilt = (ThreadPoolExecutor) factory.invoke(coordinator);
        ReflectionUtils.setField(coordinator, "executorService", rebuilt);
        try (AdmissionBlock ignored = blockAdmission(coordinator)) {
            Method activate = CoordinatorService.class.getDeclaredMethod("checkNewActiveMaster");
            activate.setAccessible(true);
            activate.invoke(coordinator);
            await().atMost(90, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                JobMaster restored = coordinator.getJobMaster(jobId);
                                return restored != null
                                        && restored != previous
                                        && restored.getJobStatus() == JobStatus.RUNNING;
                            });
            JobMaster restored = coordinator.getJobMaster(jobId);
            Assertions.assertNotSame(previous.getExecutorService(), restored.getExecutorService());
            coordinator.stopJob(jobId).get(30, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    restored.getJobMasterCompleteFuture().get(60, TimeUnit.SECONDS).getStatus());
        } finally {
            coordinator.cancelJob(jobId).get(30, TimeUnit.SECONDS);
        }
    }

    private void submitStreamingJob(long jobId) throws Exception {
        LogicalDag dag =
                TestUtils.createTestLogicalPlan(
                        "stream_fake_to_console.conf", "saturated-admission-" + jobId, jobId);
        JobImmutableInformation information =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        nodeEngine.getSerializationService(),
                        dag,
                        Collections.emptyList(),
                        Collections.emptyList());
        server.getCoordinatorService()
                .submitJob(jobId, nodeEngine.getSerializationService().toData(information), false)
                .get(30, TimeUnit.SECONDS);
    }

    private CompletedCheckpoint latestCheckpoint(CheckpointCoordinator checkpoint) {
        try {
            Field field = CheckpointCoordinator.class.getDeclaredField("latestCompletedCheckpoint");
            field.setAccessible(true);
            return (CompletedCheckpoint) field.get(checkpoint);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Cannot inspect completed checkpoint", e);
        }
    }

    private AdmissionBlock blockAdmission(CoordinatorService coordinator) throws Exception {
        ExecutorService executor =
                (ExecutorService) ReflectionUtils.getField(coordinator, "executorService").get();
        return new AdmissionBlock(executor);
    }

    private static final class AdmissionBlock implements AutoCloseable {
        private final CountDownLatch release = new CountDownLatch(1);
        private final Future<?> blocker;

        private AdmissionBlock(ExecutorService executor) throws Exception {
            CountDownLatch started = new CountDownLatch(1);
            blocker =
                    executor.submit(
                            () -> {
                                started.countDown();
                                release.await();
                                return null;
                            });
            Assertions.assertTrue(started.await(10, TimeUnit.SECONDS));
        }

        @Override
        public void close() throws Exception {
            release.countDown();
            blocker.get(10, TimeUnit.SECONDS);
        }
    }
}
