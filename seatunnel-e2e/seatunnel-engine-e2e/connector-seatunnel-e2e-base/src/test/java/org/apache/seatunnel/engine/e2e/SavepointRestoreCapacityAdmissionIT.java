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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end guard for the capacity-admission boundary of a savepoint restore.
 *
 * <p>A Zeta restore is not a separate scheduling path: the client submits the restored job into the
 * same pending queue as a fresh submission, and the coordinator must pre-apply the job's whole
 * resource request before any pipeline is dispatched. That matters most when the cluster cannot fit
 * the restored job at all: the restore must wait (under {@code job-schedule-strategy = WAIT})
 * without holding slices, without running tasks and without advancing checkpoints, and it must then
 * be dispatched onto capacity that becomes free, resuming from the savepoint rather than from
 * scratch.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> The restore path has its own
 * checkpoint-state rehydration, checkpoint-id restoration and pipeline reset logic ({@code
 * PendingSourceState.RESTORE} in {@code CoordinatorService#pendingJobSchedule}). A bug here can (1)
 * dispatch the restored pipelines before the resource pre-apply succeeds, so the job owns a partial
 * slot set, (2) leave the job {@code PENDING} forever because the pending scheduler treats a
 * restore like a terminal job, or (3) restart the checkpoint sequence at id 1 and overwrite the
 * savepoint that the restore was selected from. None of those failures is visible in the existing
 * restore tests, which all restore into a cluster that already has enough free capacity.
 *
 * <p><b>Existing coverage.</b> {@code SavepointRestoreIT}, {@code
 * SavepointClusterRestartRestoreIT}, {@code MultiPipelineSavepointRestoreIT} and {@code
 * RecoveryLifecycleRestApiIT} all restore onto free capacity; {@code JobRestoreIT} only checks that
 * automatic restore re-applies resources inside a container. {@code
 * SplitClusterPendingJobLifecycleFailoverIT} covers pending <em>fresh</em> jobs, and the separate
 * pending-savepoint guard covers a savepoint request aimed at a pending job - neither restores a
 * savepoint into a starved pool. This test therefore pins the restore admission contract itself.
 *
 * <p><b>Contract asserted here.</b>
 *
 * <ol>
 *   <li>With the source job's three fixed slices fully occupied, a savepoint restore of that job
 *       must report {@code PENDING} to both the coordinator and the client, own no slice, keep the
 *       occupying job's slices untouched, and neither complete nor start any checkpoint.
 *   <li>When the occupying job is cancelled, the pending restore must be dispatched onto the freed
 *       slices within the scheduler retry budget, reach {@code RUNNING} and keep the source job id.
 *   <li>The restored run must continue the savepoint's checkpoint sequence: its first completed
 *       periodic checkpoint id must be greater than the savepoint id, so the savepoint is neither
 *       reused nor overwritten.
 *   <li>Cancelling the restored job must release its slices and return the whole fixed pool to
 *       unassigned, so the pending/restore path leaks no admission state.
 * </ol>
 */
@Slf4j
public class SavepointRestoreCapacityAdmissionIT {

    private static final String CONF_FILE = "restore-capacity/stream_fake_to_inmemory_longrun.conf";

    private static final String JOB_NAME = "savepoint-restore-capacity";

    /**
     * Fixed pool of the single worker. One job occupies the whole pool: the split-enumerator task
     * group, the fused reader/writer task group and the sink task group of the {@code InMemory}
     * sink's aggregated committer.
     */
    private static final int SLOT_NUM = 3;

    private static final int SLOTS_PER_JOB = 3;

    /** The fixture has exactly one source-to-sink pipeline, so its id is always 1. */
    private static final int PIPELINE_ID = 1;

    private static final long STATE_TIMEOUT_SECONDS = 180;

    @Test
    public void testSavepointRestoreWaitsForCapacityAndResumes() throws Exception {
        String testCaseName = "testSavepointRestoreWaitsForCapacityAndResumes";
        String clusterName = "SavepointRestoreCapacityAdmissionIT_" + testCaseName;

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy sourceJob = null;
        ClientJobProxy occupyingJob = null;
        ClientJobProxy restoredJob = null;

        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Fixed slices make slot ownership observable; WAIT is what turns "no capacity yet"
            // into PENDING instead of an immediate failure, which is the admission contract here.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
            seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            NodeEngineImpl nodeEngine = node.node.nodeEngine;
            Address workerAddress = node.node.address;
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = server.getCoordinatorService();
            ResourceManager resourceManager = coordinatorService.getResourceManager();
            CheckpointMonitorService checkpointMonitor = server.getCheckpointMonitorService();

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                WorkerProfile profile =
                                        workerProfile(resourceManager, workerAddress);
                                Assertions.assertNotNull(profile, "worker should register");
                                Assertions.assertEquals(
                                        SLOT_NUM,
                                        profile.getUnassignedSlots().length,
                                        "worker should advertise all fixed slices");
                            });

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            // ---- Phase 1: a checkpoint-enabled job owns the whole pool, then savepoints.
            sourceJob = submit(engineClient, seaTunnelConfig, "restore-capacity-source");
            long jobId = sourceJob.getJobId();

            awaitStatus(coordinatorService, jobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, jobId, SLOTS_PER_JOB);
            awaitCompletedCheckpoints(checkpointMonitor, jobId, 1L);

            long savepointId = takeSavepoint(engineClient, jobClient, jobId);
            awaitStatus(coordinatorService, jobId, JobStatus.SAVEPOINT_DONE);

            // A restore is only meaningful once the savepointed run has fully left the running map;
            // otherwise submitJob deliberately treats the restore as a duplicate submission.
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertNull(
                                        coordinatorService.getJobMaster(jobId),
                                        "the savepointed run must leave the running map");
                                Assertions.assertTrue(
                                        slicesOwnedBy(resourceManager, workerAddress, jobId)
                                                .isEmpty(),
                                        "the savepointed run must release every fixed slice");
                                Assertions.assertEquals(
                                        SLOT_NUM,
                                        workerProfile(resourceManager, workerAddress)
                                                .getUnassignedSlots()
                                                .length);
                            });

            // ---- Phase 2: a second job occupies the whole fixed pool again.
            occupyingJob = submit(engineClient, seaTunnelConfig, "restore-capacity-occupier");
            long occupyingJobId = occupyingJob.getJobId();

            awaitStatus(coordinatorService, occupyingJobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, occupyingJobId, SLOTS_PER_JOB);
            Assertions.assertEquals(
                    0,
                    workerProfile(resourceManager, workerAddress).getUnassignedSlots().length,
                    "the occupying job should fill the fixed pool");

            // ---- Phase 3: restore the savepoint while no capacity is left.
            JobConfig restoreJobConfig = new JobConfig();
            restoreJobConfig.setName(JOB_NAME);
            restoredJob =
                    engineClient
                            .restoreExecutionContext(
                                    TestUtils.getResource(CONF_FILE),
                                    restoreJobConfig,
                                    seaTunnelConfig,
                                    jobId)
                            .execute();
            final ClientJobProxy pendingRestore = restoredJob;

            Assertions.assertEquals(
                    jobId,
                    pendingRestore.getJobId(),
                    "a savepoint restore must keep the source job id");

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        coordinatorService.getJobStatus(jobId),
                                        "a restore that cannot fit must be admitted as pending");
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        pendingRestore.getJobStatus(),
                                        "the client must see the pending restore");
                            });

            long completedBefore = completedCheckpoints(checkpointMonitor, jobId);
            long inProgressBefore = inProgressCheckpoints(checkpointMonitor, jobId);

            // Three scheduler retries (3s apart) without progress prove the job really waits for
            // capacity instead of being dispatched partially and failing later.
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING, coordinatorService.getJobStatus(jobId));
                                Assertions.assertTrue(
                                        slicesOwnedBy(resourceManager, workerAddress, jobId)
                                                .isEmpty(),
                                        "a pending restore must not own any fixed slice");
                                Assertions.assertEquals(
                                        SLOTS_PER_JOB,
                                        slicesOwnedBy(
                                                        resourceManager,
                                                        workerAddress,
                                                        occupyingJobId)
                                                .size(),
                                        "the occupying job's slices must not be touched");
                                Assertions.assertEquals(
                                        completedBefore,
                                        completedCheckpoints(checkpointMonitor, jobId),
                                        "a pending restore must not complete checkpoints");
                                Assertions.assertEquals(
                                        inProgressBefore,
                                        inProgressCheckpoints(checkpointMonitor, jobId),
                                        "a pending restore must not start checkpoints");
                            });

            // ---- Phase 4: free the pool; the pending restore must be dispatched onto it.
            occupyingJob.cancelJob();
            occupyingJob = null;
            awaitStatus(coordinatorService, occupyingJobId, JobStatus.CANCELED);
            awaitStatus(coordinatorService, jobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, jobId, SLOTS_PER_JOB);
            Assertions.assertTrue(
                    slicesOwnedBy(resourceManager, workerAddress, occupyingJobId).isEmpty(),
                    "the cancelled occupying job must own no slice");

            // ---- Phase 5: the restored run must resume the savepoint checkpoint chain.
            JobPipelineCheckpointData firstPostRestore =
                    awaitCheckpoint(
                            jobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            savepointId,
                            "periodic checkpoint after the restore");
            Assertions.assertTrue(
                    firstPostRestore.getCheckpointId() > savepointId,
                    "the restored run must continue after the savepoint id, savepoint="
                            + savepointId
                            + ", first periodic="
                            + firstPostRestore.getCheckpointId());
            Assertions.assertEquals(
                    CheckpointType.CHECKPOINT_TYPE,
                    firstPostRestore.getCheckpointType(),
                    "the first checkpoint after the restore must be a periodic checkpoint");

            // The savepoint stays selectable: it is the only durable recovery point of this job.
            List<JobPipelineCheckpointData> savepoints =
                    jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
            Assertions.assertEquals(
                    1,
                    savepoints.size(),
                    "exactly one savepoint must remain selectable: " + savepoints);
            Assertions.assertEquals(
                    savepointId,
                    savepoints.get(0).getCheckpointId(),
                    "the restored run must not overwrite the savepoint id");

            // ---- Phase 6: the restored job ends and the fixed pool becomes fully free.
            restoredJob.cancelJob();
            restoredJob = null;
            awaitStatus(coordinatorService, jobId, JobStatus.CANCELED);

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                WorkerProfile profile =
                                        workerProfile(resourceManager, workerAddress);
                                Assertions.assertEquals(
                                        0,
                                        profile.getAssignedSlots().length,
                                        "no slice may stay assigned after every job ended: "
                                                + slicesByJob(resourceManager, workerAddress));
                                Assertions.assertEquals(
                                        SLOT_NUM,
                                        profile.getUnassignedSlots().length,
                                        "the whole fixed pool must be unassigned again");
                            });
        } finally {
            cancelQuietly(sourceJob);
            cancelQuietly(occupyingJob);
            cancelQuietly(restoredJob);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig, String jobName)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(
                        TestUtils.getResource(CONF_FILE), jobConfig, seaTunnelConfig)
                .execute();
    }

    /**
     * Takes a stop-with-savepoint and returns the persisted savepoint id of the single pipeline.
     * The client call blocks until the job reaches a terminal state, so it is bounded here to keep
     * a stuck {@code DOING_SAVEPOINT} from hanging the suite.
     */
    private static long takeSavepoint(SeaTunnelClient engineClient, JobClient jobClient, long jobId)
            throws Exception {
        CompletableFuture<Void> savepoint =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        savepoint.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        AtomicReference<JobPipelineCheckpointData> persisted = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
                            JobPipelineCheckpointData newest =
                                    checkpoints.stream()
                                            .filter(data -> data.getPipelineId() == PIPELINE_ID)
                                            .max(
                                                    Comparator.comparingLong(
                                                            JobPipelineCheckpointData
                                                                    ::getCheckpointId))
                                            .orElse(null);
                            Assertions.assertNotNull(
                                    newest,
                                    "the savepoint must be persisted before the restore: "
                                            + checkpoints);
                            Assertions.assertEquals(
                                    CheckpointType.SAVEPOINT_TYPE,
                                    newest.getCheckpointType(),
                                    "the persisted checkpoint must be a savepoint");
                            persisted.set(newest);
                        });
        return persisted.get().getCheckpointId();
    }

    private static JobPipelineCheckpointData awaitCheckpoint(
            JobClient jobClient,
            long jobId,
            RestoreMode restoreMode,
            long minExclusiveCheckpointId,
            String description) {
        AtomicReference<JobPipelineCheckpointData> latest = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, restoreMode);
                            JobPipelineCheckpointData newest =
                                    checkpoints.stream()
                                            .filter(data -> data.getPipelineId() == PIPELINE_ID)
                                            .max(
                                                    Comparator.comparingLong(
                                                            JobPipelineCheckpointData
                                                                    ::getCheckpointId))
                                            .orElse(null);
                            Assertions.assertNotNull(
                                    newest, "no " + restoreMode + " checkpoint for " + description);
                            Assertions.assertTrue(
                                    newest.getCheckpointId() > minExclusiveCheckpointId,
                                    "waiting for a "
                                            + description
                                            + " above id "
                                            + minExclusiveCheckpointId
                                            + " but the latest is "
                                            + newest.getCheckpointId());
                            latest.set(newest);
                        });
        return latest.get();
    }

    private static void awaitStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        coordinatorService.getJobStatus(jobId),
                                        "status of job " + jobId));
    }

    private static void awaitOwnsSlices(
            ResourceManager resourceManager, Address workerAddress, long jobId, int expected) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        slicesOwnedBy(resourceManager, workerAddress, jobId).size(),
                                        "slices owned by job "
                                                + jobId
                                                + ": "
                                                + slicesByJob(resourceManager, workerAddress)));
    }

    private static void awaitCompletedCheckpoints(
            CheckpointMonitorService checkpointMonitor, long jobId, long expectedAtLeast) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completedCheckpoints(checkpointMonitor, jobId)
                                                >= expectedAtLeast,
                                        "completed checkpoints of job "
                                                + jobId
                                                + ": "
                                                + completedCheckpoints(checkpointMonitor, jobId)));
    }

    private static long completedCheckpoints(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(pipeline -> pipeline.getCounts().getCompleted())
                                        .sum())
                .orElse(0L);
    }

    private static long inProgressCheckpoints(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(
                        overview ->
                                overview.getPipelines().values().stream()
                                        .mapToLong(
                                                pipeline ->
                                                        pipeline.getCounts().getInProgress()
                                                                + pipeline.getInProgress().size())
                                        .sum())
                .orElse(0L);
    }

    private static WorkerProfile workerProfile(
            ResourceManager resourceManager, Address workerAddress) {
        return resourceManager.getRegisterWorker().get(workerAddress);
    }

    private static Map<Long, Set<Integer>> slicesByJob(
            ResourceManager resourceManager, Address workerAddress) {
        Map<Long, Set<Integer>> slices = new TreeMap<>();
        for (SlotProfile slot : workerProfile(resourceManager, workerAddress).getAssignedSlots()) {
            slices.computeIfAbsent(slot.getOwnerJobID(), ignored -> new TreeSet<>())
                    .add(slot.getSlotID());
        }
        return slices;
    }

    private static Set<Integer> slicesOwnedBy(
            ResourceManager resourceManager, Address workerAddress, long jobId) {
        return slicesByJob(resourceManager, workerAddress)
                .getOrDefault(jobId, Collections.emptySet());
    }

    private static void cancelQuietly(ClientJobProxy jobProxy) {
        if (jobProxy == null) {
            return;
        }
        try {
            jobProxy.cancelJob();
        } catch (Throwable e) {
            log.info(
                    "Ignoring failure while cancelling job {} during cleanup",
                    jobProxy.getJobId(),
                    e);
        }
    }
}
