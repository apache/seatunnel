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
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
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
 * End-to-end guard for a savepoint restore that is still waiting for capacity when the active
 * master fails over.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A restore is admitted through a
 * different pending path than a fresh submission: the coordinator creates the restored {@link
 * JobMaster} immediately, but with {@code PendingSourceState.RESTORE} and without deploying any
 * pipeline until the resource pre-apply succeeds. A master switch tears that instance down ({@code
 * CoordinatorService#clearCoordinatorService}) and has to rebuild it purely from distributed state
 * ({@code runningJobInfoIMap} + persisted job state) through {@code
 * restoreAllRunningJobFromMasterNodeSwitch}. Several silent failures are possible in that
 * combination:
 *
 * <ul>
 *   <li>the restored pending entry is dropped instead of re-enqueued, so the job stays {@code
 *       PENDING} for ever with no scheduler entry (a "lost restore");
 *   <li>the recreated {@link JobMaster} is dispatched without rehydrating the savepoint state, so
 *       the job restarts from scratch, reuses checkpoint ids and overwrites the savepoint;
 *   <li>the restore is admitted twice (once by the old generation, once by the new one), consuming
 *       the savepoint twice or leaving a zombie pipeline writing alongside the live one;
 *   <li>the pending restore holds slices while it waits, starving the very capacity it needs.
 * </ul>
 *
 * <p><b>Why existing coverage does not reach it.</b> {@code SavepointRestoreCapacityAdmissionIT}
 * covers restore admission under slot starvation without a master switch. {@code
 * SavepointRestoreMasterFailoverIT} covers a restore that is already {@code RUNNING} before the
 * failover. {@code SavepointInFlightMasterFailoverIT} covers a savepoint request in flight during
 * the switch. {@code SplitClusterPendingJobLifecycleFailoverIT} covers pending <em>fresh</em> jobs.
 * No existing E2E fails over the active master while a restore is still queued behind occupied
 * slots, which is exactly where the rebuilt-from-distributed-state path and the savepoint
 * rehydration path meet.
 *
 * <p><b>Contract asserted here.</b>
 *
 * <ol>
 *   <li>A savepoint restore that cannot fit into the fixed slot pool is {@code PENDING}, owns no
 *       slice, holds no in-flight checkpoint and keeps the occupying job's slices untouched.
 *   <li>When the active master fails over, the pending restore survives as a single pending entry
 *       on the new master: the client and coordinator still report {@code PENDING}, the pending
 *       queue still contains exactly that job id, and a stability window shows no slice ownership
 *       and no checkpoint progress.
 *   <li>When the occupying job is cancelled, the pending restore is dispatched exactly once onto
 *       the freed slices, keeps the source job id and reaches {@code RUNNING}. This step also
 *       guards <a href="https://github.com/apache/seatunnel/issues/13536">#13536</a>: cancelling
 *       the restored job while it is still {@code PENDING} must stop its already-deployed worker
 *       tasks and release their slices instead of only dropping the coordinator bookkeeping, which
 *       would leave the fixed pool permanently occupied.
 *   <li>The restored run resumes the savepoint checkpoint chain (first periodic checkpoint id above
 *       the savepoint id) and leaves the savepoint selectable, so the failover did not restart the
 *       job from scratch.
 *   <li>Cancelling the restored job returns the whole fixed pool to unassigned, so neither the
 *       pending restore nor the failover leaked admission or slot state.
 * </ol>
 *
 * <p>Kept as its own class rather than added to an existing IT: the scenario needs a split cluster
 * (two master candidates plus one worker) whose active master is replaced mid-test, while the other
 * relevant classes either run a single hybrid node or never fail over at all.
 */
@Slf4j
public class PendingRestoreMasterFailoverIT {

    private static final String CONF_FILE =
            "pending-restore-failover/stream_fake_to_inmemory_pending_restore.conf";

    private static final String JOB_NAME = "pending-restore-master-failover";

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
    public void testPendingSavepointRestoreSurvivesMasterFailoverAndRunsOnce() throws Exception {
        String testCaseName = "testPendingSavepointRestoreSurvivesMasterFailoverAndRunsOnce";
        String clusterName = "PendingRestoreMasterFailoverIT_" + testCaseName;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy sourceJob = null;
        ClientJobProxy occupyingJob = null;
        ClientJobProxy restoredJob = null;

        try {
            SeaTunnelConfig masterNode1Config = newSeaTunnelConfig(clusterName);
            SeaTunnelConfig masterNode2Config = newSeaTunnelConfig(clusterName);
            SeaTunnelConfig workerNodeConfig = newSeaTunnelConfig(clusterName);

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);
            Address workerAddress = workerNode.node.address;

            awaitClusterSize(workerNode, 3);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(clusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;
            CoordinatorService coordinatorService = coordinatorOf(activeMaster);
            ResourceManager resourceManager = coordinatorService.getResourceManager();
            CheckpointMonitorService checkpointMonitor =
                    serverOf(activeMaster).getCheckpointMonitorService();

            awaitWorkerAdvertisesWholePool(resourceManager, workerAddress);

            // ---- Phase 1: a checkpoint-enabled job owns the whole pool, then savepoints.
            sourceJob = submit(engineClient, masterNode1Config, "pending-restore-source");
            long jobId = sourceJob.getJobId();

            awaitStatus(coordinatorService, jobId, JobStatus.RUNNING);
            awaitOwnsSlices(resourceManager, workerAddress, jobId, SLOTS_PER_JOB);
            awaitCompletedCheckpoints(checkpointMonitor, jobId, 1L);

            long savepointId = takeSavepoint(engineClient, jobClient, jobId);
            awaitStatus(coordinatorService, jobId, JobStatus.SAVEPOINT_DONE);

            // A restore is only meaningful once the savepointed run has fully left the running map;
            // otherwise submitJob deliberately treats the restore as a duplicate submission.
            awaitJobLeftRunningMapAndReleasedSlices(
                    coordinatorService, resourceManager, workerAddress, jobId);

            // ---- Phase 2: a second job occupies the whole fixed pool again.
            occupyingJob = submit(engineClient, masterNode1Config, "pending-restore-occupier");
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
                                    masterNode1Config,
                                    jobId)
                            .execute();
            final ClientJobProxy pendingRestore = restoredJob;

            Assertions.assertEquals(
                    jobId,
                    pendingRestore.getJobId(),
                    "a savepoint restore must keep the source job id");

            awaitStatus(coordinatorService, jobId, JobStatus.PENDING);
            awaitStatus(pendingRestore, JobStatus.PENDING);
            awaitPendingQueueContains(activeMaster, jobId);

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

            // ---- Phase 4: fail over the active master while the restore is still pending.
            activeMaster.shutdown();
            awaitCoordinatorActive(standbyMaster, 60);
            awaitClusterSize(workerNode, 2);

            HazelcastInstanceImpl newActiveMaster = standbyMaster;
            CoordinatorService newCoordinatorService = coordinatorOf(newActiveMaster);
            ResourceManager newResourceManager = newCoordinatorService.getResourceManager();
            CheckpointMonitorService newCheckpointMonitor =
                    serverOf(newActiveMaster).getCheckpointMonitorService();

            // Re-acquire proxies from a fresh JobClient: the old proxies were bound to the dead
            // master and would otherwise obscure whether the state really came from the new one.
            JobClient reconnectedJobClient = engineClient.createJobClient();
            ClientJobProxy restoreAfterFailover = reconnectedJobClient.getJobProxy(jobId);
            ClientJobProxy occupierAfterFailover = reconnectedJobClient.getJobProxy(occupyingJobId);

            awaitStatus(newCoordinatorService, jobId, JobStatus.PENDING);
            awaitStatus(restoreAfterFailover, JobStatus.PENDING);
            awaitPendingQueueContains(newActiveMaster, jobId);

            // The rebuilt pending entry must be a real JobMaster, not just an IMap status: a
            // coordinator that lost the entry would leave the status PENDING with no owner.
            awaitRestoredPendingJobMaster(newActiveMaster, jobId);

            // Hold a stability window before freeing capacity: the new master must neither dispatch
            // the restore early nor drop it while the scheduler retries. A duplicate admission
            // would also show up as a second slice owner or a started checkpoint here.
            long completedAfterFailover = completedCheckpoints(newCheckpointMonitor, jobId);
            long inProgressAfterFailover = inProgressCheckpoints(newCheckpointMonitor, jobId);
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        newCoordinatorService.getJobStatus(jobId),
                                        "the restored pending job must survive the master switch");
                                Assertions.assertTrue(
                                        newCoordinatorService.getPendingJobQueue().contains(jobId),
                                        "the pending restore must stay in the new master's queue");
                                Assertions.assertTrue(
                                        slicesOwnedBy(newResourceManager, workerAddress, jobId)
                                                .isEmpty(),
                                        "a pending restore must own no slice after failover");
                                Assertions.assertEquals(
                                        SLOTS_PER_JOB,
                                        slicesOwnedBy(
                                                        newResourceManager,
                                                        workerAddress,
                                                        occupyingJobId)
                                                .size(),
                                        "the occupying job must keep its slices across failover");
                                Assertions.assertEquals(
                                        completedAfterFailover,
                                        completedCheckpoints(newCheckpointMonitor, jobId),
                                        "a pending restore must not complete checkpoints");
                                Assertions.assertEquals(
                                        inProgressAfterFailover,
                                        inProgressCheckpoints(newCheckpointMonitor, jobId),
                                        "a pending restore must not start checkpoints");
                            });

            // ---- Phase 5: free the pool; the pending restore must be dispatched onto it.
            occupierAfterFailover.cancelJob();
            awaitStatus(newCoordinatorService, occupyingJobId, JobStatus.CANCELED);
            awaitStatus(newCoordinatorService, jobId, JobStatus.RUNNING);
            awaitOwnsSlices(newResourceManager, workerAddress, jobId, SLOTS_PER_JOB);
            Assertions.assertTrue(
                    slicesOwnedBy(newResourceManager, workerAddress, occupyingJobId).isEmpty(),
                    "the cancelled occupying job must own no slice");

            // ---- Phase 6: the restored run must resume the savepoint checkpoint chain.
            JobPipelineCheckpointData firstPostRestore =
                    awaitCheckpoint(
                            reconnectedJobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            savepointId,
                            "periodic checkpoint after the restored pending job was dispatched");
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

            // The savepoint stays selectable: it is the only durable recovery point of this job. A
            // restore that restarted from scratch would have overwritten it with a low checkpoint
            // id.
            List<JobPipelineCheckpointData> savepoints =
                    reconnectedJobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
            Assertions.assertEquals(
                    1,
                    savepoints.size(),
                    "exactly one savepoint must remain selectable: " + savepoints);
            Assertions.assertEquals(
                    savepointId,
                    savepoints.get(0).getCheckpointId(),
                    "the restored run must not overwrite the savepoint id");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    savepoints.get(0).getCheckpointType(),
                    "the record selected for SAVEPOINT restore must still be a savepoint");

            // ---- Phase 7: the restored job ends and the fixed pool becomes fully free.
            restoredJob = restoreAfterFailover;
            restoredJob.cancelJob();
            restoredJob = null;
            awaitStatus(newCoordinatorService, jobId, JobStatus.CANCELED);

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                WorkerProfile profile =
                                        workerProfile(newResourceManager, workerAddress);
                                Assertions.assertEquals(
                                        0,
                                        profile.getAssignedSlots().length,
                                        "no slice may stay assigned after every job ended: "
                                                + slicesByJob(newResourceManager, workerAddress));
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
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
        }
    }

    private static SeaTunnelConfig newSeaTunnelConfig(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(clusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        // Fixed slices make slot ownership observable; WAIT is what turns "no capacity yet" into
        // PENDING instead of an immediate failure, which is the admission contract here.
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        return seaTunnelConfig;
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

    private static void awaitJobLeftRunningMapAndReleasedSlices(
            CoordinatorService coordinatorService,
            ResourceManager resourceManager,
            Address workerAddress,
            long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertNull(
                                    coordinatorService.getJobMaster(jobId),
                                    "the savepointed run must leave the running map");
                            Assertions.assertTrue(
                                    slicesOwnedBy(resourceManager, workerAddress, jobId).isEmpty(),
                                    "the savepointed run must release every fixed slice");
                            Assertions.assertEquals(
                                    SLOT_NUM,
                                    workerProfile(resourceManager, workerAddress)
                                            .getUnassignedSlots()
                                            .length);
                        });
    }

    private static void awaitWorkerAdvertisesWholePool(
            ResourceManager resourceManager, Address workerAddress) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            WorkerProfile profile = workerProfile(resourceManager, workerAddress);
                            Assertions.assertNotNull(profile, "worker should register");
                            Assertions.assertEquals(
                                    SLOT_NUM,
                                    profile.getUnassignedSlots().length,
                                    "worker should advertise all fixed slices");
                        });
    }

    private static void awaitRestoredPendingJobMaster(
            HazelcastInstanceImpl activeMaster, long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = coordinatorOf(activeMaster).getJobMaster(jobId);
                            Assertions.assertNotNull(
                                    jobMaster,
                                    "the new master must rebuild a JobMaster for the pending restore");
                            Assertions.assertEquals(
                                    JobStatus.PENDING,
                                    jobMaster.getJobStatus(),
                                    "the rebuilt JobMaster must still be pending");
                        });
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

    private static void awaitStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        jobProxy.getJobStatus(),
                                        "client status of job " + jobProxy.getJobId()));
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

    private static void awaitPendingQueueContains(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            CoordinatorService coordinatorService = coordinatorOf(masterNode);
                            Assertions.assertTrue(
                                    coordinatorService.isCoordinatorActive(),
                                    "coordinator should be active when asserting the pending queue");
                            Assertions.assertTrue(
                                    coordinatorService.getPendingJobQueue().contains(jobId),
                                    "the pending queue should contain job "
                                            + jobId
                                            + ", queue size "
                                            + coordinatorService.getPendingJobCount());
                        });
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitCoordinatorActive(
            HazelcastInstanceImpl masterNode, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(masterNode.getLifecycleService().isRunning());
                            Assertions.assertTrue(
                                    isCoordinatorActive(masterNode),
                                    "standby master should become active after failover");
                        });
    }

    private static HazelcastInstanceImpl waitAndFindActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        AtomicReference<HazelcastInstanceImpl> activeMaster = new AtomicReference<>();
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            if (isCoordinatorActive(masterNode1)) {
                                activeMaster.set(masterNode1);
                            } else if (isCoordinatorActive(masterNode2)) {
                                activeMaster.set(masterNode2);
                            }
                            Assertions.assertNotNull(
                                    activeMaster.get(),
                                    "should find an active master after coordinator initialization");
                        });
        return activeMaster.get();
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl node) {
        if (node == null || !node.getLifecycleService().isRunning()) {
            return false;
        }
        try {
            return coordinatorOf(node).isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }

    private static CoordinatorService coordinatorOf(HazelcastInstanceImpl node) {
        return serverOf(node).getCoordinatorService();
    }

    private static SeaTunnelServer serverOf(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
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
        WorkerProfile profile = workerProfile(resourceManager, workerAddress);
        if (profile == null) {
            return slices;
        }
        for (SlotProfile slot : profile.getAssignedSlots()) {
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
