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
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Lifecycle coverage for the scheduling side of {@code job-schedule-strategy = REJECT}: a job that
 * cannot obtain the fixed slots another job already holds must be failed on the spot, not parked.
 *
 * <p><b>The contract.</b> On a fixed-slot cluster the pool is a hard ceiling. When a second job
 * arrives while a running job owns the whole pool, the coordinator's scheduling loop evaluates the
 * queued job's resource pre-application once. Under {@code REJECT} that single failed attempt has
 * to:
 *
 * <ol>
 *   <li>resolve the client completion future as {@link JobStatus#FAILED} within seconds, carrying
 *       the {@code NoEnoughResourceException} that explains why, instead of leaving the client
 *       blocked on a job the scheduler will never run;
 *   <li>leave the rejected job as a terminal, persisted job: it must not stay behind as a {@link
 *       JobStatus#PENDING} entry in the pending queue and must keep reporting {@code FAILED} after
 *       the coordinator releases its job master (the historical symptom of a lost terminal state
 *       here is {@code UNKNOWABLE});
 *   <li>never deploy anything: the rejected job owns no slot, writes no final checkpoint, and must
 *       not disturb the running job that legitimately holds the pool;
 *   <li>never be revived later: when the running job is cancelled and the exact capacity the
 *       rejected job had been waiting for becomes free, the rejected job must stay {@code FAILED}
 *       (this is the difference from {@code WAIT}, which keeps the job queued and dispatches it
 *       when capacity appears);
 *   <li>leave the pending-job scheduler usable: a fresh replacement job with the same footprint
 *       must still reach {@link JobStatus#RUNNING} afterwards.
 * </ol>
 *
 * <p><b>Why this is a boundary with real regression risk.</b> The rejection path ({@code
 * CoordinatorService#pendingJobSchedule} -&gt; {@code CoordinatorService#completeFailJob}) runs on
 * the single pending-scheduler thread and blocks it on the rejected job's completion future, so a
 * regression there is not local to the rejected job: the whole cluster's job scheduling can stall,
 * and the job registry can be left with a queued entry or an unresolvable status. It is also the
 * only code path that ends a job before {@code JobMaster#run()} is ever entered, i.e. before task
 * deployment, checkpoint scheduling, slot ownership and the normal terminal cleanup of a running
 * job exist.
 *
 * <p><b>Why the existing coverage does not reach it.</b> No E2E in this module configures {@code
 * REJECT}; every scheduling E2E either uses the default strategy only for a single undersized
 * submission ({@code SeaTunnelSlotIT#testSlotNotEnough}, {@code
 * SeaTunnelSlotIT#testSlotReleasedAfterNotEnoughResourceFailure}) or explicitly switches to {@code
 * WAIT} ({@code SplitClusterPendingJobLifecycleFailoverIT}, {@code SplitClusterFaultToleranceIT}).
 * None of them rejects a job *because another running job holds the pool*, and none of them checks
 * the post-rejection scheduler liveness or the "capacity becomes free later" boundary.
 * Checkpoint-ID counter cleanup for jobs that end before scheduling is a separate concern and is
 * deliberately not asserted here.
 */
public class RejectStrategyCapacityBlockedJobIT {

    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming.conf";

    /**
     * One {@code pending_jobs_streaming.conf} job needs this many fixed slots, so a pool of the
     * same size can host exactly one such job and the second one is rejected.
     */
    private static final int FIXED_SLOT_NUM = 4;

    /** Upper bound for a job to reach {@code RUNNING}; a blocked start must fail the test. */
    private static final long START_TIMEOUT_SECONDS = 120;

    /** Upper bound for the rejected job's client completion future. */
    private static final long COMPLETE_TIMEOUT_SECONDS = 120;

    @Test
    public void testCapacityBlockedJobIsRejectedInsteadOfQueued() throws Exception {
        String testClusterName =
                "RejectStrategyCapacityBlockedJobIT"
                        + "_testCapacityBlockedJobIsRejectedInsteadOfQueued";
        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy rejectedJob = null;
        ClientJobProxy replacementJob = null;
        ExecutorService clientCalls = null;

        try {
            SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
            configureFixedSlotRejectCluster(seaTunnelConfig);
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            final HazelcastInstanceImpl finalNode = node;

            // Wait until the fixed pool is visible before submitting anything: the first job's own
            // pre-application must not race worker registration and be rejected for a reason
            // unrelated to this contract.
            assertUnassignedSlotCount(finalNode, FIXED_SLOT_NUM);

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            clientCalls =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "reject-strategy-client-call");
                                thread.setDaemon(true);
                                return thread;
                            });

            holderJob =
                    submitJob(
                            engineClient,
                            seaTunnelConfig,
                            "reject_strategy_capacity_holder",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, START_TIMEOUT_SECONDS);
            long holderJobId = holderJob.getJobId();
            assertUnassignedSlotCount(finalNode, 0);
            assertAssignedSlotCount(finalNode, FIXED_SLOT_NUM);
            assertJobOwnsSlots(finalNode, holderJobId, true);

            rejectedJob =
                    submitJob(
                            engineClient,
                            seaTunnelConfig,
                            "reject_strategy_capacity_rejected",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long rejectedJobId = rejectedJob.getJobId();

            // The submission RPC itself only registers the job; the scheduling decision is made on
            // the coordinator's pending-job thread and arrives through the completion future.
            final ClientJobProxy rejected = rejectedJob;
            Future<JobResult> completionFuture = clientCalls.submit(rejected::waitForJobCompleteV2);
            JobResult rejectedResult =
                    completionFuture.get(COMPLETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Assertions.assertEquals(
                    JobStatus.FAILED,
                    rejectedResult.getStatus(),
                    "A capacity-blocked job under REJECT must fail instead of waiting for resources");
            Assertions.assertNotNull(
                    rejectedResult.getError(),
                    "A rejected job must report why it could not be scheduled");
            Assertions.assertTrue(
                    rejectedResult.getError().contains("NoEnoughResourceException"),
                    "A rejected job must report the resource shortfall, but was: "
                            + rejectedResult.getError());

            // The rejection must be a complete terminal transition: no queue registration, no job
            // master left behind, and the persisted status must still resolve to FAILED.
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        rejected.getJobStatus(),
                                        "A rejected job must report FAILED, never PENDING");
                                Assertions.assertFalse(
                                        getCoordinatorService(finalNode)
                                                .getPendingJobQueue()
                                                .contains(rejectedJobId),
                                        "A rejected job must not stay in the pending queue");
                                Assertions.assertNull(
                                        getCoordinatorService(finalNode)
                                                .getJobMaster(rejectedJobId),
                                        "The coordinator must release a rejected job");
                            });

            // Nothing may have been deployed for the rejected job, and the running job that
            // legitimately owns the pool must be untouched.
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    holderJob.getJobStatus(),
                    "Rejecting a queued job must not disturb the job holding the pool");
            assertAssignedSlotCount(finalNode, FIXED_SLOT_NUM);
            assertUnassignedSlotCount(finalNode, 0);
            assertJobOwnsSlots(finalNode, holderJobId, true);
            assertJobOwnsSlots(finalNode, rejectedJobId, false);
            Assertions.assertTrue(
                    getCheckpointStorage(finalNode)
                            .getAllCheckpoints(String.valueOf(rejectedJobId))
                            .isEmpty(),
                    "A job that never ran must not leave checkpoint data behind");

            // Release exactly the capacity the rejected job had been waiting for. Under WAIT this
            // is the moment the queued job would be dispatched; under REJECT it must stay failed.
            holderJob.cancelJob();
            holderJob.waitForJobCompleteV2();
            assertUnassignedSlotCount(finalNode, FIXED_SLOT_NUM);
            assertAssignedSlotCount(finalNode, 0);

            Awaitility.await()
                    .during(5, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FAILED,
                                        rejected.getJobStatus(),
                                        "A rejected job must not be revived when capacity frees up");
                                Assertions.assertFalse(
                                        getCoordinatorService(finalNode)
                                                .getPendingJobQueue()
                                                .contains(rejectedJobId),
                                        "A rejected job must not re-enter the pending queue");
                                Assertions.assertNull(
                                        getCoordinatorService(finalNode)
                                                .getJobMaster(rejectedJobId),
                                        "A rejected job must not be re-dispatched later");
                            });
            assertJobOwnsSlots(finalNode, rejectedJobId, false);

            // The single pending-scheduler thread must have survived the rejection: a fresh job
            // with the same footprint still has to be dispatched and started.
            replacementJob =
                    submitJob(
                            engineClient,
                            seaTunnelConfig,
                            "reject_strategy_capacity_replacement",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(replacementJob, JobStatus.RUNNING, START_TIMEOUT_SECONDS);
            assertAssignedSlotCount(finalNode, FIXED_SLOT_NUM);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        } finally {
            if (clientCalls != null) {
                clientCalls.shutdownNow();
            }
            if (engineClient != null) {
                if (replacementJob != null) {
                    cancelQuietly(replacementJob);
                }
                if (holderJob != null) {
                    cancelQuietly(holderJob);
                }
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static void cancelQuietly(ClientJobProxy job) {
        try {
            job.cancelJob();
        } catch (Exception ignored) {
            // best-effort cleanup: the cluster is shut down right after this call
        }
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        return seaTunnelConfig;
    }

    /**
     * Fixed slots turn the pool into a hard ceiling, and {@code REJECT} makes a job that cannot be
     * satisfied fail immediately instead of being retried by the scheduler.
     */
    private static void configureFixedSlotRejectCluster(SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(FIXED_SLOT_NUM);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.REJECT);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String jobName,
            String jobConfigFile)
            throws ExecutionException, InterruptedException {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig)
                .execute();
    }

    private static void assertJobStatusWithTimeout(
            ClientJobProxy clientJobProxy, JobStatus expectedStatus, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus, clientJobProxy.getJobStatus()));
    }

    private static void assertAssignedSlotCount(HazelcastInstanceImpl node, int expected) {
        ResourceManager resourceManager = getResourceManager(node);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        resourceManager.getAssignedSlots(null).size(),
                                        "Unexpected number of assigned fixed slots"));
    }

    private static void assertUnassignedSlotCount(HazelcastInstanceImpl node, int expected) {
        ResourceManager resourceManager = getResourceManager(node);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        resourceManager.getUnassignedSlots(null).size(),
                                        "Unexpected number of free fixed slots"));
    }

    private static void assertJobOwnsSlots(
            HazelcastInstanceImpl node, long jobId, boolean expected) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        ownedSlotProfiles.keySet().stream()
                                                .anyMatch(location -> location.getJobId() == jobId),
                                        "Unexpected slot ownership for job " + jobId));
    }

    private static CheckpointStorage getCheckpointStorage(HazelcastInstanceImpl node) {
        return getSeaTunnelServer(node).getCheckpointService().getCheckpointStorage();
    }

    private static ResourceManager getResourceManager(HazelcastInstanceImpl node) {
        return getCoordinatorService(node).getResourceManager();
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        return getSeaTunnelServer(node).getCoordinatorService();
    }

    private static SeaTunnelServer getSeaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }
}
