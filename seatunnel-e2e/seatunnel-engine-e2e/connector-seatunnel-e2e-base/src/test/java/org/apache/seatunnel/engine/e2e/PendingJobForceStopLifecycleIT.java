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
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Lifecycle coverage for force-stopping a job that never left the pending queue.
 *
 * <p><b>The contract.</b> A job that cannot obtain its slots under {@code job.schedule.strategy =
 * WAIT} is a fully registered job: the coordinator holds a {@code JobMaster} for it and the client
 * holds a completion future. Force-stopping it (the {@code force} variant of the cancel RPC, which
 * is routed to {@code CoordinatorService#stopJob} and then {@code PhysicalPlan#stopJob}, not to a
 * savepoint) must therefore:
 *
 * <ol>
 *   <li>resolve the client completion future as {@link JobStatus#CANCELED} instead of leaving the
 *       client blocked on a job that will never run;
 *   <li>persist {@link JobStatus#CANCELED} so the status stays resolvable after the coordinator
 *       releases the job master (the historical symptom was {@code UNKNOWABLE} here);
 *   <li>drop the job from the pending queue and never dispatch it later, when the job occupying the
 *       fixed capacity releases its slots;
 *   <li>leave the fixed-slot pool fully reusable: the stopped job owns no slot, and a replacement
 *       job with the same footprint reaches {@link JobStatus#RUNNING}.
 * </ol>
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code PhysicalPlan#stopJob} has a
 * dedicated {@code status <= PENDING} branch that completes the job-end future directly, because
 * {@code JobMaster#run} - the code that normally drives the terminal-state transition, the job
 * history write and the coordinator-side unregistration - is never entered for a queued job. Any
 * change to terminal cleanup, pending-queue bookkeeping or the fixed-slot lifecycle can silently
 * break one of the four guarantees above while ordinary job submission still works: a queued job is
 * invisible to the resource dashboard, and a leaked registration only shows up later as a status of
 * {@code UNKNOWABLE} or as capacity that never comes back.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code
 * CoordinatorServiceWithCancelPendingJobTest} calls the coordinator directly with a hand-built
 * {@code JobMaster} and never goes through the client protocol, the RPC force flag, real slots or
 * the completion future. {@code SplitClusterPendingJobLifecycleFailoverIT} covers pending jobs that
 * wait for resources and a master handoff, but never force-stops the queued job itself. {@code
 * JobClientJobProxyIT} only cancels jobs that are already RUNNING. No existing E2E pins the "queued
 * job force-stop" terminal contract.
 */
public class PendingJobForceStopLifecycleIT {

    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming.conf";

    /**
     * {@code pending_jobs_streaming.conf} runs two source readers and two sink writers, so one job
     * needs four fixed slots. The holder consumes the whole pool and the victim stays PENDING.
     */
    private static final int FIXED_SLOT_NUM = 4;

    /**
     * Upper bound for the force-stop RPC itself; a blocked stop must fail the test, not hang it.
     */
    private static final long STOP_TIMEOUT_SECONDS = 60;

    /** Upper bound for the client completion future of the force-stopped job. */
    private static final long COMPLETE_TIMEOUT_SECONDS = 120;

    @Test
    public void testForceStopPendingJobKeepsTerminalStateAndReleasesCapacity() throws Exception {
        String testClusterName =
                "PendingJobForceStopLifecycleIT_"
                        + "testForceStopPendingJobKeepsTerminalStateAndReleasesCapacity";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy victimJob = null;
        ClientJobProxy replacementJob = null;
        ExecutorService clientCalls = null;

        try {
            SeaTunnelConfig masterConfig = getSeaTunnelConfig(testClusterName);
            SeaTunnelConfig workerConfig = getSeaTunnelConfig(testClusterName);
            configureFixedSlotWaitCluster(masterConfig);
            configureFixedSlotWaitCluster(workerConfig);

            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            clientCalls =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread = new Thread(runnable, "pending-job-force-stop");
                                thread.setDaemon(true);
                                return thread;
                            });

            holderJob =
                    submitJob(
                            engineClient,
                            masterConfig,
                            "force_stop_pending_holder",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);
            assertAssignedSlotCount(finalMasterNode, FIXED_SLOT_NUM);
            assertUnassignedSlotCount(finalMasterNode, 0);

            victimJob =
                    submitJob(
                            engineClient,
                            masterConfig,
                            "force_stop_pending_victim",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(victimJob, JobStatus.PENDING, 120);

            long victimJobId = victimJob.getJobId();
            Assertions.assertTrue(
                    getCoordinatorService(finalMasterNode)
                            .getPendingJobQueue()
                            .contains(victimJobId),
                    "A capacity-blocked job must be registered in the pending queue");
            Assertions.assertNotNull(
                    getCoordinatorService(finalMasterNode).getJobMaster(victimJobId),
                    "A pending job must own a job master before it is dispatched");
            // A queued job must not squat on capacity it cannot use; the fixed pool is what other
            // jobs compete for, so partial ownership would strand it.
            assertUnassignedSlotCount(finalMasterNode, 0);

            final ClientJobProxy victim = victimJob;
            Future<?> stopFuture = clientCalls.submit(() -> jobClient.cancelJob(victimJobId, true));
            stopFuture.get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            Future<JobResult> completeFuture =
                    clientCalls.submit(() -> victim.waitForJobCompleteV2());
            JobResult jobResult = completeFuture.get(COMPLETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    jobResult.getStatus(),
                    "Force-stopping a pending job must resolve its completion future as CANCELED");
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    victim.getJobStatus(),
                    "A force-stopped pending job must report CANCELED");

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        getCoordinatorService(finalMasterNode)
                                                .getPendingJobQueue()
                                                .contains(victimJobId),
                                        "A force-stopped pending job must leave the pending queue");
                                Assertions.assertNull(
                                        getCoordinatorService(finalMasterNode)
                                                .getJobMaster(victimJobId),
                                        "The coordinator must release a force-stopped pending job");
                            });

            // The status must survive the coordinator-side release: it has to come from the
            // persisted history now, which is what keeps it from degrading to UNKNOWABLE.
            Assertions.assertEquals(
                    JobStatus.CANCELED,
                    victim.getJobStatus(),
                    "The terminal state of a force-stopped pending job must be persisted");
            Assertions.assertTrue(
                    getSeaTunnelServer(finalMasterNode)
                            .getCheckpointService()
                            .getCheckpointStorage()
                            .getAllCheckpoints(String.valueOf(victimJobId))
                            .isEmpty(),
                    "Force-stopping a queued job must not trigger a final checkpoint");

            // Free the fixed pool and prove the stopped job is never dispatched afterwards, even
            // though the capacity it was waiting for becomes available.
            jobClient.cancelJob(holderJob.getJobId(), true);
            holderJob.waitForJobCompleteV2();
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertUnassignedSlotCount(finalMasterNode, FIXED_SLOT_NUM);
                                assertAssignedSlotCount(finalMasterNode, 0);
                            });

            Awaitility.await()
                    .during(5, TimeUnit.SECONDS)
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        victim.getJobStatus(),
                                        "A force-stopped pending job must not be revived by the "
                                                + "scheduler when capacity frees up");
                                Assertions.assertFalse(
                                        getCoordinatorService(finalMasterNode)
                                                .getPendingJobQueue()
                                                .contains(victimJobId),
                                        "A force-stopped pending job must not re-enter the queue");
                            });

            replacementJob =
                    submitJob(
                            engineClient,
                            masterConfig,
                            "force_stop_pending_replacement",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(replacementJob, JobStatus.RUNNING, 120);
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
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
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

    private static void configureFixedSlotWaitCluster(SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(FIXED_SLOT_NUM);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
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

    private static void assertAssignedSlotCount(HazelcastInstanceImpl masterNode, int expected) {
        ResourceManager resourceManager = getResourceManager(masterNode);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        resourceManager.getAssignedSlots(null).size(),
                                        "Unexpected number of assigned fixed slots"));
    }

    private static void assertUnassignedSlotCount(HazelcastInstanceImpl masterNode, int expected) {
        ResourceManager resourceManager = getResourceManager(masterNode);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        resourceManager.getUnassignedSlots(null).size(),
                                        "Unexpected number of free fixed slots"));
    }

    private static ResourceManager getResourceManager(HazelcastInstanceImpl masterNode) {
        return getCoordinatorService(masterNode).getResourceManager();
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl masterNode) {
        return getSeaTunnelServer(masterNode).getCoordinatorService();
    }

    private static SeaTunnelServer getSeaTunnelServer(HazelcastInstanceImpl masterNode) {
        return masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }
}
