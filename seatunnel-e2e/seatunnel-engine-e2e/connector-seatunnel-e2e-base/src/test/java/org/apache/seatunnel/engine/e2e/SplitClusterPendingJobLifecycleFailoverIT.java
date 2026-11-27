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
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Multi-node failover coverage for the lifecycle of jobs that cannot be dispatched immediately.
 *
 * <p>Every scenario here builds a real split cluster (separate master and worker Hazelcast
 * instances) and holds a {@code ScheduleStrategy.WAIT} job back with occupied worker slots, then
 * drives the coordinator's pending-job bookkeeping through a master handoff, a cancellation, or a
 * savepoint request. That exercises the coordinator against real membership changes and real
 * resource contention, rather than against mocked coordinator state as the single-JVM unit tests
 * do.
 *
 * <p>Scenarios covered:
 *
 * <ul>
 *   <li>{@code testPendingJobLifecycleInMasterFailover} - a pending job survives a single master
 *       handoff and is dispatched once the cluster grows enough capacity for it.
 *   <li>{@code testPendingJobScheduledAfterRunningJobCanceled} - a pending job is dispatched after
 *       the running job occupying its slots is canceled.
 *   <li>{@code testSavepointOnPendingJobIsRejectedWithoutTouchingLifecycle} - a savepoint request
 *       for a queued job is rejected at the coordinator boundary without moving the job out of
 *       PENDING or recording a DOING_SAVEPOINT transition.
 *   <li>{@code testPendingJobNotDuplicatedAcrossRepeatedMasterFailover} - repeated master handoffs
 *       never revive a stale schedule generation, so the job is dispatched exactly once.
 *   <li>{@code testTerminalJobCleanupSkipsWorkerWaitAfterMasterSwitch} - cleanup of a terminal job
 *       is not blocked behind the worker-wait gate that live jobs go through after a master switch.
 * </ul>
 */
@Slf4j
public class SplitClusterPendingJobLifecycleFailoverIT {
    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming_lifecycle.conf";
    private static final String UNSCHEDULABLE_JOB_CONFIG_FILE =
            "pending_jobs_streaming_unschedulable.conf";

    /**
     * Minimal STREAMING job that needs exactly two fixed slots (one split-enumerator task plus one
     * fused reader/writer task, no committer) and checkpoints every 5s, so a savepoint can be taken
     * for it. Used to fill a two-slot worker exactly.
     */
    private static final String TWO_SLOT_STREAMING_JOB_CONFIG_FILE =
            "stream_fakesource_to_console_minimal.conf";

    /**
     * Number of rapid kill-and-replace rounds used by {@link
     * #testMasterElectionLoopRecoversFromRapidFailoverChurn()}. Each round is a real Hazelcast
     * membership delta (one join, one leave) fired back-to-back without waiting for the cluster to
     * settle first, so more rounds means more chances for {@code checkNewActiveMaster()}'s 100ms
     * poll to observe still-settling cluster state.
     */
    private static final int MASTER_ELECTION_CHURN_ROUNDS = 6;

    @Test
    public void testPendingJobLifecycleInMasterFailover() {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_testPendingJobLifecycleInMasterFailover";
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy pendingJob = null;

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNode1Config);
        configurePendingLifecycleTest(masterNode2Config);
        configurePendingLifecycleTest(workerNode1Config);
        configurePendingLifecycleTest(workerNode2Config);
        // Both jobs enter the restore queue after master failover. Provide enough capacity for
        // their resource pre-allocation regardless of the restore queue order.
        workerNode2Config.getEngineConfig().getSlotServiceConfig().setSlotNum(8);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            HazelcastInstanceImpl finalMasterNode = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            holderJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "pending_job_lifecycle_holder",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            assertPendingQueueState(activeMaster, null, 0);

            pendingJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "pending_job_lifecycle_pending",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            final ClientJobProxy finalPendingJob = pendingJob;
            final long pendingJobId = finalPendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);
            assertPendingQueueState(activeMaster, pendingJobId, 1);
            activeMaster.shutdown();
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        standbyMaster.getLifecycleService().isRunning());
                                Assertions.assertEquals(
                                        2, standbyMaster.getCluster().getMembers().size());
                            });
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            isCoordinatorActive(standbyMaster),
                                            "Standby master should become active after failover"));
            ClientJobProxy pendingJobAfterFailover =
                    engineClient.createJobClient().getJobProxy(pendingJobId);
            assertPendingQueueContainsJob(standbyMaster, pendingJobId, 1);

            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertPendingQueueContainsJob(standbyMaster, pendingJobId, 1);
                                Assertions.assertEquals(
                                        JobStatus.PENDING, pendingJobAfterFailover.getJobStatus());
                            });

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, standbyMaster.getCluster().getMembers().size()));
            assertJobStatusWithTimeout(pendingJobAfterFailover, JobStatus.RUNNING, 180);
            assertPendingQueueNotContainsJob(standbyMaster, pendingJobId);
            assertRunningJobGraphWithTimeout(standbyMaster, pendingJobId, 120);

            pendingJobAfterFailover.cancelJob();
            assertEventuallyCanceled(pendingJobAfterFailover);
            engineClient.createJobClient().getJobProxy(holderJob.getJobId()).cancelJob();
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
            if (workerNode1 != null) {
                workerNode1.shutdown();
            }
            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    @Test
    public void testPendingJobScheduledAfterRunningJobCanceled() {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_testPendingJobScheduledAfterRunningJobCanceled";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy pendingJob = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNodeConfig);
        configurePendingLifecycleTest(workerNodeConfig);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            holderJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "pending_job_lifecycle_holder_cancel",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode, null);
            assertPendingQueueState(activeMaster, null, 0);

            pendingJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "pending_job_lifecycle_pending_after_cancel",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long pendingJobId = pendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);
            assertPendingQueueState(activeMaster, pendingJobId, 1);

            holderJob.cancelJob();
            assertEventuallyCanceled(holderJob);

            assertJobStatusWithTimeout(pendingJob, JobStatus.RUNNING, 180);
            assertPendingQueueNotContainsJob(activeMaster, pendingJobId);

            pendingJob.cancelJob();
            assertEventuallyCanceled(pendingJob);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    /**
     * Verifies the current (as of this writing) head-of-line blocking behavior of {@code
     * CoordinatorService#pendingJobSchedule} under {@link ScheduleStrategy#WAIT}: a job whose
     * resource request can never be satisfied is retried at the queue head forever, and every job
     * submitted after it -- however trivially schedulable -- is starved for as long as the stuck
     * head job remains queued.
     *
     * <p>{@code pendingJobSchedule} is driven by a single long-lived scheduler thread per master
     * epoch (see {@code CoordinatorService#startPendingJobScheduleThread}), so under normal
     * single-master operation (no failover, i.e. no epoch transition) there is never more than one
     * thread calling {@code reservePendingJobInfo}/{@code releasePendingJobInfo} at a time. Because
     * {@code releasePendingJobInfo} always runs before the scheduler loop peeks again, {@code
     * schedulingPendingJobIds} is empty at the start of every peek, so {@link
     * org.apache.seatunnel.engine.server.utils.PeekBlockingQueue#peekBlocking(java.util.function.Predicate)}
     * always matches the FIFO head. Under {@code WAIT}, a failed resource pre-check does not
     * dequeue the head job (unlike {@code REJECT}, which fails and removes it); it just sleeps 3
     * seconds and retries the same head job next iteration, uncapped -- {@code
     * PendingJobInfo#checkTimes} is tracked only for diagnostics, never compared against a limit. A
     * later, easily-schedulable job is never even peeked while the head remains queued.
     *
     * <p>This is the same area of code touched by <a
     * href="https://github.com/apache/seatunnel/pull/11653">#11653</a> ("[Fix][Zeta] Avoid
     * duplicate pending job scheduling after failover"), which introduced {@code
     * reservePendingJobInfo}/{@code releasePendingJobInfo} and the predicate-based peek precisely
     * so a second scheduler generation started after a master flip can skip a head job already
     * reserved by a stale, still-in-flight generation, avoiding duplicate dispatch. That predicate
     * only ever excludes a job id present in {@code schedulingPendingJobIds}, which is populated
     * solely to mark "another concurrent scheduler thread is mid-evaluation of this job id" -- it
     * is not, and was never intended to be, a "this job already failed its resource check" marker.
     * {@code clearCoordinatorService} also unconditionally drains the entire queue and bumps the
     * epoch on every step-down, so two scheduler generations can only ever coexist transiently
     * around a real master activation/deactivation. Outside that narrow failover window -- which is
     * exactly the steady-state single-master scenario this test drives -- the mechanism never
     * triggers, so it does not change the outcome verified here: head-of-line blocking under {@code
     * WAIT} is still fully reproducible on current {@code dev}.
     *
     * <p>The test proves job B was schedulable the entire time -- not merely slow -- by cancelling
     * the stuck head job (job A) and observing job B reach {@code RUNNING} promptly immediately
     * afterward, with no other change to cluster resources.
     */
    @Test
    public void testPermanentlyStuckWaitJobBlocksSchedulableJobBehindIt() {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_"
                        + "testPermanentlyStuckWaitJobBlocksSchedulableJobBehindIt";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy neverSchedulableJob = null;
        ClientJobProxy schedulableJob = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNodeConfig);
        configurePendingLifecycleTest(workerNodeConfig);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode, null);
            assertPendingQueueState(activeMaster, null, 0);

            // Job A requests 999-way parallelism against a 4-slot, dynamicSlot=false cluster (see
            // configurePendingLifecycleTest): a shortfall no worker count this suite ever
            // provisions can close, so its resource pre-check can never succeed, not even
            // transiently.
            neverSchedulableJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "hol_blocking_never_schedulable_job",
                            TestUtils.getResource(UNSCHEDULABLE_JOB_CONFIG_FILE));
            long neverSchedulableJobId = neverSchedulableJob.getJobId();
            assertJobStatusWithTimeout(neverSchedulableJob, JobStatus.PENDING, 60);
            assertPendingQueueState(activeMaster, neverSchedulableJobId, 1);

            // Job B requests parallelism 2, comfortably inside the 4 free slots: job A never holds
            // any slot (JobMaster#preApplyResources releases every partial grant back on failure),
            // so the cluster has full capacity available for job B the moment it is submitted.
            schedulableJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "hol_blocking_schedulable_job",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long schedulableJobId = schedulableJob.getJobId();
            assertJobStatusWithTimeout(schedulableJob, JobStatus.PENDING, 60);
            assertPendingQueueContainsJob(activeMaster, schedulableJobId, 2);

            // Current architecture: the scheduler only ever evaluates the queue head, so job B
            // stays behind the permanently-stuck job A for as long as A remains queued. Hold this
            // over a stability window, not a single check, before concluding it is truly stuck
            // rather than merely slow.
            final ClientJobProxy finalSchedulableJob = schedulableJob;
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        finalSchedulableJob.getJobStatus(),
                                        "Job B must still be head-of-line blocked by the "
                                                + "permanently stuck job A");
                                assertPendingQueueContainsJob(activeMaster, schedulableJobId, 2);
                            });

            // Remove the stuck head job. If job B was truly schedulable all along -- and not
            // itself resource-starved -- it must now reach RUNNING quickly with no other change to
            // cluster resources.
            neverSchedulableJob.cancelJob();
            assertEventuallyCanceled(neverSchedulableJob);
            assertJobStatusWithTimeout(schedulableJob, JobStatus.RUNNING, 60);
            assertPendingQueueNotContainsJob(activeMaster, schedulableJobId);

            schedulableJob.cancelJob();
            assertEventuallyCanceled(schedulableJob);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    /**
     * Regression test for the duplicate pending-job dispatch bug fixed by <a
     * href="https://github.com/apache/seatunnel/pull/11653">#11653</a> ("[Fix][Zeta] Avoid
     * duplicate pending job scheduling after failover"). That fix introduced a monotonic scheduling
     * epoch so a scheduler thread from a stale master generation cannot dispatch a pending job that
     * a newer generation has already claimed, and made {@code clearCoordinatorService}
     * unconditionally drop interrupted pending jobs from the local queue so a later flap-back
     * cannot re-dispatch a poisoned {@code JobMaster}. The fix already ships with unit-level
     * coverage of the epoch/lock mechanism in isolation ({@code CoordinatorServiceTest}); this test
     * proves the same invariant holds through the real multi-node integration path: a real client
     * submission, real Hazelcast membership changes, and real resource contention.
     *
     * <p>{@code ScheduleStrategy.WAIT} (set by {@link #configurePendingLifecycleTest}) sleeps a
     * fixed 3 seconds between resource re-checks in {@code CoordinatorService#pendingJobSchedule},
     * which gives this test a wide, deterministic window to land repeated master failovers while
     * the pending job is actively being re-evaluated, instead of chasing a microsecond-scale race.
     *
     * <p>The contested job is a small bounded batch job rather than the streaming holder template,
     * so it reaches FINISHED with an exactly countable output: if it were ever dispatched twice as
     * two independent {@code JobMaster} instances, the sink would end up with double the expected
     * rows instead of exactly {@code testRowNumber * testParallelism}.
     */
    @Test
    public void testPendingJobNotDuplicatedAcrossRepeatedMasterFailover() throws Exception {
        String testCaseName = "pendingJobNotDuplicatedAcrossRepeatedMasterFailover";
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_"
                        + "testPendingJobNotDuplicatedAcrossRepeatedMasterFailover";
        long testRowNumber = 20;
        int testParallelism = 1;
        int flapRounds = 4;

        HazelcastInstanceImpl workerNode = null;
        HazelcastInstanceImpl extraWorkerNode = null;
        SeaTunnelClient engineClient = null;
        List<HazelcastInstanceImpl> masterNodes = new ArrayList<>();

        try {
            SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
            SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
            SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
            configurePendingLifecycleTest(masterNode1Config);
            configurePendingLifecycleTest(masterNode2Config);
            configurePendingLifecycleTest(workerNodeConfig);

            masterNodes.add(
                    SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config));
            masterNodes.add(
                    SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config));
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalWorkerNode = workerNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorkerNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            ClientJobProxy holderJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "pending_job_duplicate_dispatch_holder",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);

            ImmutablePair<String, String> contestedJobResources =
                    createBatchTestResources(testCaseName, testRowNumber, testParallelism);
            ClientJobProxy pendingJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "pending_job_duplicate_dispatch_contested",
                            contestedJobResources.getRight());
            long pendingJobId = pendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);

            HazelcastInstanceImpl currentActive =
                    waitAndFindActiveMaster(masterNodes.get(0), masterNodes.get(1));
            HazelcastInstanceImpl currentStandby =
                    currentActive == masterNodes.get(0) ? masterNodes.get(1) : masterNodes.get(0);

            for (int round = 0; round < flapRounds; round++) {
                currentActive.shutdown();
                HazelcastInstanceImpl newActive = currentStandby;
                awaitCoordinatorActive(newActive, 30);

                SeaTunnelConfig replacementConfig = getSeaTunnelConfig(testClusterName);
                configurePendingLifecycleTest(replacementConfig);
                HazelcastInstanceImpl replacement =
                        SeaTunnelServerStarter.createMasterHazelcastInstance(replacementConfig);
                masterNodes.add(replacement);

                HazelcastInstanceImpl finalNewActive = newActive;
                Awaitility.await()
                        .atMost(30, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                3,
                                                finalNewActive.getCluster().getMembers().size()));

                currentActive = newActive;
                currentStandby = replacement;
            }

            // The job must have survived every epoch transition above as a single, still-pending
            // entry before resources are freed, ruling out both the "silently lost" and the
            // "already running under a stale generation" pre-fix failure modes.
            ClientJobProxy pendingJobAfterFlapping =
                    engineClient.createJobClient().getJobProxy(pendingJobId);
            assertJobStatusWithTimeout(pendingJobAfterFlapping, JobStatus.PENDING, 60);

            // Hand off one final time while every worker slot is still occupied by the holder, so
            // a coordinator that never saw the original submission has to rebuild the pending entry
            // purely from distributed state. It must still be a single pending copy afterwards; a
            // stale generation revived here would show up as a second dispatch below.
            currentActive.shutdown();
            awaitCoordinatorActive(currentStandby, 30);
            assertJobStatusWithTimeout(pendingJobAfterFlapping, JobStatus.PENDING, 60);

            // Free capacity by growing the cluster rather than by cancelling the holder. Cancelling
            // the holder and immediately shutting its master down would additionally require the
            // terminal job's worker slots to be reclaimed by the next coordinator, which is a
            // separate resource-lifecycle concern from the duplicate-dispatch invariant under test.
            // Adding a worker keeps the assertions below attributable to scheduling alone, and
            // matches how testPendingJobLifecycleInMasterFailover releases a pending job.
            SeaTunnelConfig extraWorkerConfig = getSeaTunnelConfig(testClusterName);
            configurePendingLifecycleTest(extraWorkerConfig);
            extraWorkerNode =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(extraWorkerConfig);

            HazelcastInstanceImpl finalCoordinator = currentStandby;
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalCoordinator.getCluster().getMembers().size()));

            assertJobStatusWithTimeout(pendingJobAfterFlapping, JobStatus.FINISHED, 180);

            Long finalLineCount =
                    FileUtils.getFileLineNumberFromDir(contestedJobResources.getLeft());
            Assertions.assertEquals(
                    testRowNumber * testParallelism,
                    finalLineCount,
                    "Contested job output must equal exactly one dispatch's worth of rows; a "
                            + "duplicate dispatch across the master flaps above would double it");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (extraWorkerNode != null) {
                extraWorkerNode.shutdown();
            }
            for (HazelcastInstanceImpl masterNode : masterNodes) {
                if (masterNode.getLifecycleService().isRunning()) {
                    masterNode.shutdown();
                }
            }
        }
    }

    /**
     * Regression test for the terminal-zombie-job restore gate fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10692">#10692</a> ("[Fix][Zeta] Prevent
     * terminal-state zombie jobs from being restored after master switch"). Before that fix, {@code
     * CoordinatorService#restoreAllRunningJobFromMasterNodeSwitch} funneled every entry found in
     * {@code runningJobInfoIMap} -- including jobs that had already reached a terminal status such
     * as FINISHED -- through the same {@code while (getResourceManager().workerCount(...) == 0)}
     * wait loop that live jobs legitimately need, so a terminal job's IMap tombstone cleanup could
     * be starved indefinitely by the absence of a worker it never needed in the first place. The
     * fix added a pre-filter that resolves terminal-state entries immediately, before the
     * worker-wait loop is ever reached.
     *
     * <p>This test constructs the precondition the fix targets -- a master switch where the new
     * active master has zero registered workers, with both a terminal job and a live job present in
     * {@code runningJobInfoIMap} at that moment -- purely by controlling node start/stop order, so
     * the outcome does not depend on winning any timing race:
     *
     * <ul>
     *   <li>A small batch job is run to completion on a temporary worker, which is only shut down
     *       after that job's tasks have already reached a terminal execution state. Tearing a
     *       worker down while it still hosts a DEPLOYING/RUNNING/CANCELING task would route through
     *       the synchronous Hazelcast membership listener {@code
     *       CoordinatorService#failedTaskOnMemberRemoved}, fail that task immediately, and (with
     *       the default {@code job.retry.times}/{@code job.retry.interval.seconds} of 3/3) drive
     *       the job to terminal FAILED roughly 9 seconds later -- turning the intended "live" job
     *       terminal before the switch and collapsing this test's mixed-state precondition.
     *       Shutting the worker down only after the batch job already finished avoids that confound
     *       entirely: no task is left DEPLOYING/RUNNING/CANCELING on it to fail.
     *   <li>A second job is then submitted with zero workers registered anywhere in the cluster.
     *       {@code ScheduleStrategy#WAIT} resource pre-checks (see {@code
     *       JobMaster#preApplyResources}) fail gracefully rather than erroring out, so this job
     *       simply lands in PENDING -- a genuinely non-end-state job that legitimately still needs
     *       a worker, without ever requiring a worker to be torn out from under a dispatched task.
     *   <li>The standby master has held a live IMap backup since before either job was submitted,
     *       so both jobs' {@code runningJobInfoIMap}/{@code runningJobStateIMap} entries are
     *       already replicated there once the active master is shut down.
     * </ul>
     *
     * <p>{@code stateCleanupDelayMillis} is set far beyond this test's lifetime so the terminal
     * job's IMap tombstone cannot be swept by its own delayed-cleanup timer mid-test; the
     * pre-filter path under test, not the timer, must be what the assertions below observe.
     */
    @Test
    public void testTerminalJobCleanupSkipsWorkerWaitAfterMasterSwitch() throws Exception {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_"
                        + "testTerminalJobCleanupSkipsWorkerWaitAfterMasterSwitch";
        String testCaseName = "terminalJobCleanupSkipsWorkerWaitAfterMasterSwitch";

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNode1Config);
        configurePendingLifecycleTest(masterNode2Config);
        configurePendingLifecycleTest(workerNode1Config);
        configurePendingLifecycleTest(workerNode2Config);
        // Keep the terminal job's IMap tombstone alive far longer than this test can possibly
        // run, so the pre-filter under test -- not the unrelated delayed-cleanup timer -- is what
        // the assertions below observe.
        long stateCleanupDelayMillis = TimeUnit.MINUTES.toMillis(10);
        masterNode1Config.getEngineConfig().setStateCleanupDelayMillis(stateCleanupDelayMillis);
        masterNode2Config.getEngineConfig().setStateCleanupDelayMillis(stateCleanupDelayMillis);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            HazelcastInstanceImpl finalMasterNode1 = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);
            HazelcastInstanceImpl finalWorkerNode1 = workerNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorkerNode1.getCluster().getMembers().size()));

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

            // Run a small batch job to completion so it reaches a terminal status (FINISHED) the
            // normal way, leaving a real pendingJobCleanupIMap tombstone behind -- the realistic
            // shape of a "zombie" job, rather than one hand-injected directly into the IMaps.
            ImmutablePair<String, String> terminalJobResources =
                    createTerminalJobResources(testCaseName, 5L, 1);
            ClientJobProxy terminalJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "terminal_zombie_job",
                            terminalJobResources.getRight());
            long terminalJobId = terminalJob.getJobId();
            assertJobStatusWithTimeout(terminalJob, JobStatus.FINISHED, 60);

            // The batch job's tasks already completed, so its slot was already released; removing
            // its worker now cannot fail any task, since none is DEPLOYING/RUNNING/CANCELING on
            // it anymore. See the class-level javadoc above for why this ordering matters.
            workerNode1.shutdown();
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, standbyMaster.getCluster().getMembers().size()));

            // With zero workers registered anywhere in the cluster, this job can only land in
            // PENDING (see JobMaster#preApplyResources): a genuinely live, non-end-state job that
            // legitimately still needs a worker before it can make progress.
            ClientJobProxy liveJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "live_pending_job",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long liveJobId = liveJob.getJobId();
            assertJobStatusWithTimeout(liveJob, JobStatus.PENDING, 60);

            // Trigger the master switch. Both jobs' IMap entries are already replicated on
            // standbyMaster, and the cluster has already been confirmed to have zero workers, so
            // restoreAllRunningJobFromMasterNodeSwitch begins with exactly the precondition this
            // test needs -- no race to win.
            activeMaster.shutdown();
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        standbyMaster.getLifecycleService().isRunning());
                                Assertions.assertTrue(
                                        isCoordinatorActive(standbyMaster),
                                        "Standby master should become active after failover");
                            });

            SeaTunnelServer standbyServer =
                    standbyMaster.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService standbyCoordinatorService = standbyServer.getCoordinatorService();

            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            0,
                                            standbyCoordinatorService
                                                    .getResourceManager()
                                                    .workerCount(Collections.emptyMap())));

            // While the new active master still has zero registered workers: the terminal job
            // must already be resolved (not queued behind the worker-wait loop), while the live
            // job must still be blocked waiting for a worker. Holding both for a stability window
            // proves they are on genuinely different code paths, not merely "both happened to
            // finish quickly."
            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        standbyCoordinatorService.getJobStatus(terminalJobId),
                                        "Terminal job must not be resurrected while restore is "
                                                + "still waiting for a worker for the live job");
                                Assertions.assertFalse(
                                        standbyCoordinatorService
                                                .getPendingJobQueue()
                                                .contains(liveJobId),
                                        "Live job must still be blocked behind the worker-wait "
                                                + "loop while zero workers are registered");
                            });

            // A worker finally registers: the live job's restore, previously gated behind the
            // worker-wait loop, now proceeds to completion; the already-resolved terminal job is
            // unaffected by it.
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, standbyMaster.getCluster().getMembers().size()));

            ClientJobProxy liveJobAfterFailover =
                    engineClient.createJobClient().getJobProxy(liveJobId);
            assertJobStatusWithTimeout(liveJobAfterFailover, JobStatus.RUNNING, 120);
            Assertions.assertEquals(
                    JobStatus.FINISHED, standbyCoordinatorService.getJobStatus(terminalJobId));

            liveJobAfterFailover.cancelJob();
            assertEventuallyCanceled(liveJobAfterFailover);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode1 != null && workerNode1.getLifecycleService().isRunning()) {
                workerNode1.shutdown();
            }
            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
            if (masterNode1 != null && masterNode1.getLifecycleService().isRunning()) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null && masterNode2.getLifecycleService().isRunning()) {
                masterNode2.shutdown();
            }
        }
    }

    /**
     * Regression test for the master-election poll loop permanently dying, fixed by <a
     * href="https://github.com/apache/seatunnel/commit/d635407ac3dc509b5b25f2c3e3738df2faa27f94">
     * d635407ac3d</a> ("[Fix] [Zeta] CoordinatorService initialization retry on failure (#10580)").
     * Before that fix, {@code CoordinatorService#checkNewActiveMaster()} (scheduled every 100ms via
     * {@code masterActiveListener.scheduleAtFixedRate}) rethrew any exception it caught during
     * master election/init. {@code ScheduledThreadPoolExecutor} semantics mean an uncaught
     * exception escaping a periodic task silently and permanently cancels every future execution of
     * that task, with no watchdog to re-arm it, so the only recovery was a full node restart. The
     * fix replaced the rethrow with a caught-and-logged retry: {@code catch (Exception e)} now
     * clears local coordinator state and lets the next 100ms tick try again.
     *
     * <p>This test proves that retry invariant holds under real, unmocked cluster churn rather than
     * a synthetically thrown exception. It repeatedly kills the active master and starts its
     * replacement from an independent thread at (almost) the same instant, without waiting for the
     * previous round to settle first, so several real Hazelcast membership deltas land back-to-back
     * while {@code checkNewActiveMaster()} is mid-poll on the surviving node. This mirrors the fix
     * author's own description of the original failure ("e.g. Hazelcast RegistrationOperation
     * timeout") -- a transient exception surfacing from {@code initCoordinatorService()}'s IMap and
     * service setup while the cluster has not fully settled -- without asserting a specific
     * exception type, since the exact transient fault Hazelcast surfaces under real timing pressure
     * cannot be dictated from outside the process. A log listener on every master-eligible node
     * records whether {@code checkNewActiveMaster()}'s retry-path log line actually fired during
     * the run, purely as diagnostic evidence (logged, never asserted on) that a given run exercised
     * the exact catch block under regression test, since a black-box E2E test has no reliable way
     * to force that deterministically.
     *
     * <p>The hard, always-enforced assertion is the invariant the fix guarantees regardless of
     * whether the retry-path log fires on a given run: after the churn, exactly one master-eligible
     * node converges on an active, genuinely functional coordinator (proven by actually running a
     * job through it, not just an internal flag check) within a bounded time and with no external
     * restart of any node. The pre-fix code could get permanently stuck the moment any qualifying
     * exception occurred anywhere in the loop, requiring a full node restart to recover.
     *
     * <p><b>Known open gap not covered by this test:</b> as of this test's authoring, {@code
     * checkNewActiveMaster()} still catches {@code Exception}, not {@code Throwable}. A {@code
     * Throwable} that is not an {@code Exception} (e.g. {@code NoClassDefFoundError} from a broken
     * plugin jar, or {@code OutOfMemoryError}) would still permanently kill this scheduled task
     * today, for the exact same {@code ScheduledThreadPoolExecutor} reason described above. That
     * gap cannot be closed by a black-box test: constructing a real, non-mocked {@code Error} on
     * demand inside this exact method without modifying production code is not achievable from
     * outside the process. See this test's originating PR description for the follow-up
     * recommendation.
     */
    @Test
    public void testMasterElectionLoopRecoversFromRapidFailoverChurn() throws Exception {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_"
                        + "testMasterElectionLoopRecoversFromRapidFailoverChurn";

        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        List<HazelcastInstanceImpl> liveMasterNodes = new ArrayList<>();
        AtomicBoolean sawRetryPathLog = new AtomicBoolean(false);

        try {
            SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
            SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
            SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
            configurePendingLifecycleTest(masterNode1Config);
            configurePendingLifecycleTest(masterNode2Config);
            configurePendingLifecycleTest(workerNodeConfig);

            HazelcastInstanceImpl masterNode1 =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            HazelcastInstanceImpl masterNode2 =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            liveMasterNodes.add(masterNode1);
            liveMasterNodes.add(masterNode2);
            installRetryPathLogListener(masterNode1, sawRetryPathLog);
            installRetryPathLogListener(masterNode2, sawRetryPathLog);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalWorkerNode = workerNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorkerNode.getCluster().getMembers().size()));
            HazelcastInstanceImpl initialActive = waitAndFindActiveMaster(masterNode1, masterNode2);

            HazelcastInstanceImpl currentActive = initialActive;
            HazelcastInstanceImpl currentStandby =
                    initialActive == masterNode1 ? masterNode2 : masterNode1;

            for (int round = 0; round < MASTER_ELECTION_CHURN_ROUNDS; round++) {
                SeaTunnelConfig replacementConfig = getSeaTunnelConfig(testClusterName);
                configurePendingLifecycleTest(replacementConfig);

                // Start the replacement master on an independent thread and kill the current
                // active master from the main thread without waiting for either step to settle
                // first, so the join and the departure land as two overlapping, real Hazelcast
                // membership deltas instead of two cleanly separated ones. This is what gives
                // checkNewActiveMaster's 100ms poll a plausible chance to observe cluster state
                // that has not finished settling.
                CompletableFuture<HazelcastInstanceImpl> replacementFuture =
                        CompletableFuture.supplyAsync(
                                () ->
                                        SeaTunnelServerStarter.createMasterHazelcastInstance(
                                                replacementConfig));
                currentActive.shutdown();
                liveMasterNodes.remove(currentActive);

                HazelcastInstanceImpl replacement = replacementFuture.get(60, TimeUnit.SECONDS);
                liveMasterNodes.add(replacement);
                installRetryPathLogListener(replacement, sawRetryPathLog);

                currentActive = currentStandby;
                currentStandby = replacement;
            }

            HazelcastInstanceImpl finalActiveCandidate = currentActive;
            HazelcastInstanceImpl finalStandbyCandidate = currentStandby;
            final HazelcastInstanceImpl[] stableActiveRef = new HazelcastInstanceImpl[1];
            // The churn loop above deliberately never waits for convergence between rounds, so
            // give the cluster a generous bounded ceiling here to digest the backlog of six
            // stacked membership deltas. This is the assertion that would fail forever (not just
            // slowly) against the pre-fix code once any round happened to hit a qualifying
            // exception: with no retry, the node that should have taken over would never try
            // again on its own.
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                stableActiveRef[0] =
                                        findActiveMaster(
                                                finalActiveCandidate, finalStandbyCandidate);
                                Assertions.assertNotNull(
                                        stableActiveRef[0],
                                        "Cluster must converge on exactly one active coordinator "
                                                + "after rapid failover churn, with no external "
                                                + "restart of any node");
                            });
            HazelcastInstanceImpl stableActive = stableActiveRef[0];
            HazelcastInstanceImpl stableStandby =
                    stableActive == finalActiveCandidate
                            ? finalStandbyCandidate
                            : finalActiveCandidate;

            // Secondary sanity check: the churn must not leave two nodes each believing they are
            // the active master.
            Awaitility.await()
                    .during(3, TimeUnit.SECONDS)
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertFalse(isCoordinatorActive(stableStandby)));

            // Prove the coordinator that emerged is genuinely functional, not just internally
            // flagged active: submit and run a real job through it. This is the concrete,
            // user-visible recovery the fix restores; the pre-fix failure mode required a full
            // node restart to reach this state again.
            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobProxy recoveryProbeJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "master_election_recovery_probe",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(recoveryProbeJob, JobStatus.RUNNING, 120);
            recoveryProbeJob.cancelJob();
            assertEventuallyCanceled(recoveryProbeJob);

            log.info(
                    "checkNewActiveMaster retry-path log observed during churn: {}",
                    sawRetryPathLog.get());
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            for (HazelcastInstanceImpl masterNode : liveMasterNodes) {
                if (masterNode.getLifecycleService().isRunning()) {
                    masterNode.shutdown();
                }
            }
        }
    }

    /**
     * Rejects a savepoint request aimed at a job that has been submitted but never scheduled,
     * without letting the request touch the job's lifecycle.
     *
     * <p><b>Why this is a boundary with real regression risk.</b> A pending job already owns a
     * {@link JobMaster} with a live {@link PhysicalPlan}, but the master only publishes it in
     * {@code pendingJobQueue}; it does not enter {@code runningJobMasterMap} until the scheduler
     * has pre-applied every slot. {@code CoordinatorService#savePoint(long)} deliberately gates on
     * that scheduled flag, while {@code cancelJob}/{@code stopJob} use {@code getJobMaster()},
     * which resolves the queue too. If a savepoint request were ever routed through the queue the
     * way cancellation is, {@code PhysicalPlan#savepointJob()} would move the unscheduled job from
     * PENDING to {@code DOING_SAVEPOINT} even though no pipeline has a deployed task. Every
     * checkpoint coordinator would then reject the savepoint with {@code
     * TASK_NOT_ALL_READY_WHEN_SAVEPOINT}, and the failure path would restore the job to RUNNING
     * (not PENDING) while it is still queued, desynchronizing the job state machine from the
     * pending queue. Before <a href="https://github.com/apache/seatunnel/pull/11489">#11489</a> the
     * same sequence left the job stuck in {@code DOING_SAVEPOINT} forever. This test pins the
     * rejection at the coordinator boundary so that a future refactor cannot silently reintroduce
     * either failure mode.
     *
     * <p><b>Why the existing coverage does not reach it.</b> {@code
     * SavepointPreconditionRecoveryIT} fires a real client savepoint while a running job's tasks
     * are still deploying, and asserts that the job recovers to RUNNING; the job is already in
     * {@code runningJobMasterMap} there. The pending-job tests in this class cover dispatch,
     * cancellation and master failover, but never a savepoint request. No test proves that an
     * unscheduled job is rejected before its physical plan leaves PENDING.
     *
     * <p><b>What this test asserts.</b> With the only worker's four fixed slots held by a running
     * job, a savepoint request for the queued job fails fast with the "not running" rejection; the
     * physical plan stays PENDING, no {@code DOING_SAVEPOINT} timestamp is recorded, the job stays
     * in the pending queue and owns no slot. A second request behaves identically over a stability
     * window. Releasing the holder's capacity then proves the rejection left no latent savepoint
     * state: the same job still reaches RUNNING and acquires its slots, and cancelling it releases
     * them again.
     */
    @Test
    public void testSavepointOnPendingJobIsRejectedWithoutTouchingLifecycle()
            throws InterruptedException {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_"
                        + "testSavepointOnPendingJobIsRejectedWithoutTouchingLifecycle";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy pendingJob = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNodeConfig);
        configurePendingLifecycleTest(workerNodeConfig);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode, null);
            assertPendingQueueState(activeMaster, null, 0);

            holderJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "savepoint_pending_holder",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);
            assertPendingQueueState(activeMaster, null, 0);

            pendingJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "savepoint_pending_victim",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long pendingJobId = pendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);
            assertPendingQueueContainsJob(activeMaster, pendingJobId, 1);

            JobMaster pendingJobMaster = getJobMaster(activeMaster, pendingJobId);
            SeaTunnelClient finalEngineClient = engineClient;
            ClientJobProxy finalPendingJob = pendingJob;
            JobMaster finalPendingJobMaster = pendingJobMaster;
            Assertions.assertNotNull(
                    pendingJobMaster,
                    "A submitted pending job must already have a job master in the pending queue");
            Assertions.assertEquals(
                    JobStatus.PENDING, pendingJobMaster.getPhysicalPlan().getJobStatus());
            assertSlotOwnership(activeMaster, pendingJobId, false);

            // The client request must be refused by the coordinator's scheduled-job gate, not
            // accepted and then repaired after the physical plan has already left PENDING.
            long requestStartNanos = System.nanoTime();
            Throwable rejection =
                    rejectedSavepointWithinBudget(finalEngineClient, pendingJobId, 120);
            long requestMillis =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - requestStartNanos);
            String rejectionTrace =
                    ExceptionUtils.getMessage(ExceptionUtils.getRootException(rejection));
            Assertions.assertTrue(
                    rejectionTrace.contains("not running")
                            && rejectionTrace.contains("save point failed"),
                    "Expected the coordinator's scheduled-job rejection, but got: "
                            + rejectionTrace);
            Assertions.assertTrue(
                    requestMillis < 30_000,
                    "Rejecting a savepoint for an unscheduled job must not wait for deployment:"
                            + " took "
                            + requestMillis
                            + " ms");

            // The rejected request must not have moved the physical plan into DOING_SAVEPOINT,
            // even transiently: the state-transition timestamp is the durable evidence that the
            // job state machine was touched at all.
            Assertions.assertEquals(
                    JobStatus.PENDING, pendingJobMaster.getPhysicalPlan().getJobStatus());
            Assertions.assertNull(
                    pendingJobMaster.getStateTimestamp(JobStatus.DOING_SAVEPOINT),
                    "A rejected savepoint must not record a DOING_SAVEPOINT transition");
            assertPendingQueueContainsJob(activeMaster, pendingJobId, 1);
            assertSlotOwnership(activeMaster, pendingJobId, false);

            // A repeated request (clients and UIs retry) must behave the same and must not leave a
            // deferred savepoint behind that fires as soon as the job is scheduled.
            Throwable repeatedRejection =
                    rejectedSavepointWithinBudget(finalEngineClient, pendingJobId, 120);
            Assertions.assertTrue(
                    ExceptionUtils.getMessage(ExceptionUtils.getRootException(repeatedRejection))
                            .contains("not running"));

            Awaitility.await()
                    .during(5, TimeUnit.SECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        finalPendingJob.getJobStatus(),
                                        "A rejected savepoint must leave the job queued as"
                                                + " PENDING");
                                Assertions.assertEquals(
                                        JobStatus.PENDING,
                                        finalPendingJobMaster.getPhysicalPlan().getJobStatus(),
                                        "The physical plan must stay PENDING, never DOING_SAVEPOINT");
                                assertPendingQueueContainsJob(activeMaster, pendingJobId, 1);
                            });

            // Free the slots. If the rejection had left a savepoint request or a RUNNING state
            // behind, the job would not go through the normal pending dispatch path here.
            cancelAndAwaitTerminalWithinBudget(holderJob, 120);
            assertJobStatusWithTimeout(pendingJob, JobStatus.RUNNING, 120);
            assertPendingQueueNotContainsJob(activeMaster, pendingJobId);
            assertSlotOwnership(activeMaster, pendingJobId, true);

            cancelAndAwaitTerminalWithinBudget(pendingJob, 120);
            assertSlotOwnership(activeMaster, pendingJobId, false);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    /**
     * Calls a savepoint request that must be rejected and returns the rejection cause, with a
     * test-owned deadline covering the remote join inside {@code JobClient#savePointJob}.
     *
     * @param engineClient client used to request the savepoint
     * @param jobId job the request targets
     * @param timeoutSeconds deadline for the remote rejection
     * @return the rejection thrown by the engine
     * @throws InterruptedException when the bounded wait is interrupted
     */
    private static Throwable rejectedSavepointWithinBudget(
            SeaTunnelClient engineClient, long jobId, long timeoutSeconds)
            throws InterruptedException {
        ExecutorService savepointExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "pending-job-savepoint-probe");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> request = savepointExecutor.submit(() -> engineClient.savePointJob(jobId));
            try {
                request.get(timeoutSeconds, TimeUnit.SECONDS);
            } catch (ExecutionException expected) {
                return expected.getCause() == null ? expected : expected.getCause();
            } catch (TimeoutException timeout) {
                request.cancel(true);
                throw new AssertionError(
                        "The savepoint request for unscheduled job "
                                + jobId
                                + " was not rejected within "
                                + timeoutSeconds
                                + " s",
                        timeout);
            }
            throw new AssertionError(
                    "A savepoint request must be rejected while job " + jobId + " is still queued");
        } finally {
            savepointExecutor.shutdownNow();
        }
    }

    /**
     * Cancels a job and waits for its terminal result with test-owned deadlines.
     *
     * @param clientJobProxy job to cancel
     * @param timeoutSeconds deadline for each remote step
     * @throws InterruptedException when a bounded wait is interrupted
     */
    private static void cancelAndAwaitTerminalWithinBudget(
            ClientJobProxy clientJobProxy, long timeoutSeconds) throws InterruptedException {
        ExecutorService cancelExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "pending-job-terminal-cancel");
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            Future<?> cancel = cancelExecutor.submit(clientJobProxy::cancelJob);
            try {
                cancel.get(timeoutSeconds, TimeUnit.SECONDS);
            } catch (ExecutionException cancelFailure) {
                throw new AssertionError(
                        "Cancelling job " + clientJobProxy.getJobId() + " failed", cancelFailure);
            } catch (TimeoutException timeout) {
                cancel.cancel(true);
                throw new AssertionError(
                        "The cancel request for job "
                                + clientJobProxy.getJobId()
                                + " did not acknowledge within "
                                + timeoutSeconds
                                + " s",
                        timeout);
            }
            Future<JobResult> terminal =
                    cancelExecutor.submit(clientJobProxy::waitForJobCompleteV2);
            JobResult jobResult;
            try {
                jobResult = terminal.get(timeoutSeconds, TimeUnit.SECONDS);
            } catch (ExecutionException terminalFailure) {
                throw new AssertionError(
                        "Waiting for job " + clientJobProxy.getJobId() + " to complete failed",
                        terminalFailure);
            } catch (TimeoutException timeout) {
                terminal.cancel(true);
                throw new AssertionError(
                        "Job "
                                + clientJobProxy.getJobId()
                                + " did not report its terminal result within "
                                + timeoutSeconds
                                + " s after the cancel",
                        timeout);
            }
            Assertions.assertEquals(JobStatus.CANCELED, jobResult.getStatus());
            Awaitility.await()
                    .atMost(timeoutSeconds, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED, clientJobProxy.getJobStatus()));
        } finally {
            cancelExecutor.shutdownNow();
        }
    }

    /**
     * End-to-end coverage for the resource side of the stop-with-savepoint terminal path: a job
     * that ends in {@code SAVEPOINT_DONE} must release the fixed slots it owns and the WAIT queue
     * must then dispatch the next job.
     *
     * <p>Contract under test:
     *
     * <ol>
     *   <li>While the holder runs, the victim needs the entire worker and is queued as {@code
     *       PENDING} in the master's pending queue without owning any slot.
     *   <li>A real client stop-with-savepoint request completes, the holder reaches {@code
     *       SAVEPOINT_DONE}, and the coordinator releases its {@code JobMaster}.
     *   <li>The victim is scheduled afterwards without any further trigger: it reaches {@code
     *       RUNNING}, leaves the queue and owns the two fixed slots. Fixed slots are inelastic, so
     *       a slot the savepoint-ended job failed to release strands the victim forever.
     * </ol>
     *
     * <p>Why this boundary has real regression risk: {@code SAVEPOINT_DONE} is the one terminal
     * state that keeps its checkpoint data ({@code CheckpointManager#clearCheckpointIfNeed} skips
     * deletion while {@code isSavePointEnd()} is true), and its pipelines end through {@code
     * stopPipelineWithCheckpointFallback} rather than the cancel or force-stop paths. A change that
     * ties slot release to checkpoint cleanup, or that keeps the finished job registered because
     * its savepoint must remain usable, silently starves every queued job on a fixed-slot cluster.
     *
     * <p>Existing coverage does not reach it: {@code SavepointBusySourceBarrierIT} runs on dynamic
     * slots and never queues a second job, {@code
     * SplitClusterFaultToleranceIT#testFixedSlotStreamJobReusesSlotsAfterMasterDown} covers a
     * master switch instead of a savepoint, and this class's queue-dispatch tests free capacity by
     * cancelling the holder rather than by stopping it with a savepoint.
     */
    @Test
    public void testSavepointStopReleasesFixedSlotsSoQueuedJobRuns() throws Exception {
        String testCaseName = "testSavepointStopReleasesFixedSlotsSoQueuedJobRuns";
        String testClusterName = "SplitClusterPendingJobLifecycleFailoverIT_" + testCaseName;
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy pendingJob = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNodeConfig);
        configurePendingLifecycleTest(workerNodeConfig);
        // The minimal streaming job needs exactly two slots (split enumerator + fused
        // reader/writer), so a two-slot worker can host exactly one job at a time.
        masterNodeConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(2);
        workerNodeConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(2);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode, null);
            assertPendingQueueState(activeMaster, null, 0);

            holderJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "savepoint_slot_holder",
                            TestUtils.getResource(TWO_SLOT_STREAMING_JOB_CONFIG_FILE));
            long holderJobId = holderJob.getJobId();
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);
            assertFixedSlotOwnership(activeMaster, holderJobId, true);

            // The victim needs the whole worker, which the holder occupies: it must be queued
            // rather than scheduled, and it must not hold any slot while queued.
            pendingJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "savepoint_slot_victim",
                            TestUtils.getResource(TWO_SLOT_STREAMING_JOB_CONFIG_FILE));
            long pendingJobId = pendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);
            assertPendingQueueContainsJob(activeMaster, pendingJobId, 1);
            assertFixedSlotOwnership(activeMaster, pendingJobId, false);

            // Stop the holder with a real client savepoint request. It ends SAVEPOINT_DONE, the
            // one terminal state that deliberately keeps its checkpoint data.
            SeaTunnelClient finalEngineClient = engineClient;
            CompletableFuture<Void> savepointRequest =
                    CompletableFuture.runAsync(() -> finalEngineClient.savePointJob(holderJobId));
            savepointRequest.get(120, TimeUnit.SECONDS);
            assertJobStatusWithTimeout(holderJob, JobStatus.SAVEPOINT_DONE, 120);

            // The coordinator must release the savepoint-ended job (a retained savepoint is not a
            // reason to keep it registered as running) and every slot it owned.
            assertJobReleasedByCoordinator(activeMaster, holderJobId);
            assertFixedSlotOwnership(activeMaster, holderJobId, false);

            // The queued job must now be dispatched by the normal scheduler, with no extra trigger.
            assertJobStatusWithTimeout(pendingJob, JobStatus.RUNNING, 120);
            assertPendingQueueNotContainsJob(activeMaster, pendingJobId);
            assertFixedSlotOwnership(activeMaster, pendingJobId, true);

            pendingJob.cancelJob();
            assertEventuallyCanceled(pendingJob);
            assertFixedSlotOwnership(activeMaster, pendingJobId, false);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    @NotNull private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        return seaTunnelConfig;
    }

    private static void configurePendingLifecycleTest(SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(4);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String jobName,
            String jobConfigFile) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig);
        try {
            return jobExecutionEnv.execute();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to submit job " + jobName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted when submitting job " + jobName, e);
        }
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

    /**
     * Failover and cancellation can leave the client-observed status at CANCELING after the cancel
     * request has already completed, so wait for the terminal job result before asserting the final
     * visible state.
     */
    private static void assertEventuallyCanceled(ClientJobProxy clientJobProxy) {
        JobResult jobResult = clientJobProxy.waitForJobCompleteV2();
        Assertions.assertEquals(JobStatus.CANCELED, jobResult.getStatus());
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, clientJobProxy.getJobStatus()));
    }

    /**
     * Waits until failover restore has rebuilt the running graph on the active master.
     *
     * <p>The top-level job status can turn RUNNING before every restored pipeline and task vertex
     * has reacquired slots and reported RUNNING. Cancelling earlier makes this test depend on a
     * restore/cancel race instead of pending-job scheduling.
     */
    private static void assertRunningJobGraphWithTimeout(
            HazelcastInstanceImpl activeMaster, long jobId, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = getJobMaster(activeMaster, jobId);
                            Assertions.assertNotNull(
                                    jobMaster,
                                    "Job master should exist before checking restored task states");
                            PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
                            Assertions.assertEquals(JobStatus.RUNNING, physicalPlan.getJobStatus());
                            physicalPlan
                                    .getPipelineList()
                                    .forEach(
                                            SplitClusterPendingJobLifecycleFailoverIT
                                                    ::assertRunningSubPlan);
                        });
    }

    /**
     * Asserts that one restored pipeline and all of its task vertices are fully running.
     *
     * <p>This keeps the following cancel assertion focused on the lifecycle transition instead of
     * racing against delayed task deployment after master failover.
     */
    private static void assertRunningSubPlan(SubPlan subPlan) {
        Assertions.assertEquals(PipelineStatus.RUNNING, subPlan.getPipelineState());
        subPlan.getCoordinatorVertexList()
                .forEach(SplitClusterPendingJobLifecycleFailoverIT::assertRunningVertex);
        subPlan.getPhysicalVertexList()
                .forEach(SplitClusterPendingJobLifecycleFailoverIT::assertRunningVertex);
    }

    /**
     * Asserts that one task vertex has completed deployment and reported RUNNING.
     *
     * <p>A restored vertex can lag behind the top-level job status while resources are assigned, so
     * the test must observe the vertex state directly before cancelling.
     */
    private static void assertRunningVertex(PhysicalVertex physicalVertex) {
        Assertions.assertEquals(ExecutionState.RUNNING, physicalVertex.getExecutionState());
    }

    /**
     * Reads the current job master from the active SeaTunnel server embedded in the test cluster.
     */
    private static JobMaster getJobMaster(HazelcastInstanceImpl activeMaster, long jobId) {
        SeaTunnelServer server =
                activeMaster.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getJobMaster(jobId);
    }

    private static HazelcastInstanceImpl waitAndFindActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        final HazelcastInstanceImpl[] activeMasterRef = new HazelcastInstanceImpl[1];
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            activeMasterRef[0] = findActiveMaster(masterNode1, masterNode2);
                            Assertions.assertNotNull(
                                    activeMasterRef[0],
                                    "Should find active master after coordinator initialization");
                        });
        return activeMasterRef[0];
    }

    private static HazelcastInstanceImpl findActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        if (isCoordinatorActive(masterNode1)) {
            return masterNode1;
        }
        if (isCoordinatorActive(masterNode2)) {
            return masterNode2;
        }
        return null;
    }

    /**
     * Registers a listener that records whether {@code CoordinatorService#checkNewActiveMaster()}'s
     * retry-path log line ("check new active master error") fired on this node. This is diagnostic
     * evidence only, logged but never asserted on, that a given run of {@link
     * #testMasterElectionLoopRecoversFromRapidFailoverChurn()} actually exercised the catch block
     * under regression test; a black-box E2E test has no reliable way to force a specific transient
     * exception deterministically, so the test's hard assertions must not depend on this listener
     * having fired.
     */
    private static void installRetryPathLogListener(
            HazelcastInstanceImpl node, AtomicBoolean sawRetryPathLog) {
        node.getLoggingService()
                .addLogListener(
                        Level.SEVERE,
                        logEvent -> {
                            String message = logEvent.getLogRecord().getMessage();
                            if (message != null
                                    && message.contains("check new active master error")) {
                                sawRetryPathLog.set(true);
                            }
                        });
    }

    /** Waits until a standby master has taken over coordinator activity after a failover. */
    private static void awaitCoordinatorActive(
            HazelcastInstanceImpl masterNode, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(masterNode.getLifecycleService().isRunning());
                            Assertions.assertTrue(
                                    isCoordinatorActive(masterNode),
                                    "Standby master should become active after failover");
                        });
    }

    /**
     * Renders a small bounded batch job from {@code cluster_batch_fake_to_localfile_template.conf}
     * so the resulting output is exactly countable once the job reaches FINISHED. Mirrors {@code
     * ClusterFaultToleranceIT#createTestResources}; kept local since it is only needed by the
     * duplicate-dispatch regression test in this class.
     *
     * @return pair of (sink output directory, generated job config file path)
     */
    private static ImmutablePair<String, String> createBatchTestResources(
            String testCaseName, long rowNumber, int parallelism) throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("dynamic_test_case_name", testCaseName);
        valueMap.put("dynamic_job_mode", JobMode.BATCH.toString());
        valueMap.put("dynamic_test_row_num_per_parallelism", String.valueOf(rowNumber));
        valueMap.put("dynamic_test_parallelism", String.valueOf(parallelism));

        String targetDir = ("/tmp/hive/warehouse/" + testCaseName).replace("/", File.separator);
        FileUtils.createNewDir(targetDir);

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                "cluster_batch_fake_to_localfile_template.conf", valueMap, targetConfigFilePath);
        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl masterNode) {
        if (masterNode == null || !masterNode.getLifecycleService().isRunning()) {
            return false;
        }
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        try {
            return server.getCoordinatorService().isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }

    private static void assertPendingQueueState(
            HazelcastInstanceImpl masterNode, Long expectedJobIdInQueue, int expectedPendingCount) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService().isCoordinatorActive(),
                                    "Coordinator should be active when asserting pending queue");
                            Assertions.assertEquals(
                                    expectedPendingCount,
                                    server.getCoordinatorService().getPendingJobCount());
                            if (expectedJobIdInQueue != null) {
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                .getPendingJobQueue()
                                                .contains(expectedJobIdInQueue),
                                        "Expected pending job should remain in pending queue");
                            }
                        });
    }

    private static void assertPendingQueueContainsJob(
            HazelcastInstanceImpl masterNode,
            long expectedJobIdInQueue,
            int minExpectedPendingCount) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService().isCoordinatorActive(),
                                    "Coordinator should be active when asserting pending queue");
                            Assertions.assertTrue(
                                    server.getCoordinatorService().getPendingJobCount()
                                            >= minExpectedPendingCount,
                                    "Pending queue count should be at least "
                                            + minExpectedPendingCount);
                            Assertions.assertTrue(
                                    server.getCoordinatorService()
                                            .getPendingJobQueue()
                                            .contains(expectedJobIdInQueue),
                                    "Expected pending job should remain in pending queue");
                        });
    }

    private static void assertPendingQueueNotContainsJob(
            HazelcastInstanceImpl masterNode, long expectedRemovedJobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        server.getCoordinatorService()
                                                .getPendingJobQueue()
                                                .contains(expectedRemovedJobId),
                                        "Pending queue should not contain job "
                                                + expectedRemovedJobId));
    }

    /**
     * Renders a small, quickly-completing batch job from {@code
     * cluster_batch_fake_to_localfile_template.conf} so it reaches FINISHED almost immediately,
     * leaving a realistic terminal-job IMap tombstone behind for {@link
     * #testTerminalJobCleanupSkipsWorkerWaitAfterMasterSwitch}. Mirrors {@code
     * ClusterFaultToleranceIT#createTestResources}; kept local since it is only needed by that test
     * in this class.
     *
     * @return pair of (sink output directory, generated job config file path)
     */
    private static ImmutablePair<String, String> createTerminalJobResources(
            String testCaseName, long rowNumber, int parallelism) throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put("dynamic_test_case_name", testCaseName);
        valueMap.put("dynamic_job_mode", JobMode.BATCH.toString());
        valueMap.put("dynamic_test_row_num_per_parallelism", String.valueOf(rowNumber));
        valueMap.put("dynamic_test_parallelism", String.valueOf(parallelism));

        String targetDir = ("/tmp/hive/warehouse/" + testCaseName).replace("/", File.separator);
        FileUtils.createNewDir(targetDir);

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                "cluster_batch_fake_to_localfile_template.conf", valueMap, targetConfigFilePath);
        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }

    /**
     * Waits until the coordinator no longer holds a {@code JobMaster} for the job.
     *
     * <p>{@code CoordinatorService#getJobMaster} is the same lookup {@code submitJob} uses to
     * detect a duplicate submission and the pending scheduler uses to decide whether a job is still
     * owned, so a finished job that is still resolvable here is still holding coordinator-side
     * state even though its status is terminal.
     */
    private static void assertJobReleasedByCoordinator(
            HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        server.getCoordinatorService().getJobMaster(jobId),
                                        "Coordinator still holds the finished job " + jobId));
    }

    /**
     * Asserts whether the master's fixed-slot ownership map contains any pipeline entry for the
     * given job.
     *
     * <p>The map is written by {@code ResourceUtils#applyResourceForPipeline} only when a pipeline
     * is actually deployed, so this distinguishes "the job was scheduled and owns its slots" from
     * "the job is still queued (or already released everything)". Asserted with {@link Awaitility}
     * because slot release happens asynchronously after the terminal status is published.
     */
    private static void assertFixedSlotOwnership(
            HazelcastInstanceImpl masterNode, long jobId, boolean expected) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                masterNode
                        .node
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        ownedSlotProfilesIMap.keySet().stream()
                                                .anyMatch(location -> location.getJobId() == jobId),
                                        "Job " + jobId + " slot ownership should be " + expected));
    }

    /**
     * Asserts whether the master's fixed-slot ownership map contains any pipeline entry for the
     * given job.
     *
     * <p>The map is written by {@code ResourceUtils#applyResourceForPipeline} only when a pipeline
     * is actually deployed, so this distinguishes "the job was scheduled and owns its slots" from
     * "the job is still queued (or already released everything)". The state is asserted with {@link
     * Awaitility} because slot release happens asynchronously after the terminal status is
     * published.
     */
    private static void assertSlotOwnership(
            HazelcastInstanceImpl masterNode, long jobId, boolean expected) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                masterNode
                        .node
                        .getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        ownedSlotProfilesIMap.keySet().stream()
                                                .anyMatch(location -> location.getJobId() == jobId),
                                        "Job " + jobId + " slot ownership should be " + expected));
    }
}
