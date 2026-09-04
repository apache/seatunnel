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
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
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
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SplitClusterPendingJobLifecycleFailoverIT {
    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming_lifecycle.conf";

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

            ClientJobProxy holderJobAfterFlapping =
                    engineClient.createJobClient().getJobProxy(holderJob.getJobId());
            holderJobAfterFlapping.cancelJob();
            assertEventuallyCanceled(holderJobAfterFlapping);

            // The active master restored both jobs while all worker slots were occupied. Hand off
            // once more after releasing the holder's slots so the new coordinator rebuilds the
            // pending entry from distributed state and schedules it with the newly available
            // resources. This also verifies that the final recovery cannot revive a stale copy.
            currentActive.shutdown();
            awaitCoordinatorActive(currentStandby, 30);
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
}
