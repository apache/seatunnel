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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

@Slf4j
public class SplitClusterPendingJobLifecycleFailoverIT {
    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming_lifecycle.conf";

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
}
