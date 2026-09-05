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
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Tier-3 scale/stress E2E for the Zeta engine master's shared {@code CoordinatorService} executor
 * (see {@code tasks/docs/design/zeta-engine-core-extreme-case-e2e-gap-analysis.md}, item L4).
 *
 * <p>{@code CoordinatorService#createCoordinatorExecutor()} builds one node-wide {@code
 * ThreadPoolExecutor} (default {@code core-thread-num=10}, {@code
 * max-thread-num=Integer.MAX_VALUE}, both configurable via {@code
 * ServerConfigOptions.MasterServerConfigOptions}, backed by a {@code SynchronousQueue} -- i.e. zero
 * queue capacity: a submitted task either hands off to an already-idle pool thread or a brand-new
 * thread is spawned immediately, there is no waiting in a buffer) that is shared across every job's
 * pipeline/task state-transition callbacks, checkpoint I/O, and the pending-job scheduler on that
 * node. One pool, no per-job or per-pipeline bound.
 *
 * <p>{@code CoordinatorService#restoreAllRunningJobFromMasterNodeSwitch()} is itself dispatched
 * onto this same executor when a node becomes the new active master, and it fans every job that
 * still needs restoring out onto it in a single unthrottled pass:
 *
 * <pre>
 * needRestoreFromMasterNodeSwitchJobs.stream()
 *     .map(entry -&gt; CompletableFuture.runAsync(() -&gt; restoreJobFromMasterActiveSwitch(...),
 *                                              executorService))
 *     .collect(Collectors.toList());
 * </pre>
 *
 * <p>With N jobs alive on the old master at the moment of failover, this submits N restore tasks to
 * the pool in one tight loop, with no batching or concurrency cap between them. Separately, a
 * systemic pattern across 20+ call sites (including {@code SubPlan#updatePipelineState}, {@code
 * SubPlan#resetPipelineState}, and {@code restoreAllRunningJobFromMasterNodeSwitch} itself) retries
 * IMap writes up to {@code Constant.OPERATION_RETRY_TIME} (30) times at {@code
 * Constant.OPERATION_RETRY_SLEEP} (2000ms) intervals via a blocking {@code Thread.sleep} inside
 * {@code RetryUtils.retryWithException} -- so a retrying callback occupies its executor thread for
 * the whole backoff -- specifically to ride out {@code HazelcastInstanceNotActiveException} /
 * {@code OperationTimeoutException} / {@code RetryableHazelcastException} turbulence during a
 * master switch (see {@code ExceptionUtil#isOperationNeedRetryException}). Because the queue is
 * synchronous and {@code max-thread-num} is unbounded, a mass-simultaneous restore has exactly two
 * possible outcomes: the pool grows to match the burst, or (only if {@code max-thread-num} were
 * ever configured finite) it starts silently dropping callbacks via {@code
 * RejectedExecutionException}. There is no third, graceful-queueing outcome available today.
 *
 * <p>This test builds a small-but-real "mass" event -- {@link #CONCURRENT_JOB_COUNT} concurrent
 * long-running streaming jobs kept alive on a 2-master/1-worker cluster -- kills the active master
 * (the same master-kill technique already used throughout this test family, e.g. {@link
 * SplitClusterPendingJobLifecycleFailoverIT}), and samples the promoted standby's {@code
 * CoordinatorService} executor via the already-public {@code
 * CoordinatorService#getThreadPoolStatusMetrics()} (used elsewhere by this engine for telemetry
 * export; not added by this test) immediately after it takes over. It is deliberately test-only:
 * the assertions document today's real, source-verified behavior -- the pool grows well past its
 * configured core size and is never observed to reject a task -- rather than proposing or requiring
 * a fix.
 */
@Slf4j
public class CoordinatorExecutorMassFailoverStormIT {

    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming_lifecycle.conf";

    /**
     * Number of concurrent long-running streaming jobs kept alive across the master failover.
     * "Dozens" scale: large enough that the restore fan-out described above clearly and repeatably
     * pushes the pool past the default {@code core-thread-num=10}, small enough to stay safe on a
     * shared CI runner. This is a JVM-embedded multi-{@code HazelcastInstance} test (no Docker, no
     * separate processes), so every one of these jobs' tasks and every coordinator thread they
     * provoke land in this one test JVM.
     */
    private static final int CONCURRENT_JOB_COUNT = 30;

    /**
     * Pure CI-runner safety backstop, not a claim that today's design bounds growth in general --
     * it does not, since {@code max-thread-num} defaults to {@code Integer.MAX_VALUE}. This only
     * guards against a pathological regression far beyond what {@link #CONCURRENT_JOB_COUNT}
     * concurrent restores could plausibly need, so a future regression fails this test loudly
     * instead of quietly spawning an unbounded number of threads on a shared runner.
     */
    private static final int POOL_SIZE_CI_SAFETY_CEILING = 500;

    @Test
    public void testCoordinatorExecutorGrowsPastCoreDuringMassFailoverRestore() throws Exception {
        String testClusterName =
                "CoordinatorExecutorMassFailoverStormIT_"
                        + "testCoordinatorExecutorGrowsPastCoreDuringMassFailoverRestore";

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        List<Long> jobIds = new ArrayList<>();

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        try {
            // Split-role cluster (master-only + master-only + worker-only), mirroring
            // SplitClusterPendingJobLifecycleFailoverIT: this keeps the standby master's
            // CoordinatorService executor free of any task-execution activity of its own, so
            // every thread we observe on it after promotion is attributable to coordination work
            // (state restore, scheduling), not to running one of the CONCURRENT_JOB_COUNT jobs'
            // actual tasks.
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode1 = masterNode1;
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMasterNode1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

            for (int i = 0; i < CONCURRENT_JOB_COUNT; i++) {
                jobIds.add(submitJob(engineClient, masterNode1Config, "mass_retry_job_" + i));
            }

            // Submission only means the active master accepted and scheduled a job; wait until
            // every one of them has genuinely reached RUNNING, so all CONCURRENT_JOB_COUNT jobs
            // are guaranteed to already be replicated into runningJobInfoIMap -- and therefore
            // genuinely in need of restoring -- by the time the active master is killed below.
            awaitAllJobsInStatus(engineClient, jobIds, JobStatus.RUNNING, 180);

            // Trigger the mass-simultaneous-restore condition: kill the active master while all
            // CONCURRENT_JOB_COUNT jobs are RUNNING and their worker is still healthy. Master
            // failover while jobs are live is the same realistic trigger already proven throughout
            // this test family; what is new here is the concurrent job count and what gets sampled
            // on the new master immediately afterward.
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

            // Sample the promoted standby's coordinator executor in a tight loop right after it
            // takes over. restoreAllRunningJobFromMasterNodeSwitch() fans every not-yet-restored
            // job out onto this exact executor in one unthrottled
            // stream().map(CompletableFuture.runAsync(...)) pass, so the burst begins within
            // (well under) a second of isCoordinatorActive() turning true and is over long before
            // the pool's 60-second keep-alive would reclaim any idle thread it spawned -- this
            // loop has to run now, immediately, not after some other setup.
            CoordinatorService standbyCoordinatorService = getCoordinatorService(standbyMaster);
            int corePoolSize =
                    standbyCoordinatorService.getThreadPoolStatusMetrics().getCorePoolSize();
            int peakPoolSize = 0;
            int peakActiveCount = 0;
            long peakRejectionCount = 0;

            long samplingDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20);
            while (System.currentTimeMillis() < samplingDeadline) {
                ThreadPoolStatus status;
                try {
                    status = standbyCoordinatorService.getThreadPoolStatusMetrics();
                } catch (Exception e) {
                    // Defensive only: getThreadPoolStatusMetrics() reads a field populated
                    // unconditionally in the CoordinatorService constructor, so this should not
                    // actually throw once isCoordinatorActive() has already been observed true;
                    // skip the sample rather than fail the test on an unrelated transient race.
                    Thread.sleep(20);
                    continue;
                }
                peakPoolSize = Math.max(peakPoolSize, status.getPoolSize());
                peakActiveCount = Math.max(peakActiveCount, status.getActiveCount());
                peakRejectionCount = Math.max(peakRejectionCount, status.getRejectionCount());
                Thread.sleep(20);
            }

            log.info(
                    "CoordinatorExecutorMassFailoverStormIT observed: concurrentJobs={}, "
                            + "corePoolSize={}, peakPoolSize={}, peakActiveCount={}, "
                            + "peakRejectionCount={}",
                    CONCURRENT_JOB_COUNT,
                    corePoolSize,
                    peakPoolSize,
                    peakActiveCount,
                    peakRejectionCount);

            // This is the documented current behavior, not a bug fix: with CONCURRENT_JOB_COUNT
            // restore tasks fanned out onto the shared executor in one unthrottled pass, and a
            // SynchronousQueue backing it (no buffering -- a task either hands off to an
            // already-idle thread or forces a brand-new one), the pool has no way to stay within
            // its configured core-thread-num=10 under this load.
            Assertions.assertTrue(
                    peakPoolSize > corePoolSize,
                    String.format(
                            "Expected the mass-failover restore of %d concurrent jobs to push the "
                                    + "shared coordinator executor past its configured core pool "
                                    + "size (observed core=%d, peak=%d); if this does not hold, the "
                                    + "restore fan-out is being throttled/queued in a way the "
                                    + "current source does not show",
                            CONCURRENT_JOB_COUNT, corePoolSize, peakPoolSize));

            // max-thread-num defaults to Integer.MAX_VALUE, so RejectedExecutionException is not
            // structurally reachable today; this executable-documentation assertion is what would
            // catch it if that default, or this test's scale, ever changed.
            Assertions.assertEquals(
                    0,
                    peakRejectionCount,
                    "The shared coordinator executor has no bounded max-thread-num today, so it "
                            + "should never reject a task; unbounded growth, not rejection, is the "
                            + "current failure mode");

            // Pure CI-runner safety backstop -- see the constant's Javadoc above.
            Assertions.assertTrue(
                    peakPoolSize < POOL_SIZE_CI_SAFETY_CEILING,
                    String.format(
                            "Observed peak coordinator pool size %d is far beyond what %d "
                                    + "concurrent job restores should plausibly need; capping here "
                                    + "so a regression fails loudly instead of destabilizing the CI "
                                    + "runner",
                            peakPoolSize, CONCURRENT_JOB_COUNT));

            // Let the restore actually finish, then clean up every job so the cluster is not left
            // with dozens of leaked running jobs once the test method returns.
            awaitAllJobsInStatus(engineClient, jobIds, JobStatus.RUNNING, 180);
            cancelAllAndAwaitTerminal(engineClient, jobIds);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode1 != null && masterNode1.getLifecycleService().isRunning()) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null && masterNode2.getLifecycleService().isRunning()) {
                masterNode2.shutdown();
            }
        }
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static long submitJob(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig, String jobName) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(
                        TestUtils.getResource(JOB_CONFIG_FILE), jobConfig, seaTunnelConfig);
        try {
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            return clientJobProxy.getJobId();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to submit job " + jobName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted when submitting job " + jobName, e);
        }
    }

    /**
     * Polls every job's current status by id until all of them report {@code expectedStatus}.
     * Re-fetching the proxy by id on every poll (rather than reusing a {@link ClientJobProxy}
     * obtained before the master failover) mirrors this test family's established post-failover
     * access pattern, since a proxy obtained against the old active master is not guaranteed to
     * keep working correctly against the new one.
     */
    private static void awaitAllJobsInStatus(
            SeaTunnelClient engineClient,
            List<Long> jobIds,
            JobStatus expectedStatus,
            long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            for (Long jobId : jobIds) {
                                JobStatus actual =
                                        engineClient
                                                .createJobClient()
                                                .getJobProxy(jobId)
                                                .getJobStatus();
                                Assertions.assertEquals(
                                        expectedStatus,
                                        actual,
                                        "Job " + jobId + " should be " + expectedStatus);
                            }
                        });
    }

    private static void cancelAllAndAwaitTerminal(SeaTunnelClient engineClient, List<Long> jobIds) {
        for (Long jobId : jobIds) {
            try {
                engineClient.createJobClient().getJobProxy(jobId).cancelJob();
            } catch (Exception e) {
                log.warn("Failed to send cancel for job {} during teardown", jobId, e);
            }
        }
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            for (Long jobId : jobIds) {
                                JobStatus actual =
                                        engineClient
                                                .createJobClient()
                                                .getJobProxy(jobId)
                                                .getJobStatus();
                                Assertions.assertTrue(
                                        actual == JobStatus.CANCELED
                                                || actual == JobStatus.FINISHED,
                                        "Job "
                                                + jobId
                                                + " should have reached a terminal status, was "
                                                + actual);
                            }
                        });
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
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

    private static boolean isCoordinatorActive(HazelcastInstanceImpl masterNode) {
        if (masterNode == null || !masterNode.getLifecycleService().isRunning()) {
            return false;
        }
        try {
            return getCoordinatorService(masterNode).isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }
}
