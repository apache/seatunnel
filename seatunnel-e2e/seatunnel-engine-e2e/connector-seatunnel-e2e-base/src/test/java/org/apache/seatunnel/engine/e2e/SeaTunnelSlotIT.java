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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SeaTunnelSlotIT {

    /**
     * Upper bound for one job cancellation request. {@link ClientJobProxy#cancelJob()} joins the
     * client response future without any timeout, so the test has to own that wait: a stalled
     * cancellation must fail the assertion with a clear message instead of hanging the suite and
     * skipping the client/node shutdown.
     */
    private static final long CANCEL_REQUEST_TIMEOUT_SECONDS = 60;

    @Test
    public void testSlotNotEnough() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testSlotNotEnough";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // slot num is 3
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(3);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // client config
            Common.setDeployMode(DeployMode.CLIENT);
            String filePath = TestUtils.getResource("batch_slot_not_enough.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testClusterName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.FAILED.equals(
                                                            objectCompletableFuture.get())));

        } finally {
            if (engineClient != null) {
                engineClient.close();
            }

            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    @Test
    public void testSlotEnough() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testSlotEnough";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // slot num is 10
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(10);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // client config
            Common.setDeployMode(DeployMode.CLIENT);
            String filePath = TestUtils.getResource("batch_slot_not_enough.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testClusterName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);

            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.FINISHED.equals(
                                                            objectCompletableFuture.get())));

        } finally {
            if (engineClient != null) {
                engineClient.close();
            }

            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    /**
     * Regression test for the slot-release bug fixed by <a
     * href="https://github.com/apache/seatunnel/pull/6763">#6763</a> ("[fix][zeta] fix can't
     * release resource issue"). Before that fix, when a pipeline needed more slots than were
     * available, the slots that had already been successfully granted before the shortfall was
     * detected were never released: {@code ResourceUtils#applyResourceForPipeline} joined every
     * per-task resource future, but a failed or missing future was silently dropped instead of
     * triggering release of the ones that did succeed, so {@code NoEnoughResourceException}
     * propagated with those slots still marked owned forever.
     *
     * <p>{@link #testSlotNotEnough()} above already proves the job reaches {@code FAILED} in this
     * scenario, but that assertion alone cannot tell a pre-fix leak apart from a regression: a job
     * with permanently-leaked slots still fails with the same status. The only accompanying test
     * the original fix shipped with ({@code FixSlotResourceTest#testNotEnoughResource} in
     * seatunnel-engine-server) asserts the release directly against a mocked, single-JVM {@code
     * ResourceManager} — it does not prove a real cluster's slot pool is actually usable again
     * afterward.
     *
     * <p>This test closes that gap functionally rather than by reaching into internal bookkeeping:
     * it drives the exact same undersized-cluster failure as {@link #testSlotNotEnough()} (slot-num
     * 3, a job that needs more), then submits a second, minimal single-slot job against the same
     * still-running cluster and asserts it reaches {@code FINISHED}. If the slots granted to the
     * first job were never released, the cluster would still show 0 free slots and the second job
     * would fail with the same {@code NoEnoughResourceException} instead.
     */
    @Test
    public void testSlotReleasedAfterNotEnoughResourceFailure() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;

        try {
            String testClusterName = "testSlotReleasedAfterNotEnoughResourceFailure";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            // Same undersized pool as testSlotNotEnough: enough to grant some, not all, of the
            // first job's slots.
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(3);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig oversizedJobConfig = new JobConfig();
            oversizedJobConfig.setName(testClusterName + "_oversized");
            ClientJobExecutionEnvironment oversizedJobEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource("batch_slot_not_enough.conf"),
                            oversizedJobConfig,
                            seaTunnelConfig);
            ClientJobProxy oversizedJobProxy = oversizedJobEnv.execute();
            CompletableFuture<JobStatus> oversizedJobComplete =
                    CompletableFuture.supplyAsync(oversizedJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            oversizedJobComplete.isDone()
                                                    && JobStatus.FAILED.equals(
                                                            oversizedJobComplete.get())));

            // The oversized job's own resource shortfall must be fully resolved on the master
            // before this proves anything: submit a second, minimal job that needs far fewer
            // slots than the pool holds, against the same still-running cluster.
            JobConfig minimalJobConfig = new JobConfig();
            minimalJobConfig.setName(testClusterName + "_minimal");
            ClientJobExecutionEnvironment minimalJobEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource("batch_fake_to_console_minimal_slot.conf"),
                            minimalJobConfig,
                            seaTunnelConfig);
            ClientJobProxy minimalJobProxy = minimalJobEnv.execute();
            CompletableFuture<JobStatus> minimalJobComplete =
                    CompletableFuture.supplyAsync(minimalJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            minimalJobComplete.isDone()
                                                    && JobStatus.FINISHED.equals(
                                                            minimalJobComplete.get()),
                                            "Minimal job could not get a slot after the oversized "
                                                    + "job failed; the earlier failure's slots "
                                                    + "were likely never released"));
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }

            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    /**
     * Guards the finality of cancelling a job that is only queued, not running: under {@link
     * ScheduleStrategy#WAIT} a job that does not fit into the slot pool stays in the master's
     * pending queue, and cancelling it must remove it from that queue for good. In particular the
     * scheduler must never dispatch it later, after the capacity it was waiting for is released.
     *
     * <p><b>Why this is a boundary with real regression risk.</b> The pending scheduler is a
     * long-lived thread that peeks the queue head, evaluates the job's slots through {@code
     * JobMaster#preApplyResources} and only then dispatches it (see {@code
     * CoordinatorService#pendingJobSchedule}). Cancelling a queued job happens on a different
     * thread and only removes the entry from {@code pendingJobQueue} before flipping the {@code
     * JobMaster} to {@code CANCELED}. If the scheduler already holds a reserved reference to that
     * entry, or if a later iteration re-reads stale queue state, the cancelled job can be
     * dispatched anyway once capacity appears: Zeta then reports a job the user cancelled as {@code
     * FAILED} (leaving terminal state is rejected) or, worse, deploys its tasks. The window is
     * invisible to a client that saw {@code CANCELED}, and it only closes once the cluster has free
     * capacity — which in a full fixed-slot cluster can be much later.
     *
     * <p><b>Why the existing coverage does not reach it.</b> {@code
     * CoordinatorServiceWithCancelPendingJobTest#testCancelPendingJob} cancels a hand-injected
     * pending {@code JobMaster} and asserts the queue entry and IMaps are gone at that moment, but
     * no scheduler runs against real resources in that test, so nothing can be dispatched after the
     * cancel and the release-then-dispatch path is never exercised. The pending-job failover E2E (
     * {@code SplitClusterPendingJobLifecycleFailoverIT}) only cancels jobs that are already
     * running, and its queue tests keep their queued job alive. {@link #testSlotNotEnough()} and
     * {@link #testSlotReleasedAfterNotEnoughResourceFailure()} use the {@code FAIL} strategy, where
     * an oversize request never enters the queue.
     *
     * <p><b>How the contract is pinned.</b> The fixture pipeline needs three slots (one split
     * enumerator plus one chain task group per parallelism index), so a fixed three-slot pool lets
     * exactly one such job run. The holder fills the pool, the victim is queued and cancelled while
     * {@code PENDING}. The test then releases the holder and keeps observing the victim for longer
     * than a full scheduler retry cycle: it must stay {@code CANCELED} and must never read a single
     * row, which is only true if the scheduler dropped it instead of dispatching it with the freed
     * slots. A fresh job submitted afterwards must still start and read rows, which proves the
     * cancellation did not break the queue or leak a slot.
     */
    @Test
    public void testCanceledQueuedJobIsNeverDispatchedAfterCapacityIsReleased() throws Exception {
        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holder = null;
        ClientJobProxy canceledQueuedJob = null;
        ClientJobProxy follower = null;
        // Owns every cancellation request so that a blocked cancel can be abandoned on timeout
        // instead of wedging the test thread; the threads are daemons so a cancel that ignores
        // interruption can never keep the JVM alive.
        ExecutorService cancelExecutor =
                Executors.newCachedThreadPool(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-slot-cancel");
                            thread.setDaemon(true);
                            return thread;
                        });

        try {
            String testClusterName =
                    "testCanceledQueuedJobIsNeverDispatchedAfterCapacityIsReleased";
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(3);
            seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            holder = submitStreamingJob(engineClient, seaTunnelConfig, "cancel-queued-holder");
            awaitJobStatus(holder, JobStatus.RUNNING);
            awaitSourceRowsRead(engineClient, holder.getJobId());

            canceledQueuedJob =
                    submitStreamingJob(engineClient, seaTunnelConfig, "cancel-queued-victim");
            final long victimJobId = canceledQueuedJob.getJobId();
            final SeaTunnelClient metricsClient = engineClient;
            final ClientJobProxy victim = canceledQueuedJob;
            awaitJobStatus(victim, JobStatus.PENDING);

            cancelJobOrFail(cancelExecutor, victim);
            awaitJobStatus(victim, JobStatus.CANCELED);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .during(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            victim.getJobStatus(),
                                            "A cancelled queued job must not be resurrected while"
                                                    + " the cluster is still full"));

            cancelJobOrFail(cancelExecutor, holder);
            awaitJobStatus(holder, JobStatus.CANCELED);

            // Wait longer than one full 3s scheduler retry cycle with free capacity available: the
            // scheduler must never turn the cancelled queue entry into a dispatched job.
            Awaitility.await()
                    .atMost(40, TimeUnit.SECONDS)
                    .during(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        victim.getJobStatus(),
                                        "A cancelled queued job must stay CANCELED after the"
                                                + " capacity it waited for was released");
                                Assertions.assertEquals(
                                        0,
                                        metricsClient
                                                .getJobMetricsSummary(victimJobId)
                                                .getSourceReadCount(),
                                        "A cancelled queued job must never deploy its source");
                            });

            follower = submitStreamingJob(engineClient, seaTunnelConfig, "cancel-queued-follower");
            awaitJobStatus(follower, JobStatus.RUNNING);
            awaitSourceRowsRead(engineClient, follower.getJobId());
            cancelJobOrFail(cancelExecutor, follower);
            awaitJobStatus(follower, JobStatus.CANCELED);
        } finally {
            // Teardown must always reach client/node shutdown, even when a cancellation request or
            // a previous close fails: hence one best-effort block per step, nested finally blocks.
            try {
                cancelQuietly(cancelExecutor, holder);
                cancelQuietly(cancelExecutor, canceledQueuedJob);
                cancelQuietly(cancelExecutor, follower);
            } finally {
                try {
                    if (engineClient != null) {
                        engineClient.close();
                    }
                } finally {
                    try {
                        if (node1 != null) {
                            node1.shutdown();
                        }
                    } finally {
                        cancelExecutor.shutdownNow();
                    }
                }
            }
        }
    }

    private ClientJobProxy submitStreamingJob(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig, String jobName)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(
                        TestUtils.getResource("cancel_queued_job_stream.conf"),
                        jobConfig,
                        seaTunnelConfig);
        return jobExecutionEnv.execute();
    }

    private void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Job "
                                                + jobProxy.getJobId()
                                                + " did not reach "
                                                + expectedStatus));
    }

    private void awaitSourceRowsRead(SeaTunnelClient engineClient, long jobId) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        engineClient
                                                        .getJobMetricsSummary(jobId)
                                                        .getSourceReadCount()
                                                > 0,
                                        "Job "
                                                + jobId
                                                + " is deployed but its source read no"
                                                + " rows"));
    }

    /**
     * Cancels a job and requires the request itself to be served within {@link
     * #CANCEL_REQUEST_TIMEOUT_SECONDS}. {@link ClientJobProxy#cancelJob()} blocks on the client
     * response future with no timeout of its own, so waiting on it directly would skip every status
     * assertion and the client/node shutdown; bounding it turns a stalled cancellation into a clear
     * test failure, and the abandoned thread is interrupted so it cannot leak.
     */
    private void cancelJobOrFail(ExecutorService cancelExecutor, ClientJobProxy jobProxy)
            throws InterruptedException {
        Assertions.assertNotNull(jobProxy, "The job to cancel must be submitted first");
        Future<?> cancelRequest = cancelExecutor.submit(jobProxy::cancelJob);
        try {
            cancelRequest.get(CANCEL_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            cancelRequest.cancel(true);
            Assertions.fail(
                    "Cancelling job "
                            + jobProxy.getJobId()
                            + " did not complete within "
                            + CANCEL_REQUEST_TIMEOUT_SECONDS
                            + " seconds, so the regression cannot be observed: a stalled"
                            + " cancellation must fail the test instead of hanging the suite");
        } catch (ExecutionException e) {
            cancelRequest.cancel(true);
            Assertions.fail("Cancelling job " + jobProxy.getJobId() + " failed", e.getCause());
        }
    }

    /**
     * Best-effort cancellation used by teardown: bounded like {@link
     * #cancelJobOrFail(ExecutorService, ClientJobProxy)}, but it never throws so it cannot mask the
     * failure that brought the test down, and the pending request is always released.
     */
    private void cancelQuietly(ExecutorService cancelExecutor, ClientJobProxy jobProxy) {
        if (jobProxy == null) {
            return;
        }
        Future<?> cancelRequest = null;
        try {
            cancelRequest = cancelExecutor.submit(jobProxy::cancelJob);
            cancelRequest.get(CANCEL_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // The interrupt status is deliberately not restored: teardown still has to close the
            // client and stop the node, and this request is being abandoned anyway.
        } catch (Exception ignored) {
            // The job may already be cancelled, the cluster may already be shut down, or the
            // bounded request timed out; cleanup must not replace the original failure.
        } finally {
            if (cancelRequest != null && !cancelRequest.isDone()) {
                cancelRequest.cancel(true);
            }
        }
    }
}
