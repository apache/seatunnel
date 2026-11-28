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
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.pipelineslot.PipelineFinishSlotReleaseGate;
import org.apache.seatunnel.engine.e2e.pipelineslot.PipelineFinishSlotReleaseSink;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Guards that a fixed-slot batch job gives back the slots of a pipeline as soon as that pipeline
 * finishes, while the job itself is still running, and that the freed capacity is usable by another
 * WAIT job.
 *
 * <p><b>The contract.</b> In a fixed-slot cluster a job reserves slots per pipeline, not per job.
 * {@code SubPlan#subPlanDone} calls {@code JobMaster#releasePipelineResource} when a pipeline
 * reaches a terminal state, which returns that pipeline's {@code SlotProfile}s to the {@code
 * ResourceManager} while every other pipeline of the same job keeps its slots. Therefore a
 * multi-pipeline batch job whose short pipeline finished must not keep that capacity reserved until
 * its long pipeline ends: a queued {@code ScheduleStrategy.WAIT} job that fits into the released
 * slots must be admitted and run, and the still-running sibling pipeline must be untouched.
 * Releasing the finished pipeline's slots twice, or releasing the whole job's slots, would instead
 * let the queued job in too early and overcommit the worker.
 *
 * <p><b>Why this boundary has real regression risk.</b> Pipeline completion is the only normal path
 * that frees capacity before a job terminates, and it runs inside the retried cleanup of {@code
 * SubPlan#subPlanDone}. A refactor that collects all slot release at job cleanup, or one that
 * releases the job's complete slot map instead of the finished pipeline's map, changes nothing
 * observable in a single-pipeline job: the job ends immediately after that pipeline. The loss is
 * only visible on a multi-pipeline job with a long sibling pipeline, i.e. exactly where this test
 * holds one pipeline and lets another finish. It also pins the exact accounting: the follow-up job
 * that needs the whole pool must stay queued while the long sibling still owns its two slots, so a
 * double release cannot pass as a successful early admission.
 *
 * <p><b>Why existing coverage does not reach it.</b> {@code SeaTunnelSlotIT} and the slot
 * accounting E2Es only create and cancel single-pipeline jobs; {@code ClusterFaultToleranceIT} and
 * {@code ClusterFaultToleranceTwoPipelineIT} exercise multi-pipeline jobs but only through worker
 * or master failure, where the whole job's resources are released or restored; {@code
 * JobClientJobProxyIT#testNoDuplicatedReleaseSlot} only checks that no "wrong target release
 * operation" is logged for a single-pipeline job. No E2E observes the capacity that becomes
 * available when one pipeline of a still-running multi-pipeline job finishes.
 *
 * <p><b>How the window is pinned.</b> The job has two independent batch pipelines, each two slots:
 * a fast pipeline whose single writer is parked on {@link PipelineFinishSlotReleaseGate} before its
 * first row, and a slow pipeline whose writer sleeps between rows. The cluster has exactly four
 * fixed slots. The test first shows that a waiting job is queued while both pipelines hold the
 * pool, then releases the gate so the fast pipeline finishes, and asserts that the waiting job
 * completes while the slow pipeline is still RUNNING and still consuming rows. It finally submits a
 * job that needs all four slots and asserts it stays queued until the holder is cancelled, which
 * proves the released slots are exactly the finished pipeline's.
 */
@Slf4j
public class FinishedPipelineSlotReleaseIT {

    private static final String TWO_PIPELINE_CONF =
            "batch_fake_to_finish_slot_release_two_pipeline.conf";

    private static final String SINGLE_PIPELINE_CONF =
            "batch_fake_to_finish_slot_release_single_pipeline.conf";

    private static final String FOUR_SLOT_CONF =
            "batch_fake_to_finish_slot_release_four_slots.conf";

    private static final int SLOT_NUM = 4;

    private static final String FAST_GATE = "a-fast-pipeline-gate";

    private static final String FAST_TAG = "a_fast";

    private static final String SLOW_TAG = "a_slow";

    private static final String WAITING_TAG = "b_waiting";

    private static final String FULL_POOL_TAG = "c_full_pool";

    /**
     * The holder's slow source produces 3000 rows with a 20 ms sink sleep, i.e. about 60 s, which
     * is far longer than the test needs to reach and verify the finished-pipeline window.
     */
    private static final long MAX_HOLDER_LIFETIME_SECONDS = 90;

    @Test
    public void testFinishedPipelineReleasesItsSlotsBeforeTheJobEnds() throws Exception {
        String testCaseName = "testFinishedPipelineReleasesItsSlotsBeforeTheJobEnds";
        String clusterName =
                TestUtils.getClusterName("FinishedPipelineSlotReleaseIT_" + testCaseName);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holder = null;
        ClientJobProxy waitingJob = null;
        ClientJobProxy replacementJob = null;

        try {
            PipelineFinishSlotReleaseSink.resetCounters();
            PipelineFinishSlotReleaseGate.reset();

            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(SLOT_NUM);
            // WAIT keeps the second job queued instead of failing it when the pool is occupied.
            seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            holder =
                    submit(
                            engineClient,
                            seaTunnelConfig,
                            TWO_PIPELINE_CONF,
                            testCaseName + "_holder");
            long holderJobId = holder.getJobId();
            HazelcastInstanceImpl finalNode = node;
            ClientJobProxy finalHolder = holder;

            // Both pipelines of the holder are RUNNING and the slow one already produced rows;
            // the fast one is parked on its gate before writing its single row.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<SubPlan> pipelines = pipelinesOf(finalNode, holderJobId);
                                Assertions.assertEquals(
                                        2,
                                        pipelines.size(),
                                        "The holder job must expose both batch pipelines");
                                for (SubPlan pipeline : pipelines) {
                                    Assertions.assertEquals(
                                            PipelineStatus.RUNNING,
                                            pipeline.getPipelineState(),
                                            "Every holder pipeline must be RUNNING");
                                }
                                Assertions.assertTrue(
                                        PipelineFinishSlotReleaseSink.rows(SLOW_TAG) > 0,
                                        "The slow holder pipeline must already be producing rows");
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        finalHolder.getJobStatus(),
                                        "The holder job must be RUNNING");
                            });
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1L,
                                            PipelineFinishSlotReleaseGate.waiting(FAST_GATE),
                                            "The fast pipeline writer must be parked before its"
                                                    + " first row, so the test controls when that"
                                                    + " pipeline finishes"));
            Assertions.assertEquals(
                    0L,
                    PipelineFinishSlotReleaseSink.rows(FAST_TAG),
                    "The parked fast pipeline must not have consumed a row yet");

            // The holder owns all four fixed slots, so the waiting job must be queued.
            waitingJob =
                    submit(
                            engineClient,
                            seaTunnelConfig,
                            SINGLE_PIPELINE_CONF,
                            testCaseName + "_waiting");
            ClientJobProxy finalWaitingJob = waitingJob;
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.PENDING,
                                            finalWaitingJob.getJobStatus(),
                                            "A job submitted into a pool fully owned by a running"
                                                    + " job must be queued"));
            assertStaysPending(
                    waitingJob,
                    holder,
                    2_000L,
                    "The waiting job must not be admitted while the holder owns the whole pool");

            // Let the fast pipeline finish. Its two slots must go back to the pool immediately,
            // although the holder job is still running the slow pipeline.
            PipelineFinishSlotReleaseGate.release(FAST_GATE);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<SubPlan> pipelines = pipelinesOf(finalNode, holderJobId);
                                long finished =
                                        pipelines.stream()
                                                .filter(
                                                        pipeline ->
                                                                pipeline.getPipelineState()
                                                                        == PipelineStatus.FINISHED)
                                                .count();
                                Assertions.assertEquals(
                                        1L, finished, "Exactly the fast pipeline must be FINISHED");
                                Assertions.assertEquals(
                                        1L,
                                        pipelines.stream()
                                                .filter(
                                                        pipeline ->
                                                                pipeline.getPipelineState()
                                                                        == PipelineStatus.RUNNING)
                                                .count(),
                                        "The slow pipeline must stay RUNNING");
                            });
            Assertions.assertEquals(
                    1L,
                    PipelineFinishSlotReleaseSink.rows(FAST_TAG),
                    "The fast pipeline must have consumed its single row exactly once");

            // The waiting job fits into the two slots released by the finished pipeline.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FINISHED,
                                            finalWaitingJob.getJobStatus(),
                                            "The waiting job must finish on the slots released by"
                                                    + " the finished pipeline of the still-running"
                                                    + " holder job"));
            Assertions.assertEquals(
                    1L,
                    PipelineFinishSlotReleaseSink.rows(WAITING_TAG),
                    "The waiting job must have written its single row exactly once");

            // The holder's sibling pipeline was not touched and keeps writing.
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    holder.getJobStatus(),
                    "The holder job must still be RUNNING after its sibling pipeline finished");
            long slowRowsBefore = PipelineFinishSlotReleaseSink.rows(SLOW_TAG);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        PipelineFinishSlotReleaseSink.rows(SLOW_TAG)
                                                > slowRowsBefore,
                                        "The slow holder pipeline must keep producing rows");
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        finalHolder.getJobStatus(),
                                        "The holder job must remain RUNNING");
                            });

            // Only the slow pipeline's two slots are still owned. A job that needs the whole
            // pool must not start: a double release of the finished pipeline (or of the whole
            // job) would free four slots and let it overtake the running holder.
            replacementJob =
                    submit(
                            engineClient,
                            seaTunnelConfig,
                            FOUR_SLOT_CONF,
                            testCaseName + "_replacement");
            ClientJobProxy finalReplacementJob = replacementJob;
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.PENDING,
                                            finalReplacementJob.getJobStatus(),
                                            "A job that needs the whole pool must stay queued while"
                                                    + " the holder's slow pipeline still owns two"
                                                    + " slots"));
            assertStaysPending(
                    replacementJob,
                    holder,
                    8_000L,
                    "The whole-pool job must not start before the holder's slow pipeline"
                            + " releases its slots; an early start means the finished pipeline was"
                            + " released more than once or the whole job's slots were released");

            // Cancelling the holder returns the remaining slots; the queued job then runs to
            // completion on the full pool.
            holder.cancelJob();
            Awaitility.await()
                    .atMost(MAX_HOLDER_LIFETIME_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            finalHolder.getJobStatus(),
                                            "The holder job must reach CANCELED"));
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FINISHED,
                                            finalReplacementJob.getJobStatus(),
                                            "The whole-pool job must finish once the holder released"
                                                    + " the remaining slots"));
            Assertions.assertEquals(
                    9L,
                    PipelineFinishSlotReleaseSink.rows(FULL_POOL_TAG),
                    "The whole-pool job must write every row of its three readers exactly once");
        } finally {
            cancelQuietly(holder);
            cancelQuietly(waitingJob);
            cancelQuietly(replacementJob);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
            PipelineFinishSlotReleaseGate.reset();
            PipelineFinishSlotReleaseSink.resetCounters();
        }
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String resource,
            String jobName)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment environment =
                engineClient.createExecutionContext(
                        TestUtils.getResource(resource), jobConfig, seaTunnelConfig);
        ClientJobProxy proxy = environment.execute();
        log.info("Submitted job {} ({})", jobName, proxy.getJobId());
        return proxy;
    }

    private static List<SubPlan> pipelinesOf(HazelcastInstanceImpl node, long jobId) {
        SeaTunnelServer server = node.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        Assertions.assertNotNull(
                jobMaster, "The holder job master must still be attached to its job " + jobId);
        return jobMaster.getPhysicalPlan().getPipelineList();
    }

    /**
     * Asserts that {@code job} keeps reporting PENDING and that {@code holder} keeps RUNNING for
     * the whole window, so a transient admission followed by a failure cannot satisfy the
     * assertion.
     */
    private static void assertStaysPending(
            ClientJobProxy job, ClientJobProxy holder, long windowMillis, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + windowMillis;
        while (System.currentTimeMillis() < deadline) {
            Assertions.assertEquals(JobStatus.PENDING, job.getJobStatus(), message);
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    holder.getJobStatus(),
                    "The holder job must keep RUNNING while the queued job waits");
            Thread.sleep(200L);
        }
    }

    private static void cancelQuietly(ClientJobProxy job) {
        if (job == null) {
            return;
        }
        try {
            if (!job.getJobStatus().isEndState()) {
                job.cancelJob();
            }
        } catch (Throwable ignored) {
            // The assertion path already failed; cleanup must not hide the original failure.
        }
    }
}
