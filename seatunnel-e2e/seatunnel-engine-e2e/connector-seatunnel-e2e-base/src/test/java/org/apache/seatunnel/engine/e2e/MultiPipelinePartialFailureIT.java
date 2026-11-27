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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage for what a Zeta job does when only one of its pipelines fails at runtime.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A job is not a pipeline: one
 * submission can carry several independent pipeline chains, and a runtime task failure only ever
 * concerns one of them. The failure handling therefore has two halves that must both happen, and
 * neither is visible in the job status alone: the job has to reach a terminal {@code FAILED} (not
 * sit in {@code FAILING}), and every sibling pipeline still running healthy work has to be canceled
 * by the job-level failure path ({@code PhysicalPlan#addPipelineEndCallback} -> {@code
 * updateJobState(FAILING)} -> {@code stateProcess} -> {@code SubPlan#cancelPipeline}). Getting the
 * second half wrong is silent and expensive: the job reports {@code FAILED} to the client while the
 * healthy pipeline keeps running as a zombie, keeps its fixed slots, and keeps writing its sink
 * until the worker restarts. Getting the first half wrong is equally silent: a streaming sibling
 * pipeline never completes on its own, so the job would stay in {@code FAILING} forever and the
 * client would never see a terminal state.
 *
 * <p><b>Why the existing coverage does not reach it.</b> Every runtime-failure E2E in this suite
 * uses a single-pipeline job, so the job-level failure path never has a healthy sibling to cancel:
 * {@code SinkErrorToMysqlIT}, {@code TransformErrorToMysqlIT} and {@code
 * JobClientJobProxyIT#testMultiTableSinkFailedWithThrowable} all fail one pipeline and assert only
 * the process exit code or the message. The multi-pipeline E2Es cover other contracts: {@code
 * ClusterFaultToleranceTwoPipelineIT} kills a worker during a batch restore, {@code
 * MultiPipelineSavepointRestoreIT} takes and restores per-pipeline savepoints, and {@code
 * MultiTableMetricsIT} reads per-table metrics. None of them injects a runtime failure into one
 * pipeline of a multi-pipeline job.
 *
 * <p>This test makes the failure deterministic instead of racy: the failing pipeline's sink waits
 * three seconds before throwing on its first write, and the test first observes both pipelines
 * {@code RUNNING}, so the sibling is a genuinely live pipeline when the failure arrives. It then
 * asserts the terminal job state and error, the per-pipeline outcomes (exactly one {@code FAILED},
 * the sibling {@code CANCELED}), and - as the resource half of the contract - that a fresh job with
 * the same five-slot footprint can still be scheduled on the five fixed slots the failed job held.
 */
@Slf4j
public class MultiPipelinePartialFailureIT {

    private static final String FAILING_JOB_CONF =
            "multi-pipeline-partial-failure/stream_two_pipeline_one_sink_fails.conf";

    private static final String CAPACITY_PROBE_CONF =
            "multi-pipeline-partial-failure/stream_two_pipeline_healthy.conf";

    /** The failing job carries two independent pipelines. */
    private static final int PIPELINE_COUNT = 2;

    /**
     * Fixed slots the failing job needs: two for the {@code Console} pipeline (split enumerator
     * plus the fused source/writer task) and three for the {@code InMemory} pipeline (its
     * aggregated committer is a third task group). The capacity probe below deliberately needs the
     * same five.
     */
    private static final int FIXED_SLOTS = 5;

    private static final int PIPELINE_ID_START = 1;

    @Test
    public void testOneFailingPipelineStopsTheJobAndReleasesItsSlots() throws Exception {
        String clusterName =
                TestUtils.getClusterName(
                        "MultiPipelinePartialFailureIT_testOneFailingPipelineStopsTheJob");
        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy failingJob = null;
        ClientJobProxy capacityProbe = null;

        SeaTunnelConfig seaTunnelConfig = newSeaTunnelConfig(clusterName);
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            final HazelcastInstanceImpl testNode = node;
            engineClient = new SeaTunnelClient(newClientConfig(clusterName));

            failingJob =
                    submit(engineClient, seaTunnelConfig, FAILING_JOB_CONF, "two_pipeline_failure");
            long jobId = failingJob.getJobId();

            // Both pipelines must be live before the injected failure fires, otherwise the
            // sibling-cancellation half of the contract is not exercised at all.
            awaitPipelineStatus(testNode, jobId, PipelineStatus.RUNNING, PIPELINE_COUNT);

            JobStatus terminalStatus = awaitTerminalStatus(failingJob);
            Assertions.assertEquals(
                    JobStatus.FAILED,
                    terminalStatus,
                    "A job whose pipeline failed at runtime must end FAILED, not "
                            + terminalStatus);

            JobResult jobResult = failingJob.getJobResultCache();
            Assertions.assertNotNull(jobResult, "The failed job must expose its JobResult");
            Assertions.assertEquals(JobStatus.FAILED, jobResult.getStatus());
            Assertions.assertNotNull(
                    jobResult.getError(),
                    "The failed job must report the error of the pipeline that failed");
            log.info("Job {} failed with: {}", jobId, jobResult.getError());

            // The failure belongs to one pipeline; the healthy sibling must be canceled by the
            // job-level failure path instead of being left running.
            awaitOneFailedOneCanceledPipeline(testNode, jobId);

            // Resource half: the failed job must have returned every fixed slot it held. The probe
            // needs the same five slots, so a slot retained by a zombie sibling leaves it PENDING.
            capacityProbe =
                    submit(engineClient, seaTunnelConfig, CAPACITY_PROBE_CONF, "capacity_probe");
            final ClientJobProxy probe = capacityProbe;
            long probeJobId = capacityProbe.getJobId();
            try {
                Awaitility.await()
                        .atMost(120, TimeUnit.SECONDS)
                        .ignoreExceptions()
                        .untilAsserted(
                                () ->
                                        Assertions.assertEquals(
                                                JobStatus.RUNNING,
                                                probe.getJobStatus(),
                                                "A five-slot probe job cannot be scheduled after"
                                                        + " the failed job ended; slots are still"
                                                        + " held"));
            } catch (AssertionError e) {
                log.error(
                        "Capacity probe job {} (status {}) could not be scheduled after failed job"
                                + " {}",
                        probeJobId,
                        probe.getJobStatus(),
                        jobId,
                        e);
                throw e;
            }
        } finally {
            cancelQuietly(capacityProbe);
            cancelQuietly(failingJob);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static SeaTunnelConfig newSeaTunnelConfig(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        // The REST endpoint is not used here and would otherwise contend for a fixed port.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        // Fixed slots make a leaked slot observable: with dynamic slots the worker would hand out
        // capacity for the probe job even if the failed job never returned its own slots. WAIT
        // keeps the probe queued (instead of failing it) while the slots are still held.
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(FIXED_SLOTS);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        return seaTunnelConfig;
    }

    private static ClientConfig newClientConfig(String clusterName) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        return clientConfig;
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String confFile,
            String jobName)
            throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(TestUtils.getResource(confFile), jobConfig, seaTunnelConfig)
                .execute();
    }

    private static JobStatus awaitTerminalStatus(ClientJobProxy job) {
        CompletableFuture<JobStatus> completeFuture =
                CompletableFuture.supplyAsync(job::waitForJobComplete);
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completeFuture.isDone(),
                                        "Job "
                                                + job.getJobId()
                                                + " never reached a terminal state; a job stuck in"
                                                + " FAILING because a sibling pipeline was not"
                                                + " canceled looks exactly like this"));
        return completeFuture.getNow(JobStatus.UNKNOWABLE);
    }

    private static void awaitPipelineStatus(
            HazelcastInstanceImpl node, long jobId, PipelineStatus expected, int pipelineCount) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            List<PipelineStatus> statuses = pipelineStatuses(node, jobId);
                            Assertions.assertEquals(
                                    pipelineCount,
                                    statuses.size(),
                                    "Expected one state entry per deployed pipeline: " + statuses);
                            for (PipelineStatus status : statuses) {
                                Assertions.assertEquals(
                                        expected,
                                        status,
                                        "Pipelines of job "
                                                + jobId
                                                + " must all be "
                                                + expected
                                                + " before the injected failure fires: "
                                                + statuses);
                            }
                        });
    }

    private static void awaitOneFailedOneCanceledPipeline(HazelcastInstanceImpl node, long jobId) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            List<PipelineStatus> statuses = pipelineStatuses(node, jobId);
                            Assertions.assertEquals(
                                    PIPELINE_COUNT,
                                    statuses.size(),
                                    "Expected one state entry per pipeline: " + statuses);
                            long failed =
                                    statuses.stream()
                                            .filter(status -> status == PipelineStatus.FAILED)
                                            .count();
                            long canceled =
                                    statuses.stream()
                                            .filter(status -> status == PipelineStatus.CANCELED)
                                            .count();
                            Assertions.assertEquals(
                                    1,
                                    failed,
                                    "Exactly the pipeline with the injected failure must end"
                                            + " FAILED: "
                                            + statuses);
                            Assertions.assertEquals(
                                    1,
                                    canceled,
                                    "The healthy sibling pipeline must be canceled by the job-level"
                                            + " failure path instead of being left RUNNING or"
                                            + " ending on its own: "
                                            + statuses);
                        });
    }

    /** Reads the per-pipeline states that are still present in the running job state map. */
    private static List<PipelineStatus> pipelineStatuses(HazelcastInstanceImpl node, long jobId) {
        IMap<Object, Object> runningJobState = node.getMap(Constant.IMAP_RUNNING_JOB_STATE);
        List<PipelineStatus> statuses = new ArrayList<>();
        for (int pipelineId = PIPELINE_ID_START;
                pipelineId < PIPELINE_ID_START + PIPELINE_COUNT;
                pipelineId++) {
            Object state = runningJobState.get(new PipelineLocation(jobId, pipelineId));
            if (state instanceof PipelineStatus) {
                statuses.add((PipelineStatus) state);
            }
        }
        return statuses;
    }

    private static void cancelQuietly(ClientJobProxy job) {
        if (job == null) {
            return;
        }
        try {
            if (job.getJobStatus() != null && !job.getJobStatus().isEndState()) {
                job.cancelJob();
            }
        } catch (Exception e) {
            log.warn("Failed to cancel job {} during cleanup: {}", job.getJobId(), e.getMessage());
        }
    }
}
