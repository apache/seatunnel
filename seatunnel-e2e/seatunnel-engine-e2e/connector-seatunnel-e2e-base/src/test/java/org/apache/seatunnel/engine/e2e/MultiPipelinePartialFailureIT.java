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
import org.apache.seatunnel.common.utils.FileUtils;
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
import org.apache.seatunnel.engine.e2e.partialfailure.PartialFailureGate;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
 * <p><b>How the failure is injected.</b> The failing pipeline writes into {@link
 * PartialFailureGateSink}, whose writer parks inside {@code write()} on the hold key the test armed
 * before submitting the job ({@link PartialFailureGate}). The test releases that key only after it
 * observed three things: both pipelines {@code RUNNING}, the failing writer parked at the injection
 * point, and the healthy sibling's sink file growing. So the failure arrives <em>after</em>
 * readiness was observed instead of after a fixed delay, and a slow or descheduled test thread can
 * no longer make the sibling die before the test saw it alive. The test then asserts the terminal
 * job state and error, the per-pipeline outcomes (exactly one {@code FAILED}, the sibling {@code
 * CANCELED}), and - as the resource half of the contract - that a fresh job with a larger
 * fixed-slot footprint can still be scheduled on the five fixed slots the failed job held.
 */
@Slf4j
public class MultiPipelinePartialFailureIT {

    /** Template of the job under test; the {@code dynamic_*} placeholders are rendered per run. */
    private static final String FAILING_JOB_TEMPLATE =
            "multi-pipeline-partial-failure/stream_two_pipeline_one_sink_fails_template.conf";

    /** Capacity probe job: two healthy pipelines that need every fixed slot of the cluster. */
    private static final String CAPACITY_PROBE_CONF =
            "multi-pipeline-partial-failure/stream_two_pipeline_healthy.conf";

    /** Placeholder in the template that is substituted with the per-run hold key. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /** Placeholder in the template that is substituted with the healthy sink output directory. */
    private static final String DYNAMIC_OUTPUT_PATH = "dynamic_output_path";

    /** The failing job carries two independent pipelines. */
    private static final int PIPELINE_COUNT = 2;

    /**
     * Fixed slots of the in-JVM cluster. The failing job needs four of them: two per pipeline (the
     * split enumerator plus the fused source-and-sink task) because its test sink has no committer.
     * The capacity probe below keeps a five-slot footprint instead (its {@code InMemory} pipeline
     * adds an aggregated committer task group), so it can only start once every slot the failed job
     * held is free again and a single leaked slot leaves it PENDING.
     */
    private static final int FIXED_SLOTS = 5;

    private static final int PIPELINE_ID_START = 1;

    /** Upper bound to wait for both pipelines to be deployed and RUNNING. */
    private static final long PIPELINE_RUNNING_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for the failing writer to reach and park at its injection point. */
    private static final long WRITER_PARKED_TIMEOUT_SECONDS = 60L;

    /**
     * Upper bound to wait for the healthy sibling to write another row while the failure is held.
     */
    private static final long SIBLING_PROGRESS_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the job to reach a terminal state after the failure was injected. */
    private static final long TERMINAL_STATUS_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for the per-pipeline outcomes of the failed job. */
    private static final long PIPELINE_OUTCOME_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for the five-slot capacity probe to be scheduled. */
    private static final long CAPACITY_PROBE_TIMEOUT_SECONDS = 120L;

    @Test
    public void testOneFailingPipelineStopsTheJobAndReleasesItsSlots() throws Exception {
        String testCaseName = "testOneFailingPipelineStopsTheJobAndReleasesItsSlots";
        String clusterName =
                TestUtils.getClusterName("MultiPipelinePartialFailureIT_" + testCaseName);
        String holdKey = testCaseName + "_" + System.nanoTime();

        // Armed before the job is submitted: a writer that cannot find its gate fails immediately
        // instead of parking at the injection point the test wants to observe.
        PartialFailureGate.arm(holdKey);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy failingJob = null;
        ClientJobProxy capacityProbe = null;
        ExecutorService completionWaiter = null;
        Future<JobStatus> terminalFuture = null;

        SeaTunnelConfig seaTunnelConfig = newSeaTunnelConfig(clusterName);
        try {
            String fixtureDir = prepareFixtureDirectory(testCaseName);
            String healthySinkOutputPath = fixtureDir + File.separator + "healthy_sink_output";
            FileUtils.createNewDir(healthySinkOutputPath);
            String failingJobConfPath = createJobConfig(fixtureDir, holdKey, healthySinkOutputPath);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            final HazelcastInstanceImpl testNode = node;
            engineClient = new SeaTunnelClient(newClientConfig(clusterName));

            failingJob =
                    submit(
                            engineClient,
                            seaTunnelConfig,
                            failingJobConfPath,
                            "two_pipeline_failure");
            long jobId = failingJob.getJobId();

            // Readiness barrier 1: both pipelines must be live before the injected failure fires,
            // otherwise the sibling-cancellation half of the contract is not exercised at all.
            awaitPipelineStatus(testNode, jobId, PipelineStatus.RUNNING, PIPELINE_COUNT);

            // Readiness barrier 2: the failing writer reached its first row and is parked there, so
            // the failure provably has not fired yet, however slow this thread was until now.
            awaitWriterParked(holdKey);

            // Readiness barrier 3: the healthy sibling wrote at least one more row while the
            // failure was still held, so it is really progressing and not merely reported RUNNING.
            long healthyRowsBeforeRelease = healthyRowsWritten(healthySinkOutputPath);
            awaitSiblingProgress(healthySinkOutputPath, healthyRowsBeforeRelease);

            // Only now inject the failure. The blocking client call is already registered so the
            // client-visible terminal transition cannot be missed.
            completionWaiter =
                    Executors.newSingleThreadExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(runnable, "partial-failure-job-completion");
                                thread.setDaemon(true);
                                return thread;
                            });
            terminalFuture = completionWaiter.submit(failingJob::waitForJobComplete);
            log.warn(
                    "Releasing hold key {}: both pipelines were RUNNING, the failing writer was"
                            + " parked and the healthy sibling had already written {} rows",
                    holdKey,
                    healthyRowsBeforeRelease);
            PartialFailureGate.release(holdKey);

            JobStatus terminalStatus = awaitTerminalStatus(failingJob, terminalFuture);
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
            // needs more slots than the failed job held, so a slot retained by a zombie sibling
            // leaves it PENDING.
            capacityProbe =
                    submit(
                            engineClient,
                            seaTunnelConfig,
                            TestUtils.getResource(CAPACITY_PROBE_CONF),
                            "capacity_probe");
            final ClientJobProxy probe = capacityProbe;
            long probeJobId = capacityProbe.getJobId();
            try {
                Awaitility.await()
                        .atMost(CAPACITY_PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
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
            // Release first: a parked writer must never outlive the test, because the task thread
            // would stay blocked. The gate's hard deadline is only the last resort.
            PartialFailureGate.clear(holdKey);
            cancelQuietly(capacityProbe);
            cancelQuietly(failingJob);
            if (terminalFuture != null) {
                // The completion waiter runs on the test's own daemon executor, so a timed-out
                // assertion can interrupt it instead of leaving a blocked thread behind.
                terminalFuture.cancel(true);
            }
            if (completionWaiter != null) {
                completionWaiter.shutdownNow();
            }
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

    /**
     * Creates the per-run directory that holds the rendered job config and the healthy sink output.
     *
     * @return absolute path of the fixture directory
     */
    private static String prepareFixtureDirectory(String testCaseName) {
        String fixtureDir =
                File.separator
                        + "tmp"
                        + File.separator
                        + "multi-pipeline-partial-failure"
                        + File.separator
                        + testCaseName;
        FileUtils.createNewDir(fixtureDir);
        return fixtureDir;
    }

    /**
     * Renders the job template into a config file for this run: the hold key binds the failing sink
     * writer to the armed gate, and the output path receives the healthy sibling's rows.
     *
     * @return absolute path of the rendered job config
     */
    private static String createJobConfig(
            String fixtureDir, String holdKey, String healthySinkOutputPath) throws IOException {
        Map<String, String> values = new HashMap<>();
        values.put(DYNAMIC_HOLD_KEY, holdKey);
        values.put(DYNAMIC_OUTPUT_PATH, healthySinkOutputPath);
        String jobConfigPath = fixtureDir + File.separator + "one_sink_fails.conf";
        TestUtils.createTestConfigFileFromTemplate(FAILING_JOB_TEMPLATE, values, jobConfigPath);
        return jobConfigPath;
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String jobConfigPath,
            String jobName)
            throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(jobConfigPath, jobConfig, seaTunnelConfig)
                .execute();
    }

    /**
     * Waits until the client's blocking completion call returned. The call runs on a dedicated
     * daemon executor owned by the test, so teardown can interrupt it on timeout instead of leaving
     * a common-pool thread blocked on a job that may never finish.
     */
    private static JobStatus awaitTerminalStatus(ClientJobProxy job, Future<JobStatus> completion)
            throws Exception {
        Awaitility.await()
                .atMost(TERMINAL_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completion.isDone(),
                                        "Job "
                                                + job.getJobId()
                                                + " never reached a terminal state; a job stuck in"
                                                + " FAILING because a sibling pipeline was not"
                                                + " canceled looks exactly like this"));
        return completion.get();
    }

    private static void awaitPipelineStatus(
            HazelcastInstanceImpl node, long jobId, PipelineStatus expected, int pipelineCount) {
        Awaitility.await()
                .atMost(PIPELINE_RUNNING_TIMEOUT_SECONDS, TimeUnit.SECONDS)
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

    /**
     * Waits until the failing pipeline's sink writer reached its first row and parked on the gate.
     * A non-zero parked-writer count is the observed proof that the injection point was reached, so
     * the failure can only fire after this method returned.
     */
    private static void awaitWriterParked(String holdKey) {
        Awaitility.await()
                .atMost(WRITER_PARKED_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        1,
                                        PartialFailureGate.parkedWriters(holdKey),
                                        "The failing sink writer has not reached its first row yet;"
                                                + " the injection point is not armed"));
    }

    /**
     * Waits until the healthy sibling's sink wrote at least one row more than before the failure
     * was released. The sink flushes every row, so this is a real progress signal of the sibling
     * pipeline rather than a status field the engine could report without doing any work.
     */
    private static void awaitSiblingProgress(
            String healthySinkOutputPath, long rowsAlreadyWritten) {
        Awaitility.await()
                .atMost(SIBLING_PROGRESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            long rowsWritten = healthyRowsWritten(healthySinkOutputPath);
                            Assertions.assertTrue(
                                    rowsWritten > rowsAlreadyWritten,
                                    "The healthy sibling pipeline wrote no row while the failure was"
                                            + " held; its sink output still has "
                                            + rowsWritten
                                            + " rows");
                        });
    }

    /**
     * Counts the rows the healthy sibling's sink has durably written so far.
     *
     * @return number of written rows, zero while the output is not readable yet
     */
    private static long healthyRowsWritten(String healthySinkOutputPath) {
        try {
            return FileUtils.getFileLineNumberFromDir(healthySinkOutputPath);
        } catch (Exception e) {
            // A writer may be creating or flushing its output file exactly while the test polls it.
            log.warn("Healthy sink output is not readable yet: {}", e.getMessage());
            return 0L;
        }
    }

    private static void awaitOneFailedOneCanceledPipeline(HazelcastInstanceImpl node, long jobId) {
        Awaitility.await()
                .atMost(PIPELINE_OUTCOME_TIMEOUT_SECONDS, TimeUnit.SECONDS)
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
