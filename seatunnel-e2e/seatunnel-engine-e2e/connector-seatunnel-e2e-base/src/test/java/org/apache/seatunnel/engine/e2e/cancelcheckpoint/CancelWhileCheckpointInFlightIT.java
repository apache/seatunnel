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

package org.apache.seatunnel.engine.e2e.cancelcheckpoint;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Regression guard for cancellation of a job whose checkpoint is unacknowledged: cancelling the job
 * must abort that checkpoint, never persist it even after the sink finally acknowledges it, drive
 * the job to CANCELED, and return the capacity for a replacement job.
 *
 * <p>Scenario: a streaming job runs on an in-process Zeta node. After a periodic checkpoint
 * completed, the test arms a test sink gate; the next checkpoint is frozen inside the sink writer's
 * {@code snapshotState(long)}, i.e. the barrier reached every task but the checkpoint is not
 * acknowledged and nothing is durable. The client then cancels the job while the writer is still
 * held.
 *
 * <p>Contract asserted here:
 *
 * <ol>
 *   <li>The accepted cancel moves the job to CANCELING while the checkpoint is still held and the
 *       abort reaches the checkpoint state machine instead of serializing behind an acknowledgement
 *       that may never come.
 *   <li>The aborted checkpoint is never persisted, even after the held writer acknowledges: the
 *       storage still contains only the last completed checkpoint. Persisting it would turn rows
 *       that the cancelled run never committed into a restore point, i.e. duplicate data after a
 *       later restore.
 *   <li>The job reaches CANCELED once the held task returns and stays CANCELED (a late
 *       acknowledgement must not revive it or its checkpoint).
 *   <li>The cancelled job's capacity is usable again: a replacement job with the same footprint
 *       reaches RUNNING, checkpoints and cancels cleanly.
 * </ol>
 *
 * <p>Existing coverage does not reach this window. {@code CancelDuringMasterFailoverIT} crashes the
 * master during an accepted cancel but no checkpoint is in flight; {@code
 * SavepointBusySourceBarrierIT} covers stop-with-savepoint while the source is busy, with the
 * checkpoint completing; {@code
 * SplitClusterFaultToleranceIT#testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck} kills a
 * worker, not a checkpoint. No existing engine E2E cancels a RUNNING job while a periodic
 * checkpoint is provably unacknowledged at the sink and then inspects storage and capacity.
 */
@Slf4j
public class CancelWhileCheckpointInFlightIT {

    private static final String JOB_TEMPLATE_FILE =
            "cancel-checkpoint-in-flight/stream_fake_to_checkpoint_hold_sink.conf";
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 30;
    private static final long JOB_STATE_TIMEOUT_SECONDS = 180;
    private static final long HOLD_WINDOW_SECONDS = 30;
    private static final long CANCEL_TIMEOUT_SECONDS = 90;

    @Test
    public void testCancelResolvesAndAbortsHeldCheckpoint() throws Exception {
        String testCaseName = "testCancelResolvesAndAbortsHeldCheckpoint";
        String testClusterName = "CancelWhileCheckpointInFlightIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        String holdKey = testCaseName + "_" + System.nanoTime();
        String jobConfigFile = createJobConfigFile(testCaseName, holdKey);

        try {
            SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            HazelcastInstanceImpl finalNode = node;
            Awaitility.await()
                    .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            CoordinatorService coordinatorService = getCoordinatorService(node);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig);
            ClientJobProxy jobProxy = jobExecutionEnv.execute();
            long jobId = jobProxy.getJobId();

            awaitJobStatus(coordinatorService, jobId, JobStatus.RUNNING);
            JobClient jobClient = engineClient.createJobClient();
            long completedBeforeCancel =
                    awaitCheckpoint(jobClient, jobId, RestoreMode.CHECKPOINT, 0L, "before cancel");
            log.info(
                    "Job {} completed checkpoint {} before the held checkpoint",
                    jobId,
                    completedBeforeCancel);

            // Freeze the next checkpoint in the sink, after the barrier reached every task.
            CheckpointInFlightHoldGate.arm(holdKey);
            Awaitility.await()
                    .atMost(HOLD_WINDOW_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            CheckpointInFlightHoldGate.holdingWriters(holdKey) > 0,
                                            "a checkpoint must reach the sink and be held"));
            long heldCheckpointId = CheckpointInFlightHoldGate.heldCheckpointId(holdKey);
            Assertions.assertTrue(
                    heldCheckpointId > completedBeforeCancel,
                    "The held checkpoint must be a new checkpoint above "
                            + completedBeforeCancel
                            + ", but was "
                            + heldCheckpointId);

            // Cancel while the checkpoint is unacknowledged. The cancel future is bounded so a
            // cancel that serializes behind the checkpoint surfaces as a test failure instead of
            // hanging the suite.
            CompletableFuture<Void> cancelFuture =
                    CompletableFuture.runAsync(() -> jobProxy.cancelJob());
            awaitCancelResolved(cancelFuture, jobId);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELING,
                                            coordinatorService.getJobStatus(jobId),
                                            "The accepted cancel must move the job to CANCELING"));
            Assertions.assertTrue(
                    CheckpointInFlightHoldGate.holdingWriters(holdKey) > 0,
                    "The cancel must reach the checkpoint while the sink still holds it");

            // The task is blocked in user code, so the terminal transition waits for it. What must
            // not wait is the checkpoint abort: once the writer acknowledges, the aborted
            // checkpoint must be discarded instead of completed and stored.
            CheckpointInFlightHoldGate.release(holdKey);
            Awaitility.await()
                    .atMost(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            coordinatorService.getJobStatus(jobId),
                                            "The cancelled job must terminate after its held task "
                                                    + "is released"));
            Awaitility.await()
                    .during(5, TimeUnit.SECONDS)
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED,
                                            coordinatorService.getJobStatus(jobId)));
            Assertions.assertEquals(
                    Set.of(completedBeforeCancel),
                    storedCheckpointIds(jobClient, jobId, RestoreMode.CHECKPOINT),
                    "The aborted checkpoint must never be persisted, and the last completed "
                            + "checkpoint must stay the only stored one");

            // The cancelled job's capacity must be usable again.
            String replacementHoldKey = testCaseName + "_replacement_" + System.nanoTime();
            String replacementConfigFile =
                    createJobConfigFile(testCaseName + "_replacement", replacementHoldKey);
            try {
                JobConfig replacementConfig = new JobConfig();
                replacementConfig.setName(testCaseName + "_replacement");
                ClientJobProxy replacement =
                        engineClient
                                .createExecutionContext(
                                        replacementConfigFile, replacementConfig, seaTunnelConfig)
                                .execute();
                long replacementId = replacement.getJobId();
                awaitJobStatus(coordinatorService, replacementId, JobStatus.RUNNING);
                awaitCheckpoint(
                        jobClient, replacementId, RestoreMode.CHECKPOINT, 0L, "replacement job");
                replacement.cancelJob();
                awaitJobStatus(coordinatorService, replacementId, JobStatus.CANCELED);
            } finally {
                FileUtils.deleteFile(replacementConfigFile);
            }
        } finally {
            CheckpointInFlightHoldGate.clear(holdKey);
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null && node.getLifecycleService().isRunning()) {
                node.shutdown();
            }
            FileUtils.deleteFile(jobConfigFile);
        }
    }

    private static void awaitCancelResolved(CompletableFuture<Void> cancelFuture, long jobId) {
        try {
            cancelFuture.get(CANCEL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            Assertions.fail(
                    "cancelJob() for job "
                            + jobId
                            + " did not resolve within "
                            + CANCEL_TIMEOUT_SECONDS
                            + "s while its checkpoint was held",
                    e);
        } catch (ExecutionException e) {
            Assertions.fail("cancelJob() for job " + jobId + " failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            Assertions.fail("Interrupted while cancelling job " + jobId, e);
        }
    }

    private static Set<Long> storedCheckpointIds(
            JobClient jobClient, long jobId, RestoreMode restoreMode) {
        List<JobPipelineCheckpointData> checkpoints =
                jobClient.getCheckpointData(jobId, restoreMode);
        return checkpoints.stream()
                .map(JobPipelineCheckpointData::getCheckpointId)
                .collect(Collectors.toSet());
    }

    private static void awaitJobStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(JOB_STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        coordinatorService.getJobStatus(jobId),
                                        "Unexpected job status for job " + jobId));
    }

    /**
     * Waits until the latest checkpoint of the single pipeline for the given restore mode is above
     * {@code minExclusiveCheckpointId} and returns its id.
     */
    private static long awaitCheckpoint(
            JobClient jobClient,
            long jobId,
            RestoreMode restoreMode,
            long minExclusiveCheckpointId,
            String description) {
        Awaitility.await()
                .atMost(JOB_STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, restoreMode);
                            JobPipelineCheckpointData newest =
                                    checkpoints.stream()
                                            .max(
                                                    java.util.Comparator.comparingLong(
                                                            JobPipelineCheckpointData
                                                                    ::getCheckpointId))
                                            .orElse(null);
                            Assertions.assertNotNull(
                                    newest,
                                    "No "
                                            + restoreMode
                                            + " checkpoint for "
                                            + description
                                            + " of job "
                                            + jobId
                                            + ": "
                                            + checkpoints);
                            Assertions.assertTrue(
                                    newest.getCheckpointId() > minExclusiveCheckpointId,
                                    "Waiting for a "
                                            + description
                                            + " checkpoint above id "
                                            + minExclusiveCheckpointId
                                            + " but the latest is "
                                            + newest.getCheckpointId());
                        });
        return jobClient.getCheckpointData(jobId, restoreMode).stream()
                .max(java.util.Comparator.comparingLong(JobPipelineCheckpointData::getCheckpointId))
                .map(JobPipelineCheckpointData::getCheckpointId)
                .orElseThrow(() -> new AssertionError("checkpoint disappeared: " + description));
    }

    private static String createJobConfigFile(String testCaseName, String holdKey)
            throws Exception {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);
        String jobConfigFile =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(JOB_TEMPLATE_FILE, valueMap, jobConfigFile);
        return jobConfigFile;
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }
}
