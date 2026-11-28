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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end coverage for the boundary between the engine's automatic checkpoint cleanup and the
 * operator-managed savepoint.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> Zeta keeps savepoints in the same
 * storage layout as periodic checkpoints, under {@code <checkpoint-namespace>/<job-id>/}. A
 * savepoint restore keeps the source job id, so the restored run writes its own periodic
 * checkpoints into the very directory that holds the savepoint it was started from. When that
 * restored run later reaches a terminal state, the engine's cleanup path deletes checkpoint data by
 * job id only ({@code CheckpointStorage#deleteCheckpoint(String)}), which cannot tell a savepoint
 * apart from a periodic checkpoint. The savepoint documented as "managed by the operator, kept
 * until manually deleted" therefore disappears without any operator action, and the next restore of
 * that savepoint fails with missing state. Nothing in the running job looks wrong: the restored job
 * still reports RUNNING and checkpoints still complete, so the damage is only visible when the
 * savepoint is needed again.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointRestoreIT} stops the job
 * right after the restore and asserts data continuity, never the storage lifecycle afterwards.
 * {@code SavepointClusterRestartRestoreIT} restarts the cluster and asserts that the restored run
 * continues the checkpoint id sequence, and it cancels the restored job as its very last action
 * without inspecting storage again. {@code CheckpointRestoreWithStopIT} only covers checkpoint
 * ({@code --restore-with-checkpoint}) restores. No test drives the sequence "stop with savepoint ->
 * restore -> let the restored run checkpoint again -> cancel the restored run" and then asks
 * storage whether the savepoint is still there.
 *
 * <p>The scenario is deliberately kept to a single pipeline ({@code parallelism = 1}) so a
 * savepoint is exactly one storage entry and "was the savepoint kept or deleted" cannot be confused
 * with cross-pipeline bookkeeping. The final re-restore is the real assertion: it proves the
 * savepoint bytes are still usable, not merely that a directory entry survived.
 */
@Slf4j
public class SavepointSurvivesRestoredJobCancelIT {

    private static final String CONF_FILE = "stream_fake_to_console_savepoint_retention.conf";
    private static final String JOB_NAME = "SavepointSurvivesRestoredJobCancelIT";

    /** Single pipeline produced by {@link #CONF_FILE} (one FakeSource operator, parallelism 1). */
    private static final int PIPELINE_ID = 1;

    private static final long STATE_TIMEOUT_SECONDS = 180;

    @Test
    public void testSavepointSurvivesCancelOfItsRestoredRun() throws Exception {
        String testClusterName =
                "SavepointSurvivesRestoredJobCancelIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint plays no role here and a fixed port would make concurrent suites flaky.
        clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            Common.setDeployMode(DeployMode.CLIENT);
            String confPath = TestUtils.getResource(CONF_FILE);

            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(JOB_NAME);
            ClientJobProxy originalJob =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, clusterConfig)
                            .execute();
            long jobId = originalJob.getJobId();
            awaitJobStatus(originalJob, JobStatus.RUNNING);

            // A periodic checkpoint before the savepoint proves the checkpoint pipeline is live and
            // gives the later cleanup real periodic data to delete for this job id.
            awaitCheckpoint(
                    jobClient, jobId, RestoreMode.CHECKPOINT, 0L, "periodic before savepoint");

            awaitSavepoint(engineClient, jobId);
            awaitJobStatus(originalJob, JobStatus.SAVEPOINT_DONE);
            JobPipelineCheckpointData savepoint =
                    awaitCheckpoint(jobClient, jobId, RestoreMode.SAVEPOINT, 0L, "savepoint");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    savepoint.getCheckpointType(),
                    "Stop-with-savepoint must persist a savepoint checkpoint");
            long savepointId = savepoint.getCheckpointId();

            // Restore the savepoint. Zeta keeps the source job id, so the restored run continues to
            // write periodic checkpoints into the same job directory that holds the savepoint.
            JobConfig restoreJobConfig = new JobConfig();
            restoreJobConfig.setName(JOB_NAME);
            ClientJobProxy restoredJob =
                    engineClient
                            .restoreExecutionContext(
                                    confPath, restoreJobConfig, clusterConfig, jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId,
                    restoredJob.getJobId(),
                    "A savepoint restore must keep the source job id");
            awaitJobStatus(restoredJob, JobStatus.RUNNING);
            JobPipelineCheckpointData checkpointAfterRestore =
                    awaitCheckpoint(
                            jobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            savepointId,
                            "periodic after restore");
            Assertions.assertTrue(
                    checkpointAfterRestore.getCheckpointId() > savepointId,
                    "The restored run must checkpoint again above the savepoint id, savepoint="
                            + savepointId
                            + ", next checkpoint="
                            + checkpointAfterRestore.getCheckpointId());

            // Cancel the restored run. Reaching CANCELED triggers the automatic checkpoint cleanup
            // for this job id, which is exactly the code path that must not touch the savepoint.
            engineClient.cancelJob(jobId);
            awaitJobStatus(restoredJob, JobStatus.CANCELED);
            awaitPeriodicCheckpointsRemoved(jobClient, jobId);

            List<JobPipelineCheckpointData> survivingSavepoints =
                    jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
            JobPipelineCheckpointData survivingSavepoint =
                    survivingSavepoints.stream()
                            .filter(data -> data.getPipelineId() == PIPELINE_ID)
                            .filter(data -> data.getCheckpointId() == savepointId)
                            .findFirst()
                            .orElse(null);
            Assertions.assertNotNull(
                    survivingSavepoint,
                    "Cancelling the run restored from savepoint "
                            + savepointId
                            + " of job "
                            + jobId
                            + " must not delete the savepoint. Savepoints left for job "
                            + jobId
                            + ": "
                            + survivingSavepoints);
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    survivingSavepoint.getCheckpointType(),
                    "The surviving restore point must still be a savepoint");

            // The storage entry alone is not enough: the savepoint has to be usable. Restoring it a
            // second time proves the cleanup left the serialized state intact instead of deleting
            // or truncating it.
            JobConfig secondRestoreJobConfig = new JobConfig();
            secondRestoreJobConfig.setName(JOB_NAME);
            ClientJobProxy secondRestore =
                    engineClient
                            .restoreExecutionContext(
                                    confPath, secondRestoreJobConfig, clusterConfig, jobId)
                            .execute();
            awaitJobStatus(secondRestore, JobStatus.RUNNING);
            engineClient.cancelJob(jobId);
            awaitJobStatus(secondRestore, JobStatus.CANCELED);

            log.info(
                    "Savepoint {} of job {} survived the cancel of its restored run and restored again",
                    savepointId,
                    jobId);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static void awaitSavepoint(SeaTunnelClient engineClient, long jobId)
            throws InterruptedException, ExecutionException {
        // SeaTunnelClient#savePointJob blocks until the savepoint resolves. Run it on a worker so a
        // job stuck in DOING_SAVEPOINT surfaces as a bounded test failure instead of a hang.
        CompletableFuture<Void> savepoint =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        try {
            savepoint.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError(
                    "Stop-with-savepoint for job "
                            + jobId
                            + " did not complete within "
                            + STATE_TIMEOUT_SECONDS
                            + "s",
                    e);
        }
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobStatus status = jobProxy.getJobStatus();
                            Assertions.assertEquals(
                                    expectedStatus,
                                    status,
                                    "Unexpected job status for job " + jobProxy.getJobId());
                        });
    }

    /**
     * Waits until the automatic cleanup has removed the periodic checkpoints of the job. Both the
     * correct and the broken behaviour end with no periodic checkpoint left, so this is only the
     * signal that the cleanup has run; what it kept is asserted separately.
     */
    private static void awaitPeriodicCheckpointsRemoved(JobClient jobClient, long jobId) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        jobClient
                                                .getCheckpointData(jobId, RestoreMode.CHECKPOINT)
                                                .isEmpty(),
                                        "Automatic checkpoint cleanup did not remove the periodic "
                                                + "checkpoints of cancelled job "
                                                + jobId));
    }

    /**
     * Waits until the latest checkpoint of the single pipeline for the given restore mode is above
     * {@code minExclusiveCheckpointId} and returns it.
     */
    private static JobPipelineCheckpointData awaitCheckpoint(
            JobClient jobClient,
            long jobId,
            RestoreMode restoreMode,
            long minExclusiveCheckpointId,
            String description) {
        AtomicReference<JobPipelineCheckpointData> latest = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, restoreMode);
                            JobPipelineCheckpointData newest =
                                    checkpoints.stream()
                                            .filter(data -> data.getPipelineId() == PIPELINE_ID)
                                            .max(
                                                    Comparator.comparingLong(
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
                            latest.set(newest);
                        });
        return latest.get();
    }
}
