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
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * E2E guard for the terminal-source precondition of a checkpoint restore.
 *
 * <p><b>Contract under test.</b> {@code --restore-with-checkpoint} copies a source job's latest
 * completed checkpoint into a new destination job. The source job id therefore addresses a
 * recoverable, historical snapshot: {@code CheckpointRestoreValidator} must reject a source job
 * that is still live (RUNNING, or waiting in the pending queue), because a running job's
 * checkpoints are still being produced and consumed by that job. A rejected attempt must be a
 * complete no-op for both sides: the running source must keep running and advancing its checkpoint
 * sequence, and the destination job id must stay unused, so the same id can later restore from the
 * source's final checkpoint.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> The validation reads the source
 * status through {@code CoordinatorService#resolveSourceJobStatusForRestore}, which checks the
 * pending queue, the running JobMaster map and then the persisted job-state map. If any of those
 * lookups regresses to returning {@code null}, the validator silently allows the restore: a second
 * job then starts from a checkpoint of the live source and consumes the same offsets while the
 * source keeps writing. Nothing about the source job's status or the client's view changes, so the
 * duplicate consumption is only visible downstream. Conversely, a rejection path that has already
 * created JobMaster/checkpoint-counter state for the destination id wedges a later, valid restore
 * of that same id. Both directions are invisible to the in-process unit test, which drives the
 * validator with a stubbed status lambda and never runs a real JobMaster or checkpoint storage.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code CheckpointRestoreValidatorTest}
 * covers the predicate itself. {@code CheckpointRestoreWithStopIT} restores only after the source
 * job has been stopped (the documented happy path), and {@code SavepointClusterRestartRestoreIT}
 * restores from a savepoint on a cold cluster. No E2E currently submits a checkpoint restore while
 * the source job is running, so the status resolution against a real master and the no-side-effect
 * guarantee are unpinned.
 *
 * <p><b>Verification.</b> The test runs a real split master/worker cluster with local-file
 * checkpoint storage, waits for the source job to complete a periodic checkpoint, and then submits
 * a CHECKPOINT restore with a fresh destination id. The submission must fail with the {@code
 * terminal source job} message naming the live status; the source must stay RUNNING and produce a
 * newer checkpoint; the destination id must have no JobMaster. The source is then cancelled with
 * checkpoints retained, and the same destination id must restore from the retained checkpoint,
 * reach RUNNING, and continue the checkpoint id sequence above the source's last checkpoint.
 * Reusing that id is deliberate: it proves the rejected attempt left no reservation behind.
 */
@Slf4j
public class CheckpointRestoreRunningSourceIT {

    private static final String CONF_FILE =
            "/restore-source-guard/stream_fake_to_console_restore_source_guard.conf";

    /** The fixture produces exactly one pipeline, so checkpoint lookups can pin its id. */
    private static final int PIPELINE_ID = 1;

    private static final long STATE_TIMEOUT_SECONDS = 180;

    private static final String CHECKPOINT_STORAGE_ROOT = "/tmp/seatunnel/checkpoint_snapshot/";

    @Test
    public void testCheckpointRestoreOfRunningSourceIsRejectedAndSourceStaysIntact()
            throws Exception {
        String testClusterName = "CheckpointRestoreRunningSourceIT_" + System.currentTimeMillis();
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy sourceJob = null;
        ClientJobProxy restoredJob = null;
        long sourceJobId = -1L;
        long destinationJobId = JobIdGenerator.newJobId();

        SeaTunnelConfig masterConfig = newSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerConfig = newSeaTunnelConfig(testClusterName);
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);
            awaitClusterSize(masterNode, 2);
            Common.setDeployMode(DeployMode.CLIENT);

            String confPath = TestUtils.getResource(CONF_FILE);
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();

            JobConfig sourceConfig = new JobConfig();
            sourceConfig.setName("restore-running-source-guard-source");
            sourceJob =
                    engineClient
                            .createExecutionContext(confPath, sourceConfig, masterConfig)
                            .execute();
            sourceJobId = sourceJob.getJobId();
            awaitJobStatus(sourceJob, JobStatus.RUNNING);

            // A completed periodic checkpoint is what makes the source a meaningful (and
            // therefore dangerous) restore source: without one the restore would fail later for
            // an unrelated reason and the precondition under test would not be exercised.
            JobPipelineCheckpointData baseline =
                    awaitCheckpoint(
                            jobClient,
                            sourceJobId,
                            RestoreMode.CHECKPOINT,
                            0L,
                            "periodic checkpoint before the rejected restore");

            // The restore must be rejected while the source is live.
            final long liveSourceJobId = sourceJobId;
            final long rejectedDestinationJobId = destinationJobId;
            final SeaTunnelClient rejectClient = engineClient;
            JobConfig rejectedRestoreConfig = new JobConfig();
            rejectedRestoreConfig.setName("restore-running-source-guard-rejected");
            Throwable rejection =
                    Assertions.assertThrows(
                            Exception.class,
                            () ->
                                    rejectClient
                                            .restoreFromCheckpointExecutionContext(
                                                    confPath,
                                                    null,
                                                    rejectedRestoreConfig,
                                                    masterConfig,
                                                    liveSourceJobId,
                                                    rejectedDestinationJobId)
                                            .execute());
            String rejectionMessages = collectMessages(rejection);
            Assertions.assertTrue(
                    rejectionMessages.contains("terminal source job"),
                    "A checkpoint restore of a live source must be rejected with the terminal-source"
                            + " precondition, but the failure was: "
                            + rejectionMessages);
            Assertions.assertTrue(
                    rejectionMessages.contains(JobStatus.RUNNING.name()),
                    "The rejection must name the live source status so the operator can act on it: "
                            + rejectionMessages);

            // The live source is untouched: still RUNNING, still checkpointing, and owning its
            // checkpoints.
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    sourceJob.getJobStatus(),
                    "A rejected restore attempt must not change the running source job status");
            JobPipelineCheckpointData afterRejection =
                    awaitCheckpoint(
                            jobClient,
                            sourceJobId,
                            RestoreMode.CHECKPOINT,
                            baseline.getCheckpointId(),
                            "periodic checkpoint after the rejected restore");

            // The destination id must not have been admitted anywhere. A leaked JobMaster here is
            // what would wedge the later valid restore of the same id.
            CoordinatorService coordinator = coordinatorOf(masterNode);
            Assertions.assertNull(
                    coordinator.getJobMaster(destinationJobId),
                    "A rejected restore must not leave a JobMaster for destination job "
                            + destinationJobId);

            // Stop the source. Retained checkpoints are its final recovery point; the earlier
            // restores must not have disturbed it.
            engineClient.cancelJob(sourceJobId);
            awaitJobStatus(sourceJob, JobStatus.CANCELED);

            JobConfig restoreConfig = new JobConfig();
            restoreConfig.setName("restore-running-source-guard-restored");
            restoredJob =
                    engineClient
                            .restoreFromCheckpointExecutionContext(
                                    confPath,
                                    null,
                                    restoreConfig,
                                    masterConfig,
                                    sourceJobId,
                                    // Deliberately the same id the rejected attempt used: if that
                                    // attempt had reserved any state for it, this restore fails.
                                    destinationJobId)
                            .execute();
            Assertions.assertEquals(
                    destinationJobId,
                    restoredJob.getJobId(),
                    "A checkpoint restore must use the requested destination job id");
            awaitJobStatus(restoredJob, JobStatus.RUNNING);

            JobPipelineCheckpointData afterRestore =
                    awaitCheckpoint(
                            jobClient,
                            destinationJobId,
                            RestoreMode.CHECKPOINT,
                            afterRejection.getCheckpointId(),
                            "periodic checkpoint of the restored job");
            Assertions.assertTrue(
                    afterRestore.getCheckpointId() > afterRejection.getCheckpointId(),
                    "The restored job must continue the source checkpoint sequence above "
                            + afterRejection.getCheckpointId()
                            + ", but its latest checkpoint is "
                            + afterRestore.getCheckpointId());
            Assertions.assertEquals(
                    PIPELINE_ID,
                    afterRestore.getPipelineId(),
                    "The restored fixture must keep its single pipeline");
        } finally {
            if (restoredJob != null) {
                try {
                    engineClient.cancelJob(restoredJob.getJobId());
                } catch (Throwable t) {
                    log.warn("Failed to cancel restored job {}", restoredJob.getJobId(), t);
                }
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (sourceJobId > 0) {
                FileUtils.deleteFile(CHECKPOINT_STORAGE_ROOT + sourceJobId);
            }
            FileUtils.deleteFile(CHECKPOINT_STORAGE_ROOT + destinationJobId);
        }
    }

    private static SeaTunnelConfig newSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static CoordinatorService coordinatorOf(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Assertions.assertNotNull(server, "SeaTunnelServer service is not available on the master");
        return server.getCoordinatorService();
    }

    private static String collectMessages(Throwable throwable) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = throwable;
                current != null && !messages.toString().contains(current.getClass().getName());
                current = current.getCause()) {
            messages.append(current.getClass().getName()).append(": ");
            if (current.getMessage() != null) {
                messages.append(current.getMessage());
            }
            messages.append(" | ");
            if (current.getCause() == current) {
                break;
            }
        }
        return messages.toString();
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expectedSize) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSize, node.getCluster().getMembers().size()));
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Unexpected job status for job " + jobProxy.getJobId()));
    }

    /**
     * Waits until the latest checkpoint of the fixture pipeline is above {@code
     * minExclusiveCheckpointId} and returns it.
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
                                    "No checkpoint for "
                                            + description
                                            + " of job "
                                            + jobId
                                            + ": "
                                            + checkpoints);
                            Assertions.assertTrue(
                                    newest.getCheckpointId() > minExclusiveCheckpointId,
                                    "Waiting for a checkpoint above id "
                                            + minExclusiveCheckpointId
                                            + " for "
                                            + description
                                            + ", but the latest is "
                                            + newest.getCheckpointId());
                            latest.set(newest);
                        });
        return latest.get();
    }
}
