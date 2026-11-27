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
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.JobPipelineCheckpointData;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * End-to-end regression coverage for the checkpoint retention decision that Zeta makes when a job
 * is cancelled without a savepoint.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> Cancelling a job without a savepoint
 * is the only stop path that destroys the job's recovery point: {@code JobMaster#cleanJob} calls
 * {@code CheckpointManager#clearCheckpointIfNeed}, which deletes every checkpoint of the job unless
 * the job asked to keep them. The switch is a per-job {@code env} option ({@code
 * checkpoint.retain-after-job-cancelled}), and its two failure modes are both silent. If the option
 * is read but not honoured, a user who cancelled a job loses the only data that a later {@code
 * --restore-with-checkpoint} could have resumed from, and the loss is only discovered when the
 * restore starts from scratch (or finds nothing). If the option leaks across jobs, every cancelled
 * job on the cluster keeps its checkpoint data forever, so the checkpoint store grows without bound
 * and nothing reports it. Neither failure changes any status the client can observe: both cancelled
 * jobs simply report {@code CANCELED}.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code CheckpointManagerTest}, {@code
 * JobMasterTest} and {@code CoordinatorServiceJobCleanupTest} assert this decision against stubbed
 * checkpoint storage, i.e. they verify that a {@code deleteCheckpoint} call is made or skipped, not
 * that real files survive a real cancel or that the surviving files are actually loadable. {@code
 * CheckpointRestoreWithStopIT} covers the opt-in side only, through the CLI in a Testcontainers
 * cluster, and never drives the default (delete) branch; no test runs two jobs on one cluster to
 * pin that the option is scoped to the job that set it. No test proves that the retained data is
 * more than dead bytes on disk.
 *
 * <p><b>How the contract is pinned.</b> The test runs one in-JVM master/worker cluster with
 * local-file checkpoint storage in a private directory and submits two jobs from two conf files
 * that differ only in the retention option. Both jobs reach {@code RUNNING} and complete a periodic
 * checkpoint, then both are cancelled. After each cancellation has fully cleaned up the job's
 * master (the point at which any deletion has already happened), the test requires that:
 *
 * <ul>
 *   <li>the job that opted in still exposes its checkpoint data through {@code
 *       JobClient#getCheckpointData} and still owns checkpoint files on disk;
 *   <li>the sibling job that did not opt in exposes no checkpoint data and owns no files, proving
 *       the decision is per job instead of per cluster;
 *   <li>the retained data is a genuine recovery point: a {@code RestoreMode#CHECKPOINT} submission
 *       with the cancelled job as source and a new job id reaches {@code RUNNING} and completes a
 *       checkpoint whose id is above the retained one. {@code CheckpointManager} seeds the
 *       checkpoint-id counter with {@code retainedId + 1} only when it actually read the retained
 *       pipeline state, so a restore that silently started from scratch would number its first
 *       checkpoint from 1 and fail this assertion.
 * </ul>
 */
@Slf4j
public class CancelledJobCheckpointRetentionIT {

    /** Streaming fixture that opts in to keeping its checkpoints after cancellation. */
    private static final String RETAINED_JOB_CONF =
            "stream_fake_to_console_cancel_retain_checkpoint.conf";

    /** The same pipeline as {@link #RETAINED_JOB_CONF}, without the retention option. */
    private static final String DEFAULT_JOB_CONF =
            "stream_fake_to_console_cancel_drop_checkpoint.conf";

    /** Upper bound to wait for the one-master/one-worker cluster to form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for a submitted or restored job to reach a given status. */
    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for a periodic checkpoint or for stored checkpoint data. */
    private static final long CHECKPOINT_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for the terminal cleanup of a cancelled job. */
    private static final long JOB_CLEANUP_TIMEOUT_SECONDS = 120L;

    @Test
    public void testCheckpointRetentionIsScopedToTheCancelledJobThatAsksForIt() throws Exception {
        String testClusterName = "CancelledJobCheckpointRetentionIT_" + System.currentTimeMillis();
        Path checkpointStorageRoot =
                Files.createTempDirectory("seatunnel-cancel-checkpoint-retention");

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy restoredJob = null;
        try {
            SeaTunnelConfig masterConfig =
                    getSeaTunnelConfig(testClusterName, checkpointStorageRoot);
            SeaTunnelConfig workerConfig =
                    getSeaTunnelConfig(testClusterName, checkpointStorageRoot);
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);
            awaitClusterSize(masterNode, 2);
            awaitWorkersRegistered(masterNode, 1);

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.getJobClient();

            // --- Job that opts in: its checkpoint data must survive the cancellation. ---
            ClientJobProxy retainedJob =
                    submitJob(engineClient, masterConfig, RETAINED_JOB_CONF, "retained-on-cancel");
            long retainedJobId = retainedJob.getJobId();
            awaitJobStatus(retainedJob, JobStatus.RUNNING);
            long retainedCheckpointId = awaitCompletedCheckpointAbove(jobClient, retainedJobId, 0L);
            log.info("Job {} completed checkpoint {}", retainedJobId, retainedCheckpointId);

            cancelAndAwaitJobCleanup(engineClient, masterNode, retainedJob);

            Assertions.assertFalse(
                    jobClient.getCheckpointData(retainedJobId, RestoreMode.CHECKPOINT).isEmpty(),
                    "checkpoint.retain-after-job-cancelled=true must keep the checkpoint data of"
                            + " job "
                            + retainedJobId
                            + " after its master was cleaned up");
            Assertions.assertTrue(
                    checkpointFileCount(checkpointStorageRoot, retainedJobId) > 0,
                    "The retained checkpoint of job "
                            + retainedJobId
                            + " must still exist in the checkpoint store at "
                            + checkpointStorageRoot);

            // --- Sibling job without the option: the engine default must still delete. ---
            ClientJobProxy defaultJob =
                    submitJob(engineClient, masterConfig, DEFAULT_JOB_CONF, "dropped-on-cancel");
            long defaultJobId = defaultJob.getJobId();
            awaitJobStatus(defaultJob, JobStatus.RUNNING);
            long defaultCheckpointId = awaitCompletedCheckpointAbove(jobClient, defaultJobId, 0L);
            log.info("Job {} completed checkpoint {}", defaultJobId, defaultCheckpointId);

            cancelAndAwaitJobCleanup(engineClient, masterNode, defaultJob);

            Assertions.assertTrue(
                    jobClient.getCheckpointData(defaultJobId, RestoreMode.CHECKPOINT).isEmpty(),
                    "A cancelled job without checkpoint.retain-after-job-cancelled must not expose"
                            + " checkpoint data, but job "
                            + defaultJobId
                            + " still does");
            Assertions.assertEquals(
                    0L,
                    checkpointFileCount(checkpointStorageRoot, defaultJobId),
                    "A cancelled job without checkpoint.retain-after-job-cancelled must not leave"
                            + " checkpoint files behind");
            Assertions.assertTrue(
                    checkpointFileCount(checkpointStorageRoot, retainedJobId) > 0,
                    "Cancelling the sibling job "
                            + defaultJobId
                            + " must not remove the retained checkpoint of job "
                            + retainedJobId);

            // --- The retained data must be a real recovery point, not just files on disk. ---
            long restoredJobId = jobClient.getNewJobId();
            JobConfig restoredJobConfig = new JobConfig();
            restoredJobConfig.setName("restored-from-retained-checkpoint");
            restoredJob =
                    engineClient
                            .restoreFromCheckpointExecutionContext(
                                    TestUtils.getResource(RETAINED_JOB_CONF),
                                    Collections.emptyList(),
                                    restoredJobConfig,
                                    masterConfig,
                                    retainedJobId,
                                    restoredJobId)
                            .execute();
            Assertions.assertEquals(
                    restoredJobId,
                    restoredJob.getJobId(),
                    "A checkpoint restore must run under the destination job id it was given");
            awaitJobStatus(restoredJob, JobStatus.RUNNING);
            long firstCheckpointAfterRestore =
                    awaitCompletedCheckpointAbove(jobClient, restoredJobId, retainedCheckpointId);
            Assertions.assertTrue(
                    firstCheckpointAfterRestore > retainedCheckpointId,
                    "The restored job "
                            + restoredJobId
                            + " resumed at checkpoint "
                            + firstCheckpointAfterRestore
                            + ", which is not above the retained checkpoint "
                            + retainedCheckpointId
                            + "; the retained state was not loaded");

            // Stop the restored job while the cluster is still up, so the teardown below cannot
            // race its cancellation cleanup.
            engineClient.cancelJob(restoredJobId);
            awaitJobStatus(restoredJob, JobStatus.CANCELED);
            awaitJobMasterRemoved(masterNode, restoredJobId);
            restoredJob = null;
        } finally {
            if (engineClient != null && restoredJob != null) {
                try {
                    engineClient.cancelJob(restoredJob.getJobId());
                } catch (Exception e) {
                    log.warn("Failed to cancel the restored job during cleanup", e);
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
            FileUtils.deleteFile(checkpointStorageRoot.toString());
        }
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String confFile,
            String jobName)
            throws Exception {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        return engineClient
                .createExecutionContext(TestUtils.getResource(confFile), jobConfig, seaTunnelConfig)
                .execute();
    }

    /**
     * Cancels the job and waits until the cancellation has been fully applied: {@code CANCELED} is
     * reported and the job's master is gone, which is after {@code JobMaster#cleanJob} has decided
     * whether to delete the job's checkpoint data.
     */
    private static void cancelAndAwaitJobCleanup(
            SeaTunnelClient engineClient,
            HazelcastInstanceImpl masterNode,
            ClientJobProxy jobProxy) {
        long jobId = jobProxy.getJobId();
        engineClient.cancelJob(jobId);
        awaitJobStatus(jobProxy, JobStatus.CANCELED);
        awaitJobMasterRemoved(masterNode, jobId);
    }

    /**
     * Waits until the job stores a completed checkpoint above {@code minimumExclusive} and returns
     * its id.
     */
    private static long awaitCompletedCheckpointAbove(
            JobClient jobClient, long jobId, long minimumExclusive) {
        Awaitility.await()
                .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<JobPipelineCheckpointData> checkpoints =
                                    jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT);
                            long latest =
                                    checkpoints.stream()
                                            .mapToLong(JobPipelineCheckpointData::getCheckpointId)
                                            .max()
                                            .orElse(-1L);
                            Assertions.assertTrue(
                                    latest > minimumExclusive,
                                    "Waiting for a completed checkpoint of job "
                                            + jobId
                                            + " above "
                                            + minimumExclusive
                                            + ", but the stored checkpoints are "
                                            + checkpoints);
                        });
        return jobClient.getCheckpointData(jobId, RestoreMode.CHECKPOINT).stream()
                .mapToLong(JobPipelineCheckpointData::getCheckpointId)
                .max()
                .orElseThrow(() -> new AssertionError("No checkpoint data was stored"));
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        jobProxy.getJobStatus(),
                                        "Waiting for job status " + expected));
    }

    private static void awaitJobMasterRemoved(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(JOB_CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        getJobMaster(masterNode, jobId),
                                        "The cancelled job's master must be cleaned up before its"
                                                + " checkpoint data is inspected"));
    }

    /** Counts the checkpoint files the local-file checkpoint storage holds for the job. */
    private static long checkpointFileCount(Path checkpointStorageRoot, long jobId) {
        Path jobDirectory = checkpointStorageRoot.resolve(String.valueOf(jobId));
        if (!Files.isDirectory(jobDirectory)) {
            return 0L;
        }
        try (Stream<Path> files = Files.walk(jobDirectory)) {
            return files.filter(Files::isRegularFile).count();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to list the checkpoint files of job " + jobId, e);
        }
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server = seaTunnelServer(masterNode);
        return server.getCoordinatorService() == null
                ? null
                : server.getCoordinatorService().getJobMaster(jobId);
    }

    private static SeaTunnelServer seaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expected) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected, node.getCluster().getMembers().size()));
    }

    private static void awaitWorkersRegistered(
            HazelcastInstanceImpl masterNode, int expectedWorkers) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            SeaTunnelServer server = seaTunnelServer(masterNode);
                            Assertions.assertNotNull(
                                    server.getCoordinatorService(),
                                    "The coordinator service is not active yet");
                            Assertions.assertEquals(
                                    expectedWorkers,
                                    server.getCoordinatorService()
                                            .getResourceManager()
                                            .workerCount(Collections.emptyMap()),
                                    "Waiting for every worker to register its slots");
                        });
    }

    /**
     * Builds a node config whose checkpoint storage is an isolated local-file namespace, so the
     * test can inspect exactly the files the engine wrote instead of sharing a global directory
     * with every other test in this module.
     */
    private static SeaTunnelConfig getSeaTunnelConfig(
            String testClusterName, Path checkpointStorageRoot) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint is irrelevant here and would otherwise contend for a fixed port.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        CheckpointStorageConfig checkpointStorage =
                seaTunnelConfig.getEngineConfig().getCheckpointConfig().getStorage();
        checkpointStorage.setStorage("localfile");
        Map<String, String> storagePluginConfig = new HashMap<>();
        storagePluginConfig.put("namespace", checkpointStorageRoot.toAbsolutePath().toString());
        checkpointStorage.setStoragePluginConfig(storagePluginConfig);
        return seaTunnelConfig;
    }
}
