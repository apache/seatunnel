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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end regression coverage for a client that retries a savepoint restore submission while the
 * first restore is already running.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code --restore} resolves the job id
 * from the savepoint, so every retry of the same restore carries the same job id. The only correct
 * answer to a retry is "the job you mean is already running": the engine must attach the caller to
 * the existing {@link JobMaster} without instantiating a second one. A second {@code JobMaster} for
 * a live job id would rebuild the physical plan and run the checkpoint manager's savepoint restore
 * path again, which resets the per-pipeline checkpoint counter to the savepoint id + 1 and
 * redeploys every task group on top of the tasks that are already committing output. Exactly-once
 * output is broken while the job still reports RUNNING, and the only trace is the checkpoint id
 * sequence moving backwards and the replaced job master. This cannot be observed from the client:
 * both submissions return a proxy that reports RUNNING.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointRestoreIT} and {@code
 * SavepointClusterRestartRestoreIT} restore once and then inspect the job. {@code
 * CheckpointCoordinatorFailoverIT} and {@code SavepointRestoreMasterFailoverIT} drive recovery
 * through a master switch, not a repeated client submission. No test submits the same restore
 * twice, so the {@code CoordinatorService#submitJob} early-return branch that exists specifically
 * to avoid "redundant job restores" is only covered by unit tests that fabricate the job master
 * map.
 *
 * <p><b>How the contract is pinned.</b> The test runs a long streaming job in an in-JVM
 * master/worker cluster, stops it with a real savepoint, restores it with the normal {@code
 * RestoreMode#SAVEPOINT} flow, lets periodic checkpoints resume, then submits the identical restore
 * again. It requires that: the retried submission returns a proxy for the same job id and status
 * instead of failing; the engine's {@link JobMaster} instance for that job id is the same object
 * before and after the retry (a second instance would replace it); completed checkpoint ids keep
 * increasing instead of falling back to the savepoint range; and the job is still fully operable
 * afterwards, which the test proves by stopping it with another savepoint whose id is newer than
 * every checkpoint seen before the retry.
 */
@Slf4j
public class SavepointRestoreRetryIT {

    /** Long-running streaming fixture: FakeSource -> Console with a short checkpoint interval. */
    private static final String CONF_FILE = "stream_fake_to_console_savepoint_cluster_restart.conf";

    /** Upper bound to wait for the one-master/one-worker cluster to form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for a submitted or restored job to reach a given status. */
    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for periodic checkpoints to complete. */
    private static final long CHECKPOINT_TIMEOUT_SECONDS = 180L;

    /** Upper bound to wait for the terminal cleanup of the stopped source job. */
    private static final long JOB_CLEANUP_TIMEOUT_SECONDS = 120L;

    @Test
    public void testRetriedRestoreSubmissionKeepsTheRunningJob() throws Exception {
        String testClusterName = "SavepointRestoreRetryIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        try {
            masterNode =
                    SeaTunnelServerStarter.createMasterHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            workerNode =
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(
                            getSeaTunnelConfig(testClusterName));
            awaitClusterSize(masterNode, 2);
            awaitWorkersRegistered(masterNode, 1);

            Common.setDeployMode(DeployMode.CLIENT);
            String confPath = TestUtils.getResource(CONF_FILE);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("savepoint-restore-retry");

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.getJobClient();

            ClientJobProxy sourceProxy =
                    engineClient
                            .createExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName))
                            .execute();
            long jobId = sourceProxy.getJobId();
            awaitJobStatus(sourceProxy, JobStatus.RUNNING);
            awaitLatestCheckpointAbove(masterNode, jobId, 0L);

            // Establish a real recovery point, then wait until the stopped job's master is gone so
            // that the restore below is a genuine new submission instead of the duplicate guard.
            engineClient.savePointJob(jobId);
            awaitJobStatus(sourceProxy, JobStatus.SAVEPOINT_DONE);
            long savepointId = awaitLatestSavepointId(jobClient, jobId);
            log.warn("Source job {} completed its savepoint {}", jobId, savepointId);
            awaitJobMasterRemoved(masterNode, jobId);

            ClientJobProxy restoredProxy =
                    engineClient
                            .restoreExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName), jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId, restoredProxy.getJobId(), "A savepoint restore must keep the job id");
            awaitJobStatus(restoredProxy, JobStatus.RUNNING);
            // The restored coordinator resumes at savepoint id + 1, so a completed checkpoint
            // above the savepoint proves the job is checkpointing on the restored instance
            // before the retried submission is issued.
            awaitLatestCheckpointAbove(masterNode, jobId, savepointId);

            JobMaster jobMasterBeforeRetry = awaitJobMaster(masterNode, jobId);
            long latestCheckpointBeforeRetry = latestCompletedCheckpointId(masterNode, jobId);

            // Retry the exact same restore submission while the restored job is running.
            ClientJobProxy retryProxy;
            try {
                retryProxy =
                        engineClient
                                .restoreExecutionContext(
                                        confPath,
                                        jobConfig,
                                        getSeaTunnelConfig(testClusterName),
                                        jobId)
                                .execute();
            } catch (Exception e) {
                throw new AssertionError(
                        "A retried restore submission must attach to the running job instead of"
                                + " failing",
                        e);
            }

            Assertions.assertEquals(
                    jobId, retryProxy.getJobId(), "The retried restore must resolve the same job");
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    retryProxy.getJobStatus(),
                    "The retried restore must observe the running job");
            Assertions.assertSame(
                    jobMasterBeforeRetry,
                    getJobMaster(masterNode, jobId),
                    "The retried restore must not replace the running job's JobMaster");
            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    restoredProxy.getJobStatus(),
                    "The original restore proxy must keep observing the running job");

            // The same instance must keep checkpointing, and its checkpoint ids must not fall back
            // into the savepoint range.
            awaitLatestCheckpointAbove(masterNode, jobId, latestCheckpointBeforeRetry);
            long latestCheckpointAfterRetry = latestCompletedCheckpointId(masterNode, jobId);

            // The retried proxy must be a real handle to that same instance: stopping through it
            // has to produce a savepoint newer than everything observed before the retry.
            engineClient.savePointJob(jobId);
            awaitJobStatus(retryProxy, JobStatus.SAVEPOINT_DONE);
            long retriedSavepointId = awaitLatestSavepointId(jobClient, jobId);
            Assertions.assertTrue(
                    retriedSavepointId > latestCheckpointAfterRetry,
                    "The stop-with-savepoint after the retry must allocate a newer checkpoint id: "
                            + retriedSavepointId
                            + " vs "
                            + latestCheckpointAfterRetry);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
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

    private static void awaitLatestCheckpointAbove(
            HazelcastInstanceImpl masterNode, long jobId, long minimumExclusive) {
        Awaitility.await()
                .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        latestCompletedCheckpointId(masterNode, jobId)
                                                > minimumExclusive,
                                        "Waiting for a completed checkpoint above "
                                                + minimumExclusive
                                                + ", latest is "
                                                + latestCompletedCheckpointId(masterNode, jobId)));
    }

    private static void awaitJobMasterRemoved(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(JOB_CLEANUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        getJobMaster(masterNode, jobId),
                                        "The stopped job's master must be cleaned up before the"
                                                + " restore is submitted"));
    }

    private static JobMaster awaitJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> getJobMaster(masterNode, jobId) != null);
        JobMaster jobMaster = getJobMaster(masterNode, jobId);
        Assertions.assertNotNull(jobMaster, "The job master is not available yet");
        return jobMaster;
    }

    private static long awaitLatestSavepointId(JobClient jobClient, long jobId) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT)
                                                .isEmpty(),
                                        "No savepoint data was stored for job " + jobId));
        return checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT).stream()
                .mapToLong(JobPipelineCheckpointData::getCheckpointId)
                .max()
                .orElseThrow(() -> new AssertionError("No savepoint data was stored"));
    }

    private static List<JobPipelineCheckpointData> checkpointData(
            JobClient jobClient, long jobId, RestoreMode restoreMode) {
        return jobClient.getCheckpointData(jobId, restoreMode);
    }

    private static long latestCompletedCheckpointId(HazelcastInstanceImpl masterNode, long jobId) {
        CheckpointOverview overview = checkpointOverview(masterNode, jobId);
        if (overview == null) {
            return -1L;
        }
        return overview.getPipelines().values().stream()
                .map(PipelineCheckpointOverview::getLatestCompleted)
                .filter(info -> info != null)
                .mapToLong(CheckpointInfo::getCheckpointId)
                .max()
                .orElse(-1L);
    }

    private static CheckpointOverview checkpointOverview(
            HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server = seaTunnelServer(masterNode);
        if (server.getEngineContext() == null) {
            return null;
        }
        return server.getEngineContext()
                .getStateStores()
                .auxiliary()
                .checkpointOverviewStateStore()
                .get(jobId);
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
                                            .workerCount(java.util.Collections.emptyMap()),
                                    "Waiting for every worker to register its slots");
                        });
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint is irrelevant here and would otherwise contend for a fixed port.
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }
}
