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
 * End-to-end regression coverage for the durability boundary of a Zeta stop-with-savepoint: the
 * savepoint must outlive the cluster that produced it, and restoring from it on a brand-new cluster
 * must continue the per-pipeline checkpoint sequence instead of restarting it.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A savepoint is only usable if it is
 * persisted in checkpoint storage and selected by the restore path; the engine keeps several pieces
 * of restore bookkeeping (job state, checkpoint id counters, the checkpoint overview) in Hazelcast
 * IMap state. When the whole cluster is shut down, that state is gone, so a restore from a
 * savepoint has to resolve everything from storage alone. Two regressions are easy to introduce
 * here and are invisible to same-JVM/same-cluster tests: (1) restore only works while the old
 * JobMaster/history is still resident, and (2) the restored pipeline reuses checkpoint ids from 1
 * upward, overwriting the savepoint file (or a later periodic checkpoint) instead of continuing
 * after the savepoint id. The second case is especially dangerous because the job still reports
 * RUNNING and checkpoints still "complete"; the damage only shows up when the savepoint is needed
 * again.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointRestoreIT} restores inside
 * the same long-lived cluster and never checks the checkpoint id sequence. {@code
 * ClusterFaultToleranceIT} restarts all nodes, but only for automatic restore of an interrupted
 * running job (backed by the file map store), not for an explicit savepoint restore after a clean
 * shutdown. {@code CheckpointCoordinatorFailoverIT} verifies that ids grow across a master switch,
 * which exercises a different restore entry point (in-memory job state present, master role moved).
 * This test therefore drives the documented "stop with savepoint, restart the cluster, restore with
 * {@code restoreMode=SAVEPOINT}" flow through the client API and asserts the storage-visible
 * contract: the savepoint survives, the restored pipeline's next periodic checkpoint id is greater
 * than the savepoint id, and the savepoint record is still intact afterwards.
 *
 * <p>Kept as its own class rather than added to {@code SavepointRestoreIT}: that test extends
 * {@code SeaTunnelEngineContainer}, whose class-scoped Testcontainers cluster is started once for
 * every method and therefore cannot model a full cluster shutdown and cold restart.
 */
@Slf4j
public class SavepointClusterRestartRestoreIT {

    private static final String CONF_FILE = "stream_fake_to_console_savepoint_cluster_restart.conf";
    private static final String JOB_NAME = "SavepointClusterRestartRestoreIT";

    /**
     * Single pipeline produced by {@link #CONF_FILE}: one FakeSource operator, even though the
     * source reader runs with parallelism 2.
     */
    private static final int PIPELINE_ID = 1;

    private static final long STATE_TIMEOUT_SECONDS = 180;

    @Test
    public void testSavepointSurvivesClusterRestartAndCheckpointIdsContinue() throws Exception {
        String testClusterName = "SavepointClusterRestartRestoreIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint is irrelevant here and would otherwise contend for a fixed port
        // across the two cluster lifetimes of this test.
        clusterConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            awaitClusterSize(node1, 2);

            Common.setDeployMode(DeployMode.CLIENT);
            String confPath = TestUtils.getResource(CONF_FILE);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(JOB_NAME);

            engineClient = new SeaTunnelClient(clientConfig);
            JobClient jobClient = engineClient.createJobClient();
            ClientJobProxy originalJob =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, clusterConfig)
                            .execute();
            long jobId = originalJob.getJobId();

            awaitJobStatus(originalJob, JobStatus.RUNNING);

            // A periodic checkpoint before the savepoint proves the checkpoint pipeline is live;
            // the savepoint id must then advance past it instead of reusing it.
            JobPipelineCheckpointData lastPeriodicCheckpoint =
                    awaitCheckpoint(
                            jobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            0L,
                            "periodic before savepoint");
            Assertions.assertEquals(
                    CheckpointType.CHECKPOINT_TYPE,
                    lastPeriodicCheckpoint.getCheckpointType(),
                    "Pre-savepoint checkpoint must be a periodic checkpoint");

            // Bound the blocking client call: a regression that leaves the job in DOING_SAVEPOINT
            // must fail this test within a fixed window instead of hanging the whole suite.
            awaitSavepoint(engineClient, jobId);
            awaitJobStatus(originalJob, JobStatus.SAVEPOINT_DONE);

            JobPipelineCheckpointData savepoint =
                    awaitCheckpoint(
                            jobClient,
                            jobId,
                            RestoreMode.SAVEPOINT,
                            lastPeriodicCheckpoint.getCheckpointId(),
                            "savepoint");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    savepoint.getCheckpointType(),
                    "The stop-with-savepoint must persist a savepoint checkpoint");
            long savepointId = savepoint.getCheckpointId();
            Assertions.assertTrue(
                    savepointId > lastPeriodicCheckpoint.getCheckpointId(),
                    "Savepoint id must advance past the last periodic checkpoint id, last periodic="
                            + lastPeriodicCheckpoint.getCheckpointId()
                            + ", savepoint="
                            + savepointId);

            // Shut the whole cluster down. Nothing about this job survives in memory: the following
            // restore can only succeed by reading the savepoint from checkpoint storage.
            engineClient.close();
            engineClient = null;
            node2.shutdown();
            node1.shutdown();
            node1 = null;
            node2 = null;
            log.info(
                    "Cluster stopped after savepoint {} of job {}; starting a fresh cluster",
                    savepointId,
                    jobId);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(clusterConfig);
            awaitClusterSize(node1, 2);

            engineClient = new SeaTunnelClient(clientConfig);
            jobClient = engineClient.createJobClient();

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

            // The resumed pipeline must continue the shared checkpoint id sequence after the
            // savepoint. A regression here reuses ids from 1 and can overwrite the savepoint.
            JobPipelineCheckpointData checkpointAfterRestore =
                    awaitCheckpoint(
                            jobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            savepointId,
                            "periodic after restore");
            Assertions.assertTrue(
                    checkpointAfterRestore.getCheckpointId() > savepointId,
                    "Restored pipeline must continue checkpoint ids after the savepoint id, "
                            + "savepoint="
                            + savepointId
                            + ", next checkpoint="
                            + checkpointAfterRestore.getCheckpointId());
            Assertions.assertEquals(
                    CheckpointType.CHECKPOINT_TYPE,
                    checkpointAfterRestore.getCheckpointType(),
                    "Post-restore checkpoint must be a periodic checkpoint");

            // The savepoint is the only durable recovery point for this job. New periodic
            // checkpoints must neither delete nor overwrite it, otherwise a second restart could
            // not restore from the documented latest savepoint.
            List<JobPipelineCheckpointData> restorePoints =
                    jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT);
            Assertions.assertEquals(
                    1,
                    restorePoints.size(),
                    "Exactly one savepoint must remain selectable after restore: " + restorePoints);
            Assertions.assertEquals(
                    savepointId,
                    restorePoints.get(0).getCheckpointId(),
                    "The restored pipeline must not overwrite the savepoint checkpoint id");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    restorePoints.get(0).getCheckpointType(),
                    "The record selected for SAVEPOINT restore must still be a savepoint");

            engineClient.cancelJob(jobId);
            awaitJobStatus(restoredJob, JobStatus.CANCELED);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    private static void awaitSavepoint(SeaTunnelClient engineClient, long jobId)
            throws InterruptedException, ExecutionException {
        // SeaTunnelClient#savePointJob blocks until the savepoint resolves. Run it on a worker so a
        // stuck DOING_SAVEPOINT state surfaces as a bounded test failure.
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
                        () -> {
                            JobStatus status = jobProxy.getJobStatus();
                            Assertions.assertEquals(
                                    expectedStatus,
                                    status,
                                    "Unexpected job status for job " + jobProxy.getJobId());
                        });
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
