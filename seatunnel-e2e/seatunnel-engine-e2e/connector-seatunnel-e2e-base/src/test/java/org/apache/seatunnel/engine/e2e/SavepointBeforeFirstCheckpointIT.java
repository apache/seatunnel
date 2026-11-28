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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end regression coverage for a stop-with-savepoint that is the very first checkpoint of a
 * job.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A savepoint is a checkpoint of type
 * {@code SAVEPOINT_TYPE} allocated from the pipeline checkpoint id counter. When the job has never
 * completed a periodic checkpoint, the counter is still at its initial value, the checkpoint
 * storage contains no state for the job, and the savepoint is the only recovery point that will
 * ever exist. The restore path then has to (a) find that savepoint in storage, (b) advance the
 * counter to {@code savepointId + 1} without a previous completed checkpoint to compare against,
 * and (c) start the job from a checkpoint id that equals the first id the job ever allocated. A
 * change in counter initialization, savepoint precondition checks, savepoint-type filtering in
 * {@code CheckpointManager}, or storage cleanup can silently break this path: the savepoint either
 * never gets written, or the restore starts the job from scratch while reporting RUNNING. Both
 * failures are invisible to the client because the job status is RUNNING in either case.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointRestoreIT}, {@code
 * SavepointClusterRestartRestoreIT}, {@code SavepointRestoreMasterFailoverIT} and {@code
 * SavepointRestoreRetryIT} all wait for a completed periodic checkpoint before requesting the
 * savepoint (the retry test additionally requires a checkpoint above the savepoint id). They
 * therefore always exercise a counter that has already advanced and a storage that already holds
 * periodic checkpoints. No test requests a savepoint on a job that has completed zero checkpoints.
 *
 * <p><b>How the contract is pinned.</b> The test runs a streaming job whose checkpoint interval is
 * longer than the whole test, waits until it is RUNNING, asserts that the job has no checkpoint
 * data at all, then stops it with a savepoint. It requires that the savepoint succeeds, that its
 * checkpoint id is the first id allocated by the job, and that it is stored as the only savepoint.
 * The job is then restored from that savepoint and the test requires the same job id, RUNNING
 * status, the savepoint still visible in storage, and a second stop-with-savepoint whose id is
 * exactly the restored savepoint id + 1. That last assertion proves the counter was re-seeded from
 * the first savepoint rather than restarted from zero.
 */
@Slf4j
public class SavepointBeforeFirstCheckpointIT {

    /** Streaming fixture with a checkpoint interval longer than the whole test. */
    private static final String CONF_FILE =
            "stream_fake_to_console_savepoint_before_first_checkpoint.conf";

    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;
    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120L;
    private static final long JOB_CLEANUP_TIMEOUT_SECONDS = 120L;

    @Test
    public void testSavepointBeforeAnyPeriodicCheckpoint() throws Exception {
        String testClusterName = "SavepointBeforeFirstCheckpointIT_" + System.currentTimeMillis();

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
            jobConfig.setName("savepoint-before-first-checkpoint");

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

            // The one-hour checkpoint interval guarantees that no periodic checkpoint can have
            // completed: the savepoint below is the first checkpoint this job ever allocates.
            Assertions.assertTrue(
                    checkpointData(jobClient, jobId, RestoreMode.CHECKPOINT).isEmpty(),
                    "The fixture must not complete a periodic checkpoint before the savepoint");
            Assertions.assertTrue(
                    checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT).isEmpty(),
                    "The job must not have a savepoint before one is requested");

            requestSavepointWhenCoordinatorReady(engineClient, sourceProxy, jobId);
            awaitJobStatus(sourceProxy, JobStatus.SAVEPOINT_DONE);

            long firstSavepointId = awaitSingleSavepointId(jobClient, jobId);
            Assertions.assertEquals(
                    1L,
                    firstSavepointId,
                    "The first checkpoint of the job, and therefore the first savepoint, must use"
                            + " checkpoint id 1");
            Assertions.assertTrue(
                    checkpointData(jobClient, jobId, RestoreMode.CHECKPOINT).isEmpty(),
                    "A savepoint must not be reported as a periodic checkpoint");

            // Wait for the stopped job's master to be cleaned so the restore below is a genuine
            // restore from storage rather than an attach to the still-running instance.
            awaitJobMasterRemoved(masterNode, jobId);

            ClientJobProxy restoredProxy =
                    engineClient
                            .restoreExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName), jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId, restoredProxy.getJobId(), "A savepoint restore must keep the job id");
            awaitJobStatus(restoredProxy, JobStatus.RUNNING);

            Assertions.assertEquals(
                    firstSavepointId,
                    awaitSingleSavepointId(jobClient, jobId),
                    "The savepoint used for the restore must still be readable after the restore");
            Assertions.assertTrue(
                    checkpointData(jobClient, jobId, RestoreMode.CHECKPOINT).isEmpty(),
                    "Restoring from a savepoint must not fabricate periodic checkpoints");

            // The restored counter must continue exactly after the savepoint it was seeded from.
            requestSavepointWhenCoordinatorReady(engineClient, restoredProxy, jobId);
            awaitJobStatus(restoredProxy, JobStatus.SAVEPOINT_DONE);

            Assertions.assertEquals(
                    firstSavepointId + 1,
                    awaitSingleSavepointId(jobClient, jobId),
                    "The second savepoint must resume at the restored savepoint id + 1");
            log.warn(
                    "Job {} completed savepoint {} as the first checkpoint and savepoint {} after"
                            + " the restore",
                    jobId,
                    firstSavepointId,
                    firstSavepointId + 1);
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

    /**
     * Requests a savepoint until the checkpoint coordinator has finished its asynchronous
     * task-readiness bookkeeping.
     *
     * <p>A savepoint requested in the same instant the job turns RUNNING can race with the
     * coordinator recording every subtask as READY_START. The engine correctly rejects that request
     * as a retryable precondition, returns the job to RUNNING, and leaves no savepoint behind; the
     * caller is expected to retry. This helper retries and fails the test if a rejected attempt
     * leaves the job in any state other than RUNNING.
     */
    private static void requestSavepointWhenCoordinatorReady(
            SeaTunnelClient engineClient, ClientJobProxy jobProxy, long jobId) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(
                        () -> {
                            try {
                                engineClient.savePointJob(jobId);
                                return true;
                            } catch (Exception e) {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        jobProxy.getJobStatus(),
                                        "A savepoint rejected before the coordinator is ready must"
                                                + " leave the job RUNNING");
                                log.warn(
                                        "Savepoint for job {} was rejected before readiness: {}",
                                        jobId,
                                        e.getMessage());
                                return false;
                            }
                        });
    }

    private static long awaitSingleSavepointId(JobClient jobClient, long jobId) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        1,
                                        checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT)
                                                .size(),
                                        "Expected exactly one savepoint for job " + jobId));
        return latestSavepointId(jobClient, jobId);
    }

    private static long latestSavepointId(JobClient jobClient, long jobId) {
        return checkpointData(jobClient, jobId, RestoreMode.SAVEPOINT).stream()
                .map(JobPipelineCheckpointData::getCheckpointId)
                .max(Comparator.naturalOrder())
                .orElse(-1L);
    }

    private static List<JobPipelineCheckpointData> checkpointData(
            JobClient jobClient, long jobId, RestoreMode restoreMode) {
        return jobClient.getCheckpointData(jobId, restoreMode);
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
                                        "The stopped job's master must be cleaned up before the"
                                                + " restore is submitted"));
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
