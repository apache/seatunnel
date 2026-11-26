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
import com.hazelcast.cluster.Member;
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
 * End-to-end regression coverage for the recovery point selected when a job that was itself started
 * from a savepoint later loses its master.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A restore stores the original source
 * job id and restore mode in the job's immutable information, and a master failover re-initializes
 * the same job with {@code restart = true}. The checkpoint coordinator constructor therefore has to
 * choose between two durable recovery points: the job's own latest completed checkpoint (written
 * while the restored job was running) and the savepoint the job was originally started from. The
 * two are indistinguishable to a naive implementation that simply looks up {@code
 * restoreSourceJobId} again, and because the job id is the same for a same-id savepoint restore,
 * the fallback silently succeeds instead of failing loudly. Resuming from the older savepoint would
 * replay every record consumed since the savepoint, so exactly-once output would be broken while
 * the job still reports RUNNING and checkpoints still complete; only the checkpoint id sequence
 * reveals it, because the counter is initialized from the selected recovery point.
 *
 * <p><b>How the contract is asserted.</b> The test runs a streaming job at parallelism 2 in an
 * in-JVM two-node cluster, waits for periodic checkpoints, stops it with a savepoint, restores it
 * with the same job id (the normal {@code seatunnel.sh --restore} flow), lets several new periodic
 * checkpoints complete, then shuts down the master member. After the remaining member takes over,
 * the test immediately requests another savepoint. A savepoint id is allocated from the restored
 * coordinator's checkpoint id counter, so an id above the last pre-failover periodic checkpoint
 * proves the coordinator resumed its own sequence; an id at or below the pre-failover checkpoint
 * range proves it fell back to the original savepoint. The gap between the savepoint id and the
 * last periodic checkpoint is deliberately larger than one checkpoint interval, so a fallback
 * cannot hide behind checkpoints that complete while the test waits for the failover.
 *
 * <p>This complements {@link SavepointClusterRestartRestoreIT}, which checks the savepoint survives
 * a full cold cluster restart, and {@code CheckpointCoordinatorFailoverIT}, which checks that a
 * normally submitted job keeps checkpointing across a master switch. Neither covers a master switch
 * of a job whose immutable information still points at the savepoint it was restored from.
 */
@Slf4j
public class SavepointRestoreMasterFailoverIT {

    private static final String CONF_FILE = "stream_fake_to_console_savepoint_master_failover.conf";

    private static final String JOB_NAME = "SavepointRestoreMasterFailoverIT";

    /** The fixture produces exactly one pipeline. */
    private static final int PIPELINE_ID = 1;

    /**
     * Periodic checkpoints that must complete after the savepoint restore and before the master
     * failover. A fallback to the savepoint would have to consume these ids before the
     * post-failover savepoint is requested, so keeping the number comfortably above one gives the
     * assertion a wide, deterministic margin instead of racing the checkpoint interval.
     */
    private static final int CHECKPOINTS_AFTER_RESTORE_BEFORE_FAILOVER = 6;

    private static final long STATE_TIMEOUT_SECONDS = 120;

    @Test
    public void testRestoredJobContinuesOwnCheckpointSequenceAfterMasterFailover()
            throws Exception {
        String testClusterName = "SavepointRestoreMasterFailoverIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig clusterConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        clusterConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        // The REST endpoint is irrelevant here and would otherwise contend for a fixed port.
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

            ClientJobProxy job =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, clusterConfig)
                            .execute();
            long jobId = job.getJobId();
            awaitJobStatus(job, JobStatus.RUNNING);

            // Prove the checkpoint pipeline is live before the savepoint, so the savepoint id is
            // guaranteed to be above the first periodic checkpoint instead of being the first id.
            JobPipelineCheckpointData periodicBeforeSavepoint =
                    awaitCheckpointAbove(
                            jobClient, jobId, RestoreMode.CHECKPOINT, 0L, "periodic checkpoint");
            awaitSavepoint(engineClient, jobId);
            awaitJobStatus(job, JobStatus.SAVEPOINT_DONE);

            JobPipelineCheckpointData savepoint =
                    awaitCheckpointAbove(
                            jobClient,
                            jobId,
                            RestoreMode.SAVEPOINT,
                            periodicBeforeSavepoint.getCheckpointId(),
                            "savepoint");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    savepoint.getCheckpointType(),
                    "The stop-with-savepoint must persist a savepoint checkpoint");
            long savepointId = savepoint.getCheckpointId();

            // Restore with the source job id, i.e. the same-id flow used by `seatunnel.sh
            // --restore <jobId>`. The job's immutable information now keeps restoreMode=SAVEPOINT
            // and restoreSourceJobId=jobId, which is exactly the state the failover path reads.
            ClientJobProxy restoredJob =
                    engineClient
                            .restoreExecutionContext(confPath, jobConfig, clusterConfig, jobId)
                            .execute();
            Assertions.assertEquals(
                    jobId, restoredJob.getJobId(), "A savepoint restore must keep the job id");
            awaitJobStatus(restoredJob, JobStatus.RUNNING);

            // Let the restored job build its own checkpoint history. The last periodic checkpoint
            // below is the recovery point a correct failover must resume from.
            JobPipelineCheckpointData lastPeriodicBeforeFailover =
                    awaitCheckpointAbove(
                            jobClient,
                            jobId,
                            RestoreMode.CHECKPOINT,
                            savepointId + CHECKPOINTS_AFTER_RESTORE_BEFORE_FAILOVER - 1,
                            "periodic checkpoint #"
                                    + CHECKPOINTS_AFTER_RESTORE_BEFORE_FAILOVER
                                    + " after restore");
            long lastPeriodicCheckpointId = lastPeriodicBeforeFailover.getCheckpointId();
            log.info(
                    "Restored job {} has periodic checkpoint {} above savepoint {}; killing master",
                    jobId,
                    lastPeriodicCheckpointId,
                    savepointId);

            // Kill the master member, not just a random node: the new master must re-initialize
            // the job's checkpoint coordinator from durable state.
            Member master = node1.getCluster().getMembers().iterator().next();
            boolean node1IsMaster =
                    master.getUuid().equals(node1.getCluster().getLocalMember().getUuid());
            HazelcastInstanceImpl survivingNode;
            if (node1IsMaster) {
                node1.shutdown();
                node1 = null;
                survivingNode = node2;
            } else {
                node2.shutdown();
                node2 = null;
                survivingNode = node1;
            }
            awaitClusterSize(survivingNode, 1);
            log.info(
                    "Killed master member {}; waiting for job {} to recover",
                    master.getAddress(),
                    jobId);

            HazelcastInstanceImpl finalSurvivingNode = survivingNode;
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1,
                                            finalSurvivingNode.getCluster().getMembers().size(),
                                            "Waiting for the surviving node to become master"));

            // Wait until the new master has re-initialized the job. The job can briefly report
            // RUNNING from before the switch, so readiness is proven by the savepoint request
            // itself, which fails fast until the coordinator owns the job again.
            long postFailoverSavepointId =
                    requestSavepointAfterFailover(
                            engineClient, jobClient, restoredJob, jobId, savepointId);
            Assertions.assertTrue(
                    postFailoverSavepointId > lastPeriodicCheckpointId,
                    "After a master failover the restored job must continue its own checkpoint id "
                            + "sequence (resume from periodic checkpoint "
                            + lastPeriodicCheckpointId
                            + "), not fall back to the savepoint it was originally started from "
                            + "(savepoint="
                            + savepointId
                            + "). Observed post-failover savepoint id "
                            + postFailoverSavepointId);

            awaitJobStatus(restoredJob, JobStatus.SAVEPOINT_DONE);
            JobPipelineCheckpointData postFailoverSavepoint =
                    jobClient.getCheckpointData(jobId, RestoreMode.SAVEPOINT).stream()
                            .filter(data -> data.getPipelineId() == PIPELINE_ID)
                            .max(
                                    Comparator.comparingLong(
                                            JobPipelineCheckpointData::getCheckpointId))
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "No savepoint recorded after master failover"));
            Assertions.assertEquals(
                    postFailoverSavepointId,
                    postFailoverSavepoint.getCheckpointId(),
                    "The savepoint requested after failover must be the one persisted");
            Assertions.assertEquals(
                    CheckpointType.SAVEPOINT_TYPE,
                    postFailoverSavepoint.getCheckpointType(),
                    "The post-failover recovery point must be a savepoint");
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

    /**
     * Requests a savepoint and returns the id of the new savepoint record.
     *
     * <p>Right after a master failover the re-elected master may not own the job yet, and a job
     * that is still restoring cannot start a savepoint; both failures are retried until the job is
     * running again. A terminal job status is never retried.
     */
    private static long requestSavepointAfterFailover(
            SeaTunnelClient engineClient,
            JobClient jobClient,
            ClientJobProxy jobProxy,
            long jobId,
            long previousSavepointId)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(STATE_TIMEOUT_SECONDS);
        RuntimeException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                awaitSavepoint(engineClient, jobId);
                return awaitCheckpointAbove(
                                jobClient,
                                jobId,
                                RestoreMode.SAVEPOINT,
                                previousSavepointId,
                                "savepoint after master failover")
                        .getCheckpointId();
            } catch (RuntimeException e) {
                lastFailure = e;
                JobStatus status;
                try {
                    status = jobProxy.getJobStatus();
                } catch (RuntimeException statusFailure) {
                    Thread.sleep(200L);
                    continue;
                }
                Assertions.assertFalse(
                        status.isEndState(),
                        "Job "
                                + jobId
                                + " reached terminal status "
                                + status
                                + " while waiting for a post-failover savepoint to be accepted");
                Thread.sleep(500L);
            }
        }
        throw new AssertionError(
                "The job "
                        + jobId
                        + " did not accept a savepoint within "
                        + STATE_TIMEOUT_SECONDS
                        + "s after the master failover",
                lastFailure);
    }

    private static void awaitSavepoint(SeaTunnelClient engineClient, long jobId)
            throws InterruptedException {
        // SeaTunnelClient#savePointJob blocks until the savepoint resolves. Run it on a worker so a
        // stuck DOING_SAVEPOINT state surfaces as a bounded failure instead of hanging the suite.
        CompletableFuture<Void> savepoint =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        try {
            savepoint.get(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw new SavepointNotAcceptedException(e.getCause());
        } catch (TimeoutException e) {
            throw new AssertionError(
                    "Savepoint for job "
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
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Unexpected job status for job " + jobProxy.getJobId()));
    }

    /**
     * Waits until the latest checkpoint for {@code restoreMode} is above {@code
     * minExclusiveCheckpointId} and returns it.
     */
    private static JobPipelineCheckpointData awaitCheckpointAbove(
            JobClient jobClient,
            long jobId,
            RestoreMode restoreMode,
            long minExclusiveCheckpointId,
            String description) {
        AtomicReference<JobPipelineCheckpointData> latest = new AtomicReference<>();
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
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

    /** Marker for a savepoint request the re-elected master did not accept yet. */
    private static final class SavepointNotAcceptedException extends RuntimeException {
        private SavepointNotAcceptedException(Throwable cause) {
            super(cause);
        }
    }
}
