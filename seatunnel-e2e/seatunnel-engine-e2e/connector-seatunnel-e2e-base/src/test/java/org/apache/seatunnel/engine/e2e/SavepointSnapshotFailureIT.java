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
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.e2e.savepointfail.SavepointFailingSinkWriter;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * End-to-end regression coverage for a stop-with-savepoint whose sink cannot snapshot.
 *
 * <p>A savepoint is the only way to stop a Zeta job while keeping a restore point, so a sink that
 * cannot produce writer state (a failed object-store upload, a commit token that cannot be
 * obtained, a database that rejects the snapshot query) turns the request into a failure. The
 * engine then owns three invariants that no connector-level test can check:
 *
 * <ul>
 *   <li>the request must settle: the client must observe a failure instead of a request that hangs
 *       in {@link JobStatus#DOING_SAVEPOINT};
 *   <li>the engine must never publish the failed savepoint: no checkpoint of the job may be
 *       persisted, because a half-built savepoint that a later restore could pick up would replay
 *       data the failed run never committed;
 *   <li>the job must reach a terminal state and release its fixed slots, so the next job can be
 *       scheduled on the same cluster.
 * </ul>
 *
 * <p>Existing coverage does not reach this path. {@link SavepointBusySourceBarrierIT} starves the
 * savepoint barrier on the source side and {@link SavepointPreconditionRecoveryIT} rejects the
 * request before it starts, while {@code org.apache.seatunnel.engine.e2e.CancelDuringSavepointIT}
 * injects a cancellation rather than a snapshot failure. None of them makes the sink itself fail
 * while the savepoint barrier is being processed, which is the branch that decides whether a failed
 * savepoint is published or discarded.
 */
@Slf4j
public class SavepointSnapshotFailureIT {

    private static final String CONF_FILE =
            "savepoint-snapshot-failure/stream_fake_to_savepoint_failing_sink.conf";

    private static final String REPLACEMENT_JOB_CONF = "batch_fake_to_console_minimal_slot.conf";

    /** Bound for the savepoint request to settle; a hang is the regression this test guards. */
    private static final long SAVEPOINT_COMPLETION_TIMEOUT_SECONDS = 90;

    /** Generous bound for job state transitions on slow CI runners. */
    private static final long STATE_TIMEOUT_SECONDS = 90;

    @Test
    public void testSavepointFailureIsNotPublishedAndReleasesSlots() throws Exception {
        String testCaseName = "testSavepointFailureIsNotPublishedAndReleasesSlots";
        String testClusterName = "SavepointSnapshotFailureIT_" + testCaseName;

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerConfig = getSeaTunnelConfig(testClusterName);
        // Every pipeline owns one fixed slot for its enumerator and one for its reader/writer task
        // group, so a single-pipeline job needs two slots. The worker has exactly two: the
        // replacement batch job can only start and finish if the failed savepoint released both
        // slots of the failed job.
        workerConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        workerConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(2);
        SavepointFailingSinkWriter.WRITTEN_ROWS.set(0);
        SavepointFailingSinkWriter.SNAPSHOT_ATTEMPTS.set(0);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            SeaTunnelClient client = new SeaTunnelClient(clientConfig);
            engineClient = client;

            ClientJobProxy jobProxy =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(CONF_FILE), jobConfig, masterConfig)
                            .execute();
            long jobId = jobProxy.getJobId();
            awaitJobStatus(jobProxy, JobStatus.RUNNING);
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            SavepointFailingSinkWriter.WRITTEN_ROWS.get() > 0,
                                            "The sink writer has not received any row yet"));

            // Stop-with-savepoint: the sink rejects the final barrier, so the request must fail and
            // the engine must decide what happens to the job and to the checkpoint storage.
            CompletableFuture<Void> savepointRequest =
                    CompletableFuture.runAsync(() -> client.savePointJob(jobId));
            Throwable savepointFailure = null;
            try {
                savepointRequest.get(SAVEPOINT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                Assertions.fail(
                        "The stop-with-savepoint request reported success even though the sink"
                                + " rejected every snapshot; job status is "
                                + readStatusQuietly(jobProxy));
            } catch (TimeoutException e) {
                Assertions.fail(
                        "The stop-with-savepoint request did not settle within "
                                + SAVEPOINT_COMPLETION_TIMEOUT_SECONDS
                                + "s; job status is "
                                + readStatusQuietly(jobProxy));
            } catch (ExecutionException e) {
                savepointFailure = e.getCause();
                log.info(
                        "stop-with-savepoint failed as expected: {}",
                        ExceptionUtils.getMessage(savepointFailure));
            }
            Assertions.assertNotNull(
                    savepointFailure, "The savepoint request must fail when a sink snapshot fails");
            Assertions.assertTrue(
                    SavepointFailingSinkWriter.SNAPSHOT_ATTEMPTS.get() > 0,
                    "The savepoint barrier never reached the sink writer, so the request failed"
                            + " for an unrelated precondition: "
                            + ExceptionUtils.getMessage(savepointFailure));

            // The job must leave DOING_SAVEPOINT and end. A SAVEPOINT_DONE here would tell the user
            // that a restore point exists even though the sink never produced its state.
            Awaitility.await()
                    .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            readStatusQuietly(jobProxy).isEndState(),
                                            "The job must reach a terminal state after its"
                                                    + " savepoint failed, but its status is "
                                                    + readStatusQuietly(jobProxy)));
            JobStatus failedSavepointStatus = readStatusQuietly(jobProxy);
            log.info("Job {} reached {} after its savepoint failed", jobId, failedSavepointStatus);
            Assertions.assertEquals(
                    JobStatus.FAILED,
                    failedSavepointStatus,
                    "A stop-with-savepoint whose final checkpoint fails must fail the job instead"
                            + " of leaving it running or publishing a savepoint");

            // Nothing of the failed savepoint may be selectable by a later restore.
            CheckpointStorage checkpointStorage = checkpointStorage(masterNode);
            List<?> persistedCheckpoints = listCheckpoints(checkpointStorage, jobId);
            Assertions.assertTrue(
                    persistedCheckpoints.isEmpty(),
                    "The failed savepoint must not persist any checkpoint, but the storage holds "
                            + persistedCheckpoints.size()
                            + " checkpoint(s) for job "
                            + jobId);

            // The fixed slots must be free again: a replacement batch job can only finish if the
            // failed job no longer owns the two slots of the worker.
            JobConfig replacementJobConfig = new JobConfig();
            replacementJobConfig.setName(testCaseName + "_replacement");
            ClientJobProxy replacementProxy =
                    engineClient
                            .createExecutionContext(
                                    TestUtils.getResource(REPLACEMENT_JOB_CONF),
                                    replacementJobConfig,
                                    masterConfig)
                            .execute();
            awaitJobStatus(replacementProxy, JobStatus.FINISHED);
            log.info(
                    "Replacement batch job {} finished after the failed savepoint of job {}",
                    replacementProxy.getJobId(),
                    jobId);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        jobProxy.getJobStatus(),
                                        "Job has not reached " + expected));
    }

    /**
     * Reads the job status without failing the surrounding poll when the coordinator has already
     * released the job, which is exactly the state a terminal job moves to.
     */
    private static JobStatus readStatusQuietly(ClientJobProxy jobProxy) {
        try {
            return jobProxy.getJobStatus();
        } catch (Exception e) {
            log.warn("Failed to read job status: {}", ExceptionUtils.getMessage(e));
            return JobStatus.UNKNOWABLE;
        }
    }

    private static SeaTunnelServer seaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    private static CheckpointStorage checkpointStorage(HazelcastInstanceImpl masterNode) {
        return seaTunnelServer(masterNode).getCheckpointService().getCheckpointStorage();
    }

    /** Lists the persisted checkpoints of a job. */
    private static List<?> listCheckpoints(CheckpointStorage checkpointStorage, long jobId) {
        try {
            return checkpointStorage.getAllCheckpoints(String.valueOf(jobId));
        } catch (CheckpointStorageException e) {
            return Assertions.fail("Failed to list checkpoints of job " + jobId, e);
        }
    }
}
