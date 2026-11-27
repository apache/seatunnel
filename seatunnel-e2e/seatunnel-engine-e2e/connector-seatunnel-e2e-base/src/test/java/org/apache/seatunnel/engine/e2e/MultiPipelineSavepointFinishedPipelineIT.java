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
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end coverage for stop-with-savepoint on a multi-pipeline job whose sibling pipeline has
 * already finished cleanly.
 *
 * <p><b>The contract.</b> A multi-pipeline job only ends when every pipeline has ended, so a job
 * can be {@code RUNNING} while one of its pipelines is already {@code FINISHED}. Triggering a
 * savepoint at that moment must savepoint the pipelines that are still alive and leave the finished
 * one alone:
 *
 * <ol>
 *   <li>the savepoint request succeeds instead of being rejected with {@code
 *       CHECKPOINT_COORDINATOR_SHUTDOWN} because a completed pipeline's coordinator is no longer
 *       accepting savepoints;
 *   <li>the running pipeline's savepoint is written to checkpoint storage and the job reaches
 *       {@code SAVEPOINT_DONE}, so the caller gets a usable recovery point;
 *   <li>the finished pipeline's final checkpoint is left untouched and is not deleted by the
 *       savepoint, and the job-level savepoint-end check still recognizes the job as savepointed so
 *       a later terminal cleanup cannot delete the savepoint data;
 *   <li>the savepoint does not fall back to the failure path ({@code savepointFailed()} -> {@code
 *       stopJob()}), which would force-stop the healthy pipeline and report the savepoint as
 *       failed.
 * </ol>
 *
 * <p><b>Why this is a boundary with real regression risk.</b> {@code CheckpointManager} keeps one
 * coordinator per pipeline for the whole job, including pipelines that already completed. The
 * savepoint fan-out used to call {@code startSavepoint()} on every coordinator, and a completed
 * coordinator rejects it by contract ({@code shutdown || isCompleted()}). The rejection then raced
 * the still-running pipeline's genuine savepoint: the running pipeline suspended while the job
 * decided the savepoint had failed, force-stopped the healthy pipeline and threw to the client.
 * That turns a recovery operation into an outage, and because the job then terminates through the
 * stop path its checkpoint data can be cleaned up as well.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code MultiPipelineSavepointRestoreIT}
 * (the two-active-pipeline savepoint/restore test) and {@code SavepointRestoreIT} only exercise
 * jobs whose pipelines are all still running; {@code SavepointBusySourceBarrierIT} and {@code
 * SavepointPreconditionRecoveryIT} cover a single pipeline. No E2E submits a savepoint to a
 * multi-pipeline job with a cleanly finished sibling pipeline.
 */
public class MultiPipelineSavepointFinishedPipelineIT {

    private static final String JOB_CONF =
            "multi-pipeline-finished-savepoint/batch_two_pipeline_one_finished.conf";

    /** The fixture has two independent FakeSource -> Console pipelines. */
    private static final int PIPELINE_COUNT = 2;

    private static final int PIPELINE_ID_START = 1;

    private static final long STATUS_TIMEOUT_SECONDS = 120;

    @Test
    public void testSavepointWhileSiblingPipelineAlreadyFinished() throws Exception {
        String clusterName =
                TestUtils.getClusterName(
                        "MultiPipelineSavepointFinishedPipelineIT_testSavepointWhileSiblingFinished");
        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy job = null;

        SeaTunnelConfig seaTunnelConfig = newSeaTunnelConfig(clusterName);
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            engineClient = new SeaTunnelClient(newClientConfig(clusterName));

            job = submit(engineClient, seaTunnelConfig);
            final SeaTunnelClient client = engineClient;
            final ClientJobProxy submittedJob = job;
            final HazelcastInstanceImpl testNode = node;
            long jobId = submittedJob.getJobId();

            // Wait for the asymmetric state the contract is about: one pipeline FINISHED, the
            // sibling still RUNNING. The slow pipeline reads a split every two seconds, which
            // leaves a wide, deterministic window after the fast pipeline has ended.
            int finishedPipelineId = awaitOneFinishedOneRunning(node, jobId);
            int runningPipelineId = otherPipeline(finishedPipelineId);

            JobMaster jobMaster = getJobMaster(node, jobId);
            Assertions.assertNotNull(jobMaster, "The running job must have a job master");
            CheckpointManager checkpointManager = jobMaster.getCheckpointManager();
            Assertions.assertTrue(
                    checkpointManager.getCheckpointCoordinator(finishedPipelineId).isCompleted(),
                    "The finished pipeline's checkpoint coordinator must be completed before the"
                            + " savepoint request");
            Assertions.assertFalse(
                    checkpointManager.getCheckpointCoordinator(runningPipelineId).isCompleted(),
                    "The running pipeline's checkpoint coordinator must still be active");
            Assertions.assertFalse(
                    checkpointManager.isSavePointEnd(),
                    "A job without any savepoint must not report savepoint completion");

            // Core regression assertion: the savepoint must succeed even though one pipeline of
            // the job already ended. Pre-fix this throws a SeaTunnelEngineException caused by
            // CheckpointException(CHECKPOINT_COORDINATOR_SHUTDOWN) from the finished pipeline.
            Assertions.assertDoesNotThrow(
                    () -> client.savePointJob(jobId),
                    "A savepoint request must not fail because a sibling pipeline already"
                            + " finished");

            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.SAVEPOINT_DONE,
                                            submittedJob.getJobStatus(),
                                            "The job must end SAVEPOINT_DONE, not through the"
                                                    + " savepoint-failure stop path"));

            Assertions.assertTrue(
                    checkpointManager
                            .getCheckpointCoordinator(runningPipelineId)
                            .isEndOfSavePoint(),
                    "The running pipeline must have written a savepoint checkpoint");
            Assertions.assertFalse(
                    checkpointManager
                            .getCheckpointCoordinator(finishedPipelineId)
                            .isEndOfSavePoint(),
                    "The finished pipeline must be left alone; its last checkpoint is a completed"
                            + " final checkpoint, not a savepoint");
            Assertions.assertTrue(
                    checkpointManager.isSavePointEnd(),
                    "The job-level savepoint-end check must accept a pipeline that finished"
                            + " before the savepoint; otherwise terminal cleanup may delete the"
                            + " savepoint data");

            CheckpointStorage checkpointStorage = getCheckpointStorage(node);
            Assertions.assertTrue(
                    hasSavepointCheckpoint(checkpointStorage, jobId, runningPipelineId),
                    "The savepoint of the running pipeline must be persisted in checkpoint"
                            + " storage");
            Assertions.assertFalse(
                    checkpointStorage
                            .getCheckpointsByJobIdAndPipelineId(
                                    String.valueOf(jobId), String.valueOf(finishedPipelineId))
                            .isEmpty(),
                    "The finished pipeline's final checkpoint must not be deleted by the"
                            + " savepoint");

            // The savepoint path must release the job master instead of leaving the job in
            // DOING_SAVEPOINT with a retained coordinator.
            Awaitility.await()
                    .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .ignoreExceptions()
                    .untilAsserted(
                            () ->
                                    Assertions.assertNull(
                                            getCoordinatorService(testNode).getJobMaster(jobId),
                                            "A savepointed job must release its job master"));
        } finally {
            cancelQuietly(job);
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
        return seaTunnelConfig;
    }

    private static ClientConfig newClientConfig(String clusterName) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(clusterName);
        return clientConfig;
    }

    private static ClientJobProxy submit(
            SeaTunnelClient engineClient, SeaTunnelConfig seaTunnelConfig) throws Exception {
        Common.setDeployMode(DeployMode.CLIENT);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("multi_pipeline_finished_savepoint");
        return engineClient
                .createExecutionContext(TestUtils.getResource(JOB_CONF), jobConfig, seaTunnelConfig)
                .execute();
    }

    /**
     * Waits for the exact state under test and returns the pipeline id that already finished. If
     * the two pipelines finish in the opposite order the same contract is exercised; the test
     * discovers the direction instead of hard-coding the fixture's pipeline numbering.
     */
    private static int awaitOneFinishedOneRunning(HazelcastInstanceImpl node, long jobId) {
        final int[] finished = {-1};
        Awaitility.await()
                .atMost(STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            Map<Integer, PipelineStatus> statuses = pipelineStatuses(node, jobId);
                            Assertions.assertEquals(
                                    PIPELINE_COUNT,
                                    statuses.size(),
                                    "Expected one state entry per pipeline: " + statuses);
                            long finishedCount =
                                    statuses.values().stream()
                                            .filter(status -> status == PipelineStatus.FINISHED)
                                            .count();
                            long runningCount =
                                    statuses.values().stream()
                                            .filter(status -> status == PipelineStatus.RUNNING)
                                            .count();
                            Assertions.assertEquals(
                                    1,
                                    finishedCount,
                                    "Exactly one pipeline must have finished before the savepoint:"
                                            + " "
                                            + statuses);
                            Assertions.assertEquals(
                                    1,
                                    runningCount,
                                    "The sibling pipeline must still be running before the"
                                            + " savepoint: "
                                            + statuses);
                            finished[0] =
                                    statuses.entrySet().stream()
                                            .filter(
                                                    entry ->
                                                            entry.getValue()
                                                                    == PipelineStatus.FINISHED)
                                            .map(Map.Entry::getKey)
                                            .findFirst()
                                            .orElse(-1);
                        });
        return finished[0];
    }

    private static int otherPipeline(int pipelineId) {
        for (int candidate = PIPELINE_ID_START;
                candidate < PIPELINE_ID_START + PIPELINE_COUNT;
                candidate++) {
            if (candidate != pipelineId) {
                return candidate;
            }
        }
        throw new IllegalStateException("No sibling pipeline for " + pipelineId);
    }

    private static Map<Integer, PipelineStatus> pipelineStatuses(
            HazelcastInstanceImpl node, long jobId) {
        Map<Integer, PipelineStatus> statuses = new HashMap<>();
        for (int pipelineId = PIPELINE_ID_START;
                pipelineId < PIPELINE_ID_START + PIPELINE_COUNT;
                pipelineId++) {
            Object state =
                    node.getMap(Constant.IMAP_RUNNING_JOB_STATE)
                            .get(new PipelineLocation(jobId, pipelineId));
            if (state instanceof PipelineStatus) {
                statuses.put(pipelineId, (PipelineStatus) state);
            }
        }
        return statuses;
    }

    private static boolean hasSavepointCheckpoint(
            CheckpointStorage checkpointStorage, long jobId, int pipelineId) throws Exception {
        List<PipelineState> states =
                checkpointStorage.getCheckpointsByJobIdAndPipelineId(
                        String.valueOf(jobId), String.valueOf(pipelineId));
        ProtoStuffSerializer serializer = new ProtoStuffSerializer();
        for (PipelineState state : states) {
            CompletedCheckpoint checkpoint =
                    serializer.deserialize(state.getStates(), CompletedCheckpoint.class);
            if (checkpoint.getCheckpointType().isSavepoint()) {
                return true;
            }
        }
        return false;
    }

    private static CheckpointStorage getCheckpointStorage(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCheckpointService().getCheckpointStorage();
    }

    private static JobMaster getJobMaster(HazelcastInstanceImpl node, long jobId) {
        return getCoordinatorService(node).getJobMaster(jobId);
    }

    private static CoordinatorService getCoordinatorService(HazelcastInstanceImpl node) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService();
    }

    private static void cancelQuietly(ClientJobProxy job) {
        if (job == null) {
            return;
        }
        try {
            JobStatus status = job.getJobStatus();
            if (status != null && !status.isEndState()) {
                job.cancelJob();
            }
        } catch (Exception ignored) {
            // best-effort cleanup: the cluster is shut down right after this call
        }
    }
}
