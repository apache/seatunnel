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
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointCounts;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.InProgressCheckpoint;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * End-to-end regression coverage for a savepoint request that lands while a periodic checkpoint is
 * still in flight.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A checkpoint owns the pipeline's
 * barrier until every task acknowledges it, and a savepoint is itself a checkpoint. Whether a
 * savepoint request arrives while a periodic checkpoint is in flight is pure timing, so a
 * regression on this path surfaces as an intermittent stop-with-savepoint failure that is hard to
 * reproduce by hand. Two plausible coordinator changes break it: rejecting the savepoint while a
 * checkpoint is pending (a tempting simplification of the existing wait), and canceling the pending
 * checkpoint so the savepoint can start immediately. The first turns stop-with-savepoint into an
 * intermittent failure; the second makes the checkpoint overview that operators and the REST API
 * read report a failed checkpoint around every savepoint. Because the fixture keeps a checkpoint in
 * flight for five seconds out of every six, the test lands in that window deterministically instead
 * of hoping to catch it.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code SavepointPreconditionRecoveryIT}
 * fires a savepoint while tasks are not yet ready, which is rejected before a checkpoint is
 * created, and {@code SavepointBusySourceBarrierIT} only delays the savepoint's own final barrier.
 * {@code SavepointRestoreIT} and {@code SavepointClusterRestartRestoreIT} issue savepoints when no
 * checkpoint is in flight. No E2E triggers a savepoint deliberately inside the window of a pending
 * periodic checkpoint and then checks the in-flight checkpoint's final history status.
 *
 * <p><b>How the contract is pinned.</b> The fixture's InMemory sink sleeps five seconds in {@code
 * prepareCommit}, while the checkpoint interval is one second, so a periodic checkpoint is pending
 * for most of the job's lifetime. The test waits (white-box, through the checkpoint overview state
 * store) for a checkpoint that was triggered less than two seconds ago, immediately issues a real
 * client savepoint, and then requires that: the savepoint completes within a bounded time and the
 * job reaches {@link JobStatus#SAVEPOINT_DONE}; the previously in-flight checkpoint appears in the
 * completed history (it was waited for, not abandoned); no checkpoint is recorded as failed or
 * canceled; no pipeline was restored; and the savepoint id is newer than the checkpoint it waited
 * for.
 */
@Slf4j
public class SavepointDuringInFlightCheckpointIT {

    private static final String CONF_FILE = "stream_fake_to_inmemory_slow_checkpoint.conf";

    /** The fixture is a single FakeSource -> InMemory pipeline, so its id is always 1. */
    private static final int PIPELINE_ID = 1;

    /**
     * Only accept an in-flight checkpoint observed within this many milliseconds of its trigger, so
     * the savepoint request lands with several seconds left before the five-second commit sleep
     * finishes and actually has to wait.
     */
    private static final long IN_FLIGHT_ACCEPT_MAX_AGE_MILLIS = 2000L;

    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;

    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120L;

    private static final long CHECKPOINT_TIMEOUT_SECONDS = 180L;

    private static final long SAVEPOINT_TIMEOUT_SECONDS = 180L;

    @Test
    public void testSavepointWaitsForTheInFlightCheckpoint() throws Exception {
        String testClusterName =
                "SavepointDuringInFlightCheckpointIT_" + System.currentTimeMillis();

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

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            SeaTunnelClient finalEngineClient = engineClient;
            SeaTunnelConfig clientSeaTunnelConfig = getSeaTunnelConfig(testClusterName);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("savepoint-during-in-flight-checkpoint");
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(CONF_FILE), jobConfig, clientSeaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);
            // A completed checkpoint first, so the overview exists and the in-flight checkpoint we
            // observe afterwards is provably a later one.
            awaitCompletedCheckpoint(masterNode, jobId);

            InProgressCheckpoint inFlightCheckpoint =
                    awaitFreshInFlightCheckpoint(masterNode, jobId);
            long inFlightCheckpointId = inFlightCheckpoint.getCheckpointId();
            log.warn(
                    "Issuing savepoint for job {} while checkpoint {} is in flight (triggered at {})",
                    jobId,
                    inFlightCheckpointId,
                    inFlightCheckpoint.getTriggerTimestamp());

            // The savepoint request blocks until the in-flight checkpoint settles and its own
            // savepoint checkpoint completes; bound it so a lost wait cannot hang the suite.
            Assertions.assertTimeoutPreemptively(
                    Duration.ofSeconds(SAVEPOINT_TIMEOUT_SECONDS),
                    () -> finalEngineClient.savePointJob(jobId),
                    "A savepoint requested during an in-flight checkpoint must complete instead of"
                            + " waiting forever");

            awaitJobStatus(clientJobProxy, JobStatus.SAVEPOINT_DONE);

            CheckpointOverview overview = awaitCheckpointOverview(masterNode, jobId);
            PipelineCheckpointOverview pipelineOverview = overview.getPipelines().get(PIPELINE_ID);
            Assertions.assertNotNull(
                    pipelineOverview,
                    "The pipeline checkpoint overview disappeared after the savepoint");
            CheckpointCounts counts = pipelineOverview.getCounts();

            // The decisive assertion: the checkpoint that was pending when the savepoint was
            // requested must have been waited for and recorded as completed, not abandoned.
            // The overview state store carries the same history the client-facing history API
            // reads; it is inspected directly because that API is not usable from this test's
            // in-process master (it performs a blocking map read on an operation thread).
            Assertions.assertTrue(
                    pipelineOverview.getHistory().stream()
                            .anyMatch(
                                    entry ->
                                            entry.getCheckpointInfo().getCheckpointId()
                                                            == inFlightCheckpointId
                                                    && entry.getCheckpointInfo().getStatus()
                                                            == CheckpointStatus.COMPLETED),
                    "The checkpoint in flight when the savepoint was requested must still complete,"
                            + " but it is absent from the completed history. Observed history: "
                            + historySummary(pipelineOverview));

            Assertions.assertNull(
                    pipelineOverview.getLatestFailed(),
                    "No checkpoint may fail or be canceled just because a savepoint was requested"
                            + " while it was in flight");
            Assertions.assertEquals(
                    0L,
                    counts.getFailed(),
                    "The coordinator recorded failed checkpoints around the savepoint");
            Assertions.assertEquals(
                    0L,
                    counts.getRestored(),
                    "A pipeline was restored around the savepoint, which means the in-flight"
                            + " checkpoint was aborted instead of completed");
            Assertions.assertTrue(
                    pipelineOverview.getInProgress().isEmpty(),
                    "No checkpoint may remain pending after the job reached SAVEPOINT_DONE, but"
                            + " found: "
                            + pipelineOverview.getInProgress());

            Assertions.assertNotNull(
                    pipelineOverview.getLatestSavepoint(),
                    "The savepoint checkpoint was not recorded in the checkpoint overview");
            long savepointId = pipelineOverview.getLatestSavepoint().getCheckpointId();
            Assertions.assertTrue(
                    savepointId > inFlightCheckpointId,
                    "The savepoint must allocate an id newer than the checkpoint it waited for: "
                            + savepointId
                            + " vs "
                            + inFlightCheckpointId);
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

    private static String historySummary(PipelineCheckpointOverview pipelineOverview) {
        return pipelineOverview.getHistory().stream()
                .map(
                        entry ->
                                entry.getCheckpointInfo().getCheckpointId()
                                        + ":"
                                        + entry.getCheckpointInfo().getStatus())
                .collect(java.util.stream.Collectors.joining(", "));
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expectedStatus) {
        Awaitility.await()
                .atMost(JOB_STATUS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus,
                                        jobProxy.getJobStatus(),
                                        "Waiting for job "
                                                + jobProxy.getJobId()
                                                + " to reach "
                                                + expectedStatus));
    }

    private static void awaitCompletedCheckpoint(HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            CheckpointOverview overview = checkpointOverview(masterNode, jobId);
                            Assertions.assertNotNull(overview, "No checkpoint overview yet");
                            PipelineCheckpointOverview pipelineOverview =
                                    overview.getPipelines().get(PIPELINE_ID);
                            Assertions.assertNotNull(
                                    pipelineOverview, "No checkpoint overview for the pipeline");
                            Assertions.assertNotNull(
                                    pipelineOverview.getLatestCompleted(),
                                    "Waiting for a completed checkpoint");
                        });
    }

    /**
     * Returns an in-flight checkpoint that was triggered only a moment ago, so the subsequent
     * savepoint request is guaranteed to arrive while that checkpoint is still waiting for the
     * sink's five-second commit sleep.
     */
    private static InProgressCheckpoint awaitFreshInFlightCheckpoint(
            HazelcastInstanceImpl masterNode, long jobId) {
        AtomicReference<InProgressCheckpoint> observed = new AtomicReference<>();
        Awaitility.await()
                .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(50, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            CheckpointOverview overview = checkpointOverview(masterNode, jobId);
                            Assertions.assertNotNull(overview, "No checkpoint overview yet");
                            PipelineCheckpointOverview pipelineOverview =
                                    overview.getPipelines().get(PIPELINE_ID);
                            Assertions.assertNotNull(
                                    pipelineOverview, "No checkpoint overview for the pipeline");
                            Assertions.assertFalse(
                                    pipelineOverview.getInProgress().isEmpty(),
                                    "Waiting for a checkpoint to be in flight");
                            InProgressCheckpoint candidate =
                                    pipelineOverview.getInProgress().get(0);
                            long ageMillis =
                                    System.currentTimeMillis() - candidate.getTriggerTimestamp();
                            Assertions.assertTrue(
                                    candidate.getCheckpointId() > 0
                                            && ageMillis >= 0
                                            && ageMillis < IN_FLIGHT_ACCEPT_MAX_AGE_MILLIS,
                                    "The in-flight checkpoint is not fresh enough to guarantee the"
                                            + " savepoint has to wait for it, age="
                                            + ageMillis
                                            + "ms");
                            observed.set(candidate);
                        });
        return observed.get();
    }

    private static CheckpointOverview awaitCheckpointOverview(
            HazelcastInstanceImpl masterNode, long jobId) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .until(
                        () -> {
                            CheckpointOverview overview = checkpointOverview(masterNode, jobId);
                            return overview != null
                                    && overview.getPipelines().get(PIPELINE_ID) != null;
                        });
        return checkpointOverview(masterNode, jobId);
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

    private static SeaTunnelServer seaTunnelServer(HazelcastInstanceImpl node) {
        return node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
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

    private static void awaitClusterSize(HazelcastInstanceImpl node, int expected) {
        Awaitility.await()
                .atMost(CLUSTER_FORMATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected, node.getCluster().getMembers().size()));
    }
}
