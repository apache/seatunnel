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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * End-to-end regression coverage for the periodic checkpoint min-pause scheduling contract.
 *
 * <p><b>Why this is a boundary with real regression risk.</b> A streaming job can ask for a
 * checkpoint interval that is much shorter than the time its checkpoints take to complete. Without
 * a lower bound on the pause after a completed checkpoint, the coordinator schedules the next
 * checkpoint as soon as the interval elapses, so checkpoint barriers, storage writes and sink
 * commits follow each other back-to-back. That is a checkpoint storm: it multiplies master CPU,
 * storage traffic and end-to-end backpressure while every checkpoint still "succeeds", so no
 * existing assertion notices. The engine therefore guarantees that a regular checkpoint is not
 * triggered until {@code min-pause} milliseconds have passed since the previous checkpoint
 * completed. The pause is part of the user-facing configuration documented in the checkpoint and
 * fault-tolerance guide, which allows configuring it through the job {@code env} block.
 *
 * <p><b>Why the existing coverage does not reach it.</b> {@code CheckpointEnableIT} only asserts
 * that a streaming job checkpoints at all with a given interval. {@code UnifyEnvParameterIT} maps a
 * {@code min-pause} value from the job env block but never inspects when checkpoints are actually
 * triggered. The savepoint/failover suites wait for checkpoint ids to advance and never measure the
 * spacing between them. The min-pause branch in {@code CheckpointCoordinator
 * #tryTriggerPendingCheckpoint} is therefore only covered indirectly, if at all, and a regression
 * that drops the branch would leave every existing test green.
 *
 * <p><b>How the contract is pinned.</b> The job runs with {@code checkpoint.interval = 300} and
 * {@code min-pause = 3000}. The test waits until four regular checkpoints complete, reads their
 * trigger and completion timestamps from the cluster-wide checkpoint overview, and requires that
 * each checkpoint starts no earlier than {@code min-pause} after the previous one completed. With
 * the configured values a broken min-pause produces a new trigger roughly every 300 ms, which is an
 * order of magnitude below the asserted bound, so the test cannot pass by accident. The job must
 * also still be {@code RUNNING} afterwards, proving the pause delays checkpoints instead of
 * stalling the coordinator.
 */
@Slf4j
public class CheckpointMinPauseIT {

    /** Streaming fixture: FakeSource -> Console with a short interval and a long min-pause. */
    private static final String CONF_FILE = "stream_fake_to_console_checkpoint_min_pause.conf";

    /** Must match {@code min-pause} in {@link #CONF_FILE}. */
    private static final long MIN_PAUSE_MILLIS = 3000L;

    /** Absorbs scheduler jitter between the recorded completion and the next scheduled trigger. */
    private static final long SCHEDULER_TOLERANCE_MILLIS = 250L;

    /** Number of regular checkpoints whose spacing is asserted. */
    private static final int CHECKPOINTS_TO_OBSERVE = 4;

    /** Upper bound to wait for the one-master/one-worker cluster to form. */
    private static final long CLUSTER_FORMATION_TIMEOUT_SECONDS = 60L;

    /** Upper bound to wait for a submitted job to reach a given status. */
    private static final long JOB_STATUS_TIMEOUT_SECONDS = 120L;

    /** Upper bound to wait for the requested number of periodic checkpoints to complete. */
    private static final long CHECKPOINT_TIMEOUT_SECONDS = 180L;

    @Test
    public void testMinPauseHoldsBackTheNextCheckpointAfterACompletion() throws Exception {
        String testClusterName = "CheckpointMinPauseIT_" + System.currentTimeMillis();

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy jobProxy = null;
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
            jobConfig.setName("checkpoint-min-pause");

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            jobProxy =
                    engineClient
                            .createExecutionContext(
                                    confPath, jobConfig, getSeaTunnelConfig(testClusterName))
                            .execute();
            long jobId = jobProxy.getJobId();
            awaitJobStatus(jobProxy, JobStatus.RUNNING);

            List<CheckpointInfo> completed =
                    awaitCompletedCheckpoints(masterNode, jobId, CHECKPOINTS_TO_OBSERVE);
            List<CheckpointInfo> observed =
                    completed.subList(0, Math.min(CHECKPOINTS_TO_OBSERVE, completed.size()));

            Assertions.assertEquals(
                    CHECKPOINTS_TO_OBSERVE,
                    observed.size(),
                    "The job must complete enough periodic checkpoints to measure the pause");

            for (int i = 1; i < observed.size(); i++) {
                CheckpointInfo previous = observed.get(i - 1);
                CheckpointInfo next = observed.get(i);

                Assertions.assertTrue(
                        next.getTriggerTimestamp() > previous.getTriggerTimestamp(),
                        () ->
                                "Checkpoint triggers must move forward in time: "
                                        + next
                                        + " vs "
                                        + previous);

                long pauseAfterCompletion =
                        next.getTriggerTimestamp() - previous.getCompletedTimestamp();
                Assertions.assertTrue(
                        pauseAfterCompletion >= MIN_PAUSE_MILLIS - SCHEDULER_TOLERANCE_MILLIS,
                        () ->
                                String.format(
                                        "Checkpoint %d started %d ms after checkpoint %d completed."
                                                + " min-pause is %d ms, so the next regular"
                                                + " checkpoint must not be triggered earlier: %s -> %s",
                                        next.getCheckpointId(),
                                        pauseAfterCompletion,
                                        previous.getCheckpointId(),
                                        MIN_PAUSE_MILLIS,
                                        previous,
                                        next));
            }

            Assertions.assertEquals(
                    JobStatus.RUNNING,
                    jobProxy.getJobStatus(),
                    "The min-pause must delay checkpoints without stopping the job");
            log.info(
                    "Observed {} completed checkpoints for job {}: {}",
                    observed.size(),
                    jobId,
                    observed);
        } finally {
            ClientJobProxy canceledProxy = jobProxy;
            if (canceledProxy != null) {
                try {
                    canceledProxy.cancelJob();
                    Awaitility.await()
                            .atMost(60, TimeUnit.SECONDS)
                            .pollInterval(200, TimeUnit.MILLISECONDS)
                            .untilAsserted(
                                    () ->
                                            Assertions.assertTrue(
                                                    canceledProxy.getJobStatus().isEndState(),
                                                    "Waiting for the job to reach a terminal state"));
                } catch (Exception e) {
                    log.warn("Failed to cancel job during cleanup: {}", e.getMessage());
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
        }
    }

    private static List<CheckpointInfo> awaitCompletedCheckpoints(
            HazelcastInstanceImpl masterNode, long jobId, int expected) {
        Awaitility.await()
                .atMost(CHECKPOINT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        completedCheckpoints(masterNode, jobId).size() >= expected,
                                        () ->
                                                "Waiting for "
                                                        + expected
                                                        + " completed checkpoints, saw "
                                                        + completedCheckpoints(masterNode, jobId)
                                                                .size()));
        return completedCheckpoints(masterNode, jobId);
    }

    /**
     * Returns the completed regular checkpoints of pipeline 1 ordered by checkpoint id. Savepoints
     * and failed checkpoints are excluded because only the periodic scheduling path is asserted
     * here.
     */
    private static List<CheckpointInfo> completedCheckpoints(
            HazelcastInstanceImpl masterNode, long jobId) {
        CheckpointOverview overview = checkpointOverview(masterNode, jobId);
        if (overview == null) {
            return Collections.emptyList();
        }
        PipelineCheckpointOverview pipeline = overview.getPipelines().get(1);
        if (pipeline == null) {
            return Collections.emptyList();
        }
        return pipeline.getHistory().stream()
                .map(CheckpointHistoryEntry::getCheckpointInfo)
                .filter(info -> info.getStatus() == CheckpointStatus.COMPLETED)
                .filter(info -> info.getCheckpointType() == CheckpointType.CHECKPOINT_TYPE)
                .sorted(Comparator.comparingLong(CheckpointInfo::getCheckpointId))
                .collect(Collectors.toList());
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
