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
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Regression test for <a href="https://github.com/apache/seatunnel/issues/13504">#13504</a>: {@code
 * JobClient#getCheckpointHistory} (and the sibling {@code #getCheckpointOverview}) used to fail on
 * every call with {@code AssertionError: Waiting for response on this thread is illegal}.
 *
 * <p>The client sends both requests to the active master, so {@code
 * GetCheckpointHistoryOperation#run}/{@code GetCheckpointOverviewOperation#run} executed on a
 * Hazelcast operation thread. Both read the checkpoint monitor state through {@code
 * CheckpointMonitorService}, whose backing {@code CheckpointOverviewStateStore} is a distributed
 * {@code IMap}; {@code IMap#get} blocks the calling thread until the response arrives, and
 * Hazelcast forbids that wait on an operation thread. The REST endpoints kept working because their
 * servlet runs on a Jetty thread, which hid the bug from the only end-to-end coverage that existed
 * ({@link RestApiIT#testCheckpointOverviewAndHistoryApi()} exercises the HTTP path). The Java
 * client API - and therefore {@code bin/seatunnel.sh --checkpoint-history-job-id} - was unusable.
 *
 * <p>The fix serves both requests asynchronously (the monitor state is read with a non-blocking
 * {@code IMap#getAsync}, and the operation completes its response from that future), so this test
 * drives the exact path the CLI and the Java client use against a real split master/worker cluster
 * and a real streaming job with real periodic checkpoints. It asserts the history is a coherent
 * view of the same monitor state the overview exposes, and that the job id / pipeline id / status /
 * limit filters are actually applied by the master, because a regression that returns an empty or
 * unfiltered list is exactly the kind of silent breakage this API has no other coverage for.
 */
public class CheckpointHistoryClientIT {

    private static final String CONF = "stream_fake_to_console_checkpoint_history.conf";

    /** The job has a single FakeSource -&gt; Console pipeline, so its id is always 1. */
    private static final int PIPELINE_ID = 1;

    private static final int HISTORY_LIMIT = 20;

    @Test
    public void testCheckpointHistoryAndOverviewThroughClientProtocol() throws Exception {
        String testCaseName = "testCheckpointHistoryAndOverviewThroughClientProtocol";
        String clusterName = TestUtils.getClusterName(testCaseName);

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterConfig = getSeaTunnelConfig(clusterName);
        SeaTunnelConfig workerConfig = getSeaTunnelConfig(clusterName);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(CONF), jobConfig, masterConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();
            JobClient jobClient = engineClient.getJobClient();

            // Wait for the real checkpoint schedule to complete more than one checkpoint so the
            // history has a deterministic, non-empty window to filter. The overview itself is part
            // of the client protocol (GetCheckpointOverviewOperation), so this also proves the
            // sibling request that shared the same blocking read is served to clients again.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                CheckpointOverview overview =
                                        jobClient.getCheckpointOverview(jobId);
                                Assertions.assertNotNull(
                                        overview,
                                        "The master must serve the checkpoint overview of a"
                                                + " running job to a client request");
                                Assertions.assertTrue(
                                        completedCheckpoints(overview) >= 2,
                                        "Expected at least two completed checkpoints before"
                                                + " asserting on the history, but the overview"
                                                + " reported "
                                                + completedCheckpoints(overview));
                            });

            // Read the overview first and the history second: a checkpoint that completes between
            // the two calls is then guaranteed to be present in the history, so this assertion
            // cannot be broken by the ordinary checkpoint schedule racing the two RPCs.
            CheckpointOverview overview = jobClient.getCheckpointOverview(jobId);
            CheckpointInfo latestCompleted =
                    overview.getPipelines().get(PIPELINE_ID).getLatestCompleted();
            Assertions.assertNotNull(
                    latestCompleted, "The overview must track the latest completed checkpoint");

            List<CheckpointHistoryEntry> history =
                    jobClient.getCheckpointHistory(jobId, null, HISTORY_LIMIT, null);

            Assertions.assertFalse(
                    history.isEmpty(),
                    "Checkpoint history must not be empty for a job whose overview reports"
                            + " completed checkpoints; before the fix this call threw"
                            + " AssertionError instead of returning a list");
            Assertions.assertTrue(
                    history.size() <= HISTORY_LIMIT,
                    "The master must honor the requested history limit, but returned "
                            + history.size()
                            + " entries for limit "
                            + HISTORY_LIMIT);
            history.forEach(
                    entry -> {
                        Assertions.assertEquals(
                                jobId,
                                entry.getJobId(),
                                "Every entry must belong to the requested job");
                        Assertions.assertTrue(
                                entry.getCheckpointInfo().getCheckpointId() > 0,
                                "Every entry must carry a positive checkpoint id");
                    });
            for (int i = 1; i < history.size(); i++) {
                Assertions.assertTrue(
                        history.get(i - 1).getCheckpointInfo().getTriggerTimestamp()
                                >= history.get(i).getCheckpointInfo().getTriggerTimestamp(),
                        "History must be ordered newest first");
            }
            Assertions.assertTrue(
                    history.stream()
                            .anyMatch(
                                    entry ->
                                            entry.getCheckpointInfo().getCheckpointId()
                                                    == latestCompleted.getCheckpointId()),
                    "History must contain the checkpoint the overview reports as the latest"
                            + " completed one ("
                            + latestCompleted.getCheckpointId()
                            + "); the two client APIs read the same monitor state");

            // The pipeline/status filters are applied on the master, not silently dropped.
            List<CheckpointHistoryEntry> latestCompletedOnly =
                    jobClient.getCheckpointHistory(
                            jobId, PIPELINE_ID, 1, CheckpointStatus.COMPLETED);
            Assertions.assertEquals(
                    1,
                    latestCompletedOnly.size(),
                    "A completed-only history limited to one entry must return exactly one entry");
            CheckpointHistoryEntry newestCompleted = latestCompletedOnly.get(0);
            Assertions.assertEquals(
                    CheckpointStatus.COMPLETED, newestCompleted.getCheckpointInfo().getStatus());
            Assertions.assertEquals(PIPELINE_ID, newestCompleted.getPipelineId());

            Assertions.assertEquals(
                    1,
                    jobClient.getCheckpointHistory(jobId, null, 1, null).size(),
                    "A history limited to one entry must return at most one entry across"
                            + " pipelines");

            Assertions.assertTrue(
                    jobClient
                            .getCheckpointHistory(jobId, PIPELINE_ID + 100, HISTORY_LIMIT, null)
                            .isEmpty(),
                    "The history of an unknown pipeline must be empty");

            Assertions.assertTrue(
                    jobClient
                            .getCheckpointHistory(jobId + 1_000_000L, null, HISTORY_LIMIT, null)
                            .isEmpty(),
                    "The history of an unknown job must be empty");

            // A job without any checkpoint monitor state must still get a usable answer. The
            // protocol response is a non-nullable Data field, so answering with a null payload made
            // the response encoding fail with an NPE as soon as getCheckpointOverview was called
            // before the job's first checkpoint; like the REST endpoint, the empty overview has no
            // pipelines.
            CheckpointOverview unknownOverview =
                    jobClient.getCheckpointOverview(jobId + 1_000_000L);
            Assertions.assertNotNull(
                    unknownOverview,
                    "The overview of a job without monitor state must not be null");
            Assertions.assertTrue(
                    unknownOverview.getPipelines().isEmpty(),
                    "The overview of a job without monitor state must not report pipelines");
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

    private static long completedCheckpoints(CheckpointOverview overview) {
        PipelineCheckpointOverview pipeline = overview.getPipelines().get(PIPELINE_ID);
        return pipeline == null ? 0L : pipeline.getCounts().getCompleted();
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }
}
