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
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Regression guard for the scope of a checkpoint failure inside a multi-pipeline Zeta job: a
 * checkpoint that expires in one pipeline may only restart that pipeline, and must leave the
 * sibling pipeline, its checkpoint chain and the job itself untouched.
 *
 * <p>Scenario: a single in-process master-and-worker node runs one STREAMING job with two
 * independent pipelines, both checkpointing every 2 s with a 3 s checkpoint timeout. The sink of
 * the first pipeline is the test {@code InMemory} sink with {@code checkpoint_sleep = true}, whose
 * {@code prepareCommit()} sleeps 5 s, so every one of its checkpoints is guaranteed to expire
 * instead of completing; the second pipeline's sink is fast and keeps completing checkpoints. The
 * job raises {@code job.retry.times} well above the two restore cycles observed here, so the
 * per-pipeline restore budget cannot run out while the assertions run.
 *
 * <p>Contract asserted here:
 *
 * <ol>
 *   <li>An expired checkpoint is reported as {@code FAILED} with the checkpoint-expiry reason,
 *       never as {@code COMPLETED}, and the expiring pipeline owns no completed checkpoint while
 *       its only checkpoint is stuck (no partially persisted checkpoint).
 *   <li>The checkpoint error restarts only the failing pipeline: its restore count increases and
 *       its next checkpoint attempt comes from a run that started after the restore, reusing
 *       neither the expired checkpoint nor its id (the persisted checkpoint-id counter must survive
 *       a checkpoint-error restore).
 *   <li>The sibling pipeline is untouched across that restore: no failed checkpoint, no pipeline
 *       restore, its completed-checkpoint count and history survive instead of being cleared, and
 *       it keeps completing new checkpoints.
 *   <li>The job never leaves {@code RUNNING} while the expiring pipeline restarts within its retry
 *       budget, and it still cancels cleanly back to {@code CANCELED} at the end.
 * </ol>
 *
 * <p>Existing coverage does not exercise this window: {@code RestApiIT} injects a failed checkpoint
 * directly into {@code CheckpointMonitorService}, {@code SavepointBusySourceBarrierIT} and {@code
 * CheckpointCoordinatorFailoverIT} fail a single-pipeline job over, and the only multi-pipeline
 * engine E2E ({@code ClusterFaultToleranceTwoPipelineIT}) never lets one pipeline's checkpoint
 * expire while the other keeps checkpointing, so nothing asserts that checkpoint-error handling and
 * the restore bookkeeping it clears are scoped to the failing pipeline.
 */
@Slf4j
public class CheckpointExpiryPipelineScopeIT {

    private static final String TWO_PIPELINE_CONF =
            "checkpoint-expiry-scope/stream_two_pipeline_expiring_checkpoint.conf";

    /** Pipelines in the job: one whose checkpoints always expire, one that stays healthy. */
    private static final int PIPELINE_COUNT = 2;

    /** Checkpoint history depth used when reading the monitor's per-pipeline history. */
    private static final int HISTORY_LIMIT = 500;

    private HazelcastInstanceImpl node;
    private SeaTunnelClient engineClient;

    @Test
    public void testCheckpointExpiryRestartsOnlyItsOwnPipeline() throws Exception {
        String clusterName = "CheckpointExpiryPipelineScopeIT_" + System.currentTimeMillis();
        ClientJobProxy job = null;
        try {
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
            seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);

            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            NodeEngineImpl nodeEngine = node.node.nodeEngine;
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = server.getCoordinatorService();
            CheckpointMonitorService checkpointMonitor = server.getCheckpointMonitorService();

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("checkpoint-expiry-pipeline-scope");
            ClientJobExecutionEnvironment executionEnvironment =
                    engineClient.createExecutionContext(
                            TestUtils.getResource(TWO_PIPELINE_CONF), jobConfig, seaTunnelConfig);
            job = executionEnvironment.execute();
            long jobId = job.getJobId();

            awaitStatus(coordinatorService, jobId, JobStatus.RUNNING);
            awaitPipelineCount(checkpointMonitor, jobId, PIPELINE_COUNT);

            // ---- Phase 1: exactly one pipeline's checkpoint expires instead of completing.
            AtomicInteger expiringPipelineId = new AtomicInteger(-1);
            AtomicLong expiredCheckpointId = new AtomicLong(-1L);
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Map<Integer, PipelineCheckpointOverview> pipelines =
                                        pipelines(checkpointMonitor, jobId);
                                Assertions.assertEquals(
                                        PIPELINE_COUNT,
                                        pipelines.size(),
                                        "the job should report two pipelines: "
                                                + describe(pipelines));
                                PipelineCheckpointOverview expiring =
                                        pipelines.values().stream()
                                                .filter(
                                                        pipeline ->
                                                                pipeline.getCounts().getFailed()
                                                                        > 0)
                                                .findFirst()
                                                .orElseThrow(
                                                        () ->
                                                                new AssertionError(
                                                                        "one pipeline's checkpoint "
                                                                                + "should expire "
                                                                                + "instead of "
                                                                                + "completing: "
                                                                                + describe(
                                                                                        pipelines)));
                                PipelineCheckpointOverview sibling = siblingOf(pipelines, expiring);
                                expiringPipelineId.set(pipelineIdOf(pipelines, expiring));
                                expiredCheckpointId.set(
                                        expiring.getLatestFailed().getCheckpointId());
                                Assertions.assertTrue(
                                        sibling.getCounts().getCompleted() >= 2L,
                                        "the healthy pipeline should already have completed "
                                                + "checkpoints: "
                                                + describe(pipelines));
                            });

            int expiringId = expiringPipelineId.get();
            long expiredId = expiredCheckpointId.get();
            Map<Integer, PipelineCheckpointOverview> afterFirstExpiry =
                    pipelines(checkpointMonitor, jobId);
            PipelineCheckpointOverview expiring = afterFirstExpiry.get(expiringId);
            PipelineCheckpointOverview sibling = siblingOf(afterFirstExpiry, expiring);

            CheckpointInfo expired = expiring.getLatestFailed();
            Assertions.assertEquals(
                    expiredId,
                    expired.getCheckpointId(),
                    "the recorded expired checkpoint id should be stable");
            Assertions.assertEquals(
                    CheckpointStatus.FAILED,
                    expired.getStatus(),
                    "an expired checkpoint must be recorded as FAILED, not COMPLETED");
            Assertions.assertNotNull(expired.getFailureReason(), "the expiry reason must be kept");
            Assertions.assertTrue(
                    expired.getFailureReason().contains("Checkpoint expired"),
                    "the recorded failure reason must be the checkpoint expiry, but was: "
                            + expired.getFailureReason());
            // The compacted sink never acknowledges the barrier in time, so the expiring pipeline
            // must not end up with a completed checkpoint while its only checkpoint is stuck: a
            // checkpoint that expired may not be persisted as a successful one.
            Assertions.assertEquals(
                    0L,
                    expiring.getCounts().getCompleted(),
                    "the expiring pipeline must not report a completed checkpoint: "
                            + describe(afterFirstExpiry));
            Assertions.assertTrue(
                    completedCheckpointIds(checkpointMonitor, jobId, expiringId).isEmpty(),
                    "the expired checkpoint must not appear in the completed history: "
                            + completedCheckpointIds(checkpointMonitor, jobId, expiringId));
            // The sibling is fast: by the time the first expiry is reported it already completed
            // more than one checkpoint and must not have failed or been restored at all.
            Assertions.assertEquals(
                    0L,
                    sibling.getCounts().getFailed(),
                    "the healthy pipeline must not have a failed checkpoint: "
                            + describe(afterFirstExpiry));
            Assertions.assertEquals(
                    0L,
                    sibling.getCounts().getRestored(),
                    "the healthy pipeline must not be restored by its sibling's expiry: "
                            + describe(afterFirstExpiry));
            Assertions.assertTrue(
                    sibling.getCounts().getCompleted() >= 2L,
                    "the healthy pipeline should keep completing checkpoints: "
                            + describe(afterFirstExpiry));
            List<Long> siblingCompletedIds =
                    completedCheckpointIds(
                            checkpointMonitor, jobId, pipelineIdOf(afterFirstExpiry, sibling));
            Assertions.assertTrue(
                    strictlyIncreasing(siblingCompletedIds),
                    "the healthy pipeline's checkpoint chain must stay unique and increasing "
                            + "across its sibling's expiry: "
                            + siblingCompletedIds);
            assertRunning(coordinatorService, jobId);

            long siblingCompletedBeforeRestore = sibling.getCounts().getCompleted();
            int siblingHistorySizeBeforeRestore = sibling.getHistory().size();
            List<Long> siblingCompletedIdsBeforeRestore =
                    completedCheckpointIds(
                            checkpointMonitor, jobId, pipelineIdOf(afterFirstExpiry, sibling));

            // ---- Phase 2: the checkpoint error restarts the failing pipeline, not the job.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                PipelineCheckpointOverview pipeline =
                                        pipelines(checkpointMonitor, jobId).get(expiringId);
                                Assertions.assertNotNull(
                                        pipeline, "the expiring pipeline should still be reported");
                                Assertions.assertTrue(
                                        pipeline.getCounts().getRestored() >= 1L,
                                        "the expiring pipeline must be restored after its "
                                                + "checkpoint expired, overview: "
                                                + describe(pipelines(checkpointMonitor, jobId)));
                            });
            assertRunning(coordinatorService, jobId);
            Assertions.assertEquals(
                    PIPELINE_COUNT,
                    pipelines(checkpointMonitor, jobId).size(),
                    "restoring one pipeline must not drop checkpoint bookkeeping of the job: "
                            + describe(pipelines(checkpointMonitor, jobId)));
            PipelineCheckpointOverview siblingAfterRestore =
                    pipelines(checkpointMonitor, jobId).get(siblingPipelineId(expiringId));
            Assertions.assertNotNull(
                    siblingAfterRestore, "the healthy pipeline should still be reported");
            Assertions.assertEquals(
                    0L,
                    siblingAfterRestore.getCounts().getFailed(),
                    "restoring the expiring pipeline must not fail the sibling's checkpoints: "
                            + describe(pipelines(checkpointMonitor, jobId)));
            Assertions.assertEquals(
                    0L,
                    siblingAfterRestore.getCounts().getRestored(),
                    "restoring the expiring pipeline must not restore the sibling: "
                            + describe(pipelines(checkpointMonitor, jobId)));
            Assertions.assertTrue(
                    siblingAfterRestore.getCounts().getCompleted() >= siblingCompletedBeforeRestore,
                    "the restore of the expiring pipeline must not clear the sibling's completed "
                            + "checkpoint count (was "
                            + siblingCompletedBeforeRestore
                            + ", now "
                            + siblingAfterRestore.getCounts().getCompleted()
                            + ")");
            Assertions.assertTrue(
                    siblingAfterRestore.getHistory().size() >= siblingHistorySizeBeforeRestore,
                    "the restore of the expiring pipeline must not clear the sibling's checkpoint "
                            + "history (was "
                            + siblingHistorySizeBeforeRestore
                            + ", now "
                            + siblingAfterRestore.getHistory().size()
                            + ")");
            List<Long> siblingCompletedIdsAfterRestore =
                    completedCheckpointIds(checkpointMonitor, jobId, siblingPipelineId(expiringId));
            Assertions.assertTrue(
                    siblingCompletedIdsAfterRestore.containsAll(siblingCompletedIdsBeforeRestore),
                    "the restore of the expiring pipeline must not drop the sibling's completed "
                            + "checkpoints (before "
                            + siblingCompletedIdsBeforeRestore
                            + ", after "
                            + siblingCompletedIdsAfterRestore
                            + ")");

            // ---- Phase 3: the restored run resumes checkpointing on a fresh id and a fresh run.
            AtomicLong resumedCheckpointId = new AtomicLong(-1L);
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                List<CheckpointInfo> history =
                                        history(checkpointMonitor, jobId, expiringId);
                                CheckpointInfo resumed =
                                        history.stream()
                                                .filter(info -> info.getCheckpointId() > expiredId)
                                                .findFirst()
                                                .orElseThrow(
                                                        () ->
                                                                new AssertionError(
                                                                        "the restored pipeline "
                                                                                + "should start a "
                                                                                + "new checkpoint after "
                                                                                + expiredId
                                                                                + ", history: "
                                                                                + describe(
                                                                                        history)));
                                resumedCheckpointId.set(resumed.getCheckpointId());
                            });
            List<CheckpointInfo> expiringHistory = history(checkpointMonitor, jobId, expiringId);
            Assertions.assertTrue(
                    resumedCheckpointId.get() > expiredId,
                    "the restored pipeline must not reuse the expired checkpoint id "
                            + expiredId
                            + ", history: "
                            + describe(expiringHistory));
            Assertions.assertEquals(
                    1L,
                    expiringHistory.stream()
                            .filter(info -> info.getCheckpointId() == expiredId)
                            .count(),
                    "the expired checkpoint must be recorded exactly once, history: "
                            + describe(expiringHistory));
            Assertions.assertTrue(
                    uniqueCheckpointIds(expiringHistory),
                    "checkpoint ids of the restored pipeline must not be reused, history: "
                            + describe(expiringHistory));

            // ---- Phase 4: the sibling kept checkpointing through the restore and after it.
            long siblingCompletedBeforeResume =
                    pipelines(checkpointMonitor, jobId)
                            .get(siblingPipelineId(expiringId))
                            .getCounts()
                            .getCompleted();
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            pipelines(checkpointMonitor, jobId)
                                                            .get(siblingPipelineId(expiringId))
                                                            .getCounts()
                                                            .getCompleted()
                                                    > siblingCompletedBeforeResume,
                                            "the healthy pipeline must keep completing checkpoints "
                                                    + "after its sibling was restored: "
                                                    + describe(
                                                            pipelines(checkpointMonitor, jobId))));
            assertRunning(coordinatorService, jobId);

            // ---- Phase 5: the job stays RUNNING and the sibling stays clean while the expiring
            // pipeline keeps cycling through its own restore budget.
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Map<Integer, PipelineCheckpointOverview> stable =
                                        pipelines(checkpointMonitor, jobId);
                                PipelineCheckpointOverview healthy =
                                        stable.get(siblingPipelineId(expiringId));
                                Assertions.assertNotNull(healthy);
                                Assertions.assertEquals(
                                        0L,
                                        healthy.getCounts().getFailed(),
                                        "the healthy pipeline must stay free of failed checkpoints: "
                                                + describe(stable));
                                Assertions.assertEquals(
                                        0L,
                                        healthy.getCounts().getRestored(),
                                        "the healthy pipeline must stay unrestored: "
                                                + describe(stable));
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(jobId),
                                        "the job must stay RUNNING while the expiring pipeline "
                                                + "restarts within its retry budget");
                            });

            // ---- Phase 6: the job still cancels cleanly.
            job.cancelJob();
            awaitStatus(coordinatorService, jobId, JobStatus.CANCELED);
        } finally {
            if (job != null) {
                try {
                    job.cancelJob();
                } catch (Throwable e) {
                    log.info("Ignoring failure while cancelling job during cleanup", e);
                }
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private static void awaitStatus(
            CoordinatorService coordinatorService, long jobId, JobStatus expected) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        coordinatorService.getJobStatus(jobId),
                                        "status of job " + jobId));
    }

    private static void assertRunning(CoordinatorService coordinatorService, long jobId) {
        Assertions.assertEquals(
                JobStatus.RUNNING,
                coordinatorService.getJobStatus(jobId),
                "the job must stay RUNNING across a single pipeline's checkpoint expiry");
    }

    private static void awaitPipelineCount(
            CheckpointMonitorService checkpointMonitor, long jobId, int expected) {
        Awaitility.await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expected,
                                        pipelines(checkpointMonitor, jobId).size(),
                                        "number of pipelines reported for job " + jobId));
    }

    private static Map<Integer, PipelineCheckpointOverview> pipelines(
            CheckpointMonitorService checkpointMonitor, long jobId) {
        return checkpointMonitor
                .getOverview(jobId)
                .map(CheckpointOverview::getPipelines)
                .map(HashMap::new)
                .orElseGet(HashMap::new);
    }

    private static PipelineCheckpointOverview siblingOf(
            Map<Integer, PipelineCheckpointOverview> pipelines,
            PipelineCheckpointOverview expiring) {
        return pipelines.values().stream()
                .filter(pipeline -> pipeline != expiring)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no sibling pipeline in " + pipelines));
    }

    private static int pipelineIdOf(
            Map<Integer, PipelineCheckpointOverview> pipelines,
            PipelineCheckpointOverview pipeline) {
        return pipelines.entrySet().stream()
                .filter(entry -> entry.getValue() == pipeline)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("pipeline is not in the overview"));
    }

    private static int siblingPipelineId(int expiringPipelineId) {
        // The fixture has exactly two pipelines, ids 1 and 2, so the sibling is the other id.
        return expiringPipelineId == 1 ? 2 : 1;
    }

    private static List<CheckpointInfo> history(
            CheckpointMonitorService checkpointMonitor, long jobId, int pipelineId) {
        return checkpointMonitor.getHistory(jobId, pipelineId, HISTORY_LIMIT, null).stream()
                .map(CheckpointHistoryEntry::getCheckpointInfo)
                .collect(Collectors.toList());
    }

    private static List<Long> completedCheckpointIds(
            CheckpointMonitorService checkpointMonitor, long jobId, int pipelineId) {
        return checkpointMonitor
                .getHistory(jobId, pipelineId, HISTORY_LIMIT, CheckpointStatus.COMPLETED).stream()
                .map(entry -> entry.getCheckpointInfo().getCheckpointId())
                .sorted()
                .collect(Collectors.toList());
    }

    private static boolean uniqueCheckpointIds(List<CheckpointInfo> history) {
        List<Long> ids = new ArrayList<>();
        for (CheckpointInfo info : history) {
            if (info.getStatus() == CheckpointStatus.CANCELED) {
                continue;
            }
            ids.add(info.getCheckpointId());
        }
        return ids.stream().distinct().count() == ids.size();
    }

    private static boolean strictlyIncreasing(List<Long> checkpointIds) {
        for (int index = 1; index < checkpointIds.size(); index++) {
            if (checkpointIds.get(index) <= checkpointIds.get(index - 1)) {
                return false;
            }
        }
        return true;
    }

    private static String describe(Map<Integer, PipelineCheckpointOverview> pipelines) {
        StringBuilder builder = new StringBuilder("{");
        pipelines.forEach(
                (pipelineId, pipeline) ->
                        builder.append("pipeline")
                                .append(pipelineId)
                                .append(": completed=")
                                .append(pipeline.getCounts().getCompleted())
                                .append(", failed=")
                                .append(pipeline.getCounts().getFailed())
                                .append(", restored=")
                                .append(pipeline.getCounts().getRestored())
                                .append(", inProgress=")
                                .append(pipeline.getCounts().getInProgress())
                                .append("; "));
        return builder.append("}").toString();
    }

    private static String describe(List<CheckpointInfo> history) {
        return history.stream()
                .map(
                        info ->
                                info.getCheckpointId()
                                        + "/"
                                        + info.getStatus()
                                        + (info.getFailureReason() == null
                                                ? ""
                                                : "(" + info.getFailureReason() + ")"))
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
