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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * End-to-end regression test for <a
 * href="https://github.com/apache/seatunnel/issues/11473">#11473</a> ("[Bug] [Zeta] stop-job with
 * savepoint hangs in DOING_SAVEPOINT"), fixed by <a
 * href="https://github.com/apache/seatunnel/pull/11489">#11489</a>.
 *
 * <p>The reported job was a plain streaming {@code FakeSource -> Console} pipeline whose single
 * split carried 100,000,000 rows. Injecting a savepoint barrier requires the source task's
 * checkpoint lock ({@code SourceFlowLifeCycle#triggerBarrier}), but {@code
 * FakeSourceReader#pollNext} used to emit the entire split while holding that same lock, so the
 * barrier trigger dispatched by {@code CheckpointBarrierTriggerOperation} on the worker could not
 * run until the split was drained many minutes later. The savepoint checkpoint expired first, and
 * because {@code JobMaster#savePoint()} had no failure exit, the job sat in {@link
 * JobStatus#DOING_SAVEPOINT} forever with its slots still occupied. #11489 fixed this on both ends:
 * the reader now emits at most a bounded batch per {@code pollNext} call and requeues the remainder
 * of the split, releasing the lock between batches so the barrier can be injected mid-split, and
 * {@code JobMaster#savePoint} now leaves {@code DOING_SAVEPOINT} deterministically when a savepoint
 * cannot complete.
 *
 * <p>Existing coverage does not exercise this mechanism end to end. {@code SavePointBusySourceTest}
 * (seatunnel-engine-server) reproduces the busy split, but only inside a single in-process server,
 * invoking {@code CoordinatorService#savePoint} directly, so the barrier never crosses a node
 * boundary and no client is involved. {@link SavepointPreconditionRecoveryIT} in this package
 * covers a different branch of the same fix: the precondition rejection ({@code
 * TASK_NOT_ALL_READY_WHEN_SAVEPOINT}) that restores the job to RUNNING, which never involves a busy
 * source or a starved barrier at all.
 *
 * <p>This test drives the literal #11473 scenario through the real multi-node path: a split
 * master/worker cluster, the reproduction job config from the issue, and a real {@link
 * SeaTunnelClient#savePointJob(Long)} request fired only once job metrics prove the source is
 * actively emitting its huge split. The savepoint must complete within a bounded window (pre-fix it
 * never did), and the job metrics recorded at completion must show that the split was still being
 * emitted when the barrier went through, proving the barrier was injected mid-split rather than
 * after the split drained. It then restores the job from that savepoint (which {@code
 * RestoreMode#SAVEPOINT} only accepts from a savepoint-type checkpoint), verifies the restored
 * source resumes emitting from its restored split state, and takes a second stop-with-savepoint
 * against the restored, still-busy source to prove the savepoint is genuinely usable rather than
 * merely reported as done.
 */
@Slf4j
public class SavepointBusySourceBarrierIT {

    /**
     * The reproduction job of #11473: a single 100,000,000-row FakeSource split streamed into a
     * Console sink that discards rows.
     */
    private static final String CONF_FILE = "stream_fake_busy_split_to_console_savepoint.conf";

    /**
     * Must match {@code row.num} in {@link #CONF_FILE}. Even at millions of rows per second the
     * split cannot be drained within the bounds used below, so a source metric strictly below this
     * value at savepoint completion proves the barrier was injected while the split was still in
     * flight.
     */
    private static final long SPLIT_ROW_NUM = 100_000_000L;

    /**
     * Upper bound for a stop-with-savepoint request to complete. It deliberately exceeds the job's
     * {@code checkpoint.timeout} (60s) so that a starved barrier surfaces as a checkpoint expiry
     * and a failed request rather than as a silent hang past this bound; pre-fix the job never left
     * DOING_SAVEPOINT at all, which this bound converts into a clean failure.
     */
    private static final long SAVEPOINT_COMPLETION_TIMEOUT_SECONDS = 120;

    /** Generous bound for job state transitions and metric progress on slow CI runners. */
    private static final long STATE_TIMEOUT_SECONDS = 120;

    @Test
    public void testStopWithSavepointCompletesWhileSourceEmitsLargeSplit() throws Exception {
        String testCaseName = "testStopWithSavepointCompletesWhileSourceEmitsLargeSplit";
        String testClusterName = "SavepointBusySourceBarrierIT_" + testCaseName;

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerConfig = getSeaTunnelConfig(testClusterName);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            String confPath = TestUtils.getResource(CONF_FILE);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            ClientJobProxy clientJobProxy =
                    engineClient
                            .createExecutionContext(confPath, jobConfig, masterConfig)
                            .execute();
            long jobId = clientJobProxy.getJobId();
            awaitJobStatus(clientJobProxy, JobStatus.RUNNING);

            // Fire the savepoint only once the source is provably in the middle of emitting its
            // huge split. This is the exact condition that starved the barrier pre-fix, and it also
            // guarantees every subtask has started, so the request cannot be rejected by the
            // unrelated not-all-tasks-ready precondition covered by
            // SavepointPreconditionRecoveryIT.
            long rowsBeforeSavepoint = awaitSourceActivelyEmitting(engineClient, jobId);
            long rowsAtSavepoint =
                    stopWithSavepointWithinBound(
                            engineClient, clientJobProxy, masterNode, jobId, "initial run");
            log.info(
                    "Job {}: SourceReceivedCount was {} right before the savepoint request and {}"
                            + " when the pipeline finished (split size {})",
                    jobId,
                    rowsBeforeSavepoint,
                    rowsAtSavepoint,
                    SPLIT_ROW_NUM);

            // Mid-split proof: the counter recorded when the pipeline finished must sit strictly
            // between the last live sample and the split size. Pre-fix the barrier could only pass
            // once the whole split had been emitted (or never, once the checkpoint expired).
            Assertions.assertTrue(
                    rowsAtSavepoint >= rowsBeforeSavepoint,
                    "SourceReceivedCount recorded at savepoint completion ("
                            + rowsAtSavepoint
                            + ") must not be lower than the live sample taken before the request ("
                            + rowsBeforeSavepoint
                            + "); the finished-job metrics were not captured");
            Assertions.assertTrue(
                    rowsAtSavepoint < SPLIT_ROW_NUM,
                    "Savepoint completed only after the whole "
                            + SPLIT_ROW_NUM
                            + "-row split drained (SourceReceivedCount="
                            + rowsAtSavepoint
                            + "); the barrier was not injected mid-split");

            // Prove the savepoint is usable: RestoreMode.SAVEPOINT only accepts a savepoint-type
            // checkpoint, and the restored reader must pick up the requeued remainder of the split
            // and keep emitting, then survive a second stop-with-savepoint while still busy.
            ClientJobProxy restoredJobProxy =
                    engineClient
                            .restoreExecutionContext(confPath, jobConfig, masterConfig, jobId)
                            .execute();
            awaitJobStatus(restoredJobProxy, JobStatus.RUNNING);
            long rowsBeforeSecondSavepoint = awaitSourceActivelyEmitting(engineClient, jobId);
            long rowsAtSecondSavepoint =
                    stopWithSavepointWithinBound(
                            engineClient, restoredJobProxy, masterNode, jobId, "restored run");
            log.info(
                    "Job {} (restored): SourceReceivedCount was {} right before the second savepoint"
                            + " request and {} when the pipeline finished",
                    jobId,
                    rowsBeforeSecondSavepoint,
                    rowsAtSecondSavepoint);
            Assertions.assertTrue(
                    rowsAtSecondSavepoint > 0 && rowsAtSecondSavepoint < SPLIT_ROW_NUM,
                    "Second savepoint against the restored run must also complete mid-split, but"
                            + " SourceReceivedCount was "
                            + rowsAtSecondSavepoint);
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

    @NotNull private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }

    /**
     * Issues a real client stop-with-savepoint request and requires it to complete within {@link
     * #SAVEPOINT_COMPLETION_TIMEOUT_SECONDS}. {@link SeaTunnelClient#savePointJob(Long)} blocks
     * until the master's {@code JobMaster#savePoint()} future settles, so it is driven from a
     * separate thread to keep the bound under the test's control rather than the client invocation
     * timeout. A timeout here is the #11473 hang; an exception is the deterministic failure exit
     * added by #11489, which is equally a regression for a source that yields its checkpoint lock.
     *
     * @return the {@code SourceReceivedCount} recorded in the finished-job history once the job has
     *     reached SAVEPOINT_DONE and been released by the coordinator
     */
    private static long stopWithSavepointWithinBound(
            SeaTunnelClient engineClient,
            ClientJobProxy jobProxy,
            HazelcastInstanceImpl masterNode,
            long jobId,
            String phase)
            throws InterruptedException {
        long requestedAt = System.currentTimeMillis();
        CompletableFuture<Void> savepointRequest =
                CompletableFuture.runAsync(() -> engineClient.savePointJob(jobId));
        try {
            savepointRequest.get(SAVEPOINT_COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.info(
                    "stop-with-savepoint ({}) of job {} completed in {} ms",
                    phase,
                    jobId,
                    System.currentTimeMillis() - requestedAt);
        } catch (TimeoutException e) {
            Assertions.fail(
                    "stop-with-savepoint ("
                            + phase
                            + ") did not complete within "
                            + SAVEPOINT_COMPLETION_TIMEOUT_SECONDS
                            + "s against a source busy emitting a large split; job status is "
                            + jobProxy.getJobStatus()
                            + " (#11473 hang)");
        } catch (ExecutionException e) {
            Assertions.fail(
                    "stop-with-savepoint ("
                            + phase
                            + ") failed instead of completing: "
                            + ExceptionUtils.getMessage(e.getCause()));
        }

        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.SAVEPOINT_DONE,
                                        jobProxy.getJobStatus(),
                                        "Job must end in SAVEPOINT_DONE after the "
                                                + phase
                                                + " stop-with-savepoint request completed"));

        // The coordinator drops the JobMaster shortly after the terminal state is reached. Waiting
        // for that release makes the metrics read below come from the finished-job history and, for
        // the restore that follows, prevents CoordinatorService#submitJob from treating the still
        // registered job id as already running and silently skipping the restore.
        awaitJobReleasedByCoordinator(masterNode, jobId);
        return engineClient.getJobMetricsSummary(jobId).getSourceReadCount();
    }

    /**
     * Waits until {@code SourceReceivedCount} of {@code jobId} is positive and then grows again
     * between two samples, which proves the reader is actively emitting the split at that moment.
     *
     * @return the last observed count, taken immediately before the caller's next action
     */
    private static long awaitSourceActivelyEmitting(SeaTunnelClient engineClient, long jobId) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        readSourceReceivedCount(engineClient, jobId) > 0,
                                        "Source has not emitted any rows yet"));
        long firstSample = readSourceReceivedCount(engineClient, jobId);
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertTrue(
                                        readSourceReceivedCount(engineClient, jobId) > firstSample,
                                        "Source stopped emitting at " + firstSample + " rows"));
        return readSourceReceivedCount(engineClient, jobId);
    }

    /**
     * Reads the aggregated {@code SourceReceivedCount} of {@code jobId}. The metrics RPC can fail
     * transiently while task groups are being deployed or torn down; such a failure is logged and
     * reported as -1 so the surrounding poll simply retries instead of aborting on the first miss.
     */
    private static long readSourceReceivedCount(SeaTunnelClient engineClient, long jobId) {
        try {
            return engineClient.getJobMetricsSummary(jobId).getSourceReadCount();
        } catch (RuntimeException e) {
            log.warn(
                    "Transient failure reading metrics of job {}: {}",
                    jobId,
                    ExceptionUtils.getMessage(e));
            return -1L;
        }
    }

    private static void awaitJobStatus(ClientJobProxy jobProxy, JobStatus expected) {
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, jobProxy.getJobStatus()));
    }

    /**
     * Waits until the master's {@code CoordinatorService} no longer holds a JobMaster for {@code
     * jobId} in either its pending queue or its running map, which is the same public lookup {@code
     * CoordinatorService#submitJob} uses to decide whether a submission is a duplicate.
     */
    private static void awaitJobReleasedByCoordinator(
            HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(STATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        server.getCoordinatorService().getJobMaster(jobId),
                                        "Coordinator still holds the finished job " + jobId));
    }
}
