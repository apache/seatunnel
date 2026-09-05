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

import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.ImmutablePair;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.PendingCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class CheckpointCoordinatorFailoverIT {

    private static final String BATCH_TEMPLATE_CONF =
            "batch_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_TEMPLATE_CONF =
            "stream_fake_to_localfile_master_failover_template.conf";

    private static final String SAVEPOINT_TEMPLATE_CONF =
            "stream_fake_to_localfile_savepoint_failover_template.conf";

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    /** Must match the parallelism value set in the conf templates (env.parallelism). */
    private static final int SOURCE_PARALLELISM = 5;

    /**
     * {@link #SAVEPOINT_TEMPLATE_CONF} defines exactly one pipeline; SeaTunnel numbers pipelines
     * starting from 1.
     */
    private static final int SAVEPOINT_TEST_PIPELINE_ID = 1;

    @Test
    public void testBatchJobCompletesAfterMasterFailover() throws Exception {
        String testCaseName = "testBatchJobCompletesAfterMasterFailover";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testBatchJobCompletesAfterMasterFailover";
        // Per-source row.num must match batch_fake_to_localfile_master_failover_template.conf.
        // All sources use the same configuration (row.num=500) for stable failover timing.
        long rowNumPerSource = 500;
        int sourceCount = 5;
        final long expectedTotalRows = rowNumPerSource * sourceCount * SOURCE_PARALLELISM;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config1.getEngineConfig().getHttpConfig().setEnabled(false);

        SeaTunnelConfig config2 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config2.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config2.getEngineConfig().getHttpConfig().setEnabled(false);

        try {
            masterNode1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            masterNode2 = SeaTunnelServerStarter.createHazelcastInstance(config2);

            HazelcastInstanceImpl finalMaster1 = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMaster1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, BATCH_TEMPLATE_CONF);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, config1);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            long jobId = clientJobProxy.getJobId();
            long triggerThreshold = expectedTotalRows / 4;
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                long observedRows =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                Assertions.assertTrue(
                                        observedRows > triggerThreshold,
                                        String.format(
                                                "Waiting for sufficient output before failover "
                                                        + "(rows=%d, threshold=%d)",
                                                observedRows, triggerThreshold));
                            });

            log.info(
                    "Job {} is RUNNING with over {} rows written. "
                            + "Triggering master failover by shutting down masterNode1.",
                    jobId,
                    triggerThreshold);

            masterNode1.shutdown();
            masterNode1 = null;

            HazelcastInstanceImpl finalMaster2 = masterNode2;
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMaster2.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(5, TimeUnit.MINUTES)
                    .pollInterval(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = clientJobProxy.getJobStatus();
                                Assertions.assertTrue(
                                        status == JobStatus.RUNNING || status == JobStatus.FINISHED,
                                        "Waiting for job status to recover after master failover, "
                                                + "current status: "
                                                + status);
                            });
            Assertions.assertEquals(JobStatus.FINISHED, clientJobProxy.waitForJobComplete());

            long actualRows = FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertTrue(
                    actualRows >= expectedTotalRows,
                    String.format(
                            "Expected at least %d rows after failover, but got %d",
                            expectedTotalRows, actualRows));
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
        }
    }

    @Test
    public void testStreamJobContinuesAfterMasterFailover() throws Exception {
        String testCaseName = "testStreamJobContinuesAfterMasterFailover";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testStreamJobContinuesAfterMasterFailover";
        // Per-source row.num must match stream_fake_to_localfile_master_failover_template.conf.
        long[] rowNumPerSource = {100, 150, 200, 250, 300};
        long maxBoundedRows = 0;
        for (long rows : rowNumPerSource) {
            maxBoundedRows += rows * SOURCE_PARALLELISM;
        }

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config1.getEngineConfig().getHttpConfig().setEnabled(false);

        SeaTunnelConfig config2 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config2.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config2.getEngineConfig().getHttpConfig().setEnabled(false);

        try {
            masterNode1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            masterNode2 = SeaTunnelServerStarter.createHazelcastInstance(config2);

            HazelcastInstanceImpl finalMaster1 = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMaster1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, STREAM_TEMPLATE_CONF);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, config1);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            long jobId = clientJobProxy.getJobId();

            // Trigger failover after ~1/4 of the bounded data has been written; FakeSource
            // in STREAMING mode is UNBOUNDED, so total rows are still bounded by row.num
            // per split but the source itself never finishes.
            long triggerThreshold = maxBoundedRows / 4;
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft())
                                                > triggerThreshold);
                            });

            log.info(
                    "Over {} rows written for streaming job {}. "
                            + "Triggering master failover by shutting down masterNode1.",
                    triggerThreshold,
                    jobId);

            masterNode1.shutdown();
            masterNode1 = null;

            HazelcastInstanceImpl finalMaster2 = masterNode2;
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMaster2.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            // Verify at least one pipeline's checkpoint id strictly grows on the new master.
            CounterStateStore<String> checkpointCounterStore = checkpointCounterStore(masterNode2);
            Map<Integer, Long> checkpointBefore = new HashMap<>();
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                checkpointBefore.clear();
                                for (int pipelineId = 1;
                                        pipelineId <= rowNumPerSource.length;
                                        pipelineId++) {
                                    String ckIdKey =
                                            StateStoreCheckpointIDCounter.convertLongIntToBase64(
                                                    jobId, pipelineId);
                                    Long value = checkpointCounterStore.get(ckIdKey);
                                    if (value != null) {
                                        checkpointBefore.put(pipelineId, value);
                                    }
                                }
                                Assertions.assertFalse(
                                        checkpointBefore.isEmpty(),
                                        "Waiting for checkpoint ids after failover");
                            });

            AtomicInteger observedPipelineId = new AtomicInteger(-1);
            AtomicLong ckIdBefore = new AtomicLong(-1);
            AtomicLong ckIdAfter = new AtomicLong(-1);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                boolean grew = false;
                                for (Map.Entry<Integer, Long> entry : checkpointBefore.entrySet()) {
                                    int pid = entry.getKey();
                                    long before = entry.getValue();
                                    String ckIdKey =
                                            StateStoreCheckpointIDCounter.convertLongIntToBase64(
                                                    jobId, pid);
                                    Long current = checkpointCounterStore.get(ckIdKey);
                                    if (current != null && current > before) {
                                        observedPipelineId.set(pid);
                                        ckIdBefore.set(before);
                                        ckIdAfter.set(current);
                                        grew = true;
                                        break;
                                    }
                                }
                                Assertions.assertTrue(
                                        grew,
                                        "Checkpoint id should grow after failover for at least"
                                                + " one pipeline");
                            });
            Assertions.assertTrue(
                    ckIdAfter.get() > ckIdBefore.get(),
                    String.format(
                            "Checkpoint id must continue to grow on the new master for at least"
                                    + " one pipeline (pipelineId=%d, before=%d, after=%d)",
                            observedPipelineId.get(), ckIdBefore.get(), ckIdAfter.get()));

            clientJobProxy.cancelJob();
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
        }
    }

    /**
     * Regression test for the third scenario named in the original bug report (apache/seatunnel
     * #10834, fixed by #10836): a STREAMING job is asked to stop via savepoint, and a master
     * failover happens while that savepoint's shutdown sequence is genuinely in flight -- some
     * parallel source subtasks have already acknowledged the savepoint's checkpoint barrier while
     * others have not.
     *
     * <p>#10836 only fixed {@code readyToCloseStartingTask} loss on failover (see {@link
     * #testBatchJobCompletesAfterMasterFailover} and {@link
     * #testStreamJobContinuesAfterMasterFailover} above), which guards the natural end-of-data
     * completion path used by BATCH jobs and by a STREAMING source that exhausts itself. A
     * client-triggered savepoint is a different code path with no equivalent protection, confirmed
     * by reading the current restore path end to end:
     *
     * <ul>
     *   <li>{@code JobMaster#savePoint()} (seatunnel-engine-server/.../master/JobMaster.java,
     *       around line 1295) sets the job's top-level status to {@code JobStatus#DOING_SAVEPOINT}
     *       via {@code PhysicalPlan#savepointJob()} and then calls {@code
     *       CheckpointManager#triggerSavePointsAndWaitComplete()}, which creates a {@code
     *       SAVEPOINT_TYPE} {@code PendingCheckpoint} per pipeline and waits for every subtask to
     *       acknowledge it.
     *   <li>If the master dies before that {@code PendingCheckpoint} is fully acknowledged, {@code
     *       CoordinatorService#restoreJobFromMasterActiveSwitch}
     *       (seatunnel-engine-server/.../CoordinatorService.java, around line 1159) unconditionally
     *       calls {@code PhysicalPlan#updateJobState(JobStatus.PENDING)} on the restored job.
     *       {@code PhysicalPlan#stateProcess()}'s {@code PENDING} case
     *       (dag/physical/PhysicalPlan.java, around line 390) unconditionally cascades this
     *       straight through to {@code JobStatus#RUNNING} -- nothing in that restore path checks
     *       whether the job's status was {@code DOING_SAVEPOINT} before the failover, so that
     *       marker is silently lost.
     *   <li>{@code CheckpointCoordinator#restoreCoordinator} (checkpoint/CheckpointCoordinator
     *       .java, around line 673) independently discards the in-flight {@code SAVEPOINT_TYPE}
     *       {@code PendingCheckpoint} via {@code cleanPendingCheckpoint
     *       (CHECKPOINT_COORDINATOR_RESET)} and, once the pipeline's tasks are confirmed running
     *       again, re-triggers only a regular {@code CHECKPOINT_TYPE} checkpoint. Nothing
     *       re-derives "a savepoint was requested" from anywhere, and {@code startSavepoint()} is
     *       never called again.
     * </ul>
     *
     * <p>Net effect: the client's original savepoint request is silently and permanently dropped.
     * The job keeps running and periodically checkpointing as if the savepoint had never been
     * requested, which is exactly the "stuck permanently, unable to complete" signature #10834
     * describes -- for the one scenario its own fix and tests never actually exercised.
     *
     * <p>This is unlike the plain {@code cancelJob()} path (see {@link
     * #testStreamJobContinuesAfterMasterFailover}), which is not affected by the same restore
     * cascade: {@code SubPlan} has a real {@code PipelineStatus#CANCELING} state that survives
     * restore (see {@code SubPlan#restorePipelineState}, dag/physical/SubPlan.java around line 565,
     * and its {@code CANCELING} case in {@code SubPlan#stateProcess}, around line 724, which simply
     * re-sends {@code task.cancel()} to every task). Savepoint has no pipeline-level equivalent of
     * {@code CANCELING} to survive on -- the only markers that "a savepoint is in progress" ever
     * existed are the job-level {@code DOING_SAVEPOINT} status and the transient, in-memory {@code
     * PendingCheckpoint}, and both are discarded unconditionally by the restore path traced above.
     * Savepoint is therefore the scenario chosen for this regression test: of the two
     * shutdown-during-failover variants the issue names, it is both reliably triggerable with this
     * harness's existing primitives (the client's {@code savePointJob} call, mirroring how the two
     * tests above already drive cancel and natural completion) and, per the source trace above, the
     * one where the underlying gap is real and independently confirmed by reading the restore path
     * rather than assumed from the issue text.
     *
     * <p><b>Precise-timing technique:</b> the exact moment to kill the master is detected by
     * white-box polling {@code CheckpointCoordinator}'s private {@code pendingCheckpoints} field
     * via {@link ReflectionUtils#getField}, the same reflection idiom this module's own {@code
     * CheckpointCoordinatorTest} unit tests already use to reach the same field, applied here at
     * the live, in-process E2E level instead of against a mock -- this harness runs {@code
     * HazelcastInstanceImpl} directly in the test JVM, the same reach-into-the-live-instance
     * pattern {@link #checkpointCounterStore} above already relies on. A blind wall-clock or
     * row-count delay is not precise enough here, because the window between the savepoint barrier
     * being dispatched and the last parallel subtask acknowledging it is short and not observable
     * from outside the coordinator: the test instead waits until (a) the job's top-level status has
     * actually flipped to {@code DOING_SAVEPOINT} and (b) the coordinator holds a {@code
     * SAVEPOINT_TYPE} {@code PendingCheckpoint} whose completable future is not yet done -- i.e.
     * the barrier has been dispatched but not every subtask has acknowledged it -- before
     * terminating the master.
     */
    @Test
    public void testStreamJobResolvesToSavepointDoneAfterMasterFailoverDuringSavepointShutdown()
            throws Exception {
        String testCaseName =
                "testStreamJobResolvesToSavepointDoneAfterMasterFailoverDuringSavepointShutdown";
        String testClusterName = "CheckpointCoordinatorFailoverIT_" + testCaseName;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;
        ExecutorService savepointCaller = Executors.newSingleThreadExecutor();

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config1.getEngineConfig().getHttpConfig().setEnabled(false);

        SeaTunnelConfig config2 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config2.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config2.getEngineConfig().getHttpConfig().setEnabled(false);

        try {
            masterNode1 = SeaTunnelServerStarter.createHazelcastInstance(config1);
            masterNode2 = SeaTunnelServerStarter.createHazelcastInstance(config2);

            HazelcastInstanceImpl finalMaster1 = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMaster1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, SAVEPOINT_TEMPLATE_CONF);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, config1);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            long jobId = clientJobProxy.getJobId();

            // Wait until the job is genuinely streaming (row.num in the conf template is large
            // enough that it never finishes naturally within this test) and has produced some
            // output, mirroring the "wait for real progress before acting" pattern used above.
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft())
                                                > 50);
                            });

            log.info("Job {} is RUNNING with output flowing. Triggering client savepoint.", jobId);

            // SeaTunnelClient#savePointJob blocks (join()) until the server-side savepoint
            // sequence finishes, so it must run off this thread: the test thread needs to keep
            // polling masterNode1's internal state and then kill masterNode1 while that sequence
            // is still genuinely in flight.
            SeaTunnelClient finalEngineClient = engineClient;
            Future<?> savepointCall =
                    savepointCaller.submit(
                            () -> {
                                try {
                                    finalEngineClient.savePointJob(jobId);
                                } catch (Exception e) {
                                    // Expected: killing masterNode1 below breaks whatever RPC
                                    // this call is blocked on. Whether the job still reaches
                                    // SAVEPOINT_DONE afterward is verified independently below,
                                    // through the client reconnected to masterNode2 -- not
                                    // through this future's outcome.
                                    log.info(
                                            "savePointJob call for job {} ended, expected once"
                                                    + " masterNode1 is killed mid-shutdown: {}",
                                            jobId,
                                            e.toString());
                                }
                            });

            HazelcastInstanceImpl finalMaster1ForPoll = masterNode1;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(10, TimeUnit.MILLISECONDS)
                    .until(
                            () ->
                                    isSavepointGenuinelyMidShutdown(
                                            finalMaster1ForPoll,
                                            jobId,
                                            SAVEPOINT_TEST_PIPELINE_ID));

            log.info(
                    "Observed job {} DOING_SAVEPOINT with an unacknowledged savepoint checkpoint"
                            + " in flight. Triggering master failover now by shutting down"
                            + " masterNode1.",
                    jobId);

            masterNode1.shutdown();
            masterNode1 = null;

            HazelcastInstanceImpl finalMaster2 = masterNode2;
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMaster2.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = clientJobProxy.getJobStatus();
                                Assertions.assertTrue(
                                        status.isEndState(),
                                        "Waiting for the job to resolve to a terminal state"
                                                + " after master failover during savepoint"
                                                + " shutdown, current status: "
                                                + status);
                            });
            Assertions.assertEquals(
                    JobStatus.SAVEPOINT_DONE,
                    clientJobProxy.getJobStatus(),
                    "Job must resolve to SAVEPOINT_DONE, not silently resume running or finish"
                            + " through the wrong terminal status, after a master failover that"
                            + " hit mid-way through a savepoint shutdown");

            savepointCall.get(30, TimeUnit.SECONDS);
        } finally {
            savepointCaller.shutdownNow();
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
        }
    }

    private ImmutablePair<String, String> createTestResources(
            @NonNull String testCaseName, String templateConf) throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);

        String targetDir = "/tmp/hive/warehouse/" + testCaseName;
        targetDir = targetDir.replace("/", File.separator);
        FileUtils.createNewDir(targetDir);

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(templateConf, valueMap, targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }

    private CounterStateStore<String> checkpointCounterStore(HazelcastInstanceImpl instance) {
        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getEngineContext().getStateStores().checkpointCounterStore();
    }

    /**
     * Checks whether {@code jobId}'s pipeline {@code pipelineId} on {@code instance} is genuinely
     * mid-way through a client-triggered savepoint shutdown: the top-level job status has already
     * flipped to {@link JobStatus#DOING_SAVEPOINT} and the coordinator holds a savepoint {@code
     * PendingCheckpoint} whose barrier has been dispatched but not yet acknowledged by every
     * subtask. Used to pick the precise moment to kill the master in {@link
     * #testStreamJobResolvesToSavepointDoneAfterMasterFailoverDuringSavepointShutdown} -- neither
     * "job status is DOING_SAVEPOINT" alone (the checkpoint might not have been created yet) nor "a
     * savepoint PendingCheckpoint exists" alone (it could already be fully acknowledged) is enough
     * on its own to prove the shutdown is genuinely in flight.
     *
     * @param instance the live master node to inspect
     * @param jobId the job under test
     * @param pipelineId the pipeline to inspect (see {@link #SAVEPOINT_TEST_PIPELINE_ID})
     * @return true only while the savepoint's shutdown is provably neither un-started nor already
     *     complete
     */
    private boolean isSavepointGenuinelyMidShutdown(
            HazelcastInstanceImpl instance, long jobId, int pipelineId) {
        SeaTunnelServer server =
                instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        if (jobMaster == null) {
            return false;
        }
        if (jobMaster.getPhysicalPlan().getJobStatus() != JobStatus.DOING_SAVEPOINT) {
            return false;
        }
        CheckpointCoordinator coordinator =
                jobMaster.getCheckpointManager().getCheckpointCoordinator(pipelineId);
        return hasUnacknowledgedSavepointCheckpoint(coordinator);
    }

    /**
     * Reflects into {@code CheckpointCoordinator}'s private {@code pendingCheckpoints} map -- the
     * same field this module's own {@code CheckpointCoordinatorTest} unit tests already reach into
     * via {@link ReflectionUtils#getField} -- to find a savepoint checkpoint that has been created
     * (its barrier dispatched to every starting subtask) but is not yet fully acknowledged. There
     * is no public accessor for this map; {@code PendingCheckpoint#isFullyAcknowledged()} exists
     * but is {@code protected}, so this reads the map directly and uses the public {@code
     * getCompletableFuture().isDone()} (that future completes exactly when the checkpoint becomes
     * fully acknowledged, see {@code PendingCheckpoint#acknowledgeTask}) instead of reflecting into
     * that method too.
     *
     * @param coordinator the pipeline's checkpoint coordinator to inspect
     * @return true if a savepoint checkpoint is currently pending and not yet fully acknowledged
     */
    @SuppressWarnings("unchecked")
    private boolean hasUnacknowledgedSavepointCheckpoint(CheckpointCoordinator coordinator) {
        Map<Long, PendingCheckpoint> pendingCheckpoints =
                (Map<Long, PendingCheckpoint>)
                        ReflectionUtils.getField(coordinator, "pendingCheckpoints").orElse(null);
        if (pendingCheckpoints == null || pendingCheckpoints.isEmpty()) {
            return false;
        }
        for (PendingCheckpoint pendingCheckpoint : pendingCheckpoints.values()) {
            if (pendingCheckpoint.getCheckpointType().isSavepoint()
                    && !pendingCheckpoint.getCompletableFuture().isDone()) {
                return true;
            }
        }
        return false;
    }
}
