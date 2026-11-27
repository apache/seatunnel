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
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.core.checkpoint.PipelineCheckpointOverview;
import org.apache.seatunnel.engine.e2e.checkpoint.BatchSourceGate;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class CheckpointCoordinatorFailoverIT {

    private static final String BATCH_TEMPLATE_CONF =
            "batch_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_TEMPLATE_CONF =
            "stream_fake_to_localfile_master_failover_template.conf";

    private static final String CLOSE_HANDSHAKE_TEMPLATE_CONF =
            "batch_fake_to_localfile_close_handshake_failover_template.conf";

    private static final String TRIGGER_DISPATCH_FAILURE_TEMPLATE_CONF =
            "stream_fake_to_localfile_checkpoint_trigger_dispatch_failure_template.conf";

    /**
     * Template of a BATCH job whose env intentionally omits {@code checkpoint.interval}: Zeta
     * disables checkpointing for the job even though the engine keeps a default interval
     * configured. The job reads from the test-only {@code GatedBatchSource}, which is bounded but
     * keeps running until the test releases its gate, so the job cannot drain before the fault is
     * injected.
     */
    private static final String CHECKPOINT_DISABLED_BATCH_CONF =
            "batch_gated_source_to_console_checkpoint_disabled.conf";

    /**
     * Engine-level default {@code checkpoint.interval} used by the failover test for the
     * checkpoint-disabled job. Kept short so that an illegally triggered periodic checkpoint shows
     * up during the test's observation window instead of hours later.
     */
    private static final long ENGINE_DEFAULT_CHECKPOINT_INTERVAL_MS = 1000L;

    /** Placeholder in the checkpoint-disabled template substituted with the per-run hold key. */
    private static final String DYNAMIC_HOLD_KEY = "dynamic_hold_key";

    /**
     * Upper bound for the worker to reach the source gate. Reaching it proves the source task runs
     * and keeps the job alive before the test injects the fault.
     */
    private static final long GATE_REACHED_TIMEOUT_SECONDS = 60L;

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    /** Must match the parallelism value set in the conf templates (env.parallelism). */
    private static final int SOURCE_PARALLELISM = 5;

    /**
     * Total starting (source) subtasks compiled from {@link #CLOSE_HANDSHAKE_TEMPLATE_CONF}: two
     * independent FakeSource operators (table_fast, table_slow), split by the pipeline generator
     * into two pipeline-local coordinators. {@code env.parallelism = 2} in that template controls
     * only the parallelism of each source's *reader* tasks; {@code
     * PhysicalPlanGenerator#getEnumeratorTask} allocates exactly one starting (split-enumerator
     * coordinator) subtask per source action regardless of reader parallelism, so the true total
     * here is one per pipeline, i.e. one for table_fast and one for table_slow. Used to detect a
     * partial close handshake: some, but not all, of these subtasks have reported ready to close.
     */
    private static final int CLOSE_HANDSHAKE_STARTING_SUBTASKS = 2;

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

    /**
     * Guards the checkpoint-enable contract of a BATCH job across a master failover.
     *
     * <p><b>Why this is a boundary with real regression risk.</b> A BATCH job without {@code
     * checkpoint.interval} has checkpointing explicitly disabled by {@code
     * JobMaster#createJobCheckpointConfig}, but the engine still carries a configured default
     * interval for jobs that do enable checkpoints. The healthy-master path honours the flag in
     * {@code CheckpointCoordinator#allTaskReady}: it never schedules a periodic checkpoint. The
     * failover path is a different entry point: {@code CheckpointCoordinator#restoreCoordinator}
     * rebuilds a coordinator on the new active master and, when the tasks already run on the
     * surviving worker ({@code alreadyStarted == true}), used to call {@code
     * tryTriggerPendingCheckpoint(CHECKPOINT_TYPE)} unconditionally. Because the restored
     * coordinator's {@code latestTriggerTimestamp} starts at {@code 0}, that call fires a periodic
     * checkpoint immediately and schedules the next one from the engine default interval. Worse,
     * the checkpoint timeout future is only armed when {@code isCheckpointEnable()} is true, so the
     * illegal periodic checkpoints run without any timeout: a stalled subtask leaves {@code
     * pendingCounter > 0} and can hang the job's final close barrier indefinitely. Existing tests
     * never combine "checkpointing disabled for the job" with "master failover while the tasks are
     * already running", because the failover tests all use a job that explicitly enables
     * checkpoints and therefore cannot distinguish a legitimate checkpoint from an illegal one.
     *
     * <p><b>Contract asserted here.</b> Before the failover the job stores no checkpoint and its
     * per-pipeline checkpoint id counter stays at the initial value. After the active master is
     * killed and the surviving master restores the job while its task runs on the worker, the job
     * must still be RUNNING, must not persist any checkpoint, and must not advance the checkpoint
     * id counter for several engine default intervals. The normal final {@code
     * COMPLETED_POINT_TYPE} checkpoint is not exercised here because the job is cancelled while it
     * is still running; this test is about the periodic schedule that must not exist.
     *
     * <p><b>How the RUNNING window is held.</b> The job must be RUNNING and checkpoint-free before
     * the fault and throughout the observation window, but a finite BATCH source drains on its own,
     * so a correct but slow run could legitimately complete - and persist its final {@code
     * COMPLETED_POINT_TYPE} checkpoint - before the fault is injected or while the absence of
     * checkpoints is observed. Sampling source progress before the kill does not reserve liveness
     * for the later window. The job therefore reads from {@code GatedBatchSource}, a test-only
     * BOUNDED source that emits one row per {@code emit_interval_ms} and never reports "no more
     * element" until the test releases its gate: the test arms the gate before submitting the job,
     * waits until the worker reports that it reached the gate (so the source is proven alive before
     * the injection), and releases the hold in its {@code finally} block after the observation
     * window. BATCH mode and the omitted {@code checkpoint.interval} stay as they are, because the
     * disabled-checkpoint precondition depends on them.
     *
     * <p>See <a href="https://github.com/apache/seatunnel/issues/13497">#13497</a>.
     */
    @Test
    public void testCheckpointDisabledBatchJobDoesNotCheckpointAfterMasterFailover()
            throws Exception {
        String testCaseName = "testCheckpointDisabledBatchJobDoesNotCheckpointAfterMasterFailover";
        String testClusterName = "CheckpointCoordinatorFailoverIT_" + testCaseName;
        String holdKey = testCaseName + "_" + System.nanoTime();

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy clientJobProxy = null;

        SeaTunnelConfig masterNode1Config = checkpointDisabledFailoverConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = checkpointDisabledFailoverConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = checkpointDisabledFailoverConfig(testClusterName);

        // Arm the gate before the job is submitted: from the moment the source starts, the job is
        // held open by the test and cannot drain into its final close while the test needs it to
        // stay RUNNING.
        BatchSourceGate.arm(holdKey);
        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMaster1 = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMaster1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            clientJobProxy =
                    engineClient
                            .createExecutionContext(
                                    createCheckpointDisabledBatchTestConfig(testCaseName, holdKey),
                                    jobConfig,
                                    masterNode1Config)
                            .execute();
            long jobId = clientJobProxy.getJobId();
            final ClientJobProxy jobProxy = clientJobProxy;
            final SeaTunnelClient client = engineClient;

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, jobProxy.getJobStatus()));
            // The gated source must have reached the hold and already emitted a row before the
            // fault is injected. From here on the job is pinned inside the test-owned window and
            // cannot complete on its own, so the later observation does not depend on how fast the
            // source would have drained.
            Assertions.assertTrue(
                    BatchSourceGate.awaitReached(
                            holdKey, TimeUnit.SECONDS.toMillis(GATE_REACHED_TIMEOUT_SECONDS)),
                    "The source of job "
                            + jobId
                            + " did not reach the test gate within "
                            + GATE_REACHED_TIMEOUT_SECONDS
                            + " seconds");
            long rowsBeforeFailover = BatchSourceGate.emittedRows(holdKey);

            // The job's env omits checkpoint.interval, so the healthy master must respect the
            // disabled flag even when several engine default intervals elapse.
            Thread.sleep(3 * ENGINE_DEFAULT_CHECKPOINT_INTERVAL_MS);
            assertNoJobCheckpoint(masterNode1, jobId, "on the healthy master");

            log.info(
                    "Checkpoint-disabled batch job {} is RUNNING with {} rows emitted by the gated"
                            + " source and no checkpoint. Triggering master failover by shutting"
                            + " down masterNode1.",
                    jobId,
                    rowsBeforeFailover);

            masterNode1.shutdown();
            masterNode1 = null;

            // The surviving members are the standby master and the checkpoint-disabled job's
            // worker; only the killed active master leaves the cluster.
            HazelcastInstanceImpl finalMaster2 = masterNode2;
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMaster2.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, jobProxy.getJobStatus()));
            // The restored job must resume producing rows on the surviving worker; this also
            // proves the new coordinator reached the alreadyStarted restore path.
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            readSourceReceivedCount(client, jobId)
                                                    > rowsBeforeFailover,
                                            "Waiting for the restored source to resume emitting"
                                                    + " rows"));

            // Observe several engine default intervals on the new master. A periodic checkpoint
            // triggered by restoreCoordinator would advance the id counter immediately and persist
            // a checkpoint within this window.
            long observationDeadline =
                    System.currentTimeMillis() + 5 * ENGINE_DEFAULT_CHECKPOINT_INTERVAL_MS;
            while (System.currentTimeMillis() < observationDeadline) {
                Assertions.assertEquals(
                        JobStatus.RUNNING,
                        jobProxy.getJobStatus(),
                        "The batch job must still be running while the absence of periodic"
                                + " checkpoints is observed");
                assertNoJobCheckpoint(masterNode2, jobId, "after master failover");
                Thread.sleep(200L);
            }

            Assertions.assertTrue(
                    readSourceReceivedCount(client, jobId) > rowsBeforeFailover,
                    "The checkpoint-disabled batch job must keep reading rows after the master"
                            + " failover");
            // Worker-side half of the liveness check: the task thread that owns the hold must
            // still be emitting on the surviving member, independent of the failover bookkeeping.
            long rowsObservedAtGate = BatchSourceGate.emittedRows(holdKey);
            Assertions.assertTrue(
                    rowsObservedAtGate > rowsBeforeFailover,
                    "The gated source stopped emitting after the master failover (emitted "
                            + rowsBeforeFailover
                            + " rows before, "
                            + rowsObservedAtGate
                            + " rows after the observation window)");
            log.info(
                    "Job {} stayed RUNNING and emitted {} rows through the source gate while master"
                            + " failover was observed.",
                    jobId,
                    rowsObservedAtGate);

            clientJobProxy.cancelJob();
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.CANCELED, jobProxy.getJobStatus()));
        } finally {
            // Release the hold first: the reader can only leave the gate - and the task can only
            // finish cancelling or closing - once the hold is dropped, and an assertion failure
            // above must never leave the task pinned. clear() then forgets the key so a rerun with
            // the same key starts from a clean registry.
            BatchSourceGate.release(holdKey);
            BatchSourceGate.clear(holdKey);
            if (clientJobProxy != null) {
                try {
                    clientJobProxy.cancelJob();
                } catch (Exception e) {
                    log.warn("Best-effort cancellation failed: {}", e.getMessage());
                }
            }
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode != null) {
                workerNode.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
        }
    }

    /**
     * Builds a node config whose engine default checkpoint interval is short enough for an illegal
     * periodic checkpoint to surface inside the test window. The job under test never inherits this
     * interval because its BATCH env omits {@code checkpoint.interval}, which disables
     * checkpointing for the job.
     */
    private static SeaTunnelConfig checkpointDisabledFailoverConfig(String testClusterName) {
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config.getEngineConfig().getHttpConfig().setEnabled(false);
        config.getEngineConfig()
                .getCheckpointConfig()
                .setCheckpointInterval(ENGINE_DEFAULT_CHECKPOINT_INTERVAL_MS);
        return config;
    }

    /**
     * Renders the checkpoint-disabled template with the per-run gate key. The job has no file sink,
     * so only the path of the generated config is returned.
     *
     * @param testCaseName used to build the unique target config path
     * @param holdKey key that arms {@link BatchSourceGate} for this run
     * @return path of the generated job config file
     */
    private static String createCheckpointDisabledBatchTestConfig(
            @NonNull String testCaseName, @NonNull String holdKey) throws IOException {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_HOLD_KEY, holdKey);

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                CHECKPOINT_DISABLED_BATCH_CONF, valueMap, targetConfigFilePath);
        return targetConfigFilePath;
    }

    /**
     * Reads the aggregated source read count; metrics RPCs can fail transiently while the job is
     * being restored, so a failure is reported as {@code -1} and the surrounding poll retries.
     */
    private static long readSourceReceivedCount(SeaTunnelClient engineClient, long jobId) {
        try {
            return engineClient.getJobMetricsSummary(jobId).getSourceReadCount();
        } catch (RuntimeException e) {
            log.warn("Transient failure reading metrics of job {}: {}", jobId, e.getMessage());
            return -1L;
        }
    }

    /**
     * Asserts that the job has persisted no checkpoint and that its single pipeline's checkpoint id
     * counter is still at the initial value. Both checks are needed: the counter advances when a
     * checkpoint is created, while storage only shows it once it completes.
     */
    private void assertNoJobCheckpoint(HazelcastInstanceImpl masterNode, long jobId, String phase)
            throws CheckpointStorageException {
        List<PipelineState> checkpoints =
                checkpointStorage(masterNode).getAllCheckpoints(String.valueOf(jobId));
        Assertions.assertTrue(
                checkpoints.isEmpty(),
                () ->
                        String.format(
                                "A batch job with checkpointing disabled must not persist any"
                                        + " checkpoint %s, but found %d: %s",
                                phase,
                                checkpoints.size(),
                                checkpoints.stream()
                                        .map(
                                                state ->
                                                        "pipeline="
                                                                + state.getPipelineId()
                                                                + ", checkpoint="
                                                                + state.getCheckpointId())
                                        .collect(java.util.stream.Collectors.toList())));

        Long counter =
                checkpointCounterStore(masterNode)
                        .get(StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1));
        Assertions.assertEquals(
                Long.valueOf(1L),
                counter,
                () ->
                        String.format(
                                "The checkpoint id counter of a checkpoint-disabled job must stay"
                                        + " at the initial value %s, but was %s",
                                1L, counter));
    }

    private static CheckpointStorage checkpointStorage(HazelcastInstanceImpl masterNode) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCheckpointService().getCheckpointStorage();
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

            CheckpointCoordinator recoveredCoordinator =
                    getJobMaster(masterNode2, jobId)
                            .getCheckpointManager()
                            .getCheckpointCoordinator(observedPipelineId.get());
            Assertions.assertNotNull(recoveredCoordinator);
            clientJobProxy.cancelJob();
            // Recovery must also resolve cancellation through the job completion future.
            Assertions.assertEquals(JobStatus.CANCELED, clientJobProxy.waitForJobComplete());
            // Check before member shutdown, which would otherwise hide a replacement scheduler.
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                ScheduledExecutorService scheduler =
                                        (ScheduledExecutorService)
                                                ReflectionUtils.getField(
                                                                recoveredCoordinator, "scheduler")
                                                        .orElseThrow(
                                                                () ->
                                                                        new IllegalStateException(
                                                                                "Missing checkpoint scheduler"));
                                Assertions.assertTrue(
                                        scheduler.isShutdown(),
                                        "A canceled recovered pipeline must not recreate its scheduler");
                                Assertions.assertTrue(
                                        scheduler.isTerminated(),
                                        "All recovered checkpoint scheduler workers must exit after cancellation");
                            });
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
     * Regression test for the job-stuck-forever bug fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10836">#10836</a> ("[Fix][Zeta] Job stuck
     * permanently after master failover, unable to complete"). That fix persists {@code
     * CheckpointCoordinator#readyToCloseStartingTask} - the set of bounded-source starting subtasks
     * that have already finished emitting data and are waiting for the final COMPLETED_POINT_TYPE
     * checkpoint to formally close the pipeline - into {@code runningJobStateIMap} (keyed by {@code
     * CheckpointCoordinator#getReadyToCloseImapKey()}) as each subtask reports in, and restores it
     * in {@code CheckpointCoordinator#restoreCoordinator} after a master failover. Before the fix
     * this bookkeeping lived only in the pre-failover master's JVM heap: a subtask that had already
     * reported ready to close never reports again (there is nothing left for it to signal), so a
     * fresh master starting from an empty set could never again observe {@code
     * readyToCloseStartingTask.size() == plan.getStartingSubtasks().size()}. The completing
     * checkpoint then never fired and the job stayed RUNNING forever even though every source had
     * already produced all of its data.
     *
     * <p>{@link #testBatchJobCompletesAfterMasterFailover()} above already covers master failover
     * for a BATCH job, but it deliberately triggers the kill once {@code observedRows >
     * expectedTotalRows / 4} - squarely in the middle of active source production, nowhere near the
     * close handshake this fix protects. This test targets that gap directly: instead of a
     * row-count threshold, the trigger condition is a white-box poll of the exact field the fix
     * introduced, so the master is killed while the close handshake itself is provably in flight.
     *
     * <p>Trigger construction: {@link #CLOSE_HANDSHAKE_TEMPLATE_CONF} defines two independent
     * FakeSource operators feeding one shared LocalFile sink. The pipeline generator splits this
     * two-input graph into two pipeline-local coordinators, each with exactly one starting subtask
     * (the split-enumerator coordinator; see {@link #CLOSE_HANDSHAKE_STARTING_SUBTASKS}). One
     * source is fast ({@code row.num = 5}, a single split) and the other is slow ({@code row.num =
     * 300} spread across 30 splits with a 300ms read interval between them, i.e. at least ~8.7
     * seconds to drain). {@code checkpoint.interval} is set far beyond this test's real runtime so
     * the completing checkpoint is the only checkpoint ever attempted. The test aggregates the two
     * coordinators' entries in {@code runningJobStateIMap}, via {@code
     * CheckpointCoordinator#getReadyToCloseImapKey()}, and waits until the aggregate is strictly
     * between 0 and {@link #CLOSE_HANDSHAKE_STARTING_SUBTASKS}, i.e. equal to 1: the fast source's
     * lone starting subtask has reported ready while the slow source's has not, so killing the
     * active master precisely exercises recovery of a non-empty, not-yet-complete close set. That
     * aggregate is transient, not stable, once it first turns non-zero -- see the tight poll
     * interval and accompanying comment on the {@code Awaitility} block below.
     *
     * <p>Unlike this class's other two tests, the cluster here uses two dedicated master-only nodes
     * (started via {@code createMasterHazelcastInstance}) plus a separate worker node (started via
     * {@code createWorkerHazelcastInstance}) that is never killed. This isolates the scenario to
     * coordinator-side recovery: the running source/sink tasks are never redeployed by the
     * failover, so no data can be duplicated or lost in flight, which lets the final assertion
     * check the row count exactly instead of with a ">=" tolerance.
     */
    @Test
    public void testBatchJobCompletesAfterMasterFailoverDuringCloseHandshake() throws Exception {
        String testCaseName = "testBatchJobCompletesAfterMasterFailoverDuringCloseHandshake";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_"
                        + "testBatchJobCompletesAfterMasterFailoverDuringCloseHandshake";
        // table_fast: parallelism 2 * row.num 5. table_slow: parallelism 2 * row.num 300.
        // Must match batch_fake_to_localfile_close_handshake_failover_template.conf.
        final long expectedTotalRows = 2 * 5 + 2 * 300;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterNode1Config = ConfigProvider.locateAndGetSeaTunnelConfig();
        masterNode1Config
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        masterNode1Config.getEngineConfig().getHttpConfig().setEnabled(false);

        SeaTunnelConfig masterNode2Config = ConfigProvider.locateAndGetSeaTunnelConfig();
        masterNode2Config
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        masterNode2Config.getEngineConfig().getHttpConfig().setEnabled(false);

        SeaTunnelConfig workerNodeConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        workerNodeConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        workerNodeConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMaster1 = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMaster1.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, CLOSE_HANDSHAKE_TEMPLATE_CONF);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, masterNode1Config);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            // Resolve both pipeline ids. PipelineGenerator intentionally splits the shared-sink
            // union into two coordinator-local pipelines, one for each FakeSource.
            HazelcastInstanceImpl finalMaster1ForPlan = masterNode1;
            AtomicInteger firstPipelineIdHolder = new AtomicInteger(-1);
            AtomicInteger secondPipelineIdHolder = new AtomicInteger(-1);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JobMaster jobMaster = getJobMaster(finalMaster1ForPlan, jobId);
                                Assertions.assertNotNull(
                                        jobMaster, "job master should be registered by now");
                                PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
                                Assertions.assertNotNull(
                                        physicalPlan, "physical plan should be built by now");
                                Assertions.assertEquals(
                                        2,
                                        physicalPlan.getPipelineList().size(),
                                        "the shared-sink union should split into one pipeline per"
                                                + " FakeSource");
                                firstPipelineIdHolder.set(
                                        physicalPlan.getPipelineList().get(0).getPipelineId());
                                secondPipelineIdHolder.set(
                                        physicalPlan.getPipelineList().get(1).getPipelineId());
                            });
            int firstPipelineId = firstPipelineIdHolder.get();
            int secondPipelineId = secondPipelineIdHolder.get();

            // Poll the fix's own persisted bookkeeping until it shows a partial close handshake:
            // table_fast's subtask has reported ready to close, table_slow's has not.
            //
            // This window is genuinely transient, not merely "eventually true and then stable":
            // CheckpointCoordinator#readyToClose persists table_fast's entry and, in the same
            // call, immediately triggers its COMPLETED_POINT_TYPE checkpoint since that pipeline
            // has only one starting subtask (see CLOSE_HANDSHAKE_STARTING_SUBTASKS); once that
            // checkpoint finishes, the pipeline reaches a terminal state and its
            // CheckpointCoordinator#shutdown removes this same IMap entry (readyToCloseImapKey)
            // because that path is a real completion, not a master-failover reset. For a 5-row,
            // one-split source this whole reported->completed->removed sequence can run to
            // completion inside a single JVM well under this loop's earlier poll interval, so a
            // sparser poll can sleep through the entire window and observe 0 both before and
            // after it existed. Polling every millisecond instead of every 20ms does not make the
            // window itself any wider, but it multiplies how many chances this loop gets to land
            // inside it before it closes.
            HazelcastInstanceImpl finalMaster1ForPoll = masterNode1;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                int readyCount =
                                        getReadyToCloseCount(
                                                        finalMaster1ForPoll, jobId, firstPipelineId)
                                                + getReadyToCloseCount(
                                                        finalMaster1ForPoll,
                                                        jobId,
                                                        secondPipelineId);
                                Assertions.assertTrue(
                                        readyCount > 0
                                                && readyCount < CLOSE_HANDSHAKE_STARTING_SUBTASKS,
                                        String.format(
                                                "Waiting for a partial close handshake"
                                                        + " (readyToClose=%d, total=%d)",
                                                readyCount, CLOSE_HANDSHAKE_STARTING_SUBTASKS));
                            });

            log.info(
                    "Job {} has partial close handshakes in flight (some but not all of {}"
                            + " starting subtasks reported ready to close). Triggering"
                            + " master failover by shutting down masterNode1.",
                    jobId,
                    CLOSE_HANDSHAKE_STARTING_SUBTASKS);

            masterNode1.shutdown();
            masterNode1 = null;

            HazelcastInstanceImpl finalMaster2 = masterNode2;
            Awaitility.await()
                    .atMost(1, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMaster2.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = clientJobProxy.getJobStatus();
                                Assertions.assertTrue(
                                        status == JobStatus.RUNNING || status == JobStatus.FINISHED,
                                        "Waiting for job status to recover after master failover,"
                                                + " current status: "
                                                + status);
                            });
            Assertions.assertEquals(JobStatus.FINISHED, clientJobProxy.waitForJobComplete());

            long actualRows = FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(
                    expectedTotalRows,
                    actualRows,
                    "The dedicated worker node hosting every source/sink task is never killed, so"
                            + " recovery from this failover is pure coordinator-side bookkeeping"
                            + " with nothing to redo: the row count must match exactly, not just"
                            + " satisfy a '>=' tolerance.");
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
            if (workerNode != null) {
                workerNode.shutdown();
            }
        }
    }

    /**
     * Closes a drained unbounded reader through the real idle-close checkpoint handshake, then
     * switches dedicated masters while its worker stays alive. A newly completed checkpoint must be
     * persisted after failover; allocating a checkpoint ID alone would miss a timeout caused by
     * waiting for the finished reader's acknowledgement.
     */
    @Test
    public void testCheckpointCompletesAfterMasterFailoverWithIdleClosedReader() throws Exception {
        String testCaseName = "testCheckpointCompletesAfterMasterFailoverWithIdleClosedReader";
        String clusterName =
                TestUtils.getClusterName("CheckpointCoordinatorFailoverIT_" + testCaseName);
        SeaTunnelConfig firstConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        firstConfig.getHazelcastConfig().setClusterName(clusterName);
        firstConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        SeaTunnelConfig secondConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        secondConfig.getHazelcastConfig().setClusterName(clusterName);
        secondConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        SeaTunnelConfig workerConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        workerConfig.getHazelcastConfig().setClusterName(clusterName);
        workerConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        HazelcastInstanceImpl first = null;
        HazelcastInstanceImpl second = null;
        HazelcastInstanceImpl worker = null;
        SeaTunnelClient client = null;
        try {
            first = SeaTunnelServerStarter.createMasterHazelcastInstance(firstConfig);
            second = SeaTunnelServerStarter.createMasterHazelcastInstance(secondConfig);
            worker = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);
            HazelcastInstanceImpl originalMaster = first;
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, originalMaster.getCluster().getMembers().size()));
            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> resources =
                    createTestResources(
                            testCaseName,
                            "stream_fake_to_localfile_idle_reader_failover_template.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(clusterName);
            client = new SeaTunnelClient(clientConfig);
            ClientJobProxy job =
                    client.createExecutionContext(resources.getRight(), jobConfig, firstConfig)
                            .execute();
            long jobId = job.getJobId();
            // FakeSource emits row.num=10 per reader at parallelism=2, then remains unbounded.
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus());
                                Assertions.assertEquals(
                                        20,
                                        FileUtils.getFileLineNumberFromDir(resources.getLeft()));
                            });
            JobMaster jobMaster = getJobMaster(first, jobId);
            PhysicalVertex idleReader =
                    jobMaster.getPhysicalPlan().getPipelineList().get(0).getPhysicalVertexList()
                            .stream()
                            .filter(
                                    vertex ->
                                            vertex.getTaskGroup().getTasks().stream()
                                                    .anyMatch(
                                                            SourceSeaTunnelTask.class::isInstance))
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Missing source reader"));
            SourceSeaTunnelTask<?, ?> source =
                    (SourceSeaTunnelTask<?, ?>)
                            idleReader.getTaskGroup().getTasks().stream()
                                    .filter(SourceSeaTunnelTask.class::isInstance)
                                    .findFirst()
                                    .get();
            jobMaster.getCheckpointManager().readyToCloseIdleTask(source.getTaskLocation());
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        ExecutionState.FINISHED, idleReader.getExecutionState());
                                Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus());
                            });
            CompletedCheckpoint checkpointBefore = latestCompletedCheckpoint(first, jobId);
            Assertions.assertNotNull(checkpointBefore);
            first.shutdown();
            first = null;
            long failoverCompletedAt = System.currentTimeMillis();
            HazelcastInstanceImpl newMaster = second;
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        2, newMaster.getCluster().getMembers().size());
                                Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus());
                                CompletedCheckpoint checkpoint =
                                        latestCompletedCheckpoint(newMaster, jobId);
                                Assertions.assertNotNull(checkpoint);
                                Assertions.assertTrue(
                                        checkpoint.getCheckpointId()
                                                > checkpointBefore.getCheckpointId());
                                Assertions.assertTrue(
                                        checkpoint.getCheckpointTimestamp() > failoverCompletedAt,
                                        "A checkpoint triggered after master shutdown must complete");
                            });
            Assertions.assertEquals(20, FileUtils.getFileLineNumberFromDir(resources.getLeft()));
            job.cancelJob();
            Assertions.assertEquals(JobStatus.CANCELED, job.waitForJobComplete());
        } finally {
            if (client != null) {
                client.close();
            }
            if (first != null) {
                first.shutdown();
            }
            if (second != null) {
                second.shutdown();
            }
            if (worker != null) {
                worker.shutdown();
            }
        }
    }

    /**
     * Regression test for the checkpoint-trigger failure in <a
     * href="https://github.com/apache/seatunnel/issues/10442">#10442</a>, fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10448">#10448</a>. A synchronous
     * barrier-dispatch failure must fail the job through {@code CHECKPOINT_INSIDE_ERROR}, rather
     * than leave it RUNNING forever with a pending checkpoint that prevents subsequent triggers.
     *
     * <p>After a healthy checkpoint, remove this pipeline's real slot-profile bookkeeping so the
     * next dispatch fails in {@code JobMaster#queryTaskGroupAddress}. Completion notifications use
     * the same address lookup, so an allocated checkpoint ID alone is not a safe injection point.
     * Wait for {@code pendingCounter == 0}, which is reached after completion notifications, and
     * remove the entry while holding the coordinator's trigger lock. This excludes a new trigger
     * between the idle check and removal without changing the production error-handling path.
     */
    @Test
    public void testStreamJobFailsAfterCheckpointTriggerDispatchFailure() throws Exception {
        String testCaseName = "testStreamJobFailsAfterCheckpointTriggerDispatchFailure";
        String testClusterName = "CheckpointCoordinatorFailoverIT_" + testCaseName;
        // Single-pipeline job (one FakeSource, one LocalFile sink): PipelineGenerator assigns
        // pipeline ids starting at 1, so this is the fixed key identifying this job's sole
        // pipeline in ownedSlotProfilesIMap.
        int pipelineId = 1;

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config.getEngineConfig().getHttpConfig().setEnabled(false);

        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(config);

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, TRIGGER_DISPATCH_FAILURE_TEMPLATE_CONF);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, config);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft())
                                                > 0,
                                        "Waiting for the source to start producing rows");
                            });

            CheckpointCoordinator coordinator =
                    getJobMaster(node, jobId)
                            .getCheckpointManager()
                            .getCheckpointCoordinator(pipelineId);
            // Reflectively access the private lock and pendingCounter to time fault injection.
            // Keep these field lookups in sync when refactoring CheckpointCoordinator.
            Object triggerLock =
                    ReflectionUtils.getField(coordinator, "lock")
                            .orElseThrow(
                                    () -> new IllegalStateException("Missing checkpoint lock"));
            AtomicInteger pendingCounter =
                    (AtomicInteger)
                            ReflectionUtils.getField(coordinator, "pendingCounter")
                                    .orElseThrow(
                                            () ->
                                                    new IllegalStateException(
                                                            "Missing pending checkpoint counter"));
            CounterStateStore<String> checkpointCounterStore = checkpointCounterStore(node);
            String checkpointIdKey =
                    StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, pipelineId);
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                    node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
            PipelineLocation pipelineLocation = new PipelineLocation(jobId, pipelineId);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                // Trigger creation and pendingCounter increment hold this lock.
                                // Completion only decrements pendingCounter after notify succeeds.
                                synchronized (triggerLock) {
                                    Long currentId = checkpointCounterStore.get(checkpointIdKey);
                                    Assertions.assertNotNull(
                                            currentId,
                                            "waiting for the first checkpoint id to be allocated");
                                    Assertions.assertTrue(
                                            currentId >= 2,
                                            "waiting for at least one checkpoint to be triggered");
                                    Assertions.assertEquals(
                                            0,
                                            pendingCounter.get(),
                                            "waiting for checkpoint completion notifications"
                                                    + " before injecting the dispatch failure");
                                    Map<TaskGroupLocation, SlotProfile> removedSlotProfiles =
                                            ownedSlotProfilesIMap.remove(pipelineLocation);
                                    Assertions.assertNotNull(
                                            removedSlotProfiles,
                                            "the running task's slot-profile bookkeeping should"
                                                    + " exist before injection");
                                    log.info(
                                            "Job {} has no pending checkpoint; removed pipeline {}'s"
                                                    + " slot-profile bookkeeping ({} task group(s))"
                                                    + " before the next checkpoint-barrier dispatch.",
                                            jobId,
                                            pipelineId,
                                            removedSlotProfiles.size());
                                }
                            });

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FAILED, clientJobProxy.getJobStatus()));

            JobResult jobResult = clientJobProxy.waitForJobCompleteV2();
            Assertions.assertEquals(JobStatus.FAILED, jobResult.getStatus());
            Assertions.assertNotNull(
                    jobResult.getError(), "a FAILED job should carry a non-null error message");
            Assertions.assertTrue(
                    jobResult
                            .getError()
                            .contains(CheckpointCloseReason.CHECKPOINT_INSIDE_ERROR.message()),
                    () ->
                            "Expected the job failure to be attributed to the checkpoint"
                                    + " coordinator's CHECKPOINT_INSIDE_ERROR path (see"
                                    + " CheckpointCoordinator#handleCoordinatorError), but got: "
                                    + jobResult.getError());
            Assertions.assertTrue(
                    jobResult.getError().contains("can't find task group address"),
                    () ->
                            "Expected the injected address lookup failure, but got: "
                                    + jobResult.getError());
            Assertions.assertTrue(
                    jobResult.getError().contains("CheckpointCoordinator.triggerCheckpoint"),
                    () -> "Expected a barrier-dispatch failure, but got: " + jobResult.getError());
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    private CompletedCheckpoint latestCompletedCheckpoint(HazelcastInstanceImpl master, long jobId)
            throws CheckpointStorageException {
        SeaTunnelServer server =
                master.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        // Completion monitoring is published only after storage finishes writing. Reading the
        // latest filename directly could race an empty/partial file in LocalFileStorage.
        CheckpointOverview overview =
                server.getEngineContext()
                        .getStateStores()
                        .auxiliary()
                        .checkpointOverviewStateStore()
                        .get(jobId);
        PipelineCheckpointOverview pipeline =
                overview == null ? null : overview.getPipelines().get(1);
        if (pipeline == null || pipeline.getLatestCompleted() == null) {
            return null;
        }
        PipelineState checkpoint =
                server.getCheckpointService()
                        .getCheckpointStorage()
                        .getCheckpoint(
                                String.valueOf(jobId),
                                "1",
                                String.valueOf(pipeline.getLatestCompleted().getCheckpointId()));
        return checkpoint == null
                ? null
                : new ProtoStuffSerializer()
                        .deserialize(checkpoint.getStates(), CompletedCheckpoint.class);
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

    /** Reads the current job master from the active SeaTunnel server embedded in the test node. */
    private static JobMaster getJobMaster(HazelcastInstanceImpl activeMaster, long jobId) {
        SeaTunnelServer server =
                activeMaster.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getJobMaster(jobId);
    }

    /**
     * Reads the exact bookkeeping the fix for <a
     * href="https://github.com/apache/seatunnel/pull/10836">#10836</a> persists: how many of a
     * pipeline's starting (source) subtasks have already called {@code
     * CheckpointCoordinator#readyToClose}, as recorded in {@code runningJobStateIMap} under the key
     * returned by {@code CheckpointCoordinator#getReadyToCloseImapKey()}. This is the same IMap
     * entry {@code restoreCoordinator} reads back after a master failover, so polling it directly -
     * rather than inferring readiness from a row-count threshold - reliably targets the exact
     * window that fix protects.
     *
     * <p>Returns 0 while the job master, checkpoint manager, or pipeline coordinator has not been
     * registered yet, instead of throwing, so callers can poll this from inside an Awaitility
     * {@code untilAsserted} block: Awaitility only retries on {@link AssertionError}, so any other
     * exception thrown here would abort the poll on its very first (too-early) invocation.
     */
    private static int getReadyToCloseCount(
            HazelcastInstanceImpl masterNode, long jobId, int pipelineId) {
        JobMaster jobMaster = getJobMaster(masterNode, jobId);
        if (jobMaster == null) {
            return 0;
        }
        CheckpointManager checkpointManager = jobMaster.getCheckpointManager();
        if (checkpointManager == null) {
            return 0;
        }
        CheckpointCoordinator coordinator;
        try {
            coordinator = checkpointManager.getCheckpointCoordinator(pipelineId);
        } catch (RuntimeException e) {
            // The coordinator for this pipeline has not been registered yet.
            return 0;
        }
        IMap<Object, Object> runningJobStateIMap =
                masterNode.getMap(Constant.IMAP_RUNNING_JOB_STATE);
        Object stored = runningJobStateIMap.get(coordinator.getReadyToCloseImapKey());
        return stored instanceof Set ? ((Set<?>) stored).size() : 0;
    }
}
