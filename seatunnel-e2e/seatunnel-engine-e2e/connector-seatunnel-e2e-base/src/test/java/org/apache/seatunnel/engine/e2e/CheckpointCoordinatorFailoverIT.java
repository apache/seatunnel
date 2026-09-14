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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
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
import java.util.concurrent.CopyOnWriteArrayList;
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

    private static final String STREAM_BARRIER_DISPATCH_TEMPLATE_CONF =
            "stream_fake_to_localfile_barrier_dispatch_rpc_template.conf";

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

    /**
     * Must match checkpoint.timeout in {@link #STREAM_BARRIER_DISPATCH_TEMPLATE_CONF}. Kept as a
     * named constant so the recovery-bound comment on {@link
     * #testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch} stays anchored
     * to the actual configured value instead of a magic number.
     */
    private static final long BARRIER_DISPATCH_CHECKPOINT_TIMEOUT_MILLIS = 8000;

    /**
     * Explicit override for {@code hazelcast.max.no.heartbeat.seconds}, applied to every node in
     * {@link #testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch}'s cluster
     * via {@link #getBarrierDispatchTestConfig}.
     *
     * <p>Hazelcast's own compiled-in default for this property is 60 seconds (verified from this
     * build's shaded {@code com.hazelcast.spi.properties.ClusterProperty} class), and this module's
     * own test {@code hazelcast.yaml} does not raise it -- unlike this repo's top-level,
     * production-only {@code config/hazelcast.yaml}, which is not on this module's test classpath
     * and therefore does not apply here. Pinning it explicitly keeps the heartbeat-based failure
     * detector out of that test's recovery window regardless of which {@code hazelcast.yaml}
     * happens to be on the classpath, now or after a future change.
     *
     * <p>This ceiling does NOT delay membership removal of a worker terminated with {@code
     * HazelcastInstance.getLifecycleService().terminate()} on the same host: termination closes the
     * worker's TCP endpoint, the master's next connection attempt fails with "Connection refused",
     * {@code TcpServerConnectionErrorHandler} drops the endpoint and {@code MembershipManager}
     * suspects and removes the member for reason "No connection" about 0.4 s after the {@code
     * terminate()} call (fork run 34349062938, both JDK legs), long before either this ceiling or
     * the job's {@code checkpoint.timeout} could matter. See the test's Javadoc for what that means
     * for the recovery path it actually exercises.
     */
    private static final String BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS = "180";

    /**
     * Fixed slots configured on every worker in {@link
     * #testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch}'s cluster via
     * {@link #getBarrierDispatchTestConfig}.
     *
     * <p>The job compiled from {@link #STREAM_BARRIER_DISPATCH_TEMPLATE_CONF} occupies three fixed
     * slots: two coordinator task groups ({@code SubPlan#getCoordinatorVertexList}: the FakeSource
     * SplitEnumerator and the transactional LocalFile sink's AggregatedCommitter) plus the single
     * parallelism=1 SourceTask group ({@code SubPlan#getPhysicalVertexList}), each taking one whole
     * slot ({@code DefaultSlotService#selectBestMatchSlot}). The surviving worker must be able to
     * host all three on its own, because the restore path gives it exactly one chance: the FAILED
     * branch of {@code SubPlan#stateProcess} calls {@code JobMaster#releasePipelineResource}, then
     * {@code JobMaster#preApplyResources(SubPlan)}, ignores that method's boolean result and
     * proceeds to {@code SubPlan#restorePipeline}, where {@code
     * ResourceUtils#applyResourceForPipeline} deploys whatever {@code
     * PhysicalPlan#getPreApplyResourceFutures()} holds. When the re-application fails, that map
     * still holds the ORIGINAL submission's slot profiles, so the redeploy targets the terminated
     * worker (its {@code DeployTaskOperation} is retried {@code
     * hazelcast.invocation.max.retry.count} times, 100 x 1 s under this module's {@code
     * hazelcast.yaml}) and hands the survivor slot profiles it has already released ({@code
     * WrongTargetSlotException: Unknown slot in slot service}), leaving the pipeline wedged in
     * DEPLOYING. With the previous value of 2 the survivor could only ever offer two of the three
     * slots, so the recovery assertion was unreachable by construction (fork run 34349062938, JDK 8
     * and JDK 11, both failing at "expected: RUNNING but was: DEPLOYING"; earlier runs failed the
     * same premise at other intermediate statuses). The test also checks this premise at runtime
     * before terminating the worker, so a template change that needs more slots fails fast with a
     * clear message instead of timing out in DEPLOYING.
     */
    private static final int BARRIER_DISPATCH_SLOTS_PER_WORKER = 3;

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
     * Regression test for the checkpoint-trigger-failure bug reported in <a
     * href="https://github.com/apache/seatunnel/issues/10442">#10442</a> and fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10448">#10448</a> ("[Fix][Zeta] make the job
     * failed when triggering checkpoint fails (apache#10442)").
     *
     * <p>Before that fix, {@code CheckpointCoordinator#startTriggerPendingCheckpoint} (see {@code
     * seatunnel-engine-server/.../checkpoint/CheckpointCoordinator.java} around lines 942-971)
     * wrapped the checkpoint-barrier dispatch call like this:
     *
     * <pre>
     * try {
     *     CompletableFuture.allOf(completableFutureArray).get();
     * } catch (InterruptedException e) {
     *     throw new RuntimeException(e);
     * } catch (Exception e) {
     *     LOG.error(ExceptionUtils.getMessage(e));
     *     return;
     * }
     * </pre>
     *
     * A {@code pendingCounter} field is incremented unconditionally right before this block ever
     * runs (line ~1003, {@code pendingCounter.incrementAndGet();}) and is only ever decremented
     * once a checkpoint fully completes (line ~1377). Before the fix, a dispatch failure here just
     * logged and returned: {@code pendingCounter} stayed stuck above zero forever, and every later
     * scheduled trigger attempt ({@code tryTriggerPendingCheckpoint}, line ~800: {@code if
     * (pendingCounter.get() > 0) { scheduleTriggerPendingCheckpoint(...); return; }}) would just
     * reschedule itself and bail out without ever calling {@code createPendingCheckpoint} again.
     * The job kept reporting {@code RUNNING} with no error and no further checkpoints, forever.
     *
     * <p>The fix (verified against the current {@code dev} HEAD before writing this test) replaces
     * both catch blocks with a call to {@code handleCoordinatorError(..., CheckpointCloseReason
     * .CHECKPOINT_INSIDE_ERROR)}, which marks the coordinator {@code FAILED}, calls {@code
     * checkpointManager.handleCheckpointError(pipelineId, false)} (cancelling the pipeline via
     * {@code SubPlan#handleCheckpointError()}), and resets {@code pendingCounter} to 0 as part of
     * {@code cleanPendingCheckpoint}. Traced end to end for a single-pipeline job with restore
     * disabled ({@code job.retry.times = 0}): {@code SubPlan#getPipelineEndState()} sees {@code
     * canceledTaskNum > 0} and, because the checkpoint coordinator's own state is already {@code
     * FAILED} by the time it calls {@code cancelCheckpoint()}, upgrades the pipeline's end state
     * from {@code CANCELED} to {@code FAILED}; with restore disabled ({@code
     * SubPlan#canRestorePipeline()} is false), {@code PhysicalPlan#addPipelineEndCallback} then
     * fails the whole (single-pipeline) job. So the documented, current behavior this test asserts
     * is: the job reaches a terminal {@code FAILED} state -- not silent-forever-{@code RUNNING}.
     *
     * <h2>Trigger mechanism</h2>
     *
     * <p>{@code CheckpointCoordinator#triggerCheckpoint} (line ~1120) is the only code that can
     * make {@code startTriggerPendingCheckpoint}'s {@code CompletableFuture.allOf(...).get()} throw
     * *synchronously*, as opposed to a per-task RPC merely failing later (a dead-letter scenario
     * this same {@code allOf} bug never even notices, since it only waits for {@code
     * triggerCheckpoint()} to return, not for the per-task futures inside its result to complete).
     * {@code triggerCheckpoint} maps every starting subtask through {@code
     * checkpointManager::sendOperationToMemberNode} (CheckpointManager.java:386-400), which calls
     * {@code jobMaster.queryTaskGroupAddress(...)} (JobMaster.java:977-994) *before* issuing the
     * RPC. That method does exactly one thing that can throw: {@code
     * ownedSlotProfilesIMap.get(pipelineLocation)} returning {@code null}, which throws {@code
     * IllegalArgumentException("can't find task group address from taskGroupLocation: ...")}.
     *
     * <p>A repo-wide search confirms {@code ownedSlotProfilesIMap}'s only entry-removal call site
     * ({@code JobMaster#releasePipelineResource}, line ~949) runs only after a pipeline has
     * *already* left {@code RUNNING}, by which point {@code cleanPendingCheckpoint} has already
     * cancelled this coordinator's own scheduler (line ~1203, {@code scheduler.shutdownNow()}), so
     * nothing in the running system naturally races this lookup against a live, scheduled trigger.
     * Killing or isolating a worker -- this class's usual technique elsewhere -- does not help
     * either: a graceful leave fails the *task* directly via {@code
     * CoordinatorService#failedTaskOnMemberRemoved} without ever touching this map, while an
     * ungraceful one leaves a *stale but present* entry (the RPC itself fails later, asynchronously
     * -- exactly the dead-letter case {@code allOf} does not notice, and a different bug/test than
     * this one).
     *
     * <p>So this test reaches for a different, still entirely real, lever instead of cluster
     * membership: {@code ownedSlotProfilesIMap} is a plain, named Hazelcast {@code IMap} ({@code
     * Constant#IMAP_OWNED_SLOT_PROFILES}), obtained the exact same way this class's own {@link
     * #getReadyToCloseCount} already reads {@code Constant#IMAP_RUNNING_JOB_STATE} directly, and
     * the same way the engine-server module's own {@code EngineStateStoreMetricExportsTest} pokes
     * this exact map in its unit tests. Removing this job's entry from that live, shared map is not
     * a mock and not a reflected exception injected into production code: it is the same real,
     * unmodified, running {@code JobMaster#queryTaskGroupAddress} that throws its own real {@code
     * IllegalArgumentException} the moment it next executes, exactly as it would if this
     * bookkeeping ever went missing for any other reason. A check of every other reader of this map
     * (metrics export, pipeline cleanup, {@code PhysicalVertex#checkTaskGroupIsExecuting} -- itself
     * only reachable via master-failover restore, never during steady-state RUNNING) confirms all
     * of them null-check and skip gracefully, so this removal cannot trip any other code path
     * first.
     *
     * <p>This is deterministic, not a narrow-window race like a worker kill: the entry is left
     * removed permanently (this pipeline is about to fail anyway), so unlike catching a kill at the
     * exact moment a barrier is dispatched, the very next scheduled trigger attempt that has not
     * already started -- or the one after that -- is guaranteed to observe the missing entry once
     * the removal completes, with no timing window to miss. To also demonstrate the fault lands on
     * a previously healthy coordinator (not one that was simply never able to checkpoint at all),
     * the test first waits for the checkpoint-id counter to reach 2, which -- since {@code
     * tryTriggerPendingCheckpoint} never allocates a new id while {@code pendingCounter > 0} (line
     * ~800) -- can only happen after checkpoint id 1 has fully completed and been acknowledged.
     *
     * <p><b>What this test proves:</b> a real checkpoint-barrier dispatch failure, on a coordinator
     * that was previously checkpointing successfully, fails the job (terminal {@code
     * JobStatus.FAILED}, with an error message traceable to {@code CheckpointCloseReason
     * #CHECKPOINT_INSIDE_ERROR}) within a bounded window. <b>What it implicitly also proves:</b>
     * the pre-fix silent-forever-{@code RUNNING} behavior from #10442 no longer occurs -- had it,
     * the bounded {@code Awaitility} wait below for {@code JobStatus.FAILED} would time out and
     * fail this test, since the old code left the job {@code RUNNING} with no further checkpoints
     * and no error, forever.
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

            // Prove checkpointing is healthy before injecting the fault: the id counter can only
            // reach 2 once checkpoint id 1 has been fully acknowledged -- see the class javadoc
            // above for why (tryTriggerPendingCheckpoint never allocates a new id while
            // pendingCounter is still above zero).
            CounterStateStore<String> checkpointCounterStore = checkpointCounterStore(node);
            String checkpointIdKey =
                    StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, pipelineId);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long currentId = checkpointCounterStore.get(checkpointIdKey);
                                Assertions.assertNotNull(
                                        currentId,
                                        "waiting for the first checkpoint id to be allocated");
                                Assertions.assertTrue(
                                        currentId >= 2,
                                        "waiting for checkpoint id 1 to be fully acknowledged"
                                                + " before injecting the fault");
                            });

            // Real-fault injection: remove this pipeline's entry from the same live, shared,
            // named Hazelcast IMap (engine_ownedSlotProfilesIMap) that
            // JobMaster#queryTaskGroupAddress consults on every checkpoint-barrier dispatch. See
            // the class javadoc above for why this is real (not mocked/reflected),
            // deterministic, and cannot be short-circuited by any other code path.
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                    node.getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
            PipelineLocation pipelineLocation = new PipelineLocation(jobId, pipelineId);
            Map<TaskGroupLocation, SlotProfile> removedSlotProfiles =
                    ownedSlotProfilesIMap.remove(pipelineLocation);
            Assertions.assertNotNull(
                    removedSlotProfiles,
                    "the running task's slot-profile bookkeeping should exist before injection");
            log.info(
                    "Job {} checkpoint id counter reached 2; removed pipeline {}'s slot-profile"
                            + " bookkeeping ({} task group(s)) so the next checkpoint-barrier"
                            + " dispatch hits CheckpointCoordinator's real, unmodified"
                            + " queryTaskGroupAddress failure path.",
                    jobId,
                    pipelineId,
                    removedSlotProfiles.size());

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
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    /**
     * Regression test for a checkpoint-barrier-dispatch dead-letter bug in {@code
     * CheckpointCoordinator#startTriggerPendingCheckpoint} (see {@code
     * seatunnel-engine-server/.../checkpoint/CheckpointCoordinator.java} around lines 942-958):
     *
     * <pre>
     * CompletableFuture&lt;InvocationFuture&lt;?&gt;[]&gt; completableFutureArray =
     *         CompletableFuture.supplyAsync(() -&gt; new CheckpointBarrier(...), executorService)
     *                 .thenApplyAsync(this::triggerCheckpoint, executorService);
     * CompletableFuture.allOf(completableFutureArray).get();
     * </pre>
     *
     * {@code completableFutureArray} is ONE {@code CompletableFuture} whose eventual VALUE is an
     * {@code InvocationFuture<?>[]}, not the array itself. {@code CompletableFuture.allOf(...)}
     * only spreads a real array argument into its varargs; handed a single future here, it waits on
     * exactly that one future -- i.e. until {@code triggerCheckpoint()} returns, meaning until the
     * per-task {@code CheckpointBarrierTriggerOperation} RPCs are FIRED. It never looks at the
     * individual {@code InvocationFuture}s inside the array it resolves to, so it never learns
     * whether any of those RPCs actually landed and were acknowledged. Contrast the correct pattern
     * the same class uses for {@code notifyTaskStart()}/{@code notifyCompleted()} (lines ~451-452,
     * ~474-481): both spread a real, already-resolved {@code InvocationFuture<?>[]} directly into
     * {@code allOf}, which genuinely waits on every element.
     *
     * <p>Net effect: if the worker hosting the checkpoint's target task becomes unreachable in the
     * window between the coordinator sending a {@code CheckpointBarrierTriggerOperation} and that
     * RPC landing, this dispatch-wait code does not notice -- it has already moved on by the time
     * the failure would show up. The only backstop is the scheduled per-pending-checkpoint timeout
     * a little further down the same method (lines ~972-1001): once {@code checkpoint.timeout}
     * elapses without the checkpoint becoming fully acknowledged, it fires {@code
     * CheckpointCloseReason#CHECKPOINT_EXPIRED}, which cancels and restarts the pipeline the same
     * way a hard task failure would.
     *
     * <p>Neither existing worker-kill test in this module exercises that specific window: {@link
     * #testBatchJobCompletesAfterMasterFailover} and {@link
     * #testStreamJobContinuesAfterMasterFailover} above kill a MASTER node (the checkpoint
     * coordinator itself), not a worker; the various {@code ClusterFaultToleranceIT}/{@code
     * SplitClusterFaultToleranceIT} worker-kill tests elsewhere kill a worker at an arbitrary point
     * during execution, never deliberately inside a barrier dispatch. This test terminates the
     * worker hosting the barrier's target task with {@code HazelcastInstance.getLifecycleService()
     * .terminate()} rather than a graceful {@code shutdown()}: per Hazelcast's own {@code
     * Node#shutdown(boolean terminate)} that skips the explicit cluster-leave notice, so the
     * coordinator's barrier RPC is genuinely fired at a member that vanished without saying
     * goodbye. (The per-task {@code InvocationFuture}s the bug discards do eventually complete
     * exceptionally on their own, per Hazelcast's separate {@code
     * hazelcast.operation.call.timeout.millis} default of 60 seconds -- but since nothing in {@code
     * startTriggerPendingCheckpoint} ever attaches a callback to those discarded array elements,
     * that eventual completion has no observable effect on the coordinator; it is a true dead
     * letter, not merely delayed handling.)
     *
     * <p><b>How recovery actually arrives, per CI evidence (fork run 34349062938, both JDK
     * legs):</b> skipping the leave notice does not keep the terminated member in the cluster.
     * Termination closes its TCP endpoint on the same host, the master's next connection attempt
     * gets "Connection refused", and Hazelcast's {@code MembershipManager} suspects and removes the
     * member for reason "No connection" about 0.4 s after the {@code terminate()} call -- long
     * before either the {@link #BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS} heartbeat ceiling or
     * this job's {@code checkpoint.timeout} ({@link #BARRIER_DISPATCH_CHECKPOINT_TIMEOUT_MILLIS})
     * could fire. {@code CoordinatorService#failedTaskOnMemberRemoved} then fails the task deployed
     * on the lost address, the pipeline goes FAILING/FAILED, waits {@code
     * job.retry.interval.seconds} (default 3 s, {@code SubPlan#prepareRestorePipeline}) and
     * redeploys onto the survivor -- which is why the survivor must be able to host the whole
     * pipeline alone, see {@link #BARRIER_DISPATCH_SLOTS_PER_WORKER}. The heartbeat override is
     * kept only so that heartbeat-based detection can never become the trigger; the
     * checkpoint-timeout backstop remains the recovery path for a worker that is unreachable
     * WITHOUT its connection being refused (a genuine network partition), which an in-JVM {@code
     * terminate()} on localhost cannot simulate.
     *
     * <p>To land the termination inside the intended window with high probability (rather than by
     * blind timing), this test tightly polls the same checkpoint-id counter state store used by
     * {@link #testStreamJobContinuesAfterMasterFailover} above, and terminates the target worker in
     * the same loop iteration that first observes the id advance -- i.e. as soon as a new
     * checkpoint's barrier dispatch is imminent or just starting. A termination that lands only
     * after that barrier was already acknowledged still yields a valid worker-loss recovery run
     * (the assertions below hold either way); it just does not exercise the dead-letter window,
     * which is why the poll is as tight as it is.
     *
     * <p><b>What this test proves:</b> the job recovers -- the killed task is redeployed onto the
     * surviving worker and resumes producing output -- within the bounded wait below, even though
     * the barrier-dispatch RPC to the lost worker was silently dropped by the bug above: the
     * discarded {@code InvocationFuture}s and the pending checkpoint they belong to do not wedge
     * the coordinator, the restore or the redeploy. <b>What it does NOT prove:</b> that the
     * barrier-dispatch RPC failure is caught immediately (per the bug above it is not, and this
     * test does not assert instant detection), nor that the checkpoint-timeout backstop alone would
     * recover a worker that stays silently unreachable, since membership removal fires first here.
     */
    @Test
    public void testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch()
            throws Exception {
        String testCaseName =
                "testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch";
        String testClusterName = "CheckpointCoordinatorFailoverIT_" + testCaseName;

        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterConfig = getBarrierDispatchTestConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getBarrierDispatchTestConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getBarrierDispatchTestConfig(testClusterName);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, STREAM_BARRIER_DISPATCH_TEMPLATE_CONF);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, masterConfig);
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

            // Identify which worker actually hosts the job's single (parallelism=1) task, so the
            // right one is terminated -- never the master, which must keep running so its
            // checkpoint coordinator (and the timeout scheduler under test) stays alive.
            HazelcastInstanceImpl targetWorker =
                    findWorkerHostingTask(masterNode, jobId, workerNode1, workerNode2);
            HazelcastInstanceImpl survivorWorker =
                    targetWorker == workerNode1 ? workerNode2 : workerNode1;

            // Premise guard: the survivor must be able to host the WHOLE pipeline by itself once
            // the target worker is gone (see BARRIER_DISPATCH_SLOTS_PER_WORKER for why the restore
            // path gives it exactly one chance). Checked before termination so a template change
            // that needs more slots fails fast here instead of timing out in DEPLOYING below.
            int slotsNeededByPipeline = slotsNeededByPipeline(getJobMaster(masterNode, jobId));
            Assertions.assertTrue(
                    slotsNeededByPipeline <= BARRIER_DISPATCH_SLOTS_PER_WORKER,
                    () ->
                            "The pipeline needs "
                                    + slotsNeededByPipeline
                                    + " fixed slots but each worker only has "
                                    + BARRIER_DISPATCH_SLOTS_PER_WORKER
                                    + ", so the surviving worker could never host the redeploy"
                                    + " alone");

            // Tight white-box poll on the checkpoint-id counter: the moment a NEW checkpoint id
            // appears, barrier dispatch for it is imminent or already underway, so terminate the
            // target worker immediately in this same iteration. See the class javadoc above for
            // why even an imperfect hit still exercises the same code path on the next cycle.
            CounterStateStore<String> checkpointCounterStore = checkpointCounterStore(masterNode);
            String checkpointIdKey = StateStoreCheckpointIDCounter.convertLongIntToBase64(jobId, 1);
            Long baselineCheckpointId = checkpointCounterStore.get(checkpointIdKey);

            long pollDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            Long observedCheckpointId = null;
            while (System.currentTimeMillis() < pollDeadline) {
                Long current = checkpointCounterStore.get(checkpointIdKey);
                if (current != null
                        && (baselineCheckpointId == null || current > baselineCheckpointId)) {
                    observedCheckpointId = current;
                    break;
                }
                Thread.sleep(10);
            }
            Assertions.assertNotNull(
                    observedCheckpointId,
                    "Timed out waiting for a new checkpoint to be triggered before termination");

            long rowsBeforeTermination =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            log.info(
                    "Job {} checkpoint id just advanced to {}; terminating worker {} ungracefully"
                            + " (not shutdown(), to simulate becoming unreachable rather than"
                            + " gracefully leaving) to land inside the barrier-dispatch window.",
                    jobId,
                    observedCheckpointId,
                    targetWorker.getCluster().getLocalMember().getAddress());
            targetWorker.getLifecycleService().terminate();

            // Bounded recovery window. The path CI actually shows (see the Javadoc above) is
            // membership removal about 0.4 s after terminate(), the pipeline's
            // job.retry.interval.seconds restore wait (default 3 s) and a sub-second redeploy, so
            // this bound is generous. It is still expressed as checkpoint.timeout plus a fixed CI
            // allowance so that it also covers the checkpoint-timeout backstop should membership
            // removal ever be delayed, and it stays nowhere near
            // BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS.
            long recoveryBoundSeconds = (BARRIER_DISPATCH_CHECKPOINT_TIMEOUT_MILLIS / 1000) + 60;
            HazelcastInstanceImpl finalSurvivorWorker = survivorWorker;
            // Every distinct job status seen while waiting, in order, so a timeout reports the
            // whole trajectory (e.g. RUNNING -> FAILING -> ... -> DEPLOYING) instead of only the
            // final snapshot. Copy-on-write because Awaitility evaluates the condition on its own
            // poll thread.
            List<JobStatus> observedJobStatuses = new CopyOnWriteArrayList<>();
            Awaitility.await()
                    .atMost(recoveryBoundSeconds, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus jobStatus = clientJobProxy.getJobStatus();
                                recordStatusTransition(observedJobStatuses, jobStatus);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        jobStatus,
                                        () ->
                                                "Job did not return to RUNNING after the worker was"
                                                        + " terminated; observed job status"
                                                        + " transitions: "
                                                        + observedJobStatuses);
                                PhysicalVertex vertex =
                                        soleTaskVertex(getJobMaster(finalMasterNode, jobId));
                                Assertions.assertEquals(
                                        ExecutionState.RUNNING,
                                        vertex.getExecutionState(),
                                        () ->
                                                "Task did not return to RUNNING (currently on "
                                                        + vertex.getCurrentExecutionAddress()
                                                        + "); observed job status transitions: "
                                                        + observedJobStatuses);
                                Assertions.assertEquals(
                                        finalSurvivorWorker
                                                .getCluster()
                                                .getLocalMember()
                                                .getAddress(),
                                        vertex.getCurrentExecutionAddress(),
                                        () ->
                                                "Task should have been redeployed onto the surviving"
                                                        + " worker; observed job status transitions: "
                                                        + observedJobStatuses);
                            });

            // End-to-end confirmation that recovery is real, not just a status flip: the source
            // resumes emitting rows once redeployed.
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            FileUtils.getFileLineNumberFromDir(
                                                            testResources.getLeft())
                                                    > rowsBeforeTermination,
                                            "Row output should keep growing after recovery"));

            clientJobProxy.cancelJob();
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (workerNode1 != null && workerNode1.getLifecycleService().isRunning()) {
                workerNode1.shutdown();
            }
            if (workerNode2 != null && workerNode2.getLifecycleService().isRunning()) {
                workerNode2.shutdown();
            }
            if (masterNode != null) {
                masterNode.shutdown();
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

    /**
     * Builds a split-deployment (master/worker role) config for {@link
     * #testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch}. A fixed,
     * non-dynamic slot pool of {@link #BARRIER_DISPATCH_SLOTS_PER_WORKER} slots per worker keeps
     * task placement deterministic and, crucially, lets the surviving worker host the entire
     * pipeline alone after the target worker is terminated -- see that constant for why the restore
     * path cannot tolerate a smaller pool. Also pins {@code hazelcast.max.no.heartbeat.seconds} to
     * {@link #BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS} so heartbeat-based failure detection can
     * never be the recovery trigger (see that constant for why it is not the trigger anyway for a
     * worker terminated on the same host).
     *
     * <p><b>Must also force {@link ScheduleStrategy#WAIT}, not just disable dynamic slot:</b> this
     * module's test {@code seatunnel.yaml} sets {@code dynamic-slot: true}, and {@code
     * YamlSeaTunnelDomConfigProcessor} reacts to that at parse time -- inside {@code
     * ConfigProvider.locateAndGetSeaTunnelConfig()}, before this method ever runs -- by
     * unconditionally setting {@code engineConfig.scheduleStrategy = REJECT} ("if dynamic slot is
     * enabled, the schedule strategy must be REJECT"). Calling {@code setDynamicSlot(false)}
     * afterwards does not revert that: {@code scheduleStrategy} is a plain, independent field that
     * is never re-derived from the slot-service config once parsing has set it. Left uncorrected,
     * this test's cluster ends up running fixed slots under a fail-fast REJECT strategy instead of
     * the intended retry-until-ready one -- confirmed on real CI (fork run 34181422045, both JDK 8
     * and JDK 11): the job is submitted only milliseconds after the workers join the Hazelcast
     * cluster, which is not enough time for their fixed slot pools to finish registering with the
     * master's {@code ResourceManager}, so the very first scheduling attempt legitimately finds no
     * assignable slot ({@code NoEnoughResourceException}); under REJECT that single transient miss
     * permanently fails the job via {@code CoordinatorService#completeFailJob} instead of retrying,
     * and {@code CoordinatorService#getJobStatus} then reports {@code UNKNOWABLE} once the job's
     * short-lived ({@code history-job-expire-minutes: 1} in this same {@code seatunnel.yaml})
     * FAILED history entry expires -- a status this job can never recover from, since nothing
     * re-submits or re-schedules it. {@link
     * #testBatchJobCompletesAfterMasterFailoverDuringCloseHandshake} above does not hit this
     * because dynamic slot mode does not need workers to pre-register a fixed pool before a job can
     * be scheduled onto it. {@link ScheduleStrategy#WAIT} is the same fix already used for an
     * identical non-dynamic-slot setup by {@code
     * SplitClusterPendingJobLifecycleFailoverIT#configurePendingLifecycleTest} and {@code
     * PendingJobsRestIT#setUp} in this module: it makes {@code
     * CoordinatorService#pendingJobSchedule} retry every 3 seconds instead of failing on the first
     * miss, which is what actually lets this test reach RUNNING once the workers' slots finish
     * registering (well within its own 2-minute bound).
     */
    private static SeaTunnelConfig getBarrierDispatchTestConfig(String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig
                .getHazelcastConfig()
                .setProperty(
                        "hazelcast.max.no.heartbeat.seconds",
                        BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig
                .getEngineConfig()
                .getSlotServiceConfig()
                .setSlotNum(BARRIER_DISPATCH_SLOTS_PER_WORKER);
        // Must be set explicitly: locateAndGetSeaTunnelConfig() already forced REJECT above
        // (see the class-level detail in this method's Javadoc), and disabling dynamic slot does
        // not undo that. Without this, the job's first scheduling attempt can lose a genuine but
        // transient race against worker slot registration and be permanently failed instead of
        // retried.
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        return seaTunnelConfig;
    }

    /**
     * Returns the single task vertex of a parallelism=1, single-pipeline job (see {@link
     * #STREAM_BARRIER_DISPATCH_TEMPLATE_CONF}).
     */
    private static PhysicalVertex soleTaskVertex(JobMaster jobMaster) {
        Assertions.assertNotNull(jobMaster, "Job master should exist while the job is running");
        List<PhysicalVertex> vertices =
                jobMaster.getPhysicalPlan().getPipelineList().get(0).getPhysicalVertexList();
        Assertions.assertEquals(
                1,
                vertices.size(),
                "This test's single-parallelism pipeline should have exactly one task vertex");
        return vertices.get(0);
    }

    /**
     * Number of fixed slots the given job's single pipeline occupies: one per coordinator task
     * group (split enumerator, aggregated committer) plus one per physical task group. Compared
     * against {@link #BARRIER_DISPATCH_SLOTS_PER_WORKER} before the target worker is terminated, so
     * the survivor is known to be able to host the whole redeploy alone.
     */
    private static int slotsNeededByPipeline(JobMaster jobMaster) {
        Assertions.assertNotNull(jobMaster, "Job master should exist while the job is running");
        List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
        Assertions.assertEquals(
                1, pipelines.size(), "This test's job should compile to exactly one pipeline");
        return pipelines.get(0).getCoordinatorVertexList().size()
                + pipelines.get(0).getPhysicalVertexList().size();
    }

    /**
     * Appends {@code current} to {@code history} only when it differs from the last recorded entry,
     * so the list reads as a compact sequence of distinct transitions rather than one entry per
     * poll. Used to enrich the recovery assertion messages in {@link
     * #testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch}.
     */
    private static <T> void recordStatusTransition(List<T> history, T current) {
        if (history.isEmpty() || !history.get(history.size() - 1).equals(current)) {
            history.add(current);
        }
    }

    /**
     * Determines which of the two given workers is currently hosting the job's single task, by
     * comparing its live execution address against each worker's cluster member address -- rather
     * than assuming a fixed placement order, which the slot allocation strategy does not guarantee.
     */
    private static HazelcastInstanceImpl findWorkerHostingTask(
            HazelcastInstanceImpl masterNode,
            long jobId,
            HazelcastInstanceImpl workerNode1,
            HazelcastInstanceImpl workerNode2) {
        Address executionAddress =
                soleTaskVertex(getJobMaster(masterNode, jobId)).getCurrentExecutionAddress();
        Assertions.assertNotNull(
                executionAddress,
                "Task should already be deployed before selecting a target worker");
        if (executionAddress.equals(workerNode1.getCluster().getLocalMember().getAddress())) {
            return workerNode1;
        }
        if (executionAddress.equals(workerNode2.getCluster().getLocalMember().getAddress())) {
            return workerNode2;
        }
        throw new IllegalStateException(
                "Task execution address "
                        + executionAddress
                        + " did not match either candidate worker");
    }
}
