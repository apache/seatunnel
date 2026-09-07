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
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class CheckpointCoordinatorFailoverIT {

    private static final String BATCH_TEMPLATE_CONF =
            "batch_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_TEMPLATE_CONF =
            "stream_fake_to_localfile_master_failover_template.conf";

    private static final String CLOSE_HANDSHAKE_TEMPLATE_CONF =
            "batch_fake_to_localfile_close_handshake_failover_template.conf";

    private static final String BOUNDED_STREAM_TEMPLATE_CONF =
            "stream_bounded_sequence_to_localfile_master_failover_template.conf";

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
     * Rows emitted by the bounded streaming template: one row per offset in [start_offset,
     * end_offset), so this must match end_offset - start_offset of
     * stream_bounded_sequence_to_localfile_master_failover_template.conf.
     */
    private static final long BOUNDED_STREAM_TOTAL_ROWS = 3000;

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
     * Scenario (b) of apache/seatunnel#10834: a STREAMING job whose source is bounded must still
     * reach {@link JobStatus#FINISHED} when the master fails over during the job shutdown phase.
     *
     * <p>None of this class's other tests cover this. {@link
     * #testBatchJobCompletesAfterMasterFailover} runs in BATCH mode and fails the master over while
     * rows are still flowing, i.e. before any source has reported ready-to-close. {@link
     * #testStreamJobContinuesAfterMasterFailover} runs an UNBOUNDED FakeSource that can never
     * finish and is cancelled by hand, so it only proves checkpoint id continuity. {@link
     * #testBatchJobCompletesAfterMasterFailoverDuringCloseHandshake} also targets the close
     * handshake, but for a BATCH job with two sources racing each other; it never proves a
     * STREAMING job can reach FINISHED at all.
     *
     * <p>Boundedness is produced through the engine's real completion path: the sequence reader
     * drains a finite offset range and signals no-more-element, the split enumerator turns that
     * into a single {@code LastCheckpointNotifyOperation} and moves to PREPARE_CLOSE, and the
     * checkpoint coordinator records the enumerator in {@code readyToCloseStartingTask}, persists
     * the set under {@code checkpoint_state_<jobId>_<pipelineId>_ready_to_close} and triggers the
     * COMPLETED_POINT checkpoint. The {@code Boundedness} a source declares plays no role in that
     * path; the engine only consults it to close idle readers, which a single reader never has.
     *
     * <p>Before the fix in apache/seatunnel#10836 the ready-to-close set was in-memory only. A
     * master failover between the notification and the completion of the final checkpoint left the
     * new coordinator waiting for a notification that is never re-sent, and the job stayed RUNNING
     * forever. The test makes that window wide and deterministic: the source holds the final
     * barrier's acknowledgement back for {@code exhausted_snapshot_delay_ms}, the test waits until
     * the persisted ready-to-close entry exists while the job is still RUNNING, and only then shuts
     * the master down. The bounded wait for FINISHED is the regression detector.
     *
     * <p>The first member runs as a MASTER-only node so that every task of the job lives on the
     * surviving member and the failover is a pure master switch: the new coordinator is restored
     * with {@code alreadyStarted=true} and has to re-drive the final checkpoint from the persisted
     * set. Had the first member also been a worker, any task placed on it would die with it and the
     * pipeline would be restarted from its last checkpoint instead, a path in which the restarted
     * enumerator re-sends the notification anyway and the regression would go unnoticed. The
     * failover itself is triggered exactly like in the sibling tests: the first member is shut down
     * and the test waits for the cluster to shrink to one member.
     */
    @Test
    public void testBoundedStreamJobCompletesAfterMasterFailover() throws Exception {
        String testCaseName = "testBoundedStreamJobCompletesAfterMasterFailover";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testBoundedStreamJobCompletesAfterMasterFailover";

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));
        config1.getEngineConfig().getHttpConfig().setEnabled(false);
        // MASTER-only: this member coordinates the job but never hosts a task, so shutting it
        // down is a pure master failover (see the method Javadoc).
        config1.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);

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
                    createTestResources(testCaseName, BOUNDED_STREAM_TEMPLATE_CONF);
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

            // Phase 1: the streaming job is genuinely running, rows flow and get committed.
            long flowingThreshold = BOUNDED_STREAM_TOTAL_ROWS / 4;
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
                                        observedRows > flowingThreshold,
                                        String.format(
                                                "Waiting for the bounded stream to flow "
                                                        + "(rows=%d, threshold=%d)",
                                                observedRows, flowingThreshold));
                            });

            // Phase 2: wait for the shutdown-phase window. The enumerator has reported
            // ready-to-close and the coordinator has persisted it, but the final checkpoint is
            // still pending because the drained reader holds its snapshot back. The key is taken
            // from the live coordinator instead of being re-derived, so the assertion follows the
            // engine if the key format ever changes.
            JobMaster jobMaster = getJobMaster(masterNode1, jobId);
            Assertions.assertNotNull(
                    jobMaster, "JobMaster of job " + jobId + " must be running on the master");
            List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
            Assertions.assertEquals(
                    1,
                    pipelines.size(),
                    "The bounded streaming template must produce a single pipeline");
            int pipelineId = pipelines.get(0).getPipelineLocation().getPipelineId();
            String readyToCloseKey =
                    jobMaster
                            .getCheckpointManager()
                            .getCheckpointCoordinator(pipelineId)
                            .getReadyToCloseImapKey();
            // Read through the surviving member so the same handle stays valid after failover.
            IMap<Object, Object> runningJobStateIMap =
                    masterNode2.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(100, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = clientJobProxy.getJobStatus();
                                if (status.isEndState()) {
                                    // Not retriable: the window was missed or the job died.
                                    throw new IllegalStateException(
                                            "Job ended as "
                                                    + status
                                                    + " before the ready-to-close window was"
                                                    + " observed");
                                }
                                Assertions.assertEquals(JobStatus.RUNNING, status);
                                Set<TaskLocation> readyToClose =
                                        persistedReadyToClose(runningJobStateIMap, readyToCloseKey);
                                // Single-source pipeline: the split enumerator is the only
                                // starting subtask, so one entry means the set is complete.
                                Assertions.assertEquals(
                                        1,
                                        readyToClose.size(),
                                        "Waiting for the enumerator to be persisted as"
                                                + " ready-to-close, current: "
                                                + readyToClose);
                                TaskLocation enumerator = readyToClose.iterator().next();
                                Assertions.assertEquals(jobId, enumerator.getJobId());
                                Assertions.assertEquals(pipelineId, enumerator.getPipelineId());
                            });

            log.info(
                    "Job {} pipeline {} is in its shutdown phase: ready-to-close persisted under"
                            + " {} while the final checkpoint is still pending. Triggering master"
                            + " failover by shutting down masterNode1.",
                    jobId,
                    pipelineId,
                    readyToCloseKey);

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

            // The surviving member restores its own JobMaster; capture its pipeline while the job
            // is still running so the restore counter can be checked after completion. The
            // counter lives in the new master's memory and starts at zero, so a pipeline restart
            // during the failover would show up there. The job cannot finish before the re-driven
            // final barrier has waited out exhausted_snapshot_delay_ms, which leaves ample time.
            AtomicReference<SubPlan> restoredPipeline = new AtomicReference<>();
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                JobMaster restoredJobMaster = getJobMaster(finalMaster2, jobId);
                                Assertions.assertNotNull(
                                        restoredJobMaster,
                                        "Waiting for the surviving member to restore the"
                                                + " JobMaster");
                                Assertions.assertNotNull(
                                        restoredJobMaster.getPhysicalPlan(),
                                        "Waiting for the restored JobMaster to finish init");
                                List<SubPlan> restoredPipelines =
                                        restoredJobMaster.getPhysicalPlan().getPipelineList();
                                Assertions.assertEquals(1, restoredPipelines.size());
                                restoredPipeline.set(restoredPipelines.get(0));
                            });

            // Bounded wait: before the fix the job stayed RUNNING forever at this point, so a
            // clean timeout is the regression signal. Five minutes mirrors the recovery budget of
            // the BATCH sibling; the expected time is far lower (the master switch plus at most
            // two exhausted-snapshot delays: the barrier in flight when the master died and the
            // COMPLETED_POINT barrier re-driven by the restored coordinator).
            Awaitility.await()
                    .atMost(5, TimeUnit.MINUTES)
                    .pollInterval(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                JobStatus status = clientJobProxy.getJobStatus();
                                if (status.isEndState() && status != JobStatus.FINISHED) {
                                    // Not retriable: a terminal failure is a different bug.
                                    throw new IllegalStateException(
                                            "Job ended as "
                                                    + status
                                                    + " instead of FINISHED after master"
                                                    + " failover");
                                }
                                Assertions.assertEquals(
                                        JobStatus.FINISHED,
                                        status,
                                        "Waiting for the bounded streaming job to finish after"
                                                + " master failover");
                            });
            Assertions.assertEquals(JobStatus.FINISHED, clientJobProxy.waitForJobComplete());

            // Pure master switch: no task was restarted, so nothing was re-emitted and the sink
            // must hold every row exactly once.
            long actualRows = FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(
                    BOUNDED_STREAM_TOTAL_ROWS,
                    actualRows,
                    String.format(
                            "Expected exactly %d rows after failover, but got %d",
                            BOUNDED_STREAM_TOTAL_ROWS, actualRows));

            // A pure master switch re-attaches to the running tasks. Had the pipeline been
            // restarted on the new master instead, the restarted enumerator would have re-sent
            // its notification and the persisted ready-to-close set would not be what completed
            // the job, so the test would pass without exercising the fix.
            Assertions.assertEquals(
                    0,
                    restoredPipeline.get().getPipelineRestoreNum(),
                    "The pipeline must complete through the re-attached coordinator, not through"
                            + " a restart on the new master");
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
     * Reads the ready-to-close set the checkpoint coordinator persists for a pipeline, or an empty
     * set while nothing has been persisted yet, so callers can poll on the size.
     */
    @SuppressWarnings("unchecked")
    private static Set<TaskLocation> persistedReadyToClose(
            IMap<Object, Object> runningJobStateIMap, String readyToCloseKey) {
        Object persisted = runningJobStateIMap.get(readyToCloseKey);
        return persisted instanceof Set ? (Set<TaskLocation>) persisted : Collections.emptySet();
    }
}
