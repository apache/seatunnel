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
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.StateStoreCheckpointIDCounter;
import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class CheckpointCoordinatorFailoverIT {

    private static final String BATCH_TEMPLATE_CONF =
            "batch_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_TEMPLATE_CONF =
            "stream_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_BARRIER_DISPATCH_TEMPLATE_CONF =
            "stream_fake_to_localfile_barrier_dispatch_rpc_template.conf";

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    /** Must match the parallelism value set in the conf templates (env.parallelism). */
    private static final int SOURCE_PARALLELISM = 5;

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
     * and therefore does not apply here. A 60-second native ceiling would leave an uncomfortably
     * thin margin against that test's recovery wait, so this constant is set far above it instead
     * of relying on whichever {@code hazelcast.yaml} happens to be on the classpath, now or after a
     * future change: it guarantees Hazelcast's own heartbeat-based membership failure detection
     * cannot fire inside the test's bounded recovery wait, so recovery observed inside that window
     * can only be attributed to the checkpoint-timeout backstop under test.
     */
    private static final String BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS = "180";

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
     * during execution via a graceful {@code HazelcastInstance.shutdown()} -- which sends an
     * explicit cluster-leave notice that {@code CoordinatorService#failedTaskOnMemberRemoved} (a
     * Hazelcast {@code MembershipAwareService} callback) reacts to almost immediately, failing the
     * task directly. That fast, generic path would dominate this test too and mask the bug above
     * entirely, so this test instead calls {@code HazelcastInstance.getLifecycleService()
     * .terminate()} -- which, per Hazelcast's own {@code Node#shutdown(boolean terminate)}, skips
     * that graceful-leave notice. With no leave notice, recovery can only come from either (a)
     * Hazelcast's own heartbeat-based membership failure detector, governed by {@code
     * hazelcast.max.no.heartbeat.seconds} -- Hazelcast's own compiled-in default is 60 seconds
     * (verified from this build's shaded {@code com.hazelcast.spi.properties.ClusterProperty}
     * class); this repo's top-level, production-only {@code config/hazelcast.yaml} raises that to
     * 180s, but that file is not on this test module's classpath, and this module's own test {@code
     * hazelcast.yaml} does not override the property, so 60 seconds is what would actually govern
     * here if left unconfigured -- or (b) the checkpoint-timeout backstop described above. (The
     * per-task {@code InvocationFuture}s the bug discards do eventually complete exceptionally on
     * their own, per Hazelcast's separate {@code hazelcast.operation.call.timeout.millis} default
     * of 60 seconds -- but since nothing in {@code startTriggerPendingCheckpoint} ever attaches a
     * callback to those discarded array elements, that eventual completion has no observable effect
     * on the coordinator; it is a true dead letter, not merely delayed handling.) A 60-second
     * native heartbeat ceiling would leave an uncomfortably thin margin against this test's
     * recovery wait, so rather than depend on whichever {@code hazelcast.yaml} happens to be on the
     * classpath, {@link #getBarrierDispatchTestConfig} explicitly overrides {@code
     * hazelcast.max.no.heartbeat.seconds} to {@link #BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS} on
     * every node in this test's cluster. That guarantees Hazelcast's own native membership failure
     * detection cannot fire inside this test's bounded recovery wait, so recovery observed inside
     * that window can only be attributed to the checkpoint-timeout backstop. This job's {@code
     * checkpoint.timeout} is deliberately configured (see {@link
     * #BARRIER_DISPATCH_CHECKPOINT_TIMEOUT_MILLIS} and {@link
     * #STREAM_BARRIER_DISPATCH_TEMPLATE_CONF}) far below the overridden native ceiling, so
     * recovering inside this test's bounded wait can only be explained by the checkpoint-timeout
     * backstop, never by incidental help from Hazelcast's own much slower native failure detection.
     *
     * <p>To land the termination inside the intended window with high probability (rather than by
     * blind timing), this test tightly polls the same checkpoint-id counter state store used by
     * {@link #testStreamJobContinuesAfterMasterFailover} above, and terminates the target worker in
     * the same loop iteration that first observes the id advance -- i.e. as soon as a new
     * checkpoint's barrier dispatch is imminent or just starting. Because the terminated worker
     * never comes back, even a slightly-late termination is not wasted: the very next checkpoint
     * cycle (one {@code checkpoint.interval} later) dispatches its barrier to what is by then
     * definitely a dead-but-not-yet-removed member, hitting the same code path this test targets.
     *
     * <p><b>What this test proves:</b> the job recovers -- the killed task is redeployed onto the
     * surviving worker and resumes producing output -- within a bounded time tied to {@code
     * checkpoint.timeout}, even though the coordinator cannot detect this specific dispatch failure
     * immediately. <b>What it does NOT prove:</b> that the barrier-dispatch RPC failure is caught
     * immediately -- per the bug above, it is not, and this test does not assert instant detection.
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

            // Bounded recovery window: generous over the configured checkpoint timeout for
            // pipeline cancel/redeploy/restore overhead under CI load, but nowhere near the
            // BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS explicitly configured in
            // getBarrierDispatchTestConfig (see the class javadoc above for why that override is
            // necessary) -- so recovering inside this window can only be attributed to the
            // checkpoint-timeout backstop, not to Hazelcast noticing the terminated worker itself.
            long recoveryBoundSeconds = (BARRIER_DISPATCH_CHECKPOINT_TIMEOUT_MILLIS / 1000) + 60;
            HazelcastInstanceImpl finalSurvivorWorker = survivorWorker;
            Awaitility.await()
                    .atMost(recoveryBoundSeconds, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                PhysicalVertex vertex =
                                        soleTaskVertex(getJobMaster(finalMasterNode, jobId));
                                Assertions.assertEquals(
                                        ExecutionState.RUNNING, vertex.getExecutionState());
                                Assertions.assertEquals(
                                        finalSurvivorWorker
                                                .getCluster()
                                                .getLocalMember()
                                                .getAddress(),
                                        vertex.getCurrentExecutionAddress(),
                                        "Task should have been redeployed onto the surviving worker");
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

    /**
     * Builds a split-deployment (master/worker role) config for {@link
     * #testStreamJobRecoversAfterWorkerUnreachableDuringCheckpointBarrierDispatch}. A fixed,
     * non-dynamic slot pool keeps task placement deterministic enough to redeploy onto the
     * surviving worker after the target worker is terminated. Also raises {@code
     * hazelcast.max.no.heartbeat.seconds} well above that test's recovery wait -- see {@link
     * #BARRIER_DISPATCH_HEARTBEAT_CEILING_SECONDS} for why this must be set explicitly rather than
     * left to whatever this module's ambient {@code hazelcast.yaml} happens to configure.
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
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(2);
        return seaTunnelConfig;
    }

    /** Reads the current job master from the SeaTunnel server embedded in the given node. */
    private static JobMaster getJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getJobMaster(jobId);
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
