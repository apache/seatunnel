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
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Cluster fault tolerance test. Test the job recovery capability and data consistency assurance
 * capability in case of cluster node failure
 */
@Slf4j
public class SplitClusterFaultToleranceIT {

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    public static final String DYNAMIC_JOB_MODE = "dynamic_job_mode";

    public static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    public static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    @Test
    public void testBatchJobRunOk() throws Exception {
        String testCaseName = "testBatchJobRunOk";
        String testClusterName = "SplitSplitClusterFaultToleranceIT_testBatchJobRunOk";
        long testRowNumber = 1000;
        int testParallelism = 6;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.BATCH, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        objectCompletableFuture.isDone()
                                                && JobStatus.FINISHED.equals(
                                                        objectCompletableFuture.get()));
                            });

            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(testRowNumber * testParallelism, fileLineNumberFromDir);
            log.info(engineClient.getJobMetrics(clientJobProxy.getJobId()));
            log.warn("========================clean test resource====================");
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

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
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
     * Create the test job config file basic on cluster_batch_fake_to_localfile_template.conf It
     * will delete the test sink target path before return the final job config file path
     *
     * @param testCaseName testCaseName
     * @param jobMode jobMode
     * @param rowNumber row.num per FakeSource parallelism
     * @param parallelism FakeSource parallelism
     */
    private ImmutablePair<String, String> createTestResources(
            @NonNull String testCaseName, @NonNull JobMode jobMode, long rowNumber, int parallelism)
            throws IOException {
        checkArgument(rowNumber > 0, "rowNumber must greater than 0");
        checkArgument(parallelism > 0, "parallelism must greater than 0");
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);
        valueMap.put(DYNAMIC_JOB_MODE, jobMode.toString());
        valueMap.put(DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM, String.valueOf(rowNumber));
        valueMap.put(DYNAMIC_TEST_PARALLELISM, String.valueOf(parallelism));

        String targetDir = "/tmp/hive/warehouse/" + testCaseName;
        targetDir = targetDir.replace("/", File.separator);

        // clear target dir before test
        FileUtils.createNewDir(targetDir);

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";
        TestUtils.createTestConfigFileFromTemplate(
                "cluster_batch_fake_to_localfile_template.conf", valueMap, targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }

    @Test
    public void testStreamJobRunOk() throws Exception {
        String testCaseName = "testStreamJobRunOk";
        String testClusterName = "SplitClusterFaultToleranceIT_testStreamJobRunOk";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);
            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.STREAMING, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && testRowNumber * testParallelism
                                                        == FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft()));
                            });

            clientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.CANCELED.equals(
                                                            objectCompletableFuture.get())));

            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(testRowNumber * testParallelism, fileLineNumberFromDir);

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

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    /**
     * Regression test for the CANCELING-stuck-forever bug fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10729">#10729</a> ("[Fix][Zeta] prevent cancel
     * stuck and downgrade tmp cleanup failure"). Before that fix, {@code
     * PhysicalVertex#noticeTaskExecutionServiceCancel} sent a {@code CancelTaskOperation} to the
     * worker owning the task and retried while that worker remained a cluster member, but once the
     * worker actually left the cluster before acking the cancel, the retry loop simply exited
     * without ever resolving the task's state: the vertex - and therefore its pipeline and job -
     * stayed CANCELING forever, because nothing else in that method drove it to a terminal state.
     * The fix added a fallback right after the retry loop: if the loop exits with the ack still
     * missing and the vertex is still CANCELING, mark it CANCELED locally.
     *
     * <p>{@code CoordinatorService#failedTaskOnMemberRemoved} also matches CANCELING tasks when a
     * member is lost, but it cannot be the one to save this race: {@code PhysicalVertex#cancel},
     * {@code #updateTaskState} and {@code #stateProcess} are all {@code synchronized} on the vertex
     * itself, and the whole call chain down into {@code noticeTaskExecutionServiceCancel} runs
     * without releasing that lock. A competing {@code failedTaskOnMemberRemoved} call has to go
     * through the same {@code synchronized updateTaskState}, so it can only run after the
     * cancelling thread has already returned - by which point the fixed code has already resolved
     * the vertex to CANCELED, and the end-state consistency check inside {@code updateTaskState}
     * rejects any later attempt to move a terminal-state task to FAILED. So the fixed method's own
     * fallback is the sole, deterministic mechanism that unsticks this specific race, which is why
     * this test asserts CANCELED rather than FAILED as the outcome.
     *
     * <p>No existing fault-tolerance test combines "cancel requested" with "worker crashes before
     * the cancel ack arrives": {@link #testStreamJobRunOk()} cancels a job on a fully healthy
     * cluster, and {@link #testStreamJobRestoreInWorkerDown()} kills a worker on a job that is
     * simply RUNNING with no cancel in flight, then cancels only after restore has completed. This
     * test constructs the narrow race directly: it white-box polls {@code
     * PhysicalVertex#getExecutionState()} (same in-JVM technique as {@code
     * SplitClusterPendingJobLifecycleFailoverIT#getJobMaster}) in a tight loop right after issuing
     * the cancel, and shuts the worker down the instant a task vertex is first observed CANCELING -
     * landing squarely inside the RPC-in-flight window instead of guessing at timing.
     */
    @Test
    public void testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck() throws Exception {
        String testCaseName = "testStreamJobCancelResolvesWhenWorkerCrashesBeforeCancelAck";
        String testClusterName = "SplitClusterFaultToleranceIT_" + testCaseName;
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);

            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            TestUtils.getResource("pending_jobs_streaming_lifecycle.conf"),
                            jobConfig,
                            seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));
            // Wait for every task vertex to actually finish deploying before cancelling, so the
            // CANCELING transition observed below is unambiguously caused by the cancel request,
            // not mixed up with an in-flight DEPLOYING -> RUNNING transition.
            assertAllVerticesRunning(masterNode, jobId, 60);

            // CoordinatorService#cancelJob runs JobMaster#cancelJob synchronously on its own
            // executor, and the client's cancelJob() blocks (PassiveCompletableFuture#join) until
            // that whole per-vertex cancel resolves. Issue it on its own thread so this thread
            // stays free to tight-poll for CANCELING and react to it immediately.
            CompletableFuture<Void> cancelInvocation =
                    CompletableFuture.runAsync(clientJobProxy::cancelJob);

            boolean observedCanceling =
                    awaitAnyVertexCanceling(masterNode, jobId, 10, TimeUnit.SECONDS);
            Assertions.assertTrue(
                    observedCanceling,
                    "Should have observed a task vertex enter CANCELING before its cancel ack "
                            + "could possibly arrive; otherwise this test never exercised the "
                            + "race it targets");

            // The vertex is CANCELING and still waiting on a CancelTaskOperation ack from
            // workerNode. Kill workerNode right now, before that ack can arrive, landing inside
            // the exact window PR #10729 fixed: cancel requested, CANCELING set, worker gone
            // before the terminal callback.
            workerNode.shutdown();

            CompletableFuture<JobResult> waitForCompleteFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobCompleteV2);
            assertEventuallyCanceled(clientJobProxy, waitForCompleteFuture);

            Assertions.assertDoesNotThrow(
                    () -> cancelInvocation.get(30, TimeUnit.SECONDS),
                    "cancelJob() should return once the vertex resolves locally, not hang");
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

    /**
     * Waits until every coordinator and task vertex of the job's single pipeline has itself
     * reported RUNNING, not just the top-level job status.
     */
    private static void assertAllVerticesRunning(
            HazelcastInstanceImpl masterNode, long jobId, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = getJobMaster(masterNode, jobId);
                            Assertions.assertNotNull(
                                    jobMaster,
                                    "Job master should exist before checking task vertex states");
                            PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
                            Assertions.assertEquals(JobStatus.RUNNING, physicalPlan.getJobStatus());
                            physicalPlan
                                    .getPipelineList()
                                    .forEach(SplitClusterFaultToleranceIT::assertRunningSubPlan);
                        });
    }

    private static void assertRunningSubPlan(SubPlan subPlan) {
        Assertions.assertEquals(PipelineStatus.RUNNING, subPlan.getPipelineState());
        subPlan.getCoordinatorVertexList()
                .forEach(SplitClusterFaultToleranceIT::assertRunningVertex);
        subPlan.getPhysicalVertexList().forEach(SplitClusterFaultToleranceIT::assertRunningVertex);
    }

    private static void assertRunningVertex(PhysicalVertex physicalVertex) {
        Assertions.assertEquals(ExecutionState.RUNNING, physicalVertex.getExecutionState());
    }

    /**
     * Tight-polls (no fixed sleep) every coordinator and task vertex of the job for CANCELING,
     * returning as soon as any one is observed. The window between a task vertex entering CANCELING
     * and its cancel ack arriving from a healthy worker can be sub-millisecond, well under
     * Awaitility's default ~100ms poll interval, so this deliberately busy-spins instead.
     */
    private static boolean awaitAnyVertexCanceling(
            HazelcastInstanceImpl masterNode, long jobId, long timeout, TimeUnit unit) {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            JobMaster jobMaster = getJobMaster(masterNode, jobId);
            if (jobMaster != null) {
                PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
                if (physicalPlan != null) {
                    for (SubPlan subPlan : physicalPlan.getPipelineList()) {
                        if (isAnyVertexCanceling(subPlan.getCoordinatorVertexList())
                                || isAnyVertexCanceling(subPlan.getPhysicalVertexList())) {
                            return true;
                        }
                    }
                }
            }
            Thread.yield();
        }
        return false;
    }

    private static boolean isAnyVertexCanceling(List<PhysicalVertex> vertices) {
        for (PhysicalVertex vertex : vertices) {
            if (ExecutionState.CANCELING.equals(vertex.getExecutionState())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads the current job master from the active SeaTunnel server embedded in the test cluster.
     */
    private static JobMaster getJobMaster(HazelcastInstanceImpl masterNode, long jobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getJobMaster(jobId);
    }

    /**
     * Waits for the job to reach a terminal state and asserts it is CANCELED, using the
     * already-in-flight {@code waitForJobCompleteV2()} future so this assertion cannot itself hang
     * forever if the CANCELING-stuck regression this test guards against were to reappear.
     */
    private static void assertEventuallyCanceled(
            ClientJobProxy clientJobProxy, CompletableFuture<JobResult> waitForCompleteFuture) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    waitForCompleteFuture.isDone(),
                                    "Job should reach a terminal state instead of staying stuck "
                                            + "in CANCELING after its worker crashed mid-cancel");
                            Assertions.assertEquals(
                                    JobStatus.CANCELED, waitForCompleteFuture.get().getStatus());
                        });
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, clientJobProxy.getJobStatus()));
    }

    @Test
    public void testBatchJobRestoreInWorkerDown() throws Exception {
        String testCaseName = "testBatchJobRestoreInWorkerDown";
        String testClusterName = "SplitClusterFaultToleranceIT_testBatchJobRestoreInWorkerDown";
        long testRowNumber = 1000;
        int testParallelism = 2;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            log.warn(
                    "===================================All node is running==========================");
            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.BATCH, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            Awaitility.await()
                    .atMost(180000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // shutdown on worker node
            log.warn(
                    "=====================================shutdown workerNode1=================================");
            workerNode1.shutdown();

            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, objectCompletableFuture.get());
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    60_000L);

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

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    @Test
    public void testStreamJobRestoreInWorkerDown() throws Exception {
        String testCaseName = "testStreamJobRestoreInWorkerDown";
        String testClusterName = "SplitClusterFaultToleranceIT_testStreamJobRestoreInWorkerDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.STREAMING, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && fileLineNumberFromDir > 1);
                            });
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Thread.sleep(5000);
            // shutdown on worker node
            workerNode1.shutdown();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    300_000L);
            clientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, objectCompletableFuture.get());
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    60_000L);

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

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    @Test
    public void testBatchJobRestoreInMasterDown() throws Exception {
        String testCaseName = "testBatchJobRestoreInMasterDown";
        String testClusterName = "SplitClusterFaultToleranceIT_testBatchJobRestoreInMasterDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.BATCH, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .pollDelay(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // shutdown master node
            masterNode2.shutdown();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                log.warn(
                                        "\n================================={}=================================\n",
                                        FileUtils.getFileLineNumberFromDir(
                                                testResources.getLeft()));
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, objectCompletableFuture.get());
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    60_000L);

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

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    @Test
    public void testStreamJobRestoreInMasterDown() throws Exception {
        String testCaseName = "testStreamJobRestoreInMasterDown";
        String testClusterName = "SplitClusterFaultToleranceIT_testStreamJobRestoreInMasterDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.STREAMING, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // shutdown master node
            masterNode2.shutdown();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    300_000L);
            clientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, objectCompletableFuture.get());
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    60_000L);

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

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    @Test
    @Disabled
    public void testFor() throws Exception {
        for (int i = 0; i < 200; i++) {
            testStreamJobRestoreInAllNodeDown();
        }
    }

    @Test
    public void testStreamJobRestoreInAllNodeDown() throws Exception {
        String testCaseName = "testStreamJobRestoreInAllNodeDown";
        String testClusterName =
                "SplitClusterFaultToleranceIT_testStreamJobRestoreInAllNodeDown_"
                        + System.currentTimeMillis();
        int testRowNumber = 1000;
        int testParallelism = 6;
        String yaml =
                "hazelcast:\n"
                        + "  cluster-name: "
                        + testClusterName
                        + "\n"
                        + "  network:\n"
                        + "    rest-api:\n"
                        + "      enabled: true\n"
                        + "      endpoint-groups:\n"
                        + "        CLUSTER_WRITE:\n"
                        + "          enabled: true\n"
                        + "    join:\n"
                        + "      tcp-ip:\n"
                        + "        enabled: true\n"
                        + "        member-list:\n"
                        + "          - localhost\n"
                        + "    port:\n"
                        + "      auto-increment: true\n"
                        + "      port-count: 100\n"
                        + "      port: 5801\n"
                        + "  map:\n"
                        + "    engine*:\n"
                        + "      map-store:\n"
                        + "        enabled: true\n"
                        + "        initial-mode: EAGER\n"
                        + "        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory\n"
                        + "        properties:\n"
                        + "          type: hdfs\n"
                        + "          namespace: /tmp/seatunnel/imap\n"
                        + "          clusterName: "
                        + testClusterName
                        + "\n"
                        + "          fs.defaultFS: file:///\n"
                        + "\n"
                        + "  properties:\n"
                        + "    hazelcast.invocation.max.retry.count: 200\n"
                        + "    hazelcast.tcp.join.port.try.count: 30\n"
                        + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                        + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                        + "    hazelcast.logging.type: log4j2\n"
                        + "    hazelcast.operation.generic.thread.count: 200\n";

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(yaml, testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(yaml, testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(yaml, testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(yaml, testClusterName);

        try {

            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName, JobMode.STREAMING, testRowNumber, testParallelism);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, masterNode1Config);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            Long jobId = clientJobProxy.getJobId();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });

            Thread.sleep(5000);
            // shutdown all node
            workerNode1.shutdown();
            workerNode2.shutdown();
            masterNode1.shutdown();
            masterNode2.shutdown();
            engineClient.close();

            log.warn(
                    "==========================================All node is done========================================");
            Thread.sleep(10000);
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            log.warn(
                    "==========================================All node is start, begin check node size ========================================");
            // waiting all node added to cluster
            HazelcastInstanceImpl restoreFinalNode = masterNode1;
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, restoreFinalNode.getCluster().getMembers().size()));

            log.warn(
                    "==========================================All node is running========================================");
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobProxy newClientJobProxy = engineClient.createJobClient().getJobProxy(jobId);
            CompletableFuture<JobStatus> waitForJobCompleteFuture =
                    CompletableFuture.supplyAsync(newClientJobProxy::waitForJobComplete);

            Thread.sleep(10000);

            Awaitility.await()
                    .atMost(100000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long fileLineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        fileLineNumberFromDir);
                                JobStatus jobStatus = null;
                                try {
                                    jobStatus = newClientJobProxy.getJobStatus();
                                } catch (Exception e) {
                                    log.error(ExceptionUtils.getMessage(e));
                                }
                                Assertions.assertEquals(JobStatus.RUNNING, jobStatus);
                                Assertions.assertTrue(fileLineNumberFromDir > 1);
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    100_000L);
            log.warn(
                    "==========================================Cancel Job========================================");
            newClientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, newClientJobProxy.getJobStatus());
                                Assertions.assertTrue(waitForJobCompleteFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.CANCELED, waitForJobCompleteFuture.get());
                            });
            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    60_000L);

        } finally {
            log.warn(
                    "==========================================Clean test resource ========================================");
            if (engineClient != null) {
                engineClient.close();
            }

            if (masterNode1 != null) {
                masterNode1.shutdown();
            }

            if (masterNode2 != null) {
                masterNode2.shutdown();
            }

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
        }
    }

    @NotNull private static SeaTunnelConfig getSeaTunnelConfig(String yaml, String testClusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        Config hazelcastConfig = Config.loadFromString(yaml);
        hazelcastConfig.setClusterName(testClusterName);
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        return seaTunnelConfig;
    }
}
