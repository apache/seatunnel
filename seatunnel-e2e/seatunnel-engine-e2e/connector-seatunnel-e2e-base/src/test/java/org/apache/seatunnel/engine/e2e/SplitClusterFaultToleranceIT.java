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
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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

    /**
     * Regression test for the deploy idempotency bug fixed by <a
     * href="https://github.com/apache/seatunnel/pull/10567">#10567</a> ("[Fix][Zeta] Make
     * deployTask idempotent during master failover recovery"). Before that fix, {@code
     * TaskExecutionService#deployTask} threw {@code RuntimeException("TaskGroupLocation: ...
     * already exists")} whenever it was invoked for a {@code TaskGroupLocation} already present in
     * {@code executionContexts}, i.e. a task that is genuinely still executing on this worker.
     *
     * <p>That collision is reachable through only one narrow window. On master failover, {@code
     * PhysicalVertex#initStateFuture()} restores each vertex's persisted {@link ExecutionState}
     * from the running-job IMap and, for a persisted state of RUNNING or DEPLOYING, pings the
     * worker via {@code checkTaskGroupIsExecuting} to self-heal a state that no longer matches
     * reality. The restored state is then replayed per vertex through {@code
     * PhysicalVertex#restoreExecutionState()}, which calls {@code PhysicalVertex#stateProcess()} —
     * the vertex-level switch on {@link ExecutionState}, not the pipeline-level one in {@code
     * SubPlan}. For RUNNING, a confirmed-alive task is simply left as RUNNING and {@code
     * PhysicalVertex#stateProcess}'s {@code case RUNNING} does nothing, so no redeploy call is ever
     * made. For DEPLOYING, that same confirmation still leaves the state as DEPLOYING (the check
     * only ever downgrades to FAILING, it never upgrades to RUNNING), and {@code
     * PhysicalVertex#stateProcess}'s {@code case DEPLOYING} unconditionally calls {@code deploy()}
     * again — this is the exact pre-fix crash site. DEPLOYING only persists in the IMap for the
     * duration of one worker deploy RPC round trip (typically low single-digit milliseconds), so
     * hitting it needs a deliberately constructed trigger rather than a blind sleep.
     *
     * <p>This test builds that trigger directly instead of gambling on timing. It submits a bounded
     * batch job with {@code testParallelism} FakeSource/LocalFile task groups and, on a dedicated
     * watcher thread, tight-polls (1 millisecond between checks, no blind sleep) the active
     * master's own in-JVM {@link JobMaster#getPhysicalPlan()} for any vertex reporting {@link
     * ExecutionState#DEPLOYING}. {@code SubPlan#stateProcess}'s {@code case DEPLOYING} drives that
     * fan-out one vertex at a time on a single thread — each vertex's {@code makeTaskGroupDeploy()}
     * synchronously runs {@code PhysicalVertex#stateProcess} and its deploy RPC before the next
     * vertex is touched. A vertex therefore becomes visibly DEPLOYING just before its RPC to the
     * worker goes out, and stays that way until the ack returns, so raising the parallelism does
     * not widen any single vertex's window — it multiplies how many independent windows the watcher
     * gets to land in across the pipeline's whole deploy fan-out. The instant any vertex is caught
     * mid-deploy, the watcher shuts the active master down, which is what a real master crash
     * inside that window looks like to the rest of the cluster. The watcher's own poll is a plain
     * in-JVM field read with no network cost, while the state it races against is gated by a real
     * IMap write plus a real deploy RPC, so the watcher can sample many times inside a window it
     * does not control. That asymmetry, not luck, is what makes the trigger reliable.
     *
     * <p>Observing DEPLOYING at kill time guarantees the persisted state driving restore is
     * DEPLOYING, which is necessary to reach {@code PhysicalVertex#stateProcess}'s redeploy branch,
     * but it does not by itself guarantee the worker had already finished deploying that vertex —
     * the other ingredient the pre-fix crash needs. Both sub-cases are legitimate outcomes of this
     * trigger, and this test's assertions are written to hold cleanly on either one: if the worker
     * had already succeeded, the fixed code makes the redeploy a no-op and that vertex needs no
     * restore at all; if it had not, the same self-heal check correctly drives it to FAILING and
     * the pipeline restores exactly once, which is ordinary, expected recovery rather than a
     * regression. What must never happen on either sub-case is a client-visible failure or more
     * than one pipeline restore for the whole scenario. A pre-fix run instead throws on the worker,
     * fails that one vertex, and — since a single failed vertex fails its whole pipeline — forces a
     * full pipeline restart that redeploys every other already-fine vertex in it as collateral
     * damage, which would show up here as {@link SubPlan#getPipelineRestoreNum()} exceeding 1.
     */
    @Test
    public void testDeployNotDuplicatedWhenMasterKilledDuringDeploy() throws Exception {
        String testCaseName = "testDeployNotDuplicatedWhenMasterKilledDuringDeploy";
        String testClusterName =
                "SplitClusterFaultToleranceIT_testDeployNotDuplicatedWhenMasterKilledDuringDeploy";
        long testRowNumber = 20;
        int testParallelism = 40;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ExecutorService masterKillExecutor = Executors.newSingleThreadExecutor();

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);

            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

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
            long jobId = clientJobProxy.getJobId();
            CompletableFuture<JobStatus> jobCompleteFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // Race the watcher against the master's own deploy fan-out: the instant it catches any
            // vertex still DEPLOYING, it kills the active master right there, reproducing the exact
            // pre-fix crash window on master failover restore.
            HazelcastInstanceImpl finalActiveMaster = activeMaster;
            Future<Boolean> deployingObserved =
                    masterKillExecutor.submit(
                            () ->
                                    killActiveMasterWhileAnyVertexDeploying(
                                            finalActiveMaster, jobId, 30));
            // Wait past the watcher's own 30s deadline on purpose. Sharing that deadline here
            // would race the watcher's normal `false` return and surface a bare TimeoutException
            // instead of the diagnostic message below, which is what tells a future maintainer the
            // trigger window was missed rather than the fix being broken.
            Assertions.assertTrue(
                    deployingObserved.get(60, TimeUnit.SECONDS),
                    "Never observed any task vertex in DEPLOYING state before the job's initial "
                            + "deploy fan-out completed; the deploy-idempotency trigger window was "
                            + "missed on this run, so this test did not exercise the fix and needs "
                            + "revisiting rather than being treated as a pass");

            awaitCoordinatorActive(standbyMaster, 30);
            assertRecoveredWithBoundedPipelineRestore(standbyMaster, jobId, 60);

            Awaitility.await()
                    .atMost(180, TimeUnit.SECONDS)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(jobCompleteFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, jobCompleteFuture.get());
                            });

            // A pre-fix regression either surfaces as a client-visible failure (already ruled out
            // above) or, on the sub-case where the redeploy races ahead of the worker's real ack,
            // as replayed FakeSource splits inflating the row count beyond a single clean run's
            // worth of output. Assert the recovered output on the same terms this file already uses
            // for its other master-down restores.
            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism,
                    testRowNumber,
                    60_000L);
        } finally {
            masterKillExecutor.shutdownNow();
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

    /**
     * Tight-polls the active master's own in-JVM physical plan for a task vertex sitting in {@link
     * ExecutionState#DEPLOYING} and, the instant one is found, shuts the active master down right
     * there. Used only by {@link #testDeployNotDuplicatedWhenMasterKilledDuringDeploy()} to
     * construct a deterministic master crash inside the narrow window that the deploy-idempotency
     * fix in <a href="https://github.com/apache/seatunnel/pull/10567">#10567</a> guards.
     *
     * @return true if a DEPLOYING vertex was observed and the active master was shut down; false if
     *     no vertex was ever caught DEPLOYING before {@code timeoutSeconds} elapsed
     */
    private static boolean killActiveMasterWhileAnyVertexDeploying(
            HazelcastInstanceImpl activeMaster, long jobId, long timeoutSeconds)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (System.currentTimeMillis() < deadline) {
            if (activeMaster.getLifecycleService().isRunning()) {
                JobMaster jobMaster = getJobMaster(activeMaster, jobId);
                if (jobMaster != null && jobMaster.getPhysicalPlan() != null) {
                    for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
                        if (isAnyVertexDeploying(subPlan)) {
                            activeMaster.shutdown();
                            return true;
                        }
                    }
                }
            }
            // Deliberately un-sleepy: the vertex state we are racing is gated by a real IMap write
            // plus a real deploy RPC, while this read is a plain in-JVM field access, so polling
            // this tightly costs little and maximizes how many samples land inside the window.
            TimeUnit.MILLISECONDS.sleep(1);
        }
        return false;
    }

    private static boolean isAnyVertexDeploying(SubPlan subPlan) {
        return subPlan.getCoordinatorVertexList().stream()
                        .anyMatch(
                                vertex ->
                                        ExecutionState.DEPLOYING.equals(vertex.getExecutionState()))
                || subPlan.getPhysicalVertexList().stream()
                        .anyMatch(
                                vertex ->
                                        ExecutionState.DEPLOYING.equals(
                                                vertex.getExecutionState()));
    }

    /**
     * Waits for the new active master to finish restoring the job after failover and asserts the
     * recovery was clean: every coordinator and task vertex of every pipeline back to RUNNING, and
     * no pipeline needed more than one restore to get there. A pre-fix deploy-idempotency bug would
     * instead fail the redeployed vertex, which fails its whole pipeline and forces a full pipeline
     * restart, pushing {@link SubPlan#getPipelineRestoreNum()} past 1.
     */
    private static void assertRecoveredWithBoundedPipelineRestore(
            HazelcastInstanceImpl activeMaster, long jobId, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobMaster jobMaster = getJobMaster(activeMaster, jobId);
                            Assertions.assertNotNull(
                                    jobMaster,
                                    "Job master should exist on the new active master after "
                                            + "failover");
                            PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
                            Assertions.assertNotNull(
                                    physicalPlan, "Physical plan should be rebuilt after failover");
                            Assertions.assertEquals(JobStatus.RUNNING, physicalPlan.getJobStatus());
                            physicalPlan
                                    .getPipelineList()
                                    .forEach(
                                            subPlan -> {
                                                assertAllVertexRunning(subPlan);
                                                Assertions.assertTrue(
                                                        subPlan.getPipelineRestoreNum() <= 1,
                                                        String.format(
                                                                "Pipeline %s required %d restores "
                                                                        + "to recover, expected at "
                                                                        + "most 1; a deploy "
                                                                        + "redeploy race should "
                                                                        + "either need none (worker "
                                                                        + "already running) or "
                                                                        + "exactly one clean "
                                                                        + "restore (worker not yet "
                                                                        + "running), not repeated "
                                                                        + "restarts",
                                                                subPlan.getPipelineLocation(),
                                                                subPlan.getPipelineRestoreNum()));
                                            });
                        });
    }

    private static void assertAllVertexRunning(SubPlan subPlan) {
        subPlan.getCoordinatorVertexList()
                .forEach(SplitClusterFaultToleranceIT::assertVertexRunning);
        subPlan.getPhysicalVertexList().forEach(SplitClusterFaultToleranceIT::assertVertexRunning);
    }

    private static void assertVertexRunning(PhysicalVertex physicalVertex) {
        Assertions.assertEquals(ExecutionState.RUNNING, physicalVertex.getExecutionState());
    }

    /**
     * Reads the current job master from the active SeaTunnel server embedded in the test cluster.
     */
    private static JobMaster getJobMaster(HazelcastInstanceImpl activeMaster, long jobId) {
        SeaTunnelServer server =
                activeMaster.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        return server.getCoordinatorService().getJobMaster(jobId);
    }

    private static HazelcastInstanceImpl waitAndFindActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        final HazelcastInstanceImpl[] activeMasterRef = new HazelcastInstanceImpl[1];
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            activeMasterRef[0] = findActiveMaster(masterNode1, masterNode2);
                            Assertions.assertNotNull(
                                    activeMasterRef[0],
                                    "Should find active master after coordinator initialization");
                        });
        return activeMasterRef[0];
    }

    private static HazelcastInstanceImpl findActiveMaster(
            HazelcastInstanceImpl masterNode1, HazelcastInstanceImpl masterNode2) {
        if (isCoordinatorActive(masterNode1)) {
            return masterNode1;
        }
        if (isCoordinatorActive(masterNode2)) {
            return masterNode2;
        }
        return null;
    }

    /** Waits until a standby master has taken over coordinator activity after a failover. */
    private static void awaitCoordinatorActive(
            HazelcastInstanceImpl masterNode, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(masterNode.getLifecycleService().isRunning());
                            Assertions.assertTrue(
                                    isCoordinatorActive(masterNode),
                                    "Standby master should become active after failover");
                        });
    }

    private static boolean isCoordinatorActive(HazelcastInstanceImpl masterNode) {
        if (masterNode == null || !masterNode.getLifecycleService().isRunning()) {
            return false;
        }
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        try {
            return server.getCoordinatorService().isCoordinatorActive();
        } catch (SeaTunnelEngineException e) {
            return false;
        }
    }
}
