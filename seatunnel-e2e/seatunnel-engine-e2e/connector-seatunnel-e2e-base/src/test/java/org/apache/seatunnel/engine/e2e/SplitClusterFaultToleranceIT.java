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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

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
     * Verified-fragile-architecture probe (not a fix regression test): {@link
     * org.apache.seatunnel.engine.server.dag.physical.ResourceUtils#applyResourceForPipeline}
     * throws {@code NoEnoughResourceException} synchronously and immediately if it cannot obtain
     * every slot a pipeline needs in a single pass -- the method has an explicit {@code TODO}
     * acknowledging there is no wait/backoff at that layer. {@link
     * org.apache.seatunnel.engine.server.dag.physical.SubPlan#stateProcess()}'s {@code SCHEDULED}
     * case turns that single exception into one consumed unit of the pipeline's fixed {@code
     * pipelineMaxRestoreNum} (default 3, from {@code job.retry.times}) restore budget, sleeping
     * {@code pipelineRestoreIntervalSeconds} (default 3, from {@code job.retry.interval.seconds})
     * before every restore attempt. A pipeline that races several siblings for a
     * momentarily-shrunken slot pool can therefore burn its entire retry budget on pure allocation
     * timing and permanently fail, even though the contention would have resolved moments later.
     *
     * <p>This test engineers genuine (not lucky/racy) multi-pipeline contention rather than relying
     * on placement luck: the job config declares 4 independent, unrelated FakeSource -> LocalFile
     * chains, so the planner splits them into 4 separate pipelines (SubPlans), each with its own
     * restore budget. Every simple 1-parallelism pipeline here needs exactly 3 slots (1
     * source-enumerator coordinator + 1 sink-committer coordinator -- LocalFile's sink always
     * registers an aggregated file-commit coordinator, independent of is_enable_transaction -- + 1
     * fused reader/writer task group) -- 4 pipelines x 3 slots = 12 total slot demand. Both workers
     * are pinned to a fixed pool (dynamic-slot=false) of 8 slots each: 16 total capacity
     * comfortably admits the initial 12-slot demand (4 spare), but no single 8-slot worker can ever
     * host all 4 pipelines (4 x 3 = 12 > 8: at most 2 full pipelines, using 6 of 8 slots, fit on
     * one worker with only 2 spare -- not enough for a 3rd pipeline's 3 slots). So killing either
     * worker is guaranteed, by construction rather than chance, to strand part of more than one
     * pipeline at once and force them to restore-race the survivor's remaining fixed capacity.
     * Every LocalFile sink also sets is_enable_transaction=true, so a canceled attempt's
     * in-progress writes stay in an uncommitted temp location and never surface in the output
     * directory this test counts -- without that, a stranded pipeline's aborted attempt could leak
     * partial rows into the count and the exact-equality assertion below would be unsound
     * regardless of the restore-budget outcome.
     *
     * <p>Whether today's fixed 9-second guaranteed-sleep budget (3 attempts x 3s, before any real
     * cancel/checkpoint-cancel/resource-release/RPC overhead per attempt) is enough patience for
     * that contention to resolve -- as the deliberately tiny, fast FakeSource pipelines finish and
     * free their slots for whoever is still waiting -- is exactly the open question this test
     * answers empirically: either the job finishes (today's behavior tolerates this contention
     * level) or some pipeline permanently fails with its restore budget exhausted purely on
     * allocation timing (the bug this item describes). Either outcome is a valid, honest finding;
     * this test only documents production behavior and never modifies production code.
     */
    @Test
    public void testManyPipelinesRestoreContentionInWorkerDown() throws Exception {
        String testCaseName = "testManyPipelinesRestoreContentionInWorkerDown";
        String testClusterName =
                "SplitClusterFaultToleranceIT_testManyPipelinesRestoreContentionInWorkerDown";
        long testRowNumber = 5000;
        int pipelineNum = 4;
        // Fixed per-worker slot pool (dynamic-slot disabled below makes this a hard ceiling, not
        // a hint). 8 < (pipelineNum * 3 slots-per-pipeline = 12), so a single worker can never
        // host all 4 pipelines -- see the class-level Javadoc above for the full arithmetic.
        int slotNumPerWorker = 8;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);
        for (SeaTunnelConfig workerConfig :
                new SeaTunnelConfig[] {workerNode1Config, workerNode2Config}) {
            workerConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
            workerConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(slotNumPerWorker);
        }

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);

            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);

            // waiting all node added to cluster (1 master + 2 workers)
            HazelcastInstanceImpl finalNode = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));

            log.warn(
                    "===================================All node is running==========================");
            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createManyPipelineTestResources(testCaseName, testRowNumber, pipelineNum);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(
                            testResources.getRight(), jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

            // Catch the job genuinely mid-flight (all 4 pipelines deployed and running across
            // both workers) before killing a worker. A tight poll minimizes the race against
            // these deliberately tiny/fast pipelines finishing on their own before we can react;
            // JobStatus only reaches RUNNING after every pipeline's own DEPLOYING step succeeds,
            // so by the time this is observed all 12 slots are already assigned on real workers.
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // Kill one worker while several pipelines are running on it: every pipeline that had
            // any slot on this worker (coordinator or task group) must release ALL of its slots,
            // not just the lost one, and restore-race the survivor's fixed 8-slot pool.
            log.warn(
                    "=====================================shutdown workerNode1=================================");
            workerNode1.shutdown();

            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

            // Generous terminal-state budget: pipelineMaxRestoreNum(3) *
            // pipelineRestoreIntervalSeconds(3) = 9s of guaranteed sleep alone is the theoretical
            // floor; real cancel/checkpoint-cancel/resource-release/RPC overhead per attempt (plus
            // this being a shared CI runner) makes the actual wall-clock budget considerably
            // larger, so this timeout is generous on purpose in both directions.
            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(1000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(objectCompletableFuture.isDone()));

            JobStatus finalStatus = objectCompletableFuture.get();
            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            log.warn(
                    "==================final job status: {}, output line count: {}==================",
                    finalStatus,
                    fileLineNumberFromDir);
            Assertions.assertEquals(JobStatus.FINISHED, finalStatus);
            Assertions.assertEquals(testRowNumber * pipelineNum, fileLineNumberFromDir);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }

            if (masterNode1 != null) {
                masterNode1.shutdown();
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
     * Create the test job config file based on {@code
     * cluster_batch_fake_to_localfile_slot_contention_template.conf}, which declares {@code
     * pipelineNum} independent (unrelated) FakeSource -> LocalFile chains so the planner splits
     * them into that many separate pipelines/SubPlans. It deletes the test sink target path before
     * returning the final job config file path, matching the sibling {@code createTestResources}
     * helpers in this class.
     *
     * @param testCaseName testCaseName, also used as the sink output directory name
     * @param rowNumber row.num for every FakeSource (parallelism is fixed at 1 in the template)
     * @param pipelineNum number of independent pipelines the template declares; must match the
     *     template file's actual source/sink count since this method only substitutes values, it
     *     does not generate the template's pipeline count
     */
    private ImmutablePair<String, String> createManyPipelineTestResources(
            @NonNull String testCaseName, long rowNumber, int pipelineNum) throws IOException {
        checkArgument(rowNumber > 0, "rowNumber must greater than 0");
        checkArgument(pipelineNum > 0, "pipelineNum must greater than 0");
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);
        valueMap.put(DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM, String.valueOf(rowNumber));

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
                "cluster_batch_fake_to_localfile_slot_contention_template.conf",
                valueMap,
                targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }
}
