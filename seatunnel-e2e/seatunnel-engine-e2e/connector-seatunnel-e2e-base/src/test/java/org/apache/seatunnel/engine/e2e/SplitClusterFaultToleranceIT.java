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
import org.apache.seatunnel.engine.core.job.JobStatus;
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
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
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
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
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
                    .atMost(600000, TimeUnit.MILLISECONDS)
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

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(180000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait some tasks commit finished
                                Thread.sleep(2000);
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft())
                                                        > 1);
                            });

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
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.FINISHED.equals(
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

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait some tasks commit finished, and we can get rows from the
                                // sink target dir
                                Thread.sleep(2000);
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft())
                                                        > 1);
                            });

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
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait job write all rows in file
                                Thread.sleep(2000);
                                log.warn(
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft())
                                                .toString());
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && testRowNumber * testParallelism
                                                        == FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft()));
                            });

            // sleep 10s and expect the job don't write more rows.
            Thread.sleep(10000);
            clientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.CANCELED.equals(
                                                            objectCompletableFuture.get())));

            // check the final rows
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

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait some tasks commit finished
                                Thread.sleep(2000);
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft())
                                                        > 1);
                            });

            // shutdown master node
            masterNode2.shutdown();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));
            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Thread.sleep(2000);
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

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait some tasks commit finished, and we can get rows from the
                                // sink target dir
                                Thread.sleep(2000);
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(clientJobProxy.getJobStatus())
                                                && FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft())
                                                        > 1);
                            });

            // shutdown master node
            masterNode2.shutdown();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalNode.getCluster().getMembers().size()));

            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait job write all rows in file
                                Thread.sleep(2000);
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

            // sleep 10s and expect the job don't write more rows.
            Thread.sleep(10000);
            clientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            objectCompletableFuture.isDone()
                                                    && JobStatus.CANCELED.equals(
                                                            objectCompletableFuture.get())));

            // check the final rows
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

            ClientJobProxy finalClientJobProxy = clientJobProxy;
            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                // Wait some tasks commit finished, and we can get rows from the
                                // sink target dir
                                Thread.sleep(2000);
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(finalClientJobProxy.getJobStatus())
                                                && FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft())
                                                        > 1);
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
                    .untilAsserted(
                            () -> {
                                // Wait job write all rows in file
                                Thread.sleep(2000);
                                log.warn(
                                        "\n================================="
                                                + FileUtils.getFileLineNumberFromDir(
                                                        testResources.getLeft())
                                                + "=================================\n");
                                JobStatus jobStatus = null;
                                try {
                                    jobStatus = newClientJobProxy.getJobStatus();
                                } catch (Exception e) {
                                    log.error(ExceptionUtils.getMessage(e));
                                }

                                Assertions.assertTrue(
                                        JobStatus.RUNNING.equals(jobStatus)
                                                && testRowNumber * testParallelism
                                                        == FileUtils.getFileLineNumberFromDir(
                                                                testResources.getLeft()));
                            });

            // sleep 10s and expect the job don't write more rows.
            Thread.sleep(10000);
            log.warn(
                    "==========================================Cancel Job========================================");
            newClientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(600000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            waitForJobCompleteFuture.isDone()
                                                    && JobStatus.CANCELED.equals(
                                                            waitForJobCompleteFuture.get())));
            // prove that the task was restarted
            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(testRowNumber * testParallelism, fileLineNumberFromDir);

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
        return seaTunnelConfig;
    }
}
