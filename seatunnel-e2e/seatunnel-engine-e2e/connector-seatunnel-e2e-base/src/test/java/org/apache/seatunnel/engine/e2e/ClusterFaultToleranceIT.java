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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * Cluster fault tolerance test. Test the job recovery capability and data consistency assurance
 * capability in case of cluster node failure
 */
@Slf4j
public class ClusterFaultToleranceIT {

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    public static final String DYNAMIC_JOB_MODE = "dynamic_job_mode";

    public static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    public static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    @Test
    public void testBatchJobRunOkIn2Node() throws ExecutionException, InterruptedException {
        String testCaseName = "testBatchJobRunOkIn2Node";
        String testClusterName = "ClusterFaultToleranceIT_testBatchJobRunOkIn2Node";
        long testRowNumber = 1000;
        int testParallelism = 6;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            System.out.println(engineClient.getJobMetrics(clientJobProxy.getJobId()));
            log.warn("========================clean test resource====================");
        } finally {
            if (engineClient != null) {
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
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
            @NonNull String testCaseName,
            @NonNull JobMode jobMode,
            long rowNumber,
            int parallelism) {
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
    public void testStreamJobRunOkIn2Node() throws ExecutionException, InterruptedException {
        String testCaseName = "testStreamJobRunOkIn2Node";
        String testClusterName = "ClusterFaultToleranceIT_testStreamJobRunOkIn2Node";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    public void testBatchJobRestoreIn2NodeWorkerDown()
            throws ExecutionException, InterruptedException {
        String testCaseName = "testBatchJobRestoreIn2NodeWorkerDown";
        String testClusterName = "ClusterFaultToleranceIT_testBatchJobRestoreIn2NodeWorkerDown";
        long testRowNumber = 1000;
        int testParallelism = 2;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

            log.info(
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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            log.info(
                    "=====================================shutdown node2=================================");
            node2.shutdown();

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
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    public void testStreamJobRestoreIn2NodeWorkerDown()
            throws ExecutionException, InterruptedException {
        String testCaseName = "testStreamJobRestoreIn2NodeWorkerDown";
        String testClusterName = "ClusterFaultToleranceIT_testStreamJobRestoreIn2NodeWorkerDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            node2.shutdown();

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
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    public void testBatchJobRestoreIn2NodeMasterDown()
            throws ExecutionException, InterruptedException {
        String testCaseName = "testBatchJobRestoreIn2NodeMasterDown";
        String testClusterName = "ClusterFaultToleranceIT_testBatchJobRestoreIn2NodeMasterDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            node1.shutdown();

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
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    public void testStreamJobRestoreIn2NodeMasterDown()
            throws ExecutionException, InterruptedException {
        String testCaseName = "testStreamJobRestoreIn2NodeMasterDown";
        String testClusterName = "ClusterFaultToleranceIT_testStreamJobRestoreIn2NodeMasterDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            node1.shutdown();

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
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    @Disabled
    public void testFor() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 200; i++) {
            testStreamJobRestoreInAllNodeDown();
        }
    }

    @Test
    public void testStreamJobRestoreInAllNodeDown()
            throws ExecutionException, InterruptedException {
        String testCaseName = "testStreamJobRestoreInAllNodeDown";
        String testClusterName =
                "ClusterFaultToleranceIT_testStreamJobRestoreInAllNodeDown_"
                        + System.currentTimeMillis();
        int testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        try {
            String yaml =
                    "hazelcast:\n"
                            + "  cluster-name: seatunnel\n"
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
            Config hazelcastConfig = Config.loadFromString(yaml);
            hazelcastConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            node1.shutdown();
            node2.shutdown();

            log.warn(
                    "==========================================All node is done========================================");
            Thread.sleep(10000);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            log.warn(
                    "==========================================All node is start, begin check node size ========================================");
            // waiting all node added to cluster
            HazelcastInstanceImpl restoreFinalNode = node1;
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, restoreFinalNode.getCluster().getMembers().size()));

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
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }

    @Test
    @Disabled
    public void testStreamJobRestoreFromOssInAllNodeDown()
            throws ExecutionException, InterruptedException {
        String OSS_BUCKET_NAME = "oss://your bucket name/";
        String OSS_ENDPOINT = "your oss endpoint";
        String OSS_ACCESS_KEY_ID = "oss accessKey id";
        String OSS_ACCESS_KEY_SECRET = "oss accessKey secret";

        String testCaseName = "testStreamJobRestoreFromOssInAllNodeDown";
        String testClusterName =
                "ClusterFaultToleranceIT_testStreamJobRestoreFromOssInAllNodeDown_"
                        + System.currentTimeMillis();
        int testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        try {
            String yaml =
                    "hazelcast:\n"
                            + "  cluster-name: seatunnel\n"
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
                            + "          namespace: /seatunnel-test/imap\n"
                            + "          storage.type: oss\n"
                            + "          clusterName: "
                            + testClusterName
                            + "\n"
                            + "          oss.bucket: "
                            + OSS_BUCKET_NAME
                            + "\n"
                            + "          fs.oss.accessKeyId: "
                            + OSS_ACCESS_KEY_ID
                            + "\n"
                            + "          fs.oss.accessKeySecret: "
                            + OSS_ACCESS_KEY_SECRET
                            + "\n"
                            + "          fs.oss.endpoint: "
                            + OSS_ENDPOINT
                            + "\n"
                            + "          fs.oss.credentials.provider: org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider\n"
                            + "  properties:\n"
                            + "    hazelcast.invocation.max.retry.count: 200\n"
                            + "    hazelcast.tcp.join.port.try.count: 30\n"
                            + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                            + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                            + "    hazelcast.logging.type: log4j2\n"
                            + "    hazelcast.operation.generic.thread.count: 200\n";

            Config hazelcastConfig = Config.loadFromString(yaml);
            hazelcastConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

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
                    engineClient.createExecutionContext(testResources.getRight(), jobConfig);
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
            node1.shutdown();
            node2.shutdown();

            log.info(
                    "==========================================All node is done========================================");
            Thread.sleep(10000);

            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            log.info(
                    "==========================================All node is start, begin check node size ========================================");
            // waiting all node added to cluster
            HazelcastInstanceImpl restoreFinalNode = node1;
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, restoreFinalNode.getCluster().getMembers().size()));

            log.info(
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
            log.info(
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
            log.info(
                    "==========================================Clean test resource ========================================");
            if (engineClient != null) {
                engineClient.shutdown();
            }

            if (node1 != null) {
                node1.shutdown();
            }

            if (node2 != null) {
                node2.shutdown();
            }
        }
    }
}
