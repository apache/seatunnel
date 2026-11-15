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
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
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
 * Cluster fault tolerance test. Test the job which have two pipelines can recovery capability and
 * data consistency assurance capability in case of cluster node failure
 */
@Slf4j
public class ClusterFaultToleranceTwoPipelineIT {

    public static final String TEST_TEMPLATE_FILE_NAME =
            "cluster_batch_fake_to_localfile_two_pipeline_template.conf";

    /**
     * Two independent pipelines: table_healthy is an ordinary bounded FakeSource -> LocalFile
     * pipeline, table_doomed's Assert sink always throws so its pipeline permanently fails. Used by
     * {@link #testOneUnrecoverablePipelineForceCancelsHealthySiblingPipeline()}.
     */
    public static final String ONE_PIPELINE_PERMANENTLY_FAILED_TEMPLATE_FILE_NAME =
            "cluster_batch_one_pipeline_permanently_failed_template.conf";

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    public static final String DYNAMIC_JOB_MODE = "dynamic_job_mode";

    public static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    public static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    @Test
    public void testTwoPipelineBatchJobRunOkIn2Node() throws Exception {
        String testCaseName = "testTwoPipelineBatchJobRunOkIn2Node";
        String testClusterName =
                "ClusterFaultToleranceTwoPipelineIT_testTwoPipelineBatchJobRunOkIn2Node";
        long testRowNumber = 1000;
        int testParallelism = 6;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);

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
                            testCaseName,
                            JobMode.BATCH,
                            testRowNumber,
                            testParallelism,
                            TEST_TEMPLATE_FILE_NAME);
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
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                log.warn(
                                        "\n================================={}=================================\n",
                                        FileUtils.getFileLineNumberFromDir(
                                                testResources.getLeft()));
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, objectCompletableFuture.get());
                            });

            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(testRowNumber * testParallelism * 2, fileLineNumberFromDir);
        } finally {
            if (engineClient != null) {
                engineClient.close();
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
            int parallelism,
            @NonNull String templateFileName)
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
                templateFileName, valueMap, targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }

    @Test
    public void testTwoPipelineStreamJobRunOkIn2Node() throws Exception {
        String testCaseName = "testTwoPipelineStreamJobRunOkIn2Node";
        String testClusterName =
                "ClusterFaultToleranceTwoPipelineIT_testTwoPipelineStreamJobRunOkIn2Node";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
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
                            testCaseName,
                            JobMode.STREAMING,
                            testRowNumber,
                            testParallelism,
                            TEST_TEMPLATE_FILE_NAME);
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
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            clientJobProxy.getJobStatus().ordinal()
                                                    >= JobStatus.RUNNING.ordinal()));
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(5, TimeUnit.MINUTES)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertEquals(
                                        testRowNumber * testParallelism * 2, lineNumberFromDir);
                            });

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

            Long fileLineNumberFromDir =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertEquals(testRowNumber * testParallelism * 2, fileLineNumberFromDir);

        } finally {
            if (engineClient != null) {
                engineClient.close();
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
    public void testTwoPipelineBatchJobRestoreIn2NodeWorkerDown() throws Exception {
        String testCaseName = "testTwoPipelineBatchJobRestoreIn2NodeWorkerDown";
        String testClusterName =
                "ClusterFaultToleranceTwoPipelineIT_testTwoPipelineBatchJobRestoreIn2NodeWorkerDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
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
                            testCaseName,
                            JobMode.BATCH,
                            testRowNumber,
                            testParallelism,
                            TEST_TEMPLATE_FILE_NAME);
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
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(lineNumberFromDir > 1);
                            });
            // In the restore case, ensure that JabStatus is in the RUNNING state before calling
            // waitForJobComplete.
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // shutdown on worker node
            node2.shutdown();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                log.warn(
                                        "\n================================={}=================================\n",
                                        FileUtils.getFileLineNumberFromDir(
                                                testResources.getLeft()));
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, objectCompletableFuture.get());
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism * 2,
                    testRowNumber,
                    60_000L);
        } finally {
            if (engineClient != null) {
                engineClient.close();
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
    public void testFor() throws Exception {
        for (int i = 0; i < 200; i++) {
            testTwoPipelineStreamJobRestoreIn2NodeMasterDown();
        }
    }

    @Test
    public void testTwoPipelineStreamJobRestoreIn2NodeWorkerDown() throws Exception {
        String testCaseName = "testTwoPipelineStreamJobRestoreIn2NodeWorkerDown";
        String testClusterName =
                "ClusterFaultToleranceTwoPipelineIT_testTwoPipelineStreamJobRestoreIn2NodeWorkerDown";
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
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
                            testCaseName,
                            JobMode.STREAMING,
                            testRowNumber,
                            testParallelism,
                            TEST_TEMPLATE_FILE_NAME);
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
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(lineNumberFromDir > 1);
                            });
            // In the restore case, ensure that JabStatus is in the RUNNING state before calling
            // waitForJobComplete.
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(() -> clientJobProxy.waitForJobComplete());

            Thread.sleep(5000);
            // shutdown on worker node
            node2.shutdown();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(lineNumberFromDir > 1);
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism * 2,
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
                    testRowNumber * testParallelism * 2,
                    testRowNumber,
                    60_000L);

        } finally {
            if (engineClient != null) {
                engineClient.close();
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
    public void testTwoPipelineBatchJobRestoreIn2NodeMasterDown() throws Exception {
        String testCaseName =
                "testTwoPipelineBatchJobRestoreIn2NodeMasterDown" + System.currentTimeMillis();
        String testClusterName =
                "ClusterFaultToleranceTwoPipelineIT_testTwoPipelineBatchJobRestoreIn2NodeMasterDown"
                        + System.currentTimeMillis();
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
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
                            testCaseName,
                            JobMode.BATCH,
                            testRowNumber,
                            testParallelism,
                            TEST_TEMPLATE_FILE_NAME);
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
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(lineNumberFromDir > 1);
                            });
            // In the restore case, ensure that JabStatus is in the RUNNING state before calling
            // waitForJobComplete.
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // shutdown master node
            node1.shutdown();

            log.info(
                    "=============================shutdown node1===================================");

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                log.warn(
                                        "\n================================={}=================================\n",
                                        FileUtils.getFileLineNumberFromDir(
                                                testResources.getLeft()));
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(objectCompletableFuture.isDone());
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, objectCompletableFuture.get());
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism * 2,
                    testRowNumber,
                    60_000L);

        } finally {
            if (engineClient != null) {
                engineClient.close();
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
    public void testTwoPipelineStreamJobRestoreIn2NodeMasterDown() throws Exception {
        String testCaseName =
                "testTwoPipelineStreamJobRestoreIn2NodeMasterDown" + System.currentTimeMillis();
        String testClusterName =
                "ClusterFaultToleranceTwoPipelineIT_testTwoPipelineStreamJobRestoreIn2NodeMasterDown"
                        + System.currentTimeMillis();
        long testRowNumber = 1000;
        int testParallelism = 6;
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
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
                            testCaseName,
                            JobMode.STREAMING,
                            testRowNumber,
                            testParallelism,
                            TEST_TEMPLATE_FILE_NAME);
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
                    .atMost(360000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(lineNumberFromDir > 1);
                            });
            // In the restore case, ensure that JabStatus is in the RUNNING state before calling
            // waitForJobComplete.
            CompletableFuture<JobStatus> objectCompletableFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // shutdown master node
            node1.shutdown();

            Awaitility.await()
                    .atMost(300000, TimeUnit.MILLISECONDS)
                    .pollInterval(2000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                log.warn(
                                        "\n================================={}=================================\n",
                                        lineNumberFromDir);
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(lineNumberFromDir > 1);
                            });

            FaultToleranceFakeSourceAssertions.assertOutputRecoveredAndStable(
                    testResources.getLeft(),
                    testRowNumber * testParallelism * 2,
                    testRowNumber,
                    300_000L);
            clientJobProxy.cancelJob();

            Awaitility.await()
                    .atMost(350000, TimeUnit.MILLISECONDS)
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
                    testRowNumber * testParallelism * 2,
                    testRowNumber,
                    60_000L);

        } finally {
            if (engineClient != null) {
                engineClient.close();
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
     * Documents PhysicalPlan's current, hardcoded cascade-cancel behavior: {@code
     * PhysicalPlan#makeJobEndWhenPipelineEnded} is unconditionally {@code true} (no config or
     * setter exists for it), so the moment ANY pipeline in a multi-pipeline job exhausts its
     * restore budget ({@code SubPlan#canRestorePipeline}, gated by {@code job.retry.times}) and
     * reaches a terminal {@link PipelineStatus#FAILED}, {@code PhysicalPlan#addPipelineEndCallback}
     * unconditionally calls {@code updateJobState(JobStatus.FAILING)}. That job-level transition's
     * {@code stateProcess()} then calls {@code jobMaster.neverNeedRestore()} followed by {@code
     * SubPlan#cancelPipeline} on every pipeline of the job, including ones that never touched the
     * failure and are still healthily RUNNING.
     *
     * <p>This is a documentation-of-current-behavior test, not a regression test for a fix: nothing
     * in the engine today isolates one pipeline's terminal failure from its siblings, so this test
     * intentionally PASSES by observing the cascade happen exactly as coded. If a future change
     * makes pipeline failures isolated (for example by making {@code makeJobEndWhenPipelineEnded}
     * configurable or conditional), this test will start failing and must be revisited deliberately
     * instead of silently.
     *
     * <p>The job submitted here has two independent pipelines built from {@link
     * #ONE_PIPELINE_PERMANENTLY_FAILED_TEMPLATE_FILE_NAME}: table_healthy is an ordinary bounded
     * FakeSource -> LocalFile pipeline that is paced (via {@code split.read-interval}) to still be
     * RUNNING for tens of seconds, and table_doomed's Assert sink always throws on its first row,
     * so it permanently fails and exhausts its (deliberately small, explicit) {@code
     * job.retry.times} restore budget within a few seconds. A single embedded node is enough here,
     * unlike the sibling tests in this class: this test targets {@code PhysicalPlan}'s
     * pipeline-level cascade logic, not cross-node fault tolerance, and {@code
     * slot-service.dynamic-slot} (see seatunnel.yaml) lets one node run both pipelines concurrently
     * without resource contention.
     */
    @Test
    public void testOneUnrecoverablePipelineForceCancelsHealthySiblingPipeline() throws Exception {
        String testCaseName = "testOneUnrecoverablePipelineForceCancelsHealthySiblingPipeline";
        String testClusterName = "ClusterFaultToleranceTwoPipelineIT_" + testCaseName;
        long testRowNumber = 500;
        int testParallelism = 2;

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(
                            testCaseName,
                            JobMode.BATCH,
                            testRowNumber,
                            testParallelism,
                            ONE_PIPELINE_PERMANENTLY_FAILED_TEMPLATE_FILE_NAME);
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

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            // Resolve the physical plan now, while the job is confirmedly RUNNING, and keep this
            // reference for the post-completion assertions below instead of re-resolving it via
            // CoordinatorService#getJobMaster(jobId) after the job ends. SubPlan#getPipelineState
            // reads a volatile field on the SubPlan instance itself, so holding this reference is
            // race-free; re-resolving after completion would not be. The client observing
            // completion (via the waitForJobComplete RPC round trip below) and the server
            // forgetting the job (CoordinatorService#runningJobMasterMap.remove, in JobMaster#run's
            // finally block, once JobMaster#jobMasterCompleteFuture completes) are both triggered
            // by the same completion event with no ordering guarantee between them, so
            // getJobMaster(jobId) can already return null by the time a caller reacts to job
            // completion.
            PhysicalPlan physicalPlan = getPhysicalPlan(node, jobId);
            Assertions.assertNotNull(
                    physicalPlan, "Physical plan should be reachable while RUNNING");
            List<SubPlan> pipelines = physicalPlan.getPipelineList();
            Assertions.assertEquals(2, pipelines.size(), "Job should have exactly two pipelines");

            CompletableFuture<JobStatus> jobCompleteFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            // job.retry.times=2 and job.retry.interval.seconds=1 in the template bound table_
            // doomed to 3 attempts (1 original + 2 restores) and 2 one-second sleeps between
            // them; each attempt fails instantly on the Assert sink's first row. 120s is a
            // generous bound even on a heavily loaded machine.
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(jobCompleteFuture.isDone());
                                // Documents today's actual, hardcoded outcome: the job ends
                                // FAILED, not a partial success, even though one of its two
                                // pipelines was healthy and would otherwise have finished on its
                                // own.
                                Assertions.assertEquals(JobStatus.FAILED, jobCompleteFuture.get());
                            });

            SubPlan doomedPipeline =
                    pipelines.stream()
                            .filter(p -> p.getPipelineState() == PipelineStatus.FAILED)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Expected exactly one pipeline (table_doomed) to end FAILED"));
            SubPlan healthyPipeline =
                    pipelines.stream()
                            .filter(p -> p != doomedPipeline)
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Expected a second pipeline (table_healthy)"));

            // This is the crux of the documented behavior: the healthy pipeline never reached its
            // own natural FINISHED state. PhysicalPlan#stateProcess (case FAILING/CANCELING)
            // force-cancelled it purely as a side effect of the sibling pipeline's failure.
            // SubPlan#cancelPipeline() is a no-op on an already-terminal pipeline, so seeing
            // CANCELED here (rather than FINISHED) also proves it had not already finished on its
            // own before the cascade reached it.
            Assertions.assertEquals(
                    PipelineStatus.CANCELED,
                    healthyPipeline.getPipelineState(),
                    "Healthy pipeline should have been force-cancelled by the doomed sibling's cascade, not left to finish");

            // Corroborate from the data plane: the healthy pipeline's transactional LocalFile
            // sink only commits when the pipeline reaches its own natural completion. No periodic
            // checkpoint fires within this test's runtime (the cluster default checkpoint.interval
            // is 300s, see seatunnel.yaml), so a force-cancel before that point commits nothing,
            // and the committed line count must stay below the full expected total.
            long healthyPipelineLineCount =
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft());
            Assertions.assertTrue(
                    healthyPipelineLineCount < testRowNumber * testParallelism,
                    "Healthy pipeline output should be incomplete because it was cancelled before "
                            + "it could finish, but found "
                            + healthyPipelineLineCount
                            + " lines");
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
     * Reads the current job master's physical plan from the SeaTunnel server embedded in the given
     * Hazelcast instance, so a test can inspect per-pipeline state directly rather than only the
     * aggregate job-level status.
     */
    private static PhysicalPlan getPhysicalPlan(HazelcastInstanceImpl node, long jobId) {
        SeaTunnelServer server = node.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        return jobMaster == null ? null : jobMaster.getPhysicalPlan();
    }
}
