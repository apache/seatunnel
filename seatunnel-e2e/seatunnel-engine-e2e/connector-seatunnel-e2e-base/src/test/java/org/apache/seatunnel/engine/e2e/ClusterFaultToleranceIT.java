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
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Cluster fault tolerance test. Test the job recovery capability and data consistency assurance capability in case of cluster node failure
 */
public class ClusterFaultToleranceIT {

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    @Test
    public void testBatchJobRunOkIn3Node() throws ExecutionException, InterruptedException {
        String testCaseName = "testBatchJobRunOkIn3Node";
        String testClusterName = "ClusterFaultToleranceIT_testBatchJobRunOkIn3Node";
        HazelcastInstanceImpl node1 =
            SeaTunnelServerStarter.createHazelcastInstance(
                TestUtils.getClusterName(testClusterName));

        HazelcastInstanceImpl node2 =
            SeaTunnelServerStarter.createHazelcastInstance(
                TestUtils.getClusterName(testClusterName));

        HazelcastInstanceImpl node3 =
            SeaTunnelServerStarter.createHazelcastInstance(
                TestUtils.getClusterName(testClusterName));

        // waiting all node added to cluster
        Awaitility.await().atMost(10000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertEquals(3, node1.getCluster().getMembers().size()));

        // TODO Need FakeSource support parallel first
        Common.setDeployMode(DeployMode.CLIENT);
        ImmutablePair<String, String> testResources = createTestResources(testCaseName);
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(testCaseName);

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(
            TestUtils.getClusterName(testClusterName));
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv =
            engineClient.createExecutionContext(testResources.getRight(), jobConfig);
        ClientJobProxy clientJobProxy = jobExecutionEnv.execute();

        CompletableFuture<JobStatus> objectCompletableFuture = CompletableFuture.supplyAsync(() -> {
            return clientJobProxy.waitForJobComplete();
        });

        Awaitility.await().atMost(20000, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> Assertions.assertTrue(
                objectCompletableFuture.isDone() && JobStatus.FINISHED.equals(objectCompletableFuture.get())));

        Long fileLineNumberFromDir = FileUtils.getFileLineNumberFromDir(testResources.getLeft());
        Assertions.assertEquals(100, fileLineNumberFromDir);
    }

    /**
     * Create the test job config file basic on cluster_batch_fake_to_localfile_template.conf
     * It will delete the test sink target path before return the final job config file path
     *
     * @param testCaseName testCaseName
     * @return
     */
    private ImmutablePair<String, String> createTestResources(@NonNull String testCaseName) {
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);

        String targetDir = "/tmp/hive/warehouse/" + testCaseName;
        targetDir = targetDir.replaceAll("/", File.separator);

        // clear target dir before test
        FileUtils.deleteFile(targetDir);

        String targetConfigFilePath =
            File.separator + "tmp" + File.separator + "test_conf" + File.separator + testCaseName +
                ".conf";
        TestUtils.createTestConfigFileFromTemplate("cluster_batch_fake_to_localfile_template.conf", valueMap,
            targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }
}
