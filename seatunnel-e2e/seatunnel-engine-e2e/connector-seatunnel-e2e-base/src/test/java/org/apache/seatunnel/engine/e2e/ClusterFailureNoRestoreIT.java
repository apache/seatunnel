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
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class ClusterFailureNoRestoreIT {

    private static final String TEST_TEMPLATE_FILE_NAME =
            "cluster_batch_fake_to_localfile_no_restore_template.conf";

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    private static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    private static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    @Test
    public void testBatchJobWithoutCheckpointAndRetryConvergesAfterWorkerShutdown()
            throws Exception {
        String testCaseName = "testBatchJobWithoutCheckpointAndRetryConvergesAfterWorkerShutdown";
        String testClusterName = "ClusterFailureNoRestoreIT_batch_no_restore";
        long testRowNumber = 10000;
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

            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLIENT);
            ImmutablePair<String, String> testResources =
                    createTestResources(testCaseName, testRowNumber, testParallelism);
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
                    .atMost(60, TimeUnit.SECONDS)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Long lineNumberFromDir =
                                        FileUtils.getFileLineNumberFromDir(testResources.getLeft());
                                JobStatus status = clientJobProxy.getJobStatus();
                                log.warn(
                                        "\n====================={}=====================\n",
                                        lineNumberFromDir);
                                Assertions.assertTrue(lineNumberFromDir > 1);
                                Assertions.assertFalse(
                                        status.isEndState(),
                                        "job finished before worker shutdown: " + status);
                            });

            CompletableFuture<JobResult> waitForCompleteFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobCompleteV2);

            log.info(
                    "=====================shutdown node2 for no-restore batch test=====================");
            node2.shutdown();

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(2, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(waitForCompleteFuture.isDone());
                                JobResult jobResult = waitForCompleteFuture.get();
                                JobStatus jobStatus = jobResult.getStatus();
                                Assertions.assertTrue(
                                        EnumSet.of(
                                                        JobStatus.FINISHED,
                                                        JobStatus.FAILED,
                                                        JobStatus.CANCELED,
                                                        JobStatus.UNKNOWABLE)
                                                .contains(jobStatus));
                                if (JobStatus.FINISHED.equals(jobStatus)) {
                                    Assertions.assertEquals(
                                            testRowNumber * testParallelism,
                                            FileUtils.getFileLineNumberFromDir(
                                                    testResources.getLeft()));
                                }
                            });
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

    private ImmutablePair<String, String> createTestResources(
            @NonNull String testCaseName, long rowNumber, int parallelism) throws IOException {
        checkArgument(rowNumber > 0, "rowNumber must greater than 0");
        checkArgument(parallelism > 0, "parallelism must greater than 0");
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);
        valueMap.put(DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM, String.valueOf(rowNumber));
        valueMap.put(DYNAMIC_TEST_PARALLELISM, String.valueOf(parallelism));

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
        TestUtils.createTestConfigFileFromTemplate(
                TEST_TEMPLATE_FILE_NAME, valueMap, targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }
}
