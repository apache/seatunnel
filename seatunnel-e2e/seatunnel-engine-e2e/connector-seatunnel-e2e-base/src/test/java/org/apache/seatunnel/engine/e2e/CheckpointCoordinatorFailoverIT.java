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
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.IMapCheckpointIDCounter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class CheckpointCoordinatorFailoverIT {

    private static final String BATCH_TEMPLATE_CONF =
            "batch_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_TEMPLATE_CONF =
            "stream_fake_to_localfile_master_failover_template.conf";

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    /** Must match the parallelism value set in the conf templates (env.parallelism). */
    private static final int SOURCE_PARALLELISM = 5;

    @Test
    public void testBatchJobCompletesAfterMasterFailover() throws Exception {
        String testCaseName = "testBatchJobCompletesAfterMasterFailover";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testBatchJobCompletesAfterMasterFailover";
        // Per-source row.num must match batch_fake_to_localfile_master_failover_template.conf.
        long[] rowNumPerSource = {100, 150, 200, 250, 300};
        int sourceCount = rowNumPerSource.length;
        long expectedTotalRows = 0;
        for (long rows : rowNumPerSource) {
            expectedTotalRows += rows * SOURCE_PARALLELISM;
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

            // Wait until a strict subset of starting enumerator tasks has reported
            // readyToClose: directly read the IMap entry and require 0 < size < total.
            long jobId = clientJobProxy.getJobId();
            int pipelineId = 1;
            String readyToCloseImapKey =
                    "checkpoint_state_" + jobId + "_" + pipelineId + "_ready_to_close";
            IMap<Object, Object> runningJobStateImap =
                    masterNode2.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            AtomicInteger observedSubsetSize = new AtomicInteger(-1);
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus());
                                Object stored = runningJobStateImap.get(readyToCloseImapKey);
                                if (stored instanceof Set) {
                                    int size = ((Set<?>) stored).size();
                                    if (size > 0 && size < sourceCount) {
                                        observedSubsetSize.compareAndSet(-1, size);
                                    }
                                }
                                Assertions.assertTrue(
                                        observedSubsetSize.get() > 0,
                                        "Waiting for a partial readyToCloseStartingTask subset"
                                                + " (1.."
                                                + (sourceCount - 1)
                                                + ")");
                            });

            log.info(
                    "Observed readyToCloseStartingTask subset of size {}/{} for job {}. "
                            + "Triggering master failover by shutting down masterNode1.",
                    observedSubsetSize.get(),
                    sourceCount,
                    jobId);

            CompletableFuture<JobStatus> waitFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            masterNode1.shutdown();
            masterNode1 = null;

            Awaitility.await()
                    .atMost(5, TimeUnit.MINUTES)
                    .pollInterval(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(waitFuture.isDone());
                                Assertions.assertEquals(JobStatus.FINISHED, waitFuture.get());
                            });

            Assertions.assertEquals(
                    expectedTotalRows, FileUtils.getFileLineNumberFromDir(testResources.getLeft()));
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
            int pipelineId = 1;

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

            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            // Verify checkpoint id strictly grows on the new master.
            String ckIdKey = IMapCheckpointIDCounter.convertLongIntToBase64(jobId, pipelineId);
            IMap<String, Long> ckIdMap = masterNode2.getMap(Constant.IMAP_CHECKPOINT_ID);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertNotNull(ckIdMap.get(ckIdKey)));
            long ckIdBefore = ckIdMap.get(ckIdKey);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            ckIdMap.get(ckIdKey) > ckIdBefore,
                                            String.format(
                                                    "Checkpoint id should grow after failover"
                                                            + " (before=%d, current=%d)",
                                                    ckIdBefore, ckIdMap.get(ckIdKey))));
            long ckIdAfter = ckIdMap.get(ckIdKey);
            Assertions.assertTrue(
                    ckIdAfter > ckIdBefore,
                    String.format(
                            "Checkpoint id must continue to grow on the new master"
                                    + " (before=%d, after=%d)",
                            ckIdBefore, ckIdAfter));

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
}
