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

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class CheckpointCoordinatorFailoverIT {

    private static final String BATCH_TEMPLATE_CONF =
            "batch_fake_to_localfile_master_failover_template.conf";

    private static final String STREAM_TEMPLATE_CONF =
            "stream_fake_to_localfile_master_failover_template.conf";

    private static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";
    private static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    @Test
    public void testBatchJobCompletesAfterMasterFailover() throws Exception {
        String testCaseName = "testBatchJobCompletesAfterMasterFailover";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testBatchJobCompletesAfterMasterFailover";
        long rowNum = 200;
        int parallelism = 2;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));

        SeaTunnelConfig config2 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config2.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            // Both nodes are master-eligible; Hazelcast picks the oldest as master.
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
                    createTestResources(testCaseName, parallelism, BATCH_TEMPLATE_CONF);
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

            // Step 1: wait until all rows are written (sink layer done).
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            rowNum * parallelism,
                                            FileUtils.getFileLineNumberFromDir(
                                                    testResources.getLeft())));

            // Step 2: confirm the fix has written readyToCloseStartingTask to IMap before failover.
            // Sink completion lags source shutdown: without this gate the failover races IMap
            // persistence.
            // Pipeline id for a single-pipeline job is always 1.
            int pipelineId = 1;
            String readyToCloseImapKey =
                    "checkpoint_state_" + jobId + "_" + pipelineId + "_ready_to_close";
            IMap<Object, Object> runningJobStateIMap =
                    masterNode2.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Object stored = runningJobStateIMap.get(readyToCloseImapKey);
                                Assertions.assertNotNull(
                                        stored,
                                        "readyToCloseStartingTask IMap entry must be persisted"
                                                + " before triggering master failover");
                            });

            log.info(
                    "All {} rows written and IMap entry '{}' confirmed for job {}. "
                            + "Triggering master failover by shutting down masterNode1.",
                    rowNum * parallelism,
                    readyToCloseImapKey,
                    jobId);

            // Register the wait-future BEFORE killing the master so it survives reconnect.
            CompletableFuture<JobStatus> waitFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            masterNode1.shutdown();
            masterNode1 = null;

            // masterNode2 is now the only node and becomes master automatically.
            // With the fix: it restores readyToCloseStartingTask from IMap and immediately
            // triggers COMPLETED_POINT_TYPE.
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(3, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, clientJobProxy.getJobStatus());
                                Assertions.assertTrue(waitFuture.isDone());
                                Assertions.assertEquals(JobStatus.FINISHED, waitFuture.get());
                            });

            Assertions.assertEquals(
                    rowNum * parallelism,
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft()));
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
    public void testBatchJobStuckWhenReadyToCloseImapDeleted() throws Exception {
        String testCaseName = "testBatchJobStuckWhenReadyToCloseImapDeleted";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testBatchJobStuckWhenReadyToCloseImapDeleted";
        long rowNum = 200;
        int parallelism = 2;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));

        SeaTunnelConfig config2 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config2.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));

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
                    createTestResources(testCaseName, parallelism, BATCH_TEMPLATE_CONF);
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

            // Wait until all rows are written so the IMap entry has been populated.
            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            rowNum * parallelism,
                                            FileUtils.getFileLineNumberFromDir(
                                                    testResources.getLeft())));

            // Derive the IMap key using the same format as
            // CheckpointCoordinator.readyToCloseImapKey:
            // "checkpoint_state_" + jobId + "_" + pipelineId + "_ready_to_close"
            // Pipeline id for a single-pipeline job is always 1.
            int pipelineId = 1;
            String readyToCloseImapKey =
                    "checkpoint_state_" + jobId + "_" + pipelineId + "_ready_to_close";

            // Get the shared IMap through the surviving node (masterNode2).
            IMap<Object, Object> runningJobStateIMap =
                    masterNode2.getMap(Constant.IMAP_RUNNING_JOB_STATE);

            // Confirm the entry exists before deleting it: sink completion lags source shutdown,
            // so poll until readyToCloseStartingTask is durably written.
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                Object stored = runningJobStateIMap.get(readyToCloseImapKey);
                                Assertions.assertNotNull(
                                        stored,
                                        "readyToCloseStartingTask IMap entry must be persisted"
                                                + " before we can delete it");
                                Assertions.assertFalse(
                                        ((Set<?>) stored).isEmpty(),
                                        "readyToCloseStartingTask IMap entry must not be empty");
                            });

            Set<?> persisted = (Set<?>) runningJobStateIMap.get(readyToCloseImapKey);
            log.info(
                    "Confirmed IMap entry '{}' exists with {} entries. "
                            + "Now deleting it to reproduce the original bug.",
                    readyToCloseImapKey,
                    persisted.size());

            // Forcibly delete the IMap entry – simulates the old behavior where it was never
            // persisted, so the new coordinator will start with an empty set.
            runningJobStateIMap.remove(readyToCloseImapKey);
            Assertions.assertNull(
                    runningJobStateIMap.get(readyToCloseImapKey),
                    "IMap entry must be gone before failover");

            log.info(
                    "IMap entry deleted. Triggering master failover by shutting down masterNode1.");

            CompletableFuture<JobStatus> waitFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            masterNode1.shutdown();
            masterNode1 = null;

            // Without the persisted state the new coordinator cannot trigger COMPLETED_POINT_TYPE.
            // The job must remain stuck (RUNNING) for at least 60 s to prove the deadlock.
            boolean finishedUnexpectedly;
            try {
                waitFuture.get(60, TimeUnit.SECONDS);
                finishedUnexpectedly = true;
            } catch (java.util.concurrent.TimeoutException e) {
                finishedUnexpectedly = false;
            } catch (java.util.concurrent.ExecutionException e) {
                // job failed rather than finished — still not "stuck", but counts as not finishing
                finishedUnexpectedly = false;
            }
            Assertions.assertFalse(
                    finishedUnexpectedly,
                    "Job should be stuck after IMap deletion + master failover (bug reproduced)");
            log.info("Bug reproduced: job is stuck as expected after IMap deletion.");

            // Verify the root cause: checkpoints are growing indefinitely.
            // Source tasks in PREPARE_CLOSE still ACK checkpoint barriers, so the new coordinator
            // keeps completing regular CHECKPOINT_TYPE checkpoints while COMPLETED_POINT_TYPE is
            // never triggered — checkpoint ID must strictly increase over time.
            String ckIdKey = IMapCheckpointIDCounter.convertLongIntToBase64(jobId, pipelineId);
            IMap<String, Long> ckIdMap = masterNode2.getMap(Constant.IMAP_CHECKPOINT_ID);
            long ckIdBefore = ckIdMap.get(ckIdKey);
            // Wait for at least 2 checkpoint intervals (conf: checkpoint.interval = 2000 ms).
            Thread.sleep(6000);
            long ckIdAfter = ckIdMap.get(ckIdKey);
            Assertions.assertTrue(
                    ckIdAfter > ckIdBefore,
                    String.format(
                            "Checkpoint ID must grow while job is stuck"
                                    + " (before=%d, after=%d) — confirms infinite checkpoint loop",
                            ckIdBefore, ckIdAfter));
            log.info(
                    "Confirmed infinite checkpoint loop: ID grew from {} to {} while job remained stuck.",
                    ckIdBefore,
                    ckIdAfter);

            // Tear down gracefully.
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

    @Test
    public void testStreamJobCompletesAfterMasterFailover() throws Exception {
        String testCaseName = "testStreamJobCompletesAfterMasterFailover";
        String testClusterName =
                "CheckpointCoordinatorFailoverIT_testStreamJobCompletesAfterMasterFailover";
        long rowNum = 500;
        int parallelism = 2;

        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig config1 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config1.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));

        SeaTunnelConfig config2 = ConfigProvider.locateAndGetSeaTunnelConfig();
        config2.getHazelcastConfig().setClusterName(TestUtils.getClusterName(testClusterName));

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
                    createTestResources(testCaseName, parallelism, STREAM_TEMPLATE_CONF);
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

            // Step 1: wait until some rows are written so there is data in flight.
            long triggerThreshold = rowNum * parallelism / 4;
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

            // Step 2: confirm at least one checkpoint is committed to IMap before failover.
            // Row writes lag checkpoint commits; without this gate the new coordinator
            // starts from scratch instead of restoring, producing duplicate rows.
            int pipelineId = 1;
            String checkpointStateImapKey = "checkpoint_state_" + jobId + "_" + pipelineId;
            IMap<Object, Object> runningJobStateIMap =
                    masterNode2.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(500, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertNotNull(
                                            runningJobStateIMap.get(checkpointStateImapKey),
                                            "At least one checkpoint must be committed to IMap"
                                                    + " before triggering master failover"));

            log.info(
                    "{} rows written and checkpoint committed to IMap for job {}. "
                            + "Triggering master failover by shutting down masterNode1.",
                    triggerThreshold,
                    jobId);

            CompletableFuture<JobStatus> waitFuture =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            masterNode1.shutdown();
            masterNode1 = null;

            // masterNode2 takes over. The job must restore from the latest checkpoint and
            // complete writing all remaining rows.
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
                    rowNum * parallelism,
                    FileUtils.getFileLineNumberFromDir(testResources.getLeft()));
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
            @NonNull String testCaseName, int parallelism, String templateConf) throws IOException {
        checkArgument(parallelism > 0, "parallelism must be greater than 0");

        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);
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
        TestUtils.createTestConfigFileFromTemplate(templateConf, valueMap, targetConfigFilePath);

        return new ImmutablePair<>(targetDir, targetConfigFilePath);
    }
}
