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
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SplitClusterPendingJobLifecycleFailoverIT {
    private static final String JOB_CONFIG_FILE = "pending_jobs_streaming_lifecycle.conf";

    @Test
    public void testPendingJobLifecycleInMasterFailover() {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_testPendingJobLifecycleInMasterFailover";
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy pendingJob = null;

        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig masterNode2Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode2Config = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNode1Config);
        configurePendingLifecycleTest(masterNode2Config);
        configurePendingLifecycleTest(workerNode1Config);
        configurePendingLifecycleTest(workerNode2Config);

        try {
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode2Config);
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);

            HazelcastInstanceImpl finalMasterNode = masterNode1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            holderJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "pending_job_lifecycle_holder",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode1, masterNode2);
            assertPendingQueueState(activeMaster, null, 0);

            pendingJob =
                    submitJob(
                            engineClient,
                            masterNode1Config,
                            "pending_job_lifecycle_pending",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            final ClientJobProxy finalPendingJob = pendingJob;
            final long pendingJobId = finalPendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);
            assertPendingQueueState(activeMaster, pendingJobId, 1);
            activeMaster.shutdown();
            HazelcastInstanceImpl standbyMaster =
                    activeMaster == masterNode1 ? masterNode2 : masterNode1;

            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        standbyMaster.getLifecycleService().isRunning());
                                Assertions.assertEquals(
                                        2, standbyMaster.getCluster().getMembers().size());
                            });
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            isCoordinatorActive(standbyMaster),
                                            "Standby master should become active after failover"));
            ClientJobProxy pendingJobAfterFailover =
                    engineClient.createJobClient().getJobProxy(pendingJobId);
            assertPendingQueueContainsJob(standbyMaster, pendingJobId, 1);

            Awaitility.await()
                    .during(10, TimeUnit.SECONDS)
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                assertPendingQueueContainsJob(standbyMaster, pendingJobId, 1);
                                Assertions.assertEquals(
                                        JobStatus.PENDING, pendingJobAfterFailover.getJobStatus());
                            });

            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode2Config);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, standbyMaster.getCluster().getMembers().size()));
            assertJobStatusWithTimeout(pendingJobAfterFailover, JobStatus.RUNNING, 180);
            assertPendingQueueNotContainsJob(standbyMaster, pendingJobId);

            pendingJobAfterFailover.cancelJob();
            assertJobStatusWithTimeout(pendingJobAfterFailover, JobStatus.CANCELED, 120);
            engineClient.createJobClient().getJobProxy(holderJob.getJobId()).cancelJob();
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
    public void testPendingJobScheduledAfterRunningJobCanceled() {
        String testClusterName =
                "SplitClusterPendingJobLifecycleFailoverIT_testPendingJobScheduledAfterRunningJobCanceled";
        HazelcastInstanceImpl masterNode = null;
        HazelcastInstanceImpl workerNode = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy holderJob = null;
        ClientJobProxy pendingJob = null;

        SeaTunnelConfig masterNodeConfig = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNodeConfig = getSeaTunnelConfig(testClusterName);
        configurePendingLifecycleTest(masterNodeConfig);
        configurePendingLifecycleTest(workerNodeConfig);

        try {
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNodeConfig);
            workerNode = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNodeConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalMasterNode.getCluster().getMembers().size()));

            Common.setDeployMode(DeployMode.CLUSTER);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);

            holderJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "pending_job_lifecycle_holder_cancel",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            assertJobStatusWithTimeout(holderJob, JobStatus.RUNNING, 120);

            HazelcastInstanceImpl activeMaster = waitAndFindActiveMaster(masterNode, null);
            assertPendingQueueState(activeMaster, null, 0);

            pendingJob =
                    submitJob(
                            engineClient,
                            masterNodeConfig,
                            "pending_job_lifecycle_pending_after_cancel",
                            TestUtils.getResource(JOB_CONFIG_FILE));
            long pendingJobId = pendingJob.getJobId();
            assertJobStatusWithTimeout(pendingJob, JobStatus.PENDING, 120);
            assertPendingQueueState(activeMaster, pendingJobId, 1);

            holderJob.cancelJob();
            assertJobStatusWithTimeout(holderJob, JobStatus.CANCELED, 120);

            assertJobStatusWithTimeout(pendingJob, JobStatus.RUNNING, 180);
            assertPendingQueueNotContainsJob(activeMaster, pendingJobId);

            pendingJob.cancelJob();
            assertJobStatusWithTimeout(pendingJob, JobStatus.CANCELED, 120);
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
            if (workerNode != null) {
                workerNode.shutdown();
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

    private static void configurePendingLifecycleTest(SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        seaTunnelConfig.getEngineConfig().getSlotServiceConfig().setSlotNum(4);
        seaTunnelConfig.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
    }

    private static ClientJobProxy submitJob(
            SeaTunnelClient engineClient,
            SeaTunnelConfig seaTunnelConfig,
            String jobName,
            String jobConfigFile) {
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(jobConfigFile, jobConfig, seaTunnelConfig);
        try {
            return jobExecutionEnv.execute();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to submit job " + jobName, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted when submitting job " + jobName, e);
        }
    }

    private static void assertJobStatusWithTimeout(
            ClientJobProxy clientJobProxy, JobStatus expectedStatus, long timeoutSeconds) {
        Awaitility.await()
                .atMost(timeoutSeconds, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedStatus, clientJobProxy.getJobStatus()));
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

    private static void assertPendingQueueState(
            HazelcastInstanceImpl masterNode, Long expectedJobIdInQueue, int expectedPendingCount) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService().isCoordinatorActive(),
                                    "Coordinator should be active when asserting pending queue");
                            Assertions.assertEquals(
                                    expectedPendingCount,
                                    server.getCoordinatorService().getPendingJobCount());
                            if (expectedJobIdInQueue != null) {
                                Assertions.assertTrue(
                                        server.getCoordinatorService()
                                                .getPendingJobQueue()
                                                .contains(expectedJobIdInQueue),
                                        "Expected pending job should remain in pending queue");
                            }
                        });
    }

    private static void assertPendingQueueContainsJob(
            HazelcastInstanceImpl masterNode,
            long expectedJobIdInQueue,
            int minExpectedPendingCount) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    server.getCoordinatorService().isCoordinatorActive(),
                                    "Coordinator should be active when asserting pending queue");
                            Assertions.assertTrue(
                                    server.getCoordinatorService().getPendingJobCount()
                                            >= minExpectedPendingCount,
                                    "Pending queue count should be at least "
                                            + minExpectedPendingCount);
                            Assertions.assertTrue(
                                    server.getCoordinatorService()
                                            .getPendingJobQueue()
                                            .contains(expectedJobIdInQueue),
                                    "Expected pending job should remain in pending queue");
                        });
    }

    private static void assertPendingQueueNotContainsJob(
            HazelcastInstanceImpl masterNode, long expectedRemovedJobId) {
        SeaTunnelServer server =
                masterNode.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        server.getCoordinatorService()
                                                .getPendingJobQueue()
                                                .contains(expectedRemovedJobId),
                                        "Pending queue should not contain job "
                                                + expectedRemovedJobId));
    }
}
