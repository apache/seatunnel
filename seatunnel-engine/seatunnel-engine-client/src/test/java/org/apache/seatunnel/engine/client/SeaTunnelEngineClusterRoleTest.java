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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
@Slf4j
public class SeaTunnelEngineClusterRoleTest {

    @SneakyThrows
    @Test
    public void testClusterWillDownWhenNoMasterNode() {
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        HazelcastInstanceImpl masterNode = null;

        String testClusterName = "Test_testClusterWillDownWhenNoMasterNode";

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            // master node must start first in ci
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMasterNode.getCluster().getMembers().size()));
            // start two worker nodes
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);

            HazelcastInstanceImpl finalWorkerNode = workerNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorkerNode.getCluster().getMembers().size()));

            masterNode.shutdown();
            HazelcastInstanceImpl finalWorkerNode1 = workerNode2;
            Awaitility.await()
                    .atMost(20000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            true,
                                            !finalWorkerNode.node.isRunning()
                                                    && !finalWorkerNode1.node.isRunning()
                                                    && !finalMasterNode.node.isRunning()));

        } finally {

            if (workerNode1 != null) {
                workerNode1.shutdown();
            }

            if (workerNode2 != null) {
                workerNode2.shutdown();
            }

            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    @SneakyThrows
    @Test
    public void canNotSubmitJobWhenHaveNoWorkerNode() {
        HazelcastInstanceImpl masterNode = null;
        String testClusterName = "Test_canNotSubmitJobWhenHaveNoWorkerNode";
        SeaTunnelClient seaTunnelClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        // submit job
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Test_canNotSubmitJobWhenHaveNoWorkerNode");

        try {
            // master node must start first in ci
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMasterNode.getCluster().getMembers().size()));

            // new seatunnel client and submit job
            seaTunnelClient = createSeaTunnelClient(testClusterName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            await().atMost(60000, TimeUnit.MILLISECONDS)
                    .until(
                            () -> {
                                try {
                                    PassiveCompletableFuture<JobResult>
                                            jobResultPassiveCompletableFuture =
                                                    clientJobProxy.doWaitForJobComplete();
                                    String mes = jobResultPassiveCompletableFuture.get().getError();
                                    return mes.contains("NoEnoughResourceException");
                                } catch (Exception e) {
                                    return false;
                                }
                            });

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (seaTunnelClient != null) {
                seaTunnelClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    @SneakyThrows
    @Test
    public void enterPendingWhenResourcesNotEnough() {
        HazelcastInstanceImpl masterNode = null;
        String testClusterName = "Test_enterPendingWhenResourcesNotEnough";
        SeaTunnelClient seaTunnelClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        // set job pending
        EngineConfig engineConfig = seaTunnelConfig.getEngineConfig();
        engineConfig.setScheduleStrategy(ScheduleStrategy.WAIT);
        engineConfig.getSlotServiceConfig().setDynamicSlot(false);
        engineConfig.getSlotServiceConfig().setSlotNum(3);
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        // submit job
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("Test_enterPendingWhenResourcesNotEnough");

        try {
            // master node must start first in ci
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);

            HazelcastInstanceImpl finalMasterNode = masterNode;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMasterNode.getCluster().getMembers().size()));

            // new seatunnel client and submit job
            seaTunnelClient = createSeaTunnelClient(testClusterName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            clientJobProxy.getJobStatus(), JobStatus.PENDING));
            // start two worker nodes
            SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);

            // There are already resources available, wait for job enter running or complete
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.FINISHED, clientJobProxy.getJobStatus()));
            System.out.println("1234:" + clientJobProxy.getJobStatus());
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (seaTunnelClient != null) {
                seaTunnelClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    @SneakyThrows
    @Test
    public void pendingJobCancel() {
        HazelcastInstanceImpl masterNode = null;
        String clusterAndJobName = "Test_pendingJobCancel";
        SeaTunnelClient seaTunnelClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        // set job pending
        EngineConfig engineConfig = seaTunnelConfig.getEngineConfig();
        engineConfig.setScheduleStrategy(ScheduleStrategy.WAIT);

        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(clusterAndJobName));

        // submit job
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("/client_test.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(clusterAndJobName);

        try {
            // master node must start first in ci
            masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);

            // new seatunnel client and submit job
            seaTunnelClient = createSeaTunnelClient(clusterAndJobName);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seaTunnelClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            clientJobProxy.getJobStatus(), JobStatus.PENDING));

            // Cancel the job in the pending state
            seaTunnelClient.getJobClient().cancelJob(clientJobProxy.getJobId());
            Awaitility.await()
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertNotEquals(
                                            clientJobProxy.getJobStatus(), JobStatus.CANCELED));

        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (seaTunnelClient != null) {
                seaTunnelClient.close();
            }
            if (masterNode != null) {
                masterNode.shutdown();
            }
        }
    }

    private SeaTunnelClient createSeaTunnelClient(String clusterName) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(TestUtils.getClusterName(clusterName));
        return new SeaTunnelClient(clientConfig);
    }
}
