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
import org.apache.seatunnel.engine.client.job.JobClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
                .setClusterName(ContentFormatUtilTest.getClusterName(testClusterName));

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
                .setClusterName(ContentFormatUtilTest.getClusterName(testClusterName));

        // submit job
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = ContentFormatUtilTest.getResource("/client_test.conf");
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
            PassiveCompletableFuture<JobResult> jobResultPassiveCompletableFuture =
                    clientJobProxy.doWaitForJobComplete();
            await().pollDelay(30, TimeUnit.SECONDS)
                    .atMost(60000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                String mes = "";
                                if (jobResultPassiveCompletableFuture.isDone()) {
                                    mes = jobResultPassiveCompletableFuture.get().getError();
                                }
                                Assertions.assertTrue(mes.contains("NoEnoughResourceException"));
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
                .setClusterName(ContentFormatUtilTest.getClusterName(testClusterName));

        // submit job
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = ContentFormatUtilTest.getResource("/client_test.conf");
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
            String status = seaTunnelClient.listJobStatus();
            status.contains("PENDING");

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
        engineConfig.getSlotServiceConfig().setDynamicSlot(false);
        engineConfig.getSlotServiceConfig().setSlotNum(1);

        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(ContentFormatUtilTest.getClusterName(clusterAndJobName));

        // submit job
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = ContentFormatUtilTest.getResource("/client_test.conf");
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
            String status = seaTunnelClient.listJobStatus();
            status.contains("PENDING");

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

    @Test
    public void testStartMasterNodeWithTcpIp() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
        Assertions.assertNotNull(instance);
        Assertions.assertEquals(1, instance.getCluster().getMembers().size());
        instance.shutdown();
    }

    @Test
    public void testStartMasterNodeWithMulticastJoin() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(Config.loadFromString(getMulticastConfig()));
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
        Assertions.assertNotNull(instance);
        Assertions.assertEquals(1, instance.getCluster().getMembers().size());
        instance.shutdown();
    }

    @Test
    public void testCannotOnlyStartWorkerNodeWithTcpIp() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
                });
    }

    @Test
    public void testCannotOnlyStartWorkerNodeWithMulticastJoin() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(Config.loadFromString(getMulticastConfig()));
        Assertions.assertThrows(
                IllegalStateException.class,
                () -> {
                    SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
                });
    }

    @SneakyThrows
    @Test
    public void testWorkerIsFirstMemberThenGetJobDetailStatus() {
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        SeaTunnelClient seatunnelClient = null;
        HazelcastClientInstanceImpl hazelcastClient = null;
        String testClusterName = "Test_testWorkerIsFirstMemberThenGetJobDetailStatus";
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(ContentFormatUtilTest.getClusterName(testClusterName));
        try {
            // master node must start first in ci
            masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
            HazelcastInstanceImpl finalMasterNode1 = masterNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            1, finalMasterNode1.getCluster().getMembers().size()));
            // start two worker nodes
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            // start another master node
            SeaTunnelConfig seaTunnelConfig2 = ConfigProvider.locateAndGetSeaTunnelConfig();
            seaTunnelConfig2
                    .getHazelcastConfig()
                    .setClusterName(ContentFormatUtilTest.getClusterName(testClusterName));
            masterNode2 = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig2);
            HazelcastInstanceImpl finalWorkerNode = workerNode1;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            4, finalWorkerNode.getCluster().getMembers().size()));
            masterNode1.shutdown();
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            3, finalWorkerNode.getCluster().getMembers().size()));
            Set<Member> members = workerNode1.getCluster().getMembers();
            Map<UUID, Member> memberMap =
                    members.stream()
                            .collect(
                                    Collectors.toMap(
                                            Member::getUuid, member -> member, (a, b) -> b));
            // get master member
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(ContentFormatUtilTest.getClusterName(testClusterName));
            hazelcastClient =
                    ((HazelcastClientProxy) HazelcastClient.newHazelcastClient(clientConfig))
                            .client;
            HazelcastClientInstanceImpl finalHazelcastClient = hazelcastClient;
            Awaitility.await()
                    .atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () -> {
                                UUID masterUuid =
                                        finalHazelcastClient
                                                .getClientClusterService()
                                                .getMasterMember()
                                                .getUuid();
                                Assertions.assertTrue(memberMap.get(masterUuid).isLiteMember());
                            });
            // start client job
            Common.setDeployMode(DeployMode.CLIENT);
            String filePath = ContentFormatUtilTest.getResource("/streaming_fake_to_console.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("testGetJobState");
            seatunnelClient = createSeaTunnelClient(testClusterName);
            JobClient jobClient = seatunnelClient.getJobClient();
            ClientJobExecutionEnvironment jobExecutionEnv =
                    seatunnelClient.createExecutionContext(filePath, jobConfig, seaTunnelConfig);
            final ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();
            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            jobClient.getJobDetailStatus(jobId).contains("RUNNING")
                                                    && jobClient
                                                            .listJobStatus(true)
                                                            .contains("RUNNING")));
            jobClient.cancelJob(jobId);
            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "CANCELED", jobClient.getJobStatus(jobId)));
        } finally {
            if (hazelcastClient != null) {
                hazelcastClient.shutdown();
            }
            if (seatunnelClient != null) {
                seatunnelClient.close();
            }
            if (workerNode1 != null) {
                workerNode1.shutdown();
            }
            if (workerNode2 != null) {
                workerNode2.shutdown();
            }
            if (masterNode1 != null) {
                masterNode1.shutdown();
            }
            if (masterNode2 != null) {
                masterNode2.shutdown();
            }
        }
    }

    private String getMulticastConfig() {
        return "hazelcast:\n"
                + "  network:\n"
                + "    join:\n"
                + "      multicast:\n"
                + "        enabled: true\n"
                + "        multicast-group: 224.2.2.3\n"
                + "        multicast-port: 54327\n"
                + "        multicast-time-to-live: 32\n"
                + "        multicast-timeout-seconds: 2\n"
                + "        trusted-interfaces:\n"
                + "          - 192.168.1.1\n";
    }

    private SeaTunnelClient createSeaTunnelClient(String clusterName) {
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(ContentFormatUtilTest.getClusterName(clusterName));
        return new SeaTunnelClient(clientConfig);
    }
}
