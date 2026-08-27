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

package org.apache.seatunnel.engine.e2e.allocatestrategy;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/** Test task allocation strategy */
@Slf4j
public class SystemLoadAllocateStrategyIT {

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    public static final String DYNAMIC_JOB_MODE = "dynamic_job_mode";

    public static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    public static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    /**
     * Test steps: <br>
     * 1. Start two tasks and occupy 5 slots on two nodes respectively <br>
     * 2. Start 3 parallel tasks ,occupy 4 slots<br>
     * 3. Expected result: Each node of the two nodes occupies 7 slots respectively <br>
     */
    @Test
    public void testSystemLoadStrategy() throws Exception {
        String testCaseName = "testSystemLoadStrategy";
        String testClusterName = "TestSystemLoadStrategy";
        long testRowNumber = 100;
        int testParallelism = 4;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        SlotServiceConfig slotServiceConfig =
                seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setSlotNum(10);
        slotServiceConfig.setDynamicSlot(false);
        // enable system load strategy
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SYSTEM_LOAD);

        // Set the node tag and submit a task that occupies 5 slots to each of the two nodes
        MemberAttributeConfig node1Tags = new MemberAttributeConfig();
        node1Tags.setAttribute("strategy", "system_load1");
        seaTunnelConfig.getHazelcastConfig().setMemberAttributeConfig(node1Tags);
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPort(5808);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            MemberAttributeConfig node2Tags = new MemberAttributeConfig();
            node2Tags.setAttribute("strategy", "system_load2");
            seaTunnelConfig.getHazelcastConfig().setMemberAttributeConfig(node2Tags);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            // waiting all node added to cluster
            HazelcastInstanceImpl finalNode = node1;
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            2, finalNode.getCluster().getMembers().size()));

            // Waiting for worker heartbeat registration
            Thread.sleep(10000);
            Common.setDeployMode(DeployMode.CLIENT);
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(testClusterName));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobProxy clientJobProxyStepOne1 =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName,
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            testParallelism,
                                            "allocate-strategy/allocate_strategy_tag1_with_system_load.conf"),
                                    jobConfig,
                                    seaTunnelConfig)
                            .execute();

            ClientJobProxy clientJobProxyStepOne2 =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName,
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            testParallelism,
                                            "allocate-strategy/allocate_strategy_tag2_with_system_load.conf"),
                                    jobConfig,
                                    seaTunnelConfig)
                            .execute();

            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address node2Address = node2.node.address;
            Address node1Address = node1.node.address;

            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

            Awaitility.await()
                    .atMost(600, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                ConcurrentMap<Address, WorkerProfile> registerWorker =
                                        resourceManager.getRegisterWorker();
                                int node1AssignedSlotsNum =
                                        registerWorker.get(node1Address).getAssignedSlots().length;
                                int node2AssignedSlotsNum =
                                        registerWorker.get(node2Address).getAssignedSlots().length;
                                Assertions.assertTrue(node1AssignedSlotsNum == 5);
                                Assertions.assertTrue(node2AssignedSlotsNum == 5);
                                Assertions.assertEquals(
                                        10, node1AssignedSlotsNum + node2AssignedSlotsNum);
                            });
            log.info("The first step is completed");

            // Waiting to collect the node's System Load information
            Thread.sleep(60000);

            // Start a task that occupies 4 slots
            jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);

            log.info("Start a task that occupies 4 slots");
            ClientJobProxy clientJobProxyStepTwo =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName,
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            3,
                                            "allocate-strategy/allocate_strategy_no_tag_with_system_load.conf"),
                                    jobConfig,
                                    seaTunnelConfig)
                            .execute();

            // Because e2e runs on the same node, the CPU and memory are almost the same, but we
            // introduced a balance factor (step 5). So the final result should also be balanced.
            // That is, the two nodes occupy 7 slots respectively.
            Awaitility.await()
                    .atMost(600, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                ConcurrentMap<Address, WorkerProfile> registerWorker =
                                        resourceManager.getRegisterWorker();
                                int node1AssignedSlotsNum =
                                        registerWorker.get(node1Address).getAssignedSlots().length;
                                int node2AssignedSlotsNum =
                                        registerWorker.get(node2Address).getAssignedSlots().length;
                                Assertions.assertEquals(7, node1AssignedSlotsNum);
                                Assertions.assertEquals(7, node2AssignedSlotsNum);
                            });
            log.info("The second step is completed");

            clientJobProxyStepOne1.cancelJob();
            clientJobProxyStepOne2.cancelJob();
            clientJobProxyStepTwo.cancelJob();
            clientJobProxyStepOne1.waitForJobCompleteV2();
            clientJobProxyStepOne2.waitForJobCompleteV2();
            clientJobProxyStepTwo.waitForJobCompleteV2();

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
    private String createTestResources(
            @NonNull String testCaseName,
            @NonNull JobMode jobMode,
            long rowNumber,
            int parallelism,
            String templateFile)
            throws IOException {
        checkArgument(rowNumber > 0, "rowNumber must greater than 0");
        checkArgument(parallelism > 0, "parallelism must greater than 0");
        Map<String, String> valueMap = new HashMap<>();
        valueMap.put(DYNAMIC_TEST_CASE_NAME, testCaseName);
        valueMap.put(DYNAMIC_JOB_MODE, jobMode.toString());
        valueMap.put(DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM, String.valueOf(rowNumber));
        valueMap.put(DYNAMIC_TEST_PARALLELISM, String.valueOf(parallelism));

        String targetConfigFilePath =
                File.separator
                        + "tmp"
                        + File.separator
                        + "test_conf"
                        + File.separator
                        + testCaseName
                        + ".conf";

        TestUtils.createTestConfigFileFromTemplate(templateFile, valueMap, targetConfigFilePath);

        return targetConfigFilePath;
    }
}
