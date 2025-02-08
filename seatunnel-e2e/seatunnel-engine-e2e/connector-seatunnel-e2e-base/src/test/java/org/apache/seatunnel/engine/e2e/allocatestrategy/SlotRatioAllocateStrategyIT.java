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
public class SlotRatioAllocateStrategyIT {

    public static final String DYNAMIC_TEST_CASE_NAME = "dynamic_test_case_name";

    public static final String DYNAMIC_JOB_MODE = "dynamic_job_mode";

    public static final String DYNAMIC_TEST_ROW_NUM_PER_PARALLELISM =
            "dynamic_test_row_num_per_parallelism";

    public static final String DYNAMIC_TEST_PARALLELISM = "dynamic_test_parallelism";

    /**
     * Test steps:<br>
     * 1. Start a task with 4 parallelisms, which actually occupies 5 slots <br>
     * 2. Expected result: one node occupies 2 slots, and one node occupies 3 slots <br>
     * 3. Start a task with 6 parallelisms, which actually occupies 7 slots <br>
     * 4. Including the first task, a total of 12 slots are occupied <br>
     * 5. Expected result: each of the two nodes occupies 6 slots <br>
     */
    @Test
    public void testSlotRatioStrategy() throws Exception {
        String testCaseName = "testSlotRatioStrategy";
        String testClusterName = "TestSlotRatioStrategy";
        long testRowNumber = 100;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPort(5805);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));
        SlotServiceConfig slotServiceConfig =
                seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setSlotNum(10);
        slotServiceConfig.setDynamicSlot(false);
        // enable slot ratio strategy
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SLOT_RATIO);

        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
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
            // Start a task
            ClientJobProxy clientJobProxyStepOne =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName,
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            4,
                                            "allocate-strategy/allocate_strategy_with_slot_ratio.conf"),
                                    jobConfig,
                                    seaTunnelConfig)
                            .execute();

            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address node2Address = node2.node.address;
            Address node1Address = node1.node.address;

            // Get the number of occupied slots through resourceManager
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

            // SLOT_RATION strategy, the task will eventually occupy 5 slots and will be distributed
            // to two nodes, one node occupies 2 slots and the other occupies 3 slots.
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
                                Assertions.assertTrue(
                                        node1AssignedSlotsNum == 2 || node1AssignedSlotsNum == 3);
                                Assertions.assertTrue(
                                        node2AssignedSlotsNum == 2 || node2AssignedSlotsNum == 3);
                                Assertions.assertEquals(
                                        5, node1AssignedSlotsNum + node2AssignedSlotsNum);
                            });

            // Start a task with 6 parallelism, which will occupy 7 slots in total, and the
            // SLOT_RATION strategy will be evenly distributed to two nodes
            ClientJobProxy clientJobProxyStepTwo =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName,
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            6,
                                            "allocate-strategy/allocate_strategy_with_slot_ratio.conf"),
                                    jobConfig,
                                    seaTunnelConfig)
                            .execute();

            // The task will eventually occupy 7 slots. Together with the first task, it will occupy
            // a total of 12 slots. The SLOT_RATION strategy will evenly distribute them to the two
            // nodes.
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
                                Assertions.assertEquals(6, node1AssignedSlotsNum);
                                Assertions.assertEquals(6, node2AssignedSlotsNum);
                            });

            clientJobProxyStepOne.cancelJob();
            clientJobProxyStepTwo.cancelJob();
            clientJobProxyStepOne.waitForJobCompleteV2();
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
