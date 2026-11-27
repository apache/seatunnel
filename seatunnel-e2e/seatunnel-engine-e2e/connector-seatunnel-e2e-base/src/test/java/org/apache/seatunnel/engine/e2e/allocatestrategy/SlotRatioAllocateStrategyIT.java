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
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.AbstractResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SlotRatioStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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

            // The resource manager only knows about a worker after its first heartbeat, and
            // job submission needs both workers to be registered, so poll for registration
            // instead of waiting a fixed time.
            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address node1Address = node1.node.address;
            Address node2Address = node2.node.address;
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        resourceManager
                                                .getRegisterWorker()
                                                .containsKey(node1Address),
                                        "Node1 has not registered yet");
                                Assertions.assertTrue(
                                        resourceManager
                                                .getRegisterWorker()
                                                .containsKey(node2Address),
                                        "Node2 has not registered yet");
                            });

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

    /** Template with two independent FakeSource -> Console pipelines inside one job. */
    private static final String TWO_PIPELINE_TEMPLATE_FILE_NAME =
            "two_pipeline_fake_to_console_template.conf";

    /**
     * Regression test for the job-scoped slot allocation state.
     *
     * <p>The allocation strategy is shared by every job on the master. Before the state was scoped
     * to a job, starting a job replaced the strategy's single counter map, so the allocation round
     * of one job discarded the in-flight placement counters of another job. This test drives two
     * live jobs through real slot allocation on a fixed-slot cluster and asserts, from the strategy
     * bookkeeping and the worker slot pools:
     *
     * <ol>
     *   <li>both jobs are recorded under their own job ID, and the counters of the job that started
     *       first survive the second job's allocation round;
     *   <li>a pipeline restore of one job resets only that job, and by contract the whole job: the
     *       sibling pipeline's provisional counters are dropped and the counters are re-seeded from
     *       the slots that are still assigned, while the other live job is untouched;
     *   <li>terminal cleanup drops the finished job's counters, and canceling both jobs returns
     *       every fixed slot to the pool.
     * </ol>
     *
     * <p>The counters are a worker's slots that were already assigned when this job first selected
     * the worker, plus the selections this job made since then. On an empty cluster the first job
     * therefore records exactly its own slots, and the second job records the first job's slots as
     * its baseline. The per-job map is read through reflection because it is internal bookkeeping:
     * the supported interface only offers selection and reset, not a per-job inspection API.
     * Pinning the wiring this way is the point of the test: a wrong job ID passed by {@code
     * ResourceRequestHandler}, a reset that erases another job, a restore that misses the job-level
     * reset, or a missing terminal reset all make an assertion below fail.
     */
    @Test
    public void testConcurrentJobsKeepSlotAllocationStateScopedToTheirJob() throws Exception {
        String testCaseName = "testConcurrentJobsKeepSlotAllocationStateScopedToTheirJob";
        String testClusterName = TestUtils.getClusterName(testCaseName);
        long testRowNumber = 100;

        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy singlePipelineJob = null;
        ClientJobProxy twoPipelineJob = null;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().getNetworkConfig().setPort(5812);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        SlotServiceConfig slotServiceConfig =
                seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setSlotNum(10);
        slotServiceConfig.setDynamicSlot(false);
        slotServiceConfig.setAllocateStrategy(AllocateStrategy.SLOT_RATIO);

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

            NodeEngineImpl nodeEngine = node1.node.nodeEngine;
            Address node1Address = node1.node.address;
            Address node2Address = node2.node.address;
            SeaTunnelServer server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
            ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
            SlotRatioStrategy strategy =
                    (SlotRatioStrategy)
                            ((AbstractResourceManager) resourceManager).getSlotAllocationStrategy();

            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        resourceManager
                                                .getRegisterWorker()
                                                .containsKey(node1Address),
                                        "Node1 has not registered yet");
                                Assertions.assertTrue(
                                        resourceManager
                                                .getRegisterWorker()
                                                .containsKey(node2Address),
                                        "Node2 has not registered yet");
                            });

            Common.setDeployMode(DeployMode.CLIENT);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            // One pipeline with parallelism 4 occupies five slots: four source tasks and one sink.
            JobConfig singlePipelineJobConfig = new JobConfig();
            singlePipelineJobConfig.setName(testCaseName + "_single_pipeline");
            singlePipelineJob =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName + "_single_pipeline",
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            4,
                                            "allocate-strategy/allocate_strategy_with_slot_ratio.conf"),
                                    singlePipelineJobConfig,
                                    seaTunnelConfig)
                            .execute();
            long singlePipelineJobId = singlePipelineJob.getJobId();
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                // The cluster starts empty, so the first job's counters are its own
                                // five slots, balanced over the two workers.
                                assertRecordedSlots(strategy, singlePipelineJobId, 2, 3);
                                Assertions.assertEquals(
                                        5,
                                        assignedSlotsTotal(resourceManager),
                                        "the first job must own five slots");
                            });

            // Two pipelines with parallelism 1 occupy four slots. Starting this job used to
            // replace the global counter map, so the first job's bookkeeping must survive it.
            JobConfig twoPipelineJobConfig = new JobConfig();
            twoPipelineJobConfig.setName(testCaseName + "_two_pipelines");
            twoPipelineJob =
                    engineClient
                            .createExecutionContext(
                                    createTestResources(
                                            testCaseName + "_two_pipelines",
                                            JobMode.STREAMING,
                                            testRowNumber,
                                            1,
                                            TWO_PIPELINE_TEMPLATE_FILE_NAME),
                                    twoPipelineJobConfig,
                                    seaTunnelConfig)
                            .execute();
            long twoPipelineJobId = twoPipelineJob.getJobId();
            Assertions.assertNotEquals(singlePipelineJobId, twoPipelineJobId);

            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Map<Long, Map<Address, SlotAssignedProfile>> recordedSlotsByJob =
                                        recordedSlotsByJob(strategy);
                                Assertions.assertEquals(
                                        Set.of(singlePipelineJobId, twoPipelineJobId),
                                        recordedSlotsByJob.keySet(),
                                        "both live jobs must be recorded under their own job ID");
                                Assertions.assertFalse(
                                        recordedSlotsByJob.containsKey(-1L),
                                        "the engine must not use the unscoped legacy key");
                                // The first job keeps its counters and its two/three split.
                                assertRecordedSlots(strategy, singlePipelineJobId, 2, 3);
                                // The second job is seeded with the first job's three/two slots and
                                // then balances its own four selections to two per worker, so the
                                // counters are five/four; a reset of the shared map would have
                                // dropped the first job or the second job's own progress.
                                assertRecordedSlots(strategy, twoPipelineJobId, 4, 5);
                                Assertions.assertEquals(
                                        9,
                                        assignedSlotsTotal(resourceManager),
                                        "both jobs must own their slots at the same time");
                            });

            JobMaster jobMaster = server.getCoordinatorService().getJobMaster(twoPipelineJobId);
            Assertions.assertNotNull(jobMaster, "the job master of job B must exist");
            PhysicalPlan physicalPlan = jobMaster.getPhysicalPlan();
            List<SubPlan> pipelines = physicalPlan.getPipelineList();
            Assertions.assertEquals(
                    2, pipelines.size(), "the second job must contain exactly two pipelines");
            SubPlan restartingPipeline = pipelines.get(0);
            SubPlan siblingPipeline = pipelines.get(1);
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING,
                                        restartingPipeline.getPipelineState());
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING, siblingPipeline.getPipelineState());
                            });

            // Restart exactly one pipeline of the job, without cancelling the job, so the engine
            // takes the pipeline-restore path: release that pipeline's slots, then
            // JobMaster#preApplyResources(SubPlan) -> ResourceManager#resetSlotAllocationState.
            restartingPipeline.cancelPipeline();
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        1,
                                        restartingPipeline.getPipelineRestoreNum(),
                                        "the cancelled pipeline must have been restored once");
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING,
                                        restartingPipeline.getPipelineState());
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING, siblingPipeline.getPipelineState());
                                // The reset is whole-job: the sibling pipeline keeps its two slots,
                                // but both pipelines' provisional counters are dropped and the
                                // counters are re-seeded from the seven slots that are still
                                // assigned (five from job A plus the sibling's two). The restored
                                // pipeline then selects its own two slots on top. A restore that
                                // did not reset the whole job would keep the four counters this job
                                // recorded before the restore and end at eleven.
                                assertRecordedSlots(strategy, twoPipelineJobId, 4, 5);
                                Assertions.assertEquals(
                                        9,
                                        assignedSlotsTotal(resourceManager),
                                        "the two-pipeline job must still own four slots");
                                // The other live job is not affected by the restore reset.
                                assertRecordedSlots(strategy, singlePipelineJobId, 2, 3);
                            });

            // Terminal cleanup: a finished job drops its counters, the other live job keeps its.
            singlePipelineJob.cancelJob();
            singlePipelineJob.waitForJobCompleteV2();
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertEquals(
                                        Set.of(twoPipelineJobId),
                                        recordedSlotsByJob(strategy).keySet(),
                                        "terminal cleanup must drop exactly the finished job");
                                assertRecordedSlots(strategy, twoPipelineJobId, 4, 5);
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING, siblingPipeline.getPipelineState());
                                Assertions.assertEquals(
                                        4,
                                        assignedSlotsTotal(resourceManager),
                                        "the surviving job's slots must not be released");
                            });

            twoPipelineJob.cancelJob();
            twoPipelineJob.waitForJobCompleteV2();
            Awaitility.await()
                    .atMost(300, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        recordedSlotsByJob(strategy).isEmpty(),
                                        "cancelling the last job must drop its counters");
                                Assertions.assertEquals(
                                        0,
                                        assignedSlots(resourceManager, node1Address),
                                        "fixed slots of node1 must be released");
                                Assertions.assertEquals(
                                        0,
                                        assignedSlots(resourceManager, node2Address),
                                        "fixed slots of node2 must be released");
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

    private static int assignedSlotsTotal(ResourceManager resourceManager) {
        return resourceManager.getRegisterWorker().values().stream()
                .mapToInt(workerProfile -> workerProfile.getAssignedSlots().length)
                .sum();
    }

    private static int assignedSlots(ResourceManager resourceManager, Address address) {
        WorkerProfile workerProfile = resourceManager.getRegisterWorker().get(address);
        return workerProfile == null ? 0 : workerProfile.getAssignedSlots().length;
    }

    private static void assertRecordedSlots(
            SlotRatioStrategy strategy, long jobId, Integer... expectedSlotsPerWorker) {
        Map<Address, SlotAssignedProfile> recordedSlots = recordedSlotsByJob(strategy).get(jobId);
        Assertions.assertNotNull(
                recordedSlots, "no slot bookkeeping was recorded for job " + jobId);
        List<Integer> actualSlotsPerWorker =
                recordedSlots.values().stream()
                        .map(SlotAssignedProfile::getCurrentTaskAssignedSlotsNum)
                        .sorted()
                        .collect(Collectors.toList());
        Assertions.assertEquals(
                Arrays.asList(expectedSlotsPerWorker),
                new ArrayList<>(actualSlotsPerWorker),
                "unexpected per-worker slot counters for job " + jobId);
    }

    /**
     * Reads the strategy's internal per-job bookkeeping.
     *
     * <p>Test-only access: the supported contract is selection plus reset, not inspection.
     */
    @SuppressWarnings("unchecked")
    private static Map<Long, Map<Address, SlotAssignedProfile>> recordedSlotsByJob(
            SlotRatioStrategy strategy) {
        try {
            Field field = SlotRatioStrategy.class.getDeclaredField("workerAssignedSlotsByJob");
            field.setAccessible(true);
            return (Map<Long, Map<Address, SlotAssignedProfile>>) field.get(strategy);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to read the per-job slot bookkeeping", e);
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
