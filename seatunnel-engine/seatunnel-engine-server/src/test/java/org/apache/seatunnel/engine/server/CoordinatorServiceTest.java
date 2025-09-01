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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.operation.PrintMessageOperation;
import org.apache.seatunnel.engine.server.operation.ReturnRetryTimesOperation;
import org.apache.seatunnel.engine.server.task.operation.ReportMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class CoordinatorServiceTest {
    @Test
    public void testMasterNodeActive() {
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName("CoordinatorServiceTest_testMasterNodeActive"));
        HazelcastInstanceImpl instance2 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName("CoordinatorServiceTest_testMasterNodeActive"));

        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SeaTunnelServer server2 =
                instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        Assertions.assertTrue(server1.isMasterNode());
        CoordinatorService coordinatorService1 = server1.getCoordinatorService();
        Assertions.assertTrue(coordinatorService1.isCoordinatorActive());

        try {
            server2.getCoordinatorService();
            Assertions.fail("Need throw SeaTunnelEngineException here but not.");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof SeaTunnelEngineException);
        }

        // shutdown instance1
        instance1.shutdown();
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                Assertions.assertTrue(server2.isMasterNode());
                                CoordinatorService coordinatorService =
                                        server2.getCoordinatorService();
                                Assertions.assertTrue(coordinatorService.isCoordinatorActive());
                            } catch (SeaTunnelEngineException e) {
                                Assertions.assertTrue(false);
                            }
                        });
        instance2.shutdown();
    }

    @Test
    public void testSeaTunnelEngineRetryableExceptionOperationCanBeRetryByHazelcast() {

        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "CoordinatorServiceTest_testSeaTunnelEngineRetryableExceptionOperationCanBeRetryByHazelcast"));
        try {
            CompletionException exception =
                    Assertions.assertThrows(
                            CompletionException.class,
                            () -> {
                                NodeEngineUtil.sendOperationToMemberNode(
                                                instance.node.getNodeEngine(),
                                                new ReturnRetryTimesOperation(),
                                                instance.getCluster().getLocalMember().getAddress())
                                        .join();
                            });
            Assertions.assertTrue(
                    exception
                            .getCause()
                            .getMessage()
                            .contains("Retryable exception occurred, retry times: 250"));
        } finally {
            instance.shutdown();
        }
    }

    @Test
    public void testInvocationFutureUseCompletableFutureExecutor() {
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "CoordinatorServiceTest_testInvocationFutureUseCompletableFutureExecutor"));

        NodeEngineUtil.sendOperationToMemberNode(
                        instance.node.getNodeEngine(),
                        new PrintMessageOperation("hello"),
                        instance.getCluster().getLocalMember().getAddress())
                .whenComplete(
                        (aVoid, error) -> {
                            Assertions.assertTrue(
                                    Thread.currentThread()
                                            .getName()
                                            .startsWith("SeaTunnel-CompletableFuture-Thread"));
                        })
                .join();

        NodeEngineUtil.sendOperationToMasterNode(
                        instance.node.getNodeEngine(), new PrintMessageOperation("hello"))
                .whenCompleteAsync(
                        (aVoid, error) -> {
                            Assertions.assertTrue(
                                    Thread.currentThread()
                                            .getName()
                                            .startsWith("SeaTunnel-CompletableFuture-Thread"));
                        })
                .join();

        instance.shutdown();
    }

    @Test
    void testCleanupPendingJobMasterMapAfterJobFailed() {
        setConfigFile("seatunnel_fixed_slots.yaml");

        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testCleanupPendingJobMasterMapAfterJobFailed",
                        "batch_slot_not_enough.conf",
                        "test_cleanup_pending_job_master_map_after_job_failed");

        Assertions.assertNotNull(
                jobInformation.coordinatorService.pendingJobMasterMap.get(jobInformation.jobId));

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNull(
                                        jobInformation.coordinatorService.pendingJobMasterMap.get(
                                                jobInformation.jobId)));

        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();

        setDefaultConfigFile();
    }

    @Test
    void testCleanupRunningJobStateIMap() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testCleanupRunningJobStateIMap",
                        "batch_fake_to_console.conf",
                        "test_cleanup_running_job_state_imap");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        IMap<Object, Object> runningJobStateIMap =
                coordinatorService.getJobMaster(jobInformation.jobId).getRunningJobStateIMap();
        Assertions.assertTrue(!runningJobStateIMap.isEmpty());

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(runningJobStateIMap.isEmpty()));

        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    void testCleanupMetricsImap() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testCleanupMetricsImap",
                        "batch_fake_to_console.conf",
                        "test_cleanup_metrics_imap");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap =
                coordinatorService.getMetricsImap();

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(metricsImap.isEmpty()));

        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();
    }

    private void setDefaultConfigFile() {
        setConfigFile("seatunnel.yaml");
    }

    private void setConfigFile(String fileName) {
        String rootModuleDir = "seatunnel-engine";
        Path path = Paths.get(System.getProperty("user.dir"));
        while (!path.endsWith(Paths.get(rootModuleDir))) {
            path = path.getParent();
        }
        String rootPath = path.getParent().toString();
        System.setProperty(
                "seatunnel.config",
                rootPath
                        + "/seatunnel-engine/seatunnel-engine-server/src/test/resources/"
                        + fileName);
    }

    private JobInformation submitJob(String testClassName, String jobConfigFile, String jobName) {
        HazelcastInstanceImpl coordinatorServiceTest =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(testClassName));
        SeaTunnelServer server1 =
                coordinatorServiceTest
                        .node
                        .getNodeEngine()
                        .getService(SeaTunnelServer.SERVICE_NAME);
        CoordinatorService coordinatorService = server1.getCoordinatorService();
        Assertions.assertTrue(coordinatorService.isCoordinatorActive());

        Long jobId =
                coordinatorServiceTest
                        .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME)
                        .newId();
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(jobConfigFile, jobName, jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        coordinatorServiceTest.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data =
                coordinatorServiceTest.getSerializationService().toData(jobImmutableInformation);

        coordinatorService
                .submitJob(jobId, data, jobImmutableInformation.isStartWithSavePoint())
                .join();
        return new JobInformation(coordinatorServiceTest, coordinatorService, jobId);
    }

    @Test
    public void testClearCoordinatorService() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testClearCoordinatorService",
                        "stream_fake_to_console.conf",
                        "test_clear_coordinator_service");

        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        Long jobId = jobInformation.jobId;
        HazelcastInstanceImpl coordinatorServiceTest = jobInformation.coordinatorServiceTest;

        // waiting for job status turn to running
        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, coordinatorService.getJobStatus(jobId)));

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        int scheduleRunnerThreadCount =
                (int)
                        Thread.getAllStackTraces().keySet().stream()
                                .filter(
                                        thread ->
                                                thread.getName()
                                                        .startsWith("pending-job-schedule-runner"))
                                .count();
        Assertions.assertTrue(scheduleRunnerThreadCount > 0);

        coordinatorService.clearCoordinatorService();

        // because runningJobMasterMap is empty, and we have no JobHistoryServer, so return
        // UNKNOWABLE.
        Assertions.assertEquals(JobStatus.UNKNOWABLE, coordinatorService.getJobStatus(jobId));
        coordinatorServiceTest.shutdown();

        Assertions.assertEquals(
                scheduleRunnerThreadCount - 1,
                Thread.getAllStackTraces().keySet().stream()
                        .filter(
                                thread ->
                                        thread.getName().startsWith("pending-job-schedule-runner"))
                        .count());
    }

    @Test
    @Disabled("disabled because we can not know")
    public void testJobRestoreWhenMasterNodeSwitch() throws InterruptedException {
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "CoordinatorServiceTest_testJobRestoreWhenMasterNodeSwitch"));
        HazelcastInstanceImpl instance2 =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "CoordinatorServiceTest_testJobRestoreWhenMasterNodeSwitch"));

        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SeaTunnelServer server2 =
                instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        CoordinatorService coordinatorService = server1.getCoordinatorService();
        Assertions.assertTrue(coordinatorService.isCoordinatorActive());

        Long jobId = instance1.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
        LogicalDag testLogicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fakesource_to_file.conf",
                        "testJobRestoreWhenMasterNodeSwitch",
                        jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        instance1.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = instance1.getSerializationService().toData(jobImmutableInformation);

        coordinatorService
                .submitJob(jobId, data, jobImmutableInformation.isStartWithSavePoint())
                .join();

        // waiting for job status turn to running
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, coordinatorService.getJobStatus(jobId)));

        // test master node shutdown
        instance1.shutdown();
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            try {
                                Assertions.assertTrue(server2.isMasterNode());
                                Assertions.assertTrue(
                                        server2.getCoordinatorService().isCoordinatorActive());
                            } catch (SeaTunnelEngineException e) {
                                Assertions.assertTrue(false);
                            }
                        });

        // pipeline will leave running state
        await().atMost(200000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertNotEquals(
                                        PipelineStatus.RUNNING,
                                        server2.getCoordinatorService()
                                                .getJobMaster(jobId)
                                                .getPhysicalPlan()
                                                .getPipelineList()
                                                .get(0)
                                                .getPipelineState()));

        // pipeline will recovery running state
        await().atMost(200000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        PipelineStatus.RUNNING,
                                        server2.getCoordinatorService()
                                                .getJobMaster(jobId)
                                                .getPhysicalPlan()
                                                .getPipelineList()
                                                .get(0)
                                                .getPipelineState()));

        server2.getCoordinatorService().cancelJob(jobId);

        // because runningJobMasterMap is empty and we have no JobHistoryServer, so return finished.
        await().atMost(200000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server2.getCoordinatorService().getJobStatus(jobId)));
        instance2.shutdown();
    }

    @Test
    @SetEnvironmentVariable(
            key = "ST_DOCKER_MEMBER_LIST",
            value = "127.0.0.1,127.0.0.2,127.0.0.3,127.0.0.4")
    public void testDockerEnvOverwrite() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        if (seaTunnelConfig
                .getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .isEnabled()) {
            Assertions.assertEquals(
                    4,
                    seaTunnelConfig
                            .getHazelcastConfig()
                            .getNetworkConfig()
                            .getJoin()
                            .getTcpIpConfig()
                            .getMembers()
                            .size());
        }
    }

    @Disabled("Performance test, not suitable for regular unit test execution")
    @Test
    void testDistributedMetricsPerformance() throws Exception {
        String clusterName = TestUtils.getClusterName("testDistributedMetricsPerformance");
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);
        HazelcastInstanceImpl instance2 =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        2, instance1.getCluster().getMembers().size()));

        ExecutorService executor = Executors.newFixedThreadPool(50);
        try {
            NodeEngineImpl nodeEngine = instance2.node.getNodeEngine();
            Map<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();

            // warm-up
            runOps(executor, nodeEngine, localMap, 2000);

            int ops = 10000;
            double seconds = runOps(executor, nodeEngine, localMap, ops);
            double tps = ops / seconds;

            System.out.printf("Distributed metrics performance:%n");
            System.out.printf("- ops: %d, seconds: %.3f, ops/s: %.0f%n", ops, seconds, tps);
        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    private double runOps(
            ExecutorService executor,
            NodeEngineImpl nodeEngine,
            Map<TaskLocation, SeaTunnelMetricsContext> localMap,
            int ops) {

        CountDownLatch startGate = new CountDownLatch(1);
        List<CompletableFuture<Void>> futures = new ArrayList<>(ops);

        for (int i = 0; i < ops; i++) {
            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                try {
                                    startGate.await();
                                    nodeEngine
                                            .getOperationService()
                                            .createInvocationBuilder(
                                                    SeaTunnelServer.SERVICE_NAME,
                                                    new ReportMetricsOperation(localMap),
                                                    nodeEngine.getMasterAddress())
                                            .invoke()
                                            .get();
                                } catch (Exception e) {
                                    throw new java.util.concurrent.CompletionException(e);
                                }
                            },
                            executor));
        }

        long startNs = System.nanoTime();
        startGate.countDown();
        CompletableFuture.allOf(futures.toArray(new java.util.concurrent.CompletableFuture[0]))
                .join();
        long elapsedNs = System.nanoTime() - startNs;

        return elapsedNs / 1_000_000_000.0;
    }

    private static class JobInformation {

        public final HazelcastInstanceImpl coordinatorServiceTest;
        public final CoordinatorService coordinatorService;
        public final Long jobId;

        public JobInformation(
                HazelcastInstanceImpl coordinatorServiceTest,
                CoordinatorService coordinatorService,
                Long jobId) {
            this.coordinatorServiceTest = coordinatorServiceTest;
            this.coordinatorService = coordinatorService;
            this.jobId = jobId;
        }
    }
}
