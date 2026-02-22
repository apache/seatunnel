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
import org.apache.seatunnel.engine.common.exception.JobNotFoundException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
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
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Slf4j
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

        Assertions.assertThrows(
                SeaTunnelEngineException.class, () -> server2.getCoordinatorService());

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
                                Assertions.fail("Should not throw SeaTunnelEngineException here.");
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

        Assertions.assertTrue(
                jobInformation
                        .coordinatorService
                        .getPendingJobQueue()
                        .contains(jobInformation.jobId));

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        jobInformation
                                                .coordinatorService
                                                .getPendingJobQueue()
                                                .contains(jobInformation.jobId)));

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

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JobStatus.RUNNING,
                                    coordinatorService.getJobStatus(jobInformation.jobId));
                            JobMaster jobMaster =
                                    coordinatorService.getJobMaster(jobInformation.jobId);
                            Assertions.assertNotNull(jobMaster);
                            Assertions.assertTrue(
                                    jobMaster
                                            .getRunningJobStateIMap()
                                            .containsKey(jobInformation.jobId));
                        });

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JobStatus.FINISHED,
                                    coordinatorService.getJobStatus(jobInformation.jobId));
                            JobMaster jobMaster =
                                    coordinatorService.getJobMaster(jobInformation.jobId);
                            // job master should be null
                            Assertions.assertNull(jobMaster);
                            Assertions.assertTrue(runningJobStateIMap.isEmpty());
                        });

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
                .untilAsserted(() -> Assertions.assertFalse(metricsImap.isEmpty()));
        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(metricsImap.isEmpty()));

        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    void testCleanupMetricsImapWithPartitionConfig() {
        setConfigFile("seatunnel_multiple_metrics_key.yaml");

        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testCleanupMetricsImapWithPartitionConfig",
                        "batch_fake_to_console.conf",
                        "test_cleanup_metrics_imap_with_partition_config");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap =
                coordinatorService.getMetricsImap();
        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertFalse(metricsImap.isEmpty()));
        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> Assertions.assertTrue(metricsImap.isEmpty()));

        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();
        setDefaultConfigFile();
    }

    @Test
    void testMetricsImapSizeWithPartitionConfig() {
        setConfigFile("seatunnel_multiple_metrics_key.yaml");

        String clusterName = TestUtils.getClusterName("testMetricsImapSizeWithPartitionConfig");
        HazelcastInstanceImpl instance1 =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);
        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        try {
            NodeEngineImpl nodeEngine = instance1.node.getNodeEngine();
            Map<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                TaskLocation taskLocation = new TaskLocation();
                taskLocation.setTaskID(i);
                localMap.put(taskLocation, new SeaTunnelMetricsContext());
            }
            IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap =
                    server1.getCoordinatorService().getMetricsImap();
            CompletableFuture.runAsync(
                    () -> {
                        try {
                            nodeEngine
                                    .getOperationService()
                                    .createInvocationBuilder(
                                            SeaTunnelServer.SERVICE_NAME,
                                            new ReportMetricsOperation(localMap),
                                            nodeEngine.getMasterAddress())
                                    .invoke()
                                    .get();
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            await().atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(10, metricsImap.size()));
        } finally {
            instance1.shutdown();
            setDefaultConfigFile();
        }
    }

    @Test
    void testCleanupPendingJobMasterMapWhenJobSubmitFutureIsExceptionally() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testCleanPendingJobMasterMap",
                        "batch_fake_to_inmemory.conf",
                        "test_clean_pending_jobmastermap");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        coordinatorService
                                                .getPendingJobQueue()
                                                .contains(jobInformation.jobId)));
    }

    @Test
    void testGetPendingJobInfo() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testGetPendingJobInfo",
                        "batch_fake_to_console.conf",
                        "test_get_pending_job_info");

        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        Long jobId = jobInformation.jobId;

        Assertions.assertTrue(coordinatorService.getPendingJobQueue().contains(jobId));

        JobDAGInfo jobDAGInfo =
                Assertions.assertDoesNotThrow(() -> coordinatorService.getJobInfo(jobId));
        Assertions.assertEquals(jobId, jobDAGInfo.getJobId());

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
    void testUpdateTaskExecutionStateWaitsForRestoreCompletion() throws Exception {
        CountDownLatch restoreLatch = new CountDownLatch(1);

        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testUpdateTaskExecutionStateWaitsForRestoreCompletion",
                        "batch_fake_to_console.conf",
                        "test_update_task_execution_state_waits_for_restore_completion");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(jobInformation.jobId)));
        JobMaster jobMaster = coordinatorService.getJobMaster(jobInformation.jobId);

        Field mapField = CoordinatorService.class.getDeclaredField("runningJobMasterMap");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, JobMaster> runningJobMasterMap =
                (Map<Long, JobMaster>) mapField.get(coordinatorService);
        runningJobMasterMap.remove(jobInformation.jobId);

        CompletableFuture<Void> delayedFuture =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                restoreLatch.await(10, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

        PassiveCompletableFuture<Void> delayedPassiveFuture =
                new PassiveCompletableFuture<>(new CompletableFuture<>(delayedFuture));

        Field futureField =
                CoordinatorService.class.getDeclaredField(
                        "restoreAllJobFromMasterNodeSwitchFuture");
        futureField.setAccessible(true);
        futureField.set(coordinatorService, delayedPassiveFuture);

        Executors.newSingleThreadExecutor()
                .submit(
                        () -> {
                            try {
                                Thread.sleep(1000);
                                runningJobMasterMap.put(jobInformation.jobId, jobMaster);
                                restoreLatch.countDown();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

        TaskGroupLocation taskGroupLocation =
                jobMaster
                        .getPhysicalPlan()
                        .getPipelineList()
                        .get(0)
                        .getPhysicalVertexList()
                        .get(0)
                        .getTaskGroupLocation();
        TaskExecutionState taskExecutionState =
                new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED);

        await().atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertDoesNotThrow(
                                        () ->
                                                coordinatorService.updateTaskExecutionState(
                                                        taskExecutionState)));

        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    void testUpdateTaskExecutionStateThrowsWhenJobNotFound() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testUpdateTaskExecutionStateThrowsWhenJobNotFound",
                        "batch_fake_to_console.conf",
                        "test_update_task_execution_state_throws_when_job_not_found");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;

        TaskGroupLocation fakeLocation = new TaskGroupLocation(99999L, 0, 0);
        TaskExecutionState taskExecutionState =
                new TaskExecutionState(fakeLocation, ExecutionState.FAILED);

        Assertions.assertThrows(
                JobNotFoundException.class,
                () -> coordinatorService.updateTaskExecutionState(taskExecutionState));

        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    void testUpdateTaskExecutionStateWhenRestoreFailed() throws Exception {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testUpdateTaskExecutionStateWhenRestoreFailed",
                        "batch_fake_to_console.conf",
                        "test_update_task_execution_state_when_restore_failed");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(jobInformation.jobId)));

        // Remove job from runningJobMasterMap to simulate master switch
        Field mapField = CoordinatorService.class.getDeclaredField("runningJobMasterMap");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, JobMaster> runningJobMasterMap =
                (Map<Long, JobMaster>) mapField.get(coordinatorService);
        runningJobMasterMap.remove(jobInformation.jobId);

        // Create a failed future to simulate restore failure
        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new SeaTunnelEngineException("Restore failed"));
        PassiveCompletableFuture<Void> failedPassiveFuture =
                new PassiveCompletableFuture<>(failedFuture);

        Field futureField =
                CoordinatorService.class.getDeclaredField(
                        "restoreAllJobFromMasterNodeSwitchFuture");
        futureField.setAccessible(true);
        futureField.set(coordinatorService, failedPassiveFuture);

        // Try to update task state
        TaskGroupLocation fakeLocation = new TaskGroupLocation(jobInformation.jobId, 0, 0);
        TaskExecutionState taskExecutionState =
                new TaskExecutionState(fakeLocation, ExecutionState.FAILED);

        Assertions.assertThrows(
                JobNotFoundException.class,
                () -> coordinatorService.updateTaskExecutionState(taskExecutionState));

        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    void testUpdateTaskExecutionStateWhenJobRemovedDuringRestore() throws Exception {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testUpdateTaskExecutionStateWhenJobRemovedDuringRestore",
                        "batch_fake_to_console.conf",
                        "test_update_task_execution_state_job_removed_during_restore");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        await().atMost(30000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        coordinatorService.getJobStatus(jobInformation.jobId)));

        Field mapField = CoordinatorService.class.getDeclaredField("runningJobMasterMap");
        mapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<Long, JobMaster> runningJobMasterMap =
                (Map<Long, JobMaster>) mapField.get(coordinatorService);

        // Remove job from map but don't add it back (simulating job cancellation during restore)
        Long jobId = jobInformation.jobId;
        runningJobMasterMap.remove(jobId);

        // Create a future that completes but doesn't restore the job
        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
        PassiveCompletableFuture<Void> completedPassiveFuture =
                new PassiveCompletableFuture<>(completedFuture);

        Field futureField =
                CoordinatorService.class.getDeclaredField(
                        "restoreAllJobFromMasterNodeSwitchFuture");
        futureField.setAccessible(true);
        futureField.set(coordinatorService, completedPassiveFuture);

        // Try to update task state
        TaskGroupLocation fakeLocation = new TaskGroupLocation(jobId, 0, 0);
        TaskExecutionState taskExecutionState =
                new TaskExecutionState(fakeLocation, ExecutionState.FAILED);

        // Should throw JobNotFoundException
        Assertions.assertThrows(
                JobNotFoundException.class,
                () -> coordinatorService.updateTaskExecutionState(taskExecutionState));

        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    @Disabled("Disabled because we can't know when the master node switches in the unit tests")
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
        HazelcastInstanceImpl instance3 =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        3, instance1.getCluster().getMembers().size()));

        ExecutorService executor = Executors.newFixedThreadPool(32);
        try {
            NodeEngineImpl nodeEngine = instance2.node.getNodeEngine();
            Map<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();
            for (int i = 0; i < 20000; i++) {
                TaskLocation taskLocation = new TaskLocation();
                taskLocation.setTaskID(i);
                localMap.put(taskLocation, new SeaTunnelMetricsContext());
            }

            // warm-up
            runOps(executor, nodeEngine, localMap, 100);

            int ops = 100;
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

        CompletableFuture<Long>[] futures = new CompletableFuture[ops];

        for (int i = 0; i < ops; i++) {
            futures[i] =
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    startGate.await();
                                    long start = System.nanoTime();
                                    nodeEngine
                                            .getOperationService()
                                            .createInvocationBuilder(
                                                    SeaTunnelServer.SERVICE_NAME,
                                                    new ReportMetricsOperation(localMap),
                                                    nodeEngine.getMasterAddress())
                                            .setCallTimeout(120_000)
                                            .invoke()
                                            .get();
                                    long end = System.nanoTime();
                                    return end - start;
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            },
                            executor);
        }

        long startNs = System.nanoTime();
        startGate.countDown();

        long[] durations = new long[ops];
        for (int i = 0; i < ops; i++) {
            durations[i] = futures[i].join();
        }

        long elapsedNs = System.nanoTime() - startNs;
        double avgSeconds = Arrays.stream(durations).average().orElse(0) / 1_000_000_000.0;

        System.out.printf("Average completion time per op: %.6f seconds%n", avgSeconds);

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
