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

import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventProcessor;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.PendingSourceState;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.operation.PrintMessageOperation;
import org.apache.seatunnel.engine.server.operation.ReturnRetryTimesOperation;
import org.apache.seatunnel.engine.server.operation.SubmitJobOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.operation.ReportMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.seatunnel.engine.core.classloader.DefaultClassLoaderService.SKIP_CHECK_JAR;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doThrow;

@Slf4j
public class CoordinatorServiceTest {
    @Test
    void testTerminalZombieJobShouldNotRestartAfterMasterSwitch() {
        String clusterName =
                TestUtils.getClusterName(
                        "CoordinatorServiceTest_testTerminalZombieJobShouldNotRestartAfterMasterSwitch");

        // instance1: initial master
        HazelcastInstanceImpl instance1 =
                createHazelcastInstanceWithJoinPortTryCount(clusterName, 100);
        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(server1.isMasterNode());
                            Assertions.assertTrue(
                                    server1.getCoordinatorService().isCoordinatorActive());
                        });

        // instance2: survives the master switch
        HazelcastInstanceImpl instance2 =
                createHazelcastInstanceWithJoinPortTryCount(clusterName, 100);
        SeaTunnelServer server2 =
                instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        2, instance1.getCluster().getMembers().size()));

        long jobId = instance1.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();

        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fake_to_console.conf", "terminal-zombie-master-switch-test", jobId);

        JobImmutableInformation jobImmutableInfo =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        instance1.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data jobData = instance1.getSerializationService().toData(jobImmutableInfo);

        CoordinatorService coordinatorService = server1.getCoordinatorService();
        coordinatorService
                .submitJob(jobId, jobData, jobImmutableInfo.isStartWithSavePoint())
                .join();

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, coordinatorService.getJobStatus(jobId)));

        // Collect restore keys before the job is terminated.
        JobMaster jobMaster = coordinatorService.getJobMaster(jobId);
        Assertions.assertNotNull(jobMaster);

        List<PipelineLocation> pipelineLocations = new ArrayList<>();
        List<TaskGroupLocation> taskGroupLocations = new ArrayList<>();
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            pipelineLocations.add(subPlan.getPipelineLocation());
            subPlan.getCoordinatorVertexList()
                    .forEach(vertex -> taskGroupLocations.add(vertex.getTaskGroupLocation()));
            subPlan.getPhysicalVertexList()
                    .forEach(vertex -> taskGroupLocations.add(vertex.getTaskGroupLocation()));
        }

        // 1) Terminate the real job first.
        coordinatorService.cancelJob(jobId).join();

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            JobStatus status = coordinatorService.getJobStatus(jobId);
                            Assertions.assertTrue(
                                    status == JobStatus.CANCELING || status == JobStatus.CANCELED,
                                    "Expected job status to be CANCELING or CANCELED, but got "
                                            + status);
                        });

        // Ensure the real execution is gone before injecting stale zombie metadata.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(
                                    allTaskGroupsInactive(server1, taskGroupLocations));
                            Assertions.assertTrue(
                                    allTaskGroupsInactive(server2, taskGroupLocations));
                        });

        // 2) Re-inject stale terminal metadata using the locations we collected earlier.
        IMap<Long, org.apache.seatunnel.engine.core.job.JobInfo> runningJobInfoIMap =
                instance1.getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateIMap =
                instance1.getMap(Constant.IMAP_RUNNING_JOB_STATE);

        runningJobInfoIMap.put(
                jobId,
                new org.apache.seatunnel.engine.core.job.JobInfo(
                        System.currentTimeMillis(), jobData));
        runningJobStateIMap.put(jobId, JobStatus.CANCELED);

        for (PipelineLocation pipelineLocation : pipelineLocations) {
            runningJobStateIMap.put(pipelineLocation, PipelineStatus.CANCELED);
        }
        for (TaskGroupLocation taskGroupLocation : taskGroupLocations) {
            runningJobStateIMap.put(taskGroupLocation, ExecutionState.CANCELED);
        }

        Assertions.assertTrue(
                runningJobInfoIMap.containsKey(jobId),
                "Zombie job metadata should be present before master switch");

        // 3) Trigger master switch.
        instance1.shutdown();

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertTrue(server2.isMasterNode());
                            Assertions.assertTrue(
                                    server2.getCoordinatorService().isCoordinatorActive());
                        });

        // 4) Metadata should be cleaned up on the new master.
        IMap<Long, org.apache.seatunnel.engine.core.job.JobInfo> runningJobInfoOnInstance2 =
                instance2.getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<Object, Object> runningJobStateOnInstance2 =
                instance2.getMap(Constant.IMAP_RUNNING_JOB_STATE);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertFalse(
                                    runningJobInfoOnInstance2.containsKey(jobId),
                                    "Zombie job must be removed from runningJobInfoIMap");
                            Assertions.assertFalse(
                                    runningJobStateOnInstance2.containsKey(jobId),
                                    "Zombie job must be removed from runningJobStateIMap");
                        });

        // 5) The important assertion:
        // terminal zombie metadata must NOT cause tasks to start running again.
        await().during(10, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            for (TaskGroupLocation location : taskGroupLocations) {
                                TaskGroupContext activeExecutionContext = null;
                                try {
                                    activeExecutionContext =
                                            server2.getTaskExecutionService()
                                                    .getActiveExecutionContext(location);
                                } catch (Exception e) {
                                }
                                Assertions.assertNull(
                                        activeExecutionContext,
                                        "Terminal zombie job should not be re-executed after "
                                                + "master switch: "
                                                + location);
                            }
                        });

        instance2.shutdown();
    }

    private boolean allTaskGroupsInactive(
            SeaTunnelServer server, List<TaskGroupLocation> taskGroupLocations) {
        for (TaskGroupLocation location : taskGroupLocations) {
            try {
                TaskGroupContext context =
                        server.getTaskExecutionService().getActiveExecutionContext(location);
                if (context != null) {
                    return false;
                }
            } catch (Exception e) {
                continue;
            }
        }
        return true;
    }

    @Test
    public void testMasterNodeActive() {
        String clusterName =
                TestUtils.getClusterName("CoordinatorServiceTest_testMasterNodeActive");
        HazelcastInstanceImpl instance1 =
                createHazelcastInstanceWithJoinPortTryCount(clusterName, 100);
        HazelcastInstanceImpl instance2 =
                createHazelcastInstanceWithJoinPortTryCount(clusterName, 100);

        SeaTunnelServer server1 =
                instance1.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        SeaTunnelServer server2 =
                instance2.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);

        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(2, instance1.getCluster().getMembers().size());
                            Assertions.assertEquals(2, instance2.getCluster().getMembers().size());
                            Assertions.assertTrue(server1.isMasterNode());
                            Assertions.assertFalse(server2.isMasterNode());
                        });

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
    void testCheckNewActiveMasterCanSchedulePendingQueue() throws Exception {
        AtomicBoolean masterFlag = new AtomicBoolean(false);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        Mockito.when(server.isMasterNode()).thenAnswer(invocation -> masterFlag.get());
        CoordinatorService coordinatorService = newMockCoordinatorService(server);
        try {
            CountDownLatch runLatch = new CountDownLatch(1);
            JobMaster jobMaster = enqueueMockPendingJob(coordinatorService, 10001L, runLatch);

            masterFlag.set(true);
            invokeCheckNewActiveMaster(coordinatorService);

            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(0L, runLatch.getCount()));
            Mockito.verify(jobMaster, Mockito.times(1)).run();
        } finally {
            coordinatorService.shutdown();
        }
    }

    @Test
    void testFailoverStopsOldPendingQueueAndNewCoordinatorCanSchedule() throws Exception {
        AtomicBoolean oldMasterFlag = new AtomicBoolean(false);
        SeaTunnelServer oldServer = Mockito.mock(SeaTunnelServer.class);
        Mockito.when(oldServer.isMasterNode()).thenAnswer(invocation -> oldMasterFlag.get());
        CoordinatorService oldCoordinator = newMockCoordinatorService(oldServer);
        try {
            oldMasterFlag.set(true);
            invokeCheckNewActiveMaster(oldCoordinator);

            await().atMost(15, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertTrue(oldCoordinator.isCoordinatorActive()));

            oldMasterFlag.set(false);
            invokeCheckNewActiveMaster(oldCoordinator);
            Assertions.assertTrue(getCoordinatorExecutor(oldCoordinator).isShutdown());

            CountDownLatch oldRunLatch = new CountDownLatch(1);
            JobMaster oldPendingJob = enqueueMockPendingJob(oldCoordinator, 20001L, oldRunLatch);
            Assertions.assertTrue(oldCoordinator.getPendingJobQueue().contains(20001L));
            await().during(1, TimeUnit.SECONDS)
                    .atMost(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(1L, oldRunLatch.getCount()));
            Mockito.verify(oldPendingJob, Mockito.never()).run();
            Assertions.assertTrue(
                    oldCoordinator.getPendingJobQueue().contains(20001L),
                    "old coordinator pending queue should not be consumed after failover");

            AtomicBoolean newMasterFlag = new AtomicBoolean(true);
            SeaTunnelServer newServer = Mockito.mock(SeaTunnelServer.class);
            Mockito.when(newServer.isMasterNode()).thenAnswer(invocation -> newMasterFlag.get());
            CoordinatorService newCoordinator = newMockCoordinatorService(newServer);
            try {
                CountDownLatch newRunLatch = new CountDownLatch(1);
                JobMaster newPendingJob =
                        enqueueMockPendingJob(newCoordinator, 30001L, newRunLatch);

                invokeCheckNewActiveMaster(newCoordinator);

                await().atMost(5, TimeUnit.SECONDS)
                        .untilAsserted(
                                () -> Assertions.assertTrue(newCoordinator.isCoordinatorActive()));
                await().atMost(5, TimeUnit.SECONDS)
                        .untilAsserted(() -> Assertions.assertEquals(0L, newRunLatch.getCount()));
                Mockito.verify(newPendingJob, Mockito.times(1)).run();
                Assertions.assertFalse(newCoordinator.getPendingJobQueue().contains(30001L));
            } finally {
                newCoordinator.shutdown();
            }
        } finally {
            if (!getCoordinatorExecutor(oldCoordinator).isShutdown()) {
                oldCoordinator.shutdown();
            }
        }
    }

    @Test
    void testCheckNewActiveMasterIsIdempotentWhenAlreadyActive() throws Exception {
        AtomicBoolean masterFlag = new AtomicBoolean(true);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        Mockito.when(server.isMasterNode()).thenAnswer(invocation -> masterFlag.get());
        CoordinatorService coordinatorService = newMockCoordinatorService(server);
        try {
            CountDownLatch runLatch = new CountDownLatch(1);
            JobMaster jobMaster = enqueueMockPendingJob(coordinatorService, 40001L, runLatch);

            invokeCheckNewActiveMaster(coordinatorService);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(0L, runLatch.getCount()));

            invokeCheckNewActiveMaster(coordinatorService);
            await().during(1, TimeUnit.SECONDS)
                    .atMost(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> Mockito.verify(jobMaster, Mockito.times(1)).run());
        } finally {
            coordinatorService.shutdown();
        }
    }

    @Test
    void testPendingJobWithInsufficientResourceRespectsWaitStrategy() throws Exception {
        AtomicBoolean masterFlag = new AtomicBoolean(true);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        Mockito.when(server.isMasterNode()).thenAnswer(invocation -> masterFlag.get());

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setScheduleStrategy(ScheduleStrategy.WAIT);
        CoordinatorService coordinatorService = newMockCoordinatorService(server, engineConfig);
        try {
            CountDownLatch runLatch = new CountDownLatch(1);
            JobMaster jobMaster =
                    enqueueMockPendingJob(coordinatorService, 50001L, runLatch, false);

            invokeCheckNewActiveMaster(coordinatorService);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));
            await().during(1, TimeUnit.SECONDS)
                    .atMost(2, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertEquals(1L, runLatch.getCount()));

            Mockito.verify(jobMaster, Mockito.never()).run();
            Assertions.assertTrue(coordinatorService.getPendingJobQueue().contains(50001L));
        } finally {
            coordinatorService.shutdown();
        }
    }

    @Test
    void testPendingJobWithInsufficientResourceRespectsRejectStrategy() throws Exception {
        AtomicBoolean masterFlag = new AtomicBoolean(true);
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        Mockito.when(server.isMasterNode()).thenAnswer(invocation -> masterFlag.get());

        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setScheduleStrategy(ScheduleStrategy.REJECT);
        CoordinatorService coordinatorService = newMockCoordinatorService(server, engineConfig);
        try {
            CountDownLatch runLatch = new CountDownLatch(1);
            JobMaster jobMaster =
                    enqueueMockPendingJob(coordinatorService, 60001L, runLatch, false);

            invokeCheckNewActiveMaster(coordinatorService);
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));
            await().atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertFalse(
                                            coordinatorService
                                                    .getPendingJobQueue()
                                                    .contains(60001L)));

            Mockito.verify(jobMaster, Mockito.never()).run();
            Assertions.assertEquals(1L, runLatch.getCount());
        } finally {
            coordinatorService.shutdown();
        }
    }

    private HazelcastInstanceImpl createHazelcastInstanceWithJoinPortTryCount(
            String clusterName, int joinPortTryCount) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig
                .getHazelcastConfig()
                .setProperty("hazelcast.tcp.join.port.try.count", String.valueOf(joinPortTryCount));
        return SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    private CoordinatorService newMockCoordinatorService(SeaTunnelServer server) {
        return newMockCoordinatorService(server, new EngineConfig());
    }

    private CoordinatorService newMockCoordinatorService(
            SeaTunnelServer server, EngineConfig engineConfig) {
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        ILogger logger = Mockito.mock(ILogger.class);
        HazelcastInstanceImpl hazelcastInstance = Mockito.mock(HazelcastInstanceImpl.class);
        IMap<Object, Object> map = Mockito.mock(IMap.class);
        Mockito.when(map.entrySet()).thenReturn(Collections.emptySet());
        Mockito.when(nodeEngine.getLogger(Mockito.any(Class.class))).thenReturn(logger);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(hazelcastInstance.getMap(Mockito.anyString())).thenReturn(map);

        CoordinatorService coordinatorService =
                new CoordinatorService(nodeEngine, server, engineConfig);
        stopCoordinatorSchedulers(coordinatorService);
        return coordinatorService;
    }

    private void stopCoordinatorSchedulers(CoordinatorService coordinatorService) {
        ReflectionUtils.getField(coordinatorService, "masterActiveListener")
                .map(ScheduledExecutorService.class::cast)
                .ifPresent(ScheduledExecutorService::shutdownNow);
        ReflectionUtils.getField(coordinatorService, "pipelineCleanupScheduler")
                .map(ScheduledExecutorService.class::cast)
                .ifPresent(ScheduledExecutorService::shutdownNow);
    }

    private JobMaster enqueueMockPendingJob(
            CoordinatorService coordinatorService, long jobId, CountDownLatch runLatch) {
        return enqueueMockPendingJob(coordinatorService, jobId, runLatch, true);
    }

    private JobMaster enqueueMockPendingJob(
            CoordinatorService coordinatorService,
            long jobId,
            CountDownLatch runLatch,
            boolean preApplyResourceResult) {
        JobMaster jobMaster = Mockito.mock(JobMaster.class);
        PhysicalPlan physicalPlan = Mockito.mock(PhysicalPlan.class);
        @SuppressWarnings("unchecked")
        PassiveCompletableFuture<JobResult> completionFuture =
                Mockito.mock(PassiveCompletableFuture.class);

        Mockito.when(jobMaster.getJobId()).thenReturn(jobId);
        Mockito.when(jobMaster.preApplyResources()).thenReturn(preApplyResourceResult);
        Mockito.when(jobMaster.getPhysicalPlan()).thenReturn(physicalPlan);
        Mockito.when(jobMaster.getJobMasterCompleteFuture()).thenReturn(completionFuture);
        Mockito.when(completionFuture.isCancelled()).thenReturn(false);
        Mockito.when(completionFuture.isCompletedExceptionally()).thenReturn(false);
        Mockito.when(physicalPlan.getJobFullName()).thenReturn("mock-job-" + jobId);
        Mockito.doAnswer(
                        invocation -> {
                            runLatch.countDown();
                            return null;
                        })
                .when(jobMaster)
                .run();

        coordinatorService
                .getPendingJobQueue()
                .put(new PendingJobInfo(PendingSourceState.SUBMIT, jobMaster));
        return jobMaster;
    }

    private ThreadPoolExecutor getCoordinatorExecutor(CoordinatorService coordinatorService) {
        return ReflectionUtils.getField(coordinatorService, "executorService")
                .map(ThreadPoolExecutor.class::cast)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "Failed to get coordinator executorService by reflection"));
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
    void testShutdownDoesNotInterruptCoordinatorCleanupThread() throws Exception {
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createHazelcastInstance(
                        TestUtils.getClusterName(
                                "CoordinatorServiceTest_testShutdownDoesNotInterruptCoordinatorCleanupThread"));
        try {
            SeaTunnelServer server =
                    instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            await().atMost(20000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            server.getCoordinatorService().isCoordinatorActive()));

            CoordinatorService coordinatorService = server.getCoordinatorService();
            BlockingEventProcessor blockingEventProcessor = new BlockingEventProcessor();
            ReflectionUtils.setField(coordinatorService, "eventProcessor", blockingEventProcessor);

            ScheduledExecutorService masterActiveListener =
                    (ScheduledExecutorService)
                            ReflectionUtils.getField(coordinatorService, "masterActiveListener")
                                    .orElseThrow(
                                            () ->
                                                    new AssertionError(
                                                            "masterActiveListener not found"));

            Future<?> clearFuture =
                    masterActiveListener.submit(coordinatorService::clearCoordinatorService);
            Assertions.assertTrue(blockingEventProcessor.awaitCloseStarted(5, TimeUnit.SECONDS));

            Thread shutdownThread =
                    new Thread(coordinatorService::shutdown, "coordinator-service-shutdown-test");
            shutdownThread.start();

            blockingEventProcessor.releaseClose();

            shutdownThread.join(TimeUnit.SECONDS.toMillis(20));
            Assertions.assertFalse(shutdownThread.isAlive());
            clearFuture.get(20, TimeUnit.SECONDS);

            Assertions.assertEquals(1, blockingEventProcessor.getCloseCount());
            Assertions.assertFalse(blockingEventProcessor.wasInterrupted());
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

    private static final class BlockingEventProcessor implements EventProcessor {
        private final CountDownLatch closeStarted = new CountDownLatch(1);
        private final CountDownLatch releaseClose = new CountDownLatch(1);
        private final AtomicBoolean interrupted = new AtomicBoolean(false);
        private final AtomicInteger closeCount = new AtomicInteger(0);

        @Override
        public void process(Event event) {}

        @Override
        public void close() {
            closeCount.incrementAndGet();
            closeStarted.countDown();
            try {
                releaseClose.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        }

        boolean awaitCloseStarted(long timeout, TimeUnit unit) throws InterruptedException {
            return closeStarted.await(timeout, unit);
        }

        void releaseClose() {
            releaseClose.countDown();
        }

        int getCloseCount() {
            return closeCount.get();
        }

        boolean wasInterrupted() {
            return interrupted.get();
        }
    }

    @Test
    void testCollectRunningWorkerAddressesIgnoresNullOwnedSlotProfiles() throws Exception {
        Set<Long> runningJobIds = Collections.singleton(1L);
        Assertions.assertTrue(
                CoordinatorService.collectRunningWorkerAddresses(null, runningJobIds).isEmpty());

        Address worker = new Address("127.0.0.1", 5801);
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                new HashMap<>();
        ownedSlotProfiles.put(null, Collections.emptyMap());
        ownedSlotProfiles.put(new PipelineLocation(1L, 1), null);

        Map<TaskGroupLocation, SlotProfile> pipelineOwnedSlotProfiles = new HashMap<>();
        pipelineOwnedSlotProfiles.put(new TaskGroupLocation(1L, 1, 1L), null);
        pipelineOwnedSlotProfiles.put(
                new TaskGroupLocation(1L, 1, 2L), new SlotProfile(worker, 1, null, "slot-1"));
        pipelineOwnedSlotProfiles.put(
                new TaskGroupLocation(2L, 1, 1L), new SlotProfile(null, 2, null, "slot-2"));
        ownedSlotProfiles.put(new PipelineLocation(1L, 2), pipelineOwnedSlotProfiles);
        ownedSlotProfiles.put(new PipelineLocation(2L, 1), pipelineOwnedSlotProfiles);

        Assertions.assertEquals(
                Collections.singleton(worker),
                CoordinatorService.collectRunningWorkerAddresses(ownedSlotProfiles, runningJobIds));
    }

    @Test
    void testForceStopRunningJob() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testForceStopRunningJob",
                        "stream_fake_to_console.conf",
                        "test_force_stop_running_job");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;

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

        coordinatorService.stopJob(jobInformation.jobId).join();
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JobStatus.CANCELED,
                                    coordinatorService.getJobStatus(jobInformation.jobId));
                        });
        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();
    }

    @Test
    void testForceStopAbnormalSavepointJob() {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testForceStopAbnormalSavepointJob",
                        "stream_fake_to_console.conf",
                        "test_force_stop_abnormal_savepoint_job");
        CoordinatorService coordinatorService = jobInformation.coordinatorService;

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

        coordinatorService
                .getJobMaster(jobInformation.jobId)
                .getPhysicalPlan()
                .updateJobState(JobStatus.DOING_SAVEPOINT);
        coordinatorService.stopJob(jobInformation.jobId).join();
        await().atMost(120000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    JobStatus.CANCELED,
                                    coordinatorService.getJobStatus(jobInformation.jobId));
                        });
        jobInformation.coordinatorService.clearCoordinatorService();
        jobInformation.coordinatorServiceTest.shutdown();
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
    @SetEnvironmentVariable(key = SKIP_CHECK_JAR, value = "true")
    void testSubmitJobOperationCanCompleteOnHazelcastOperationThread() {
        String clusterName =
                TestUtils.getClusterName(
                        "CoordinatorServiceTest_testSubmitJobOperationCanCompleteOnHazelcastOperationThread");
        HazelcastInstanceImpl instance =
                SeaTunnelServerStarter.createHazelcastInstance(clusterName);
        try {
            SeaTunnelServer server =
                    instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            Long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
            LogicalDag testLogicalDag =
                    TestUtils.createTestLogicalPlan(
                            "batch_fake_to_console.conf",
                            "test_submit_job_operation_can_complete",
                            jobId);

            JobImmutableInformation jobImmutableInformation =
                    new JobImmutableInformation(
                            jobId,
                            "Test",
                            instance.getSerializationService(),
                            testLogicalDag,
                            Collections.emptyList(),
                            Collections.emptyList());

            Data data = instance.getSerializationService().toData(jobImmutableInformation);

            Assertions.assertDoesNotThrow(
                    () ->
                            NodeEngineUtil.sendOperationToMasterNode(
                                            instance.node.getNodeEngine(),
                                            new SubmitJobOperation(
                                                    jobId,
                                                    data,
                                                    jobImmutableInformation.isStartWithSavePoint()))
                                    .join());

            await().atMost(10000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertNotEquals(
                                            JobStatus.PENDING,
                                            server.getCoordinatorService().getJobStatus(jobId)));
        } finally {
            instance.shutdown();
        }
    }

    @Test
    void testGetPendingJobInfo() {
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        CoordinatorService coordinatorService = newMockCoordinatorService(server);
        try {
            Long jobId = 70001L;
            JobHistoryService jobHistoryService = Mockito.mock(JobHistoryService.class);
            ReflectionUtils.setField(coordinatorService, "jobHistoryService", jobHistoryService);
            Mockito.when(jobHistoryService.getJobDAGInfo(jobId)).thenReturn(null);

            JobDAGInfo pendingJobDAGInfo = new JobDAGInfo();
            pendingJobDAGInfo.setJobId(jobId);
            JobMaster jobMaster =
                    enqueueMockPendingJob(coordinatorService, jobId, new CountDownLatch(1));
            Mockito.when(jobMaster.getJobDAGInfo()).thenReturn(pendingJobDAGInfo);

            Assertions.assertTrue(coordinatorService.getPendingJobQueue().contains(jobId));

            JobDAGInfo jobDAGInfo =
                    Assertions.assertDoesNotThrow(() -> coordinatorService.getJobInfo(jobId));
            Assertions.assertSame(pendingJobDAGInfo, jobDAGInfo);
            Assertions.assertEquals(jobId, jobDAGInfo.getJobId());
        } finally {
            coordinatorService.shutdown();
        }
    }

    @Test
    void testGetJobInfoFallsBackToRunningJobInfo() throws Exception {
        JobInformation jobInformation =
                submitJob(
                        "CoordinatorServiceTest_testGetJobInfoFallsBackToRunningJobInfo",
                        "batch_fake_to_console.conf",
                        "test_get_job_info_running_job_info_fallback");

        CoordinatorService coordinatorService = jobInformation.coordinatorService;
        Long jobId = jobInformation.jobId;

        coordinatorService.getPendingJobQueue().removeById(jobId);
        getRunningJobMasterMap(coordinatorService).remove(jobId);

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

    @SuppressWarnings("unchecked")
    private Map<Long, JobMaster> getRunningJobMasterMap(CoordinatorService coordinatorService)
            throws Exception {
        Field field = CoordinatorService.class.getDeclaredField("runningJobMasterMap");
        field.setAccessible(true);
        return (Map<Long, JobMaster>) field.get(coordinatorService);
    }

    private void invokeRestoreJobFromMasterActiveSwitch(
            CoordinatorService coordinatorService, long jobId, JobInfo jobInfo) throws Exception {
        Method method =
                CoordinatorService.class.getDeclaredMethod(
                        "restoreJobFromMasterActiveSwitch", Long.class, JobInfo.class);
        method.setAccessible(true);
        method.invoke(coordinatorService, jobId, jobInfo);
    }

    private void invokeCheckNewActiveMaster(CoordinatorService coordinatorService)
            throws Exception {
        Method method = CoordinatorService.class.getDeclaredMethod("checkNewActiveMaster");
        method.setAccessible(true);
        method.invoke(coordinatorService);
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
    void testRestoreUsesProvidedJobInfoInitializationTimestamp() throws Exception {
        HazelcastInstanceImpl instance =
                createHazelcastInstanceWithJoinPortTryCount(
                        TestUtils.getClusterName(
                                "CoordinatorServiceTest_testRestoreUsesProvidedJobInfoInitializationTimestamp"),
                        1);
        try {
            SeaTunnelServer server =
                    instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
            CoordinatorService coordinatorService = server.getCoordinatorService();
            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertTrue(coordinatorService.isCoordinatorActive()));

            Long jobId = instance.getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME).newId();
            long initializationTimestamp = 100L;
            LogicalDag logicalDag =
                    TestUtils.createTestLogicalPlan(
                            "stream_fake_to_console.conf",
                            "restore-job-info-initialization-timestamp-test",
                            jobId);
            JobImmutableInformation jobImmutableInformation =
                    new JobImmutableInformation(
                            jobId,
                            "Test",
                            instance.getSerializationService(),
                            logicalDag,
                            Collections.emptyList(),
                            Collections.emptyList());
            JobInfo jobInfo =
                    new JobInfo(
                            initializationTimestamp,
                            instance.getSerializationService().toData(jobImmutableInformation));

            IMap<Long, JobInfo> runningJobInfoIMap =
                    instance.getMap(Constant.IMAP_RUNNING_JOB_INFO);
            IMap<Object, Object> runningJobStateIMap =
                    instance.getMap(Constant.IMAP_RUNNING_JOB_STATE);
            IMap<Object, Long[]> runningJobStateTimestampsIMap =
                    instance.getMap(Constant.IMAP_STATE_TIMESTAMPS);
            runningJobInfoIMap.put(jobId, jobInfo);
            runningJobStateIMap.put(jobId, JobStatus.RUNNING);

            IMap<Long, JobInfo> spiedRunningJobInfoIMap = Mockito.spy(runningJobInfoIMap);
            doThrow(new RetryableHazelcastException("loading"))
                    .when(spiedRunningJobInfoIMap)
                    .get(jobId);
            ReflectionUtils.setField(
                    coordinatorService, "runningJobInfoIMap", spiedRunningJobInfoIMap);
            try {
                invokeRestoreJobFromMasterActiveSwitch(coordinatorService, jobId, jobInfo);
            } finally {
                ReflectionUtils.setField(
                        coordinatorService, "runningJobInfoIMap", runningJobInfoIMap);
            }

            await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            coordinatorService.getPendingJobQueue().contains(jobId)
                                                    || getRunningJobMasterMap(coordinatorService)
                                                            .containsKey(jobId)));
            Long[] jobStateTimestamps = runningJobStateTimestampsIMap.get(jobId);
            Assertions.assertNotNull(jobStateTimestamps);
            Assertions.assertEquals(
                    initializationTimestamp, jobStateTimestamps[JobStatus.INITIALIZING.ordinal()]);
        } finally {
            instance.shutdown();
        }
    }

    @Test
    @Disabled("Disabled because we can't know when the master node switches in the unit tests")
    void testJobRestoreWhenMasterNodeSwitch() {
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
