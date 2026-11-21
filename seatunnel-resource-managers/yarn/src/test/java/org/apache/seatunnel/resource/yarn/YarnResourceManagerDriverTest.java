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

package org.apache.seatunnel.resource.yarn;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.runtime.ApplicationStatus;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ApplicationResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.worker.SeatunnelYarnApplicationWorker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import com.hazelcast.spi.impl.NodeEngine;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "The YARN tests use a Unix container environment")
@Timeout(25)
class YarnResourceManagerDriverTest {
    @TempDir File temporary;
    private final AMRMClient<AMRMClient.ContainerRequest> resourceManager = mock(AMRMClient.class);
    private final NMClient nodeManager = mock(NMClient.class);
    private final ResourceEventHandler<YarnWorkerNode> events = mock(ResourceEventHandler.class);
    private final ScheduledExecutorService mainThreadExecutor =
            mock(ScheduledExecutorService.class);
    private final ExecutorService ioExecutor = Executors.newSingleThreadExecutor();
    private final Queue<AllocateResponse> responses = new ConcurrentLinkedQueue<>();
    private volatile Exception heartbeatFailure;
    private final WorkerSpecification specification = new WorkerSpecification(512, 1, 2);
    private YarnResourceManagerDriver driver;

    @BeforeEach
    void prepareClients() throws Exception {
        doAnswer(
                        invocation -> {
                            ((Runnable) invocation.getArgument(0)).run();
                            return null;
                        })
                .when(mainThreadExecutor)
                .execute(any());
        // The real AMRMClientAsync delivers responses from this transport to the registered
        // callbacks.
        when(resourceManager.allocate(anyFloat()))
                .thenAnswer(
                        invocation -> {
                            if (heartbeatFailure != null) {
                                throw heartbeatFailure;
                            }
                            return responses.poll();
                        });
        File archive = new File(temporary, "distribution.zip");
        Files.write(archive.toPath(), new byte[] {1});
        Files.write(
                temporary.toPath().resolve(YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME),
                new byte[] {1});
        Files.write(
                temporary.toPath().resolve(YarnConstants.DISTRIBUTION_MANIFEST),
                List.of(
                        "archive=" + archive.toURI(),
                        "archive.size=" + archive.length(),
                        "archive.timestamp=" + archive.lastModified(),
                        "root="));
    }

    @AfterEach
    void closeClients() throws Exception {
        try {
            if (driver != null) {
                driver.close();
            }
        } finally {
            verify(mainThreadExecutor, never()).shutdown();
            verify(mainThreadExecutor, never()).shutdownNow();
            assertFalse(ioExecutor.isShutdown());
            ioExecutor.shutdownNow();
            assertTrue(ioExecutor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {0, 1})
    void reportsOnlyAbnormalContainerExitsAndIgnoresIntentionalRelease(Integer exitStatus)
            throws Exception {
        startDriver(null);
        Container container = container(1);
        CompletableFuture<YarnWorkerNode> request = driver.requestWorker(specification);
        allocate(container);
        YarnWorkerNode worker = request.get(5, TimeUnit.SECONDS);
        ArgumentCaptor<ContainerLaunchContext> launch =
                ArgumentCaptor.forClass(ContainerLaunchContext.class);
        verify(nodeManager).startContainer(any(), launch.capture());
        String command = launch.getValue().getCommands().get(0);
        assertTrue(command.contains(SeatunnelYarnApplicationWorker.class.getName()));
        assertTrue(command.contains("'application-test' 'localhost:5801' '2'"));
        assertTrue(command.contains("'" + temporary + "'"));
        assertSame(container, worker.getContainer());
        assertEquals(container.getId().toString(), worker.getResourceID().getResourceIdString());

        if (exitStatus == null) {
            assertNull(driver.releaseWorker(worker).get(5, TimeUnit.SECONDS));
            assertNull(driver.releaseWorker(worker).get(5, TimeUnit.SECONDS));
            verify(nodeManager).stopContainer(container.getId(), container.getNodeId());
            verify(resourceManager).releaseAssignedContainer(container.getId());
        }
        completeAndAwaitDispatch(container, exitStatus == null ? 1 : exitStatus);
        if (Integer.valueOf(1).equals(exitStatus)) {
            verify(events)
                    .onWorkerTerminated(
                            worker, "YARN container exited with status 1: worker completed");
        } else {
            verifyNoInteractions(events);
        }
    }

    @Test
    void initializationFailureDoesNotStopAnUnopenedNodeManager() throws Exception {
        when(nodeManager.isInState(Service.STATE.NOTINITED)).thenReturn(true);
        doThrow(new IllegalStateException("RM initialization failed"))
                .when(resourceManager)
                .init(any());
        assertThrows(YarnClientException.class, () -> startDriver(null));
        driver.close();
        verify(nodeManager, never()).stop();
        verify(resourceManager, never())
                .unregisterApplicationMaster(any(), anyString(), anyString());
        verify(resourceManager).stop();
    }

    @ParameterizedTest
    @CsvSource({"SUCCEEDED, SUCCEEDED", "CANCELED, KILLED", "FAILED, FAILED"})
    void resourceManagerCloseRetriesTheOriginalFinalReport(
            ApplicationStatus status, FinalApplicationStatus expected) throws Exception {
        String diagnostics = "Job ended with " + status;
        IOException retry = new IOException("final report unavailable");
        doThrow(retry)
                .doNothing()
                .when(resourceManager)
                .unregisterApplicationMaster(expected, diagnostics, "");
        startDriver(null);
        ApplicationResourceManager<?> manager =
                (ApplicationResourceManager<?>)
                        new ResourceManagerFactory(
                                        DeployType.YARN,
                                        "application-test",
                                        SeatunnelApplicationConfig.parse("env {}", Map.of()),
                                        driver)
                                .createResourceManager(mock(NodeEngine.class), new EngineConfig());
        try {
            manager.stopApplicationWorkers();
            assertTrue(driver.requestWorker(specification).isCompletedExceptionally());
            verify(resourceManager, never())
                    .unregisterApplicationMaster(any(), anyString(), anyString());
            assertSame(
                    retry,
                    assertThrows(
                                    YarnClientException.class,
                                    () -> manager.finish(status, diagnostics))
                            .getCause());
        } finally {
            manager.close();
        }
        verify(resourceManager, times(2)).unregisterApplicationMaster(expected, diagnostics, "");
    }

    @Test
    void closingWithoutAnApplicationResultReportsFailure() throws Exception {
        startDriver(null);
        driver.close();
        verify(resourceManager)
                .unregisterApplicationMaster(
                        FinalApplicationStatus.FAILED,
                        "Application master stopped before job completion",
                        "");
    }

    @Test
    void allocationFailureCompletesWaitingWorkersAndClosesClients() throws Exception {
        startDriver(null);
        CompletableFuture<YarnWorkerNode> first = driver.requestWorker(specification);
        CompletableFuture<YarnWorkerNode> second = driver.requestWorker(specification);
        IOException transport = new IOException("resource manager unavailable");
        heartbeatFailure = transport;
        Throwable failure =
                assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS))
                        .getCause();
        assertTrue(failure instanceof YarnClientException);
        assertSame(transport, failure.getCause());
        assertSame(
                failure,
                assertThrows(ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS))
                        .getCause());
        verify(events, timeout(5000)).onError(failure);
        driver.close();
        verify(resourceManager, times(2)).removeContainerRequest(any());
        verify(nodeManager).stop();
        verify(resourceManager).stop();
    }

    @Test
    void failedRequestWithdrawalIsRetriedDuringShutdown() throws Exception {
        RuntimeException transport = new IllegalStateException("request withdrawal failed");
        doThrow(transport).doNothing().when(resourceManager).removeContainerRequest(any());
        startDriver(null);
        CompletableFuture<YarnWorkerNode> request = driver.requestWorker(specification);
        Container container = container(1);
        allocate(container);

        Throwable failure =
                assertThrows(ExecutionException.class, () -> request.get(5, TimeUnit.SECONDS))
                        .getCause();
        assertTrue(failure instanceof YarnClientException);
        assertSame(transport, failure.getCause());
        verify(events, timeout(5000)).onError(failure);
        driver.stopWorkers();
        verify(resourceManager, times(2)).removeContainerRequest(any());
        verify(resourceManager).releaseAssignedContainer(container.getId());
        verify(nodeManager, never()).startContainer(any(), any());
    }

    @Test
    void shutdownRequestStopsAdmissionAndNotifiesTheEngine() throws Exception {
        startDriver(null);
        CompletableFuture<YarnWorkerNode> waiting = driver.requestWorker(specification);
        heartbeatFailure = new ApplicationAttemptNotFoundException("attempt removed");
        Throwable failure =
                assertThrows(ExecutionException.class, () -> waiting.get(5, TimeUnit.SECONDS))
                        .getCause();
        assertTrue(failure instanceof YarnClientException);
        assertTrue(failure.getMessage().contains("requested shutdown"));
        verify(events, timeout(5000)).onError(failure);
        assertTrue(driver.requestWorker(specification).isCompletedExceptionally());
    }

    @Test
    void workerRequestsUseConfiguredNodeLabel() throws Exception {
        startDriver("worker-pool");
        driver.requestWorker(specification);
        ArgumentCaptor<AMRMClient.ContainerRequest> request =
                ArgumentCaptor.forClass(AMRMClient.ContainerRequest.class);
        verify(resourceManager).addContainerRequest(request.capture());
        assertEquals("worker-pool", request.getValue().getNodeLabelExpression());
        assertEquals(
                specification.getMemoryMb(), request.getValue().getCapability().getMemorySize());
        assertEquals(
                specification.getCpuCores(), request.getValue().getCapability().getVirtualCores());
        assertTrue(request.getValue().getRelaxLocality());
    }

    @Test
    void nodeAndResourceUpdatesLeaveTheRunningWorkerRegistered() throws Exception {
        startDriver(null);
        Container container = container(1);
        CompletableFuture<YarnWorkerNode> request = driver.requestWorker(specification);
        allocate(container);
        YarnWorkerNode worker = request.get(5, TimeUnit.SECONDS);

        NodeReport node = Records.newRecord(NodeReport.class);
        node.setNodeId(container.getNodeId());
        node.setNodeState(NodeState.DECOMMISSIONING);
        AllocateResponse updates = Records.newRecord(AllocateResponse.class);
        updates.setUpdatedNodes(List.of(node));
        Container resized = container(1);
        resized.setResource(Resource.newInstance(1024, 2));
        updates.setUpdatedContainers(
                List.of(
                        UpdatedContainer.newInstance(
                                ContainerUpdateType.INCREASE_RESOURCE, resized)));
        Container excess = container(2);
        updates.setAllocatedContainers(List.of(excess));
        responses.add(updates);
        verify(resourceManager, timeout(5000)).releaseAssignedContainer(excess.getId());
        verifyNoInteractions(events);

        completeAndAwaitDispatch(container, 1);
        verify(events).onWorkerTerminated(eq(worker), anyString());
    }

    @Test
    void lateAllocationsAreReturnedAfterShutdownWithoutLaunchingWorkers() throws Exception {
        startDriver(null);
        driver.stopWorkers();
        Container container = container(1);
        allocate(container);
        verify(resourceManager, timeout(5000)).releaseAssignedContainer(container.getId());
        verify(nodeManager, never()).startContainer(any(), any());
        verifyNoInteractions(events);
    }

    @Test
    void completedContainerIsReportedDuringAnotherNodeManagerLaunch() throws Exception {
        startDriver(null);
        Container first = container(1);
        CompletableFuture<YarnWorkerNode> requested = driver.requestWorker(specification);
        allocate(first);
        YarnWorkerNode running = requested.get(5, TimeUnit.SECONDS);
        Container second = container(2);
        CountDownLatch starting = new CountDownLatch(1);
        CountDownLatch allowStart = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            starting.countDown();
                            assertTrue(allowStart.await(10, TimeUnit.SECONDS));
                            return null;
                        })
                .when(nodeManager)
                .startContainer(eq(second), any());
        CompletableFuture<YarnWorkerNode> another = driver.requestWorker(specification);
        allocate(second);
        try {
            assertTrue(starting.await(5, TimeUnit.SECONDS));
            completeAndAwaitDispatch(first, 1);
            verify(events).onWorkerTerminated(eq(running), anyString());
            assertFalse(another.isDone());
        } finally {
            allowStart.countDown();
        }
        assertEquals(second.getId(), another.get(5, TimeUnit.SECONDS).getContainerId());
    }

    @Test
    void canceledLaunchStopsTheContainerAfterNodeManagerAcceptsIt() throws Exception {
        startDriver(null);
        Container container = container(1);
        CountDownLatch starting = new CountDownLatch(1);
        CountDownLatch allowStart = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            starting.countDown();
                            assertTrue(allowStart.await(10, TimeUnit.SECONDS));
                            return null;
                        })
                .when(nodeManager)
                .startContainer(eq(container), any());
        CompletableFuture<YarnWorkerNode> request = driver.requestWorker(specification);
        allocate(container);
        try {
            assertTrue(starting.await(5, TimeUnit.SECONDS));
            assertTrue(request.cancel(false));
            verify(nodeManager, never()).stopContainer(any(), any());
        } finally {
            allowStart.countDown();
        }
        driver.stopWorkers();
        verify(nodeManager).stopContainer(container.getId(), container.getNodeId());
        verify(resourceManager).releaseAssignedContainer(container.getId());
        verifyNoInteractions(events);
    }

    @Test
    void shutdownRetriesAnUnacknowledgedWorkerRelease() throws Exception {
        startDriver(null);
        Container container = container(1);
        CompletableFuture<YarnWorkerNode> request = driver.requestWorker(specification);
        allocate(container);
        YarnWorkerNode worker = request.get(5, TimeUnit.SECONDS);
        doThrow(new IOException("stop failed"))
                .doNothing()
                .when(nodeManager)
                .stopContainer(container.getId(), container.getNodeId());
        CompletableFuture<Void> release = driver.releaseWorker(worker);
        assertTrue(
                assertThrows(ExecutionException.class, () -> release.get(5, TimeUnit.SECONDS))
                                .getCause()
                        instanceof YarnClientException);
        driver.stopWorkers();
        verify(nodeManager, times(2)).stopContainer(container.getId(), container.getNodeId());
        verifyNoInteractions(events);
    }

    @Test
    void releaseWorkerStopsWithoutBlockingTheCaller() throws Exception {
        startDriver(null);
        Container container = container(1);
        CompletableFuture<YarnWorkerNode> request = driver.requestWorker(specification);
        allocate(container);
        YarnWorkerNode worker = request.get(5, TimeUnit.SECONDS);
        CountDownLatch stopStarted = new CountDownLatch(1);
        CountDownLatch allowStop = new CountDownLatch(1);
        doAnswer(
                        invocation -> {
                            stopStarted.countDown();
                            assertTrue(allowStop.await(10, TimeUnit.SECONDS));
                            return null;
                        })
                .when(nodeManager)
                .stopContainer(any(), any());
        CompletableFuture<Void> release = driver.releaseWorker(worker);
        try {
            assertTrue(stopStarted.await(5, TimeUnit.SECONDS));
            assertFalse(release.isDone());
            assertTrue(driver.releaseWorker(worker).cancel(false));
        } finally {
            allowStop.countDown();
        }
        assertNull(release.get(5, TimeUnit.SECONDS));
        verify(nodeManager).stopContainer(container.getId(), container.getNodeId());
    }

    private void startDriver(String nodeLabel) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        driver =
                new YarnResourceManagerDriver(
                        configuration,
                        new Path(temporary.toURI()),
                        new YarnResourceManagerDriver.YarnDriverSettings(
                                "application-test",
                                nodeLabel,
                                "test-user",
                                temporary.toString(),
                                ""),
                        resourceManager,
                        nodeManager);
        driver.initialize(events, mainThreadExecutor, ioExecutor, () -> "localhost:5801");
    }

    private static Container container(int sequence) {
        Container container = Records.newRecord(Container.class);
        container.setId(ContainerId.fromString("container_1_0001_01_" + sequence));
        container.setNodeId(NodeId.newInstance("localhost", 1234));
        return container;
    }

    private void allocate(Container container) {
        AllocateResponse response = Records.newRecord(AllocateResponse.class);
        response.setAllocatedContainers(List.of(container));
        responses.add(response);
    }

    private void completeAndAwaitDispatch(Container container, int exitStatus) {
        AllocateResponse response = Records.newRecord(AllocateResponse.class);
        response.setCompletedContainersStatuses(
                List.of(
                        ContainerStatus.newInstance(
                                container.getId(),
                                ContainerState.COMPLETE,
                                "worker completed",
                                exitStatus)));
        // Hadoop dispatches completions before allocations in the same heartbeat response.
        Container excess = container(99);
        response.setAllocatedContainers(List.of(excess));
        responses.add(response);
        verify(resourceManager, timeout(5000)).releaseAssignedContainer(excess.getId());
    }
}
