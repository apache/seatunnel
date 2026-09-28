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

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.resource.yarn.cli.SeatunnelYarnWorkerCli;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YarnResourceManagerDriverTest {
    @TempDir File temporary;

    @Test
    void returnsAllocatedContainerAndReleasesTheSameNodeOnce() throws Exception {
        Files.write(
                temporary.toPath().resolve("distribution.properties"),
                Arrays.asList("archive=distribution.zip", "root="));
        for (String file :
                Arrays.asList(
                        "distribution.zip",
                        YarnConstants.LOCALIZED_SPECIFICATION_NAME,
                        YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME)) {
            Files.write(temporary.toPath().resolve(file), new byte[] {1});
        }
        AMRMClient<AMRMClient.ContainerRequest> resourceManager = mock(AMRMClient.class);
        NMClient nodeManager = mock(NMClient.class);
        ResourceManagerContext context = mock(ResourceManagerContext.class);
        when(context.getMasterAddress()).thenReturn("localhost:5801");
        AllocateResponse empty = Records.newRecord(AllocateResponse.class);
        AtomicReference<AllocateResponse> nextResponse = new AtomicReference<>(empty);
        when(resourceManager.allocate(anyFloat()))
                .thenAnswer(invocation -> nextResponse.getAndSet(empty));
        Container container = Records.newRecord(Container.class);
        container.setId(ContainerId.fromString("container_1_0001_01_000001"));
        container.setNodeId(NodeId.newInstance("localhost", 1234));
        String previousHome = System.getProperty("seatunnel.home");
        System.setProperty("seatunnel.home", temporary.toString());
        try (YarnResourceManagerDriver driver =
                new YarnResourceManagerDriver(
                        localConfiguration(),
                        new Path(temporary.toURI()),
                        "application-test",
                        null,
                        resourceManager,
                        nodeManager)) {
            driver.initialize(context);
            CompletableFuture<YarnWorkerNode> requested =
                    driver.requestWorker(new WorkerSpecification(512, 1, 2));
            AllocateResponse allocated = Records.newRecord(AllocateResponse.class);
            allocated.setAllocatedContainers(Collections.singletonList(container));
            nextResponse.set(allocated);
            driver.heartbeat();
            YarnWorkerNode worker = requested.get(5, TimeUnit.SECONDS);
            ArgumentCaptor<ContainerLaunchContext> launch =
                    ArgumentCaptor.forClass(ContainerLaunchContext.class);
            verify(nodeManager).startContainer(any(), launch.capture());
            String command = launch.getValue().getCommands().get(0);
            assertTrue(command.contains(SeatunnelYarnWorkerCli.class.getName()));
            assertTrue(command.contains("'application-test' 'localhost:5801' '2'"));
            assertTrue(command.contains("'" + temporary + "'"));
            assertSame(container, worker.getContainer());
            assertEquals(
                    container.getId().toString(), worker.getResourceID().getResourceIdString());
            assertNull(driver.releaseWorker(worker).get());
            assertNull(driver.releaseWorker(worker).get());
            verify(nodeManager, times(1)).stopContainer(container.getId(), container.getNodeId());
            verify(resourceManager, times(1)).releaseAssignedContainer(container.getId());
            verify(context, never()).onWorkerTerminated(anyString(), anyString());
        } finally {
            if (previousHome == null) {
                System.clearProperty("seatunnel.home");
            } else {
                System.setProperty("seatunnel.home", previousHome);
            }
        }
    }

    @Test
    void initializationFailureDoesNotStopAnUnopenedNodeManager() throws Exception {
        AMRMClient<AMRMClient.ContainerRequest> resourceManager = mock(AMRMClient.class);
        NMClient nodeManager = mock(NMClient.class);
        doThrow(new IllegalStateException("RM initialization failed"))
                .when(resourceManager)
                .init(any());
        YarnResourceManagerDriver driver =
                new YarnResourceManagerDriver(
                        localConfiguration(),
                        new Path(temporary.toURI()),
                        "application-test",
                        null,
                        resourceManager,
                        nodeManager);
        assertThrows(
                IllegalStateException.class,
                () -> driver.initialize(mock(ResourceManagerContext.class)));
        driver.close();
        verify(nodeManager, never()).stop();
        verify(resourceManager).stop();
    }

    @Test
    void allocationFailureCompletesWaitingWorkersAndClosesClients() throws Exception {
        AMRMClient<AMRMClient.ContainerRequest> resourceManager = mock(AMRMClient.class);
        NMClient nodeManager = mock(NMClient.class);
        ResourceManagerContext context = mock(ResourceManagerContext.class);
        when(context.getMasterAddress()).thenReturn("localhost:5801");
        // First heartbeat is empty; a later transport failure must fail every queued request.
        AllocateResponse empty = Records.newRecord(AllocateResponse.class);
        when(resourceManager.allocate(anyFloat())).thenReturn(empty);
        YarnResourceManagerDriver driver =
                new YarnResourceManagerDriver(
                        localConfiguration(),
                        new Path(temporary.toURI()),
                        "application-test",
                        null,
                        resourceManager,
                        nodeManager);
        driver.initialize(context);
        CompletableFuture<YarnWorkerNode> worker =
                driver.requestWorker(new WorkerSpecification(512, 1, 2));
        when(resourceManager.allocate(anyFloat()))
                .thenThrow(new IOException("resource manager unavailable"));
        driver.heartbeat();
        assertTrue(worker.isCompletedExceptionally());
        driver.close();
        verify(resourceManager).removeContainerRequest(any());
        verify(nodeManager).stop();
        verify(resourceManager).stop();
    }

    @Test
    void workerRequestsUseConfiguredNodeLabel() throws Exception {
        AMRMClient<AMRMClient.ContainerRequest> resourceManager = mock(AMRMClient.class);
        NMClient nodeManager = mock(NMClient.class);
        ResourceManagerContext context = mock(ResourceManagerContext.class);
        when(context.getMasterAddress()).thenReturn("localhost:5801");
        when(resourceManager.allocate(anyFloat()))
                .thenReturn(Records.newRecord(AllocateResponse.class));
        YarnResourceManagerDriver driver =
                new YarnResourceManagerDriver(
                        localConfiguration(),
                        new Path(temporary.toURI()),
                        "application-test",
                        "worker-pool",
                        resourceManager,
                        nodeManager);
        driver.initialize(context);
        driver.requestWorker(new WorkerSpecification(512, 1, 2));
        ArgumentCaptor<AMRMClient.ContainerRequest> request =
                ArgumentCaptor.forClass(AMRMClient.ContainerRequest.class);
        verify(resourceManager).addContainerRequest(request.capture());
        assertEquals("worker-pool", request.getValue().getNodeLabelExpression());
        driver.close();
    }

    @Test
    void excessContainersAreReleasedWithoutLaunchingWorkers() throws Exception {
        AMRMClient<AMRMClient.ContainerRequest> resourceManager = mock(AMRMClient.class);
        NMClient nodeManager = mock(NMClient.class);
        ResourceManagerContext context = mock(ResourceManagerContext.class);
        when(context.getMasterAddress()).thenReturn("localhost:5801");
        AllocateResponse response = Records.newRecord(AllocateResponse.class);
        Container container = Records.newRecord(Container.class);
        container.setId(ContainerId.fromString("container_1_0001_01_000001"));
        response.setAllocatedContainers(Collections.singletonList(container));
        when(resourceManager.allocate(anyFloat())).thenReturn(response);
        YarnResourceManagerDriver driver =
                new YarnResourceManagerDriver(
                        localConfiguration(),
                        new Path(temporary.toURI()),
                        "application-test",
                        null,
                        resourceManager,
                        nodeManager);
        driver.initialize(context);
        driver.heartbeat();
        driver.close();
        verify(nodeManager, never()).startContainer(any(), any());
        verify(resourceManager, atLeastOnce()).releaseAssignedContainer(container.getId());
    }

    private Configuration localConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        return configuration;
    }
}
