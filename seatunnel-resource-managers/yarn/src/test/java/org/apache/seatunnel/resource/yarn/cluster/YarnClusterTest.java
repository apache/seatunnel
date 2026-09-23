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

package org.apache.seatunnel.resource.yarn.cluster;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.util.Records;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class YarnClusterTest {
    @TempDir File temporary;

    @Test
    void nativeTarGzRetainsArchiveTypeAndDistributionRoot() throws Exception {
        String distributionRoot = "apache-seatunnel-test-version/";
        File archive = new File(temporary, "native-distribution.tar.gz");
        try (TarArchiveOutputStream tar =
                new TarArchiveOutputStream(
                        new GZIPOutputStream(Files.newOutputStream(archive.toPath())))) {
            TarArchiveEntry entry =
                    new TarArchiveEntry(distributionRoot + "starter/seatunnel-starter.jar");
            entry.setSize(1);
            tar.putArchiveEntry(entry);
            tar.write(1);
            tar.closeArchiveEntry();
        }
        YarnDistribution layout = YarnDistribution.inspect(archive);
        assertEquals("seatunnel/" + distributionRoot, layout.localizedHome());
        assertEquals("distribution.tar.gz", layout.archive(new Path("/staging")).getName());
    }

    @Test
    void unsafeArchiveCannotBeSubmitted() throws Exception {
        File archive = new File(temporary, "unsafe.zip");
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive.toPath()))) {
            zip.putNextEntry(new ZipEntry("../starter/seatunnel-starter.jar"));
            zip.closeEntry();
        }
        assertThrows(IllegalArgumentException.class, () -> YarnDistribution.inspect(archive));
    }

    @Test
    void shellArgumentsRemainLiteral() {
        assertEquals(
                "'worker'\"'\"'s $(touch /tmp/unsafe)'",
                YarnContainerLaunch.quote("worker's $(touch /tmp/unsafe)"));
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
                        resourceManager,
                        nodeManager);
        driver.initialize(context);
        CompletableFuture<WorkerRegistration> worker =
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
