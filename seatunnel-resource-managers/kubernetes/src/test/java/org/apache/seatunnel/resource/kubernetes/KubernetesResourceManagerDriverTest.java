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

package org.apache.seatunnel.resource.kubernetes;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;
import org.apache.seatunnel.resource.kubernetes.cli.SeatunnelKubernetesApplicationCli;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KubernetesResourceManagerDriverTest {
    private final ScheduledExecutorService mainThreadExecutor =
            mock(ScheduledExecutorService.class);
    private final ExecutorService ioExecutor = Executors.newSingleThreadExecutor();

    @BeforeEach
    void setUpExecutors() {
        doAnswer(
                        invocation -> {
                            ((Runnable) invocation.getArgument(0)).run();
                            return null;
                        })
                .when(mainThreadExecutor)
                .execute(any());
    }

    @AfterEach
    void closeExecutors() throws Exception {
        verify(mainThreadExecutor, never()).shutdown();
        verify(mainThreadExecutor, never()).shutdownNow();
        assertFalse(ioExecutor.isShutdown());
        ioExecutor.shutdownNow();
        assertTrue(ioExecutor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void returnsPodIdentityAndReleasesOnlyOwnedWorkerOnce() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        ResourceEventHandler<KubernetesWorkerNode> events = mock(ResourceEventHandler.class);
        when(api.getJob("app")).thenReturn(job());
        try (KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        api,
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))),
                        "app",
                        "isolated-app")) {
            driver.initialize(events, mainThreadExecutor, ioExecutor, () -> "10.0.0.1:5801");
            KubernetesWorkerNode worker =
                    driver.requestWorker(specification().getWorkerSpecification())
                            .get(5, TimeUnit.SECONDS);
            assertEquals("app-worker-1", worker.getResourceID().getResourceIdString());
            verify(api).getJob("app");
            ArgumentCaptor<KubernetesPod> created = ArgumentCaptor.forClass(KubernetesPod.class);
            verify(api).createPod(created.capture());
            assertTrue(
                    created.getValue()
                            .getInternalResource()
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getCommand()
                            .contains("isolated-app"));
            assertTrue(
                    created.getValue()
                            .getInternalResource()
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getCommand()
                            .contains("10.0.0.1:5801"));
            assertEquals(
                    "uid-1",
                    created.getValue()
                            .getInternalResource()
                            .getMetadata()
                            .getOwnerReferences()
                            .get(0)
                            .getUid());
            assertNull(driver.releaseWorker(worker).get());
            assertNull(driver.releaseWorker(worker).get());
            driver.releaseWorker(new KubernetesWorkerNode(new ResourceID("other-worker"))).get();
            driver.checkWorkers();
            verify(api, times(1)).deletePod("app-worker-1");
            verify(api, never()).deletePod("other-worker");
            verify(events, never()).onWorkerTerminated(any(), anyString());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"Succeeded", "Failed"})
    void reportsOnlyFailedWorkersAndCleansEveryPod(String terminalPhase) throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        ResourceEventHandler<KubernetesWorkerNode> events = mock(ResourceEventHandler.class);
        when(api.getJob("app")).thenReturn(job());
        AtomicBoolean failed = new AtomicBoolean();
        when(api.listPods(anyString()))
                .thenAnswer(
                        invocation ->
                                Arrays.asList(
                                        pod(
                                                "app-worker-1",
                                                failed.get() ? terminalPhase : "Running"),
                                        pod("app-worker-2", "Running")));
        KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        api,
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))),
                        "app",
                        "isolated-app");
        driver.initialize(events, mainThreadExecutor, ioExecutor, () -> "10.0.0.1:5801");
        KubernetesWorkerNode first =
                driver.requestWorker(specification().getWorkerSpecification()).get();
        driver.requestWorker(specification().getWorkerSpecification()).get();
        failed.set(true);
        driver.checkWorkers();
        if ("Succeeded".equals(terminalPhase)) {
            verifyNoInteractions(events);
            // A previously successful Pod may later disappear through garbage collection.
            when(api.listPods(anyString()))
                    .thenReturn(Collections.singletonList(pod("app-worker-2", "Running")));
            driver.checkWorkers();
            verifyNoInteractions(events);
        } else {
            ArgumentCaptor<KubernetesWorkerNode> terminated =
                    ArgumentCaptor.forClass(KubernetesWorkerNode.class);
            ArgumentCaptor<String> diagnostics = ArgumentCaptor.forClass(String.class);
            verify(events).onWorkerTerminated(terminated.capture(), diagnostics.capture());
            assertSame(first, terminated.getValue());
            assertEquals(
                    first.getResourceID().getResourceIdString(),
                    terminated.getValue().getResourceID().getResourceIdString());
            assertTrue(diagnostics.getValue().contains("Failed"));
        }
        driver.close();
        driver.checkWorkers();
        verify(api).deleteWorkers("app");
        verify(api).close();
    }

    @Test
    void ambiguousWorkerCreationIsStillCleanedAndCleanupFailurePropagates() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.getJob("app")).thenReturn(job());
        KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        api,
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))),
                        "app",
                        "isolated-app");
        driver.initialize(
                mock(ResourceEventHandler.class),
                mainThreadExecutor,
                ioExecutor,
                () -> "10.0.0.1:5801");
        doThrow(new ApiException(0, "connection interrupted")).when(api).createPod(any());
        assertThrows(
                Exception.class,
                () -> driver.requestWorker(specification().getWorkerSpecification()).get());
        assertThrows(
                Exception.class,
                () -> driver.requestWorker(specification().getWorkerSpecification()).get());
        doThrow(new ApiException(500, "deletion failed")).when(api).deleteWorkers("app");
        assertThrows(ApiException.class, driver::close);
        verify(api).deleteWorkers("app");
        verify(api).close();
    }

    @Test
    void watchFailurePublishesDriverErrorOnlyWhileRunning() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        ResourceEventHandler<KubernetesWorkerNode> events = mock(ResourceEventHandler.class);
        when(api.getJob("app")).thenReturn(job());
        ApiException failure = new ApiException(500, "watch failed");
        when(api.listPods(anyString())).thenThrow(failure);
        doNothing().when(mainThreadExecutor).execute(any());
        try (KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        api,
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))),
                        "app",
                        "isolated-app")) {
            driver.initialize(events, mainThreadExecutor, ioExecutor, () -> "10.0.0.1:5801");
            driver.checkWorkers();
            driver.checkWorkers();
            verify(events, never()).onError(any());
            ArgumentCaptor<Runnable> callback = ArgumentCaptor.forClass(Runnable.class);
            verify(mainThreadExecutor).execute(callback.capture());
            callback.getValue().run();
            verify(events, times(1)).onError(failure);
        }
    }

    @Test
    @Timeout(10)
    void closeWaitsForRacingAllocationAndDeletesItsPod() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        when(api.getJob("app")).thenReturn(job());
        CountDownLatch creating = new CountDownLatch(1);
        CountDownLatch accepted = new CountDownLatch(1);
        AtomicBoolean created = new AtomicBoolean();
        AtomicBoolean deleted = new AtomicBoolean();
        doAnswer(
                        invocation -> {
                            creating.countDown();
                            boolean released = false;
                            while (!released) {
                                try {
                                    released = accepted.await(1, TimeUnit.SECONDS);
                                } catch (InterruptedException ignored) {
                                    /* Simulate an accepted remote create. */
                                }
                            }
                            created.set(true);
                            return null;
                        })
                .when(api)
                .createPod(any());
        doAnswer(
                        invocation -> {
                            if (created.get()) {
                                deleted.set(true);
                            }
                            return null;
                        })
                .when(api)
                .deletePod(anyString());
        KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        api,
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))),
                        "app",
                        "isolated-app");
        driver.initialize(
                mock(ResourceEventHandler.class),
                mainThreadExecutor,
                ioExecutor,
                () -> "10.0.0.1:5801");
        CompletableFuture<KubernetesWorkerNode> allocation =
                driver.requestWorker(specification().getWorkerSpecification());
        assertTrue(creating.await(2, TimeUnit.SECONDS));
        assertFalse(allocation.isDone());
        assertTrue(allocation.cancel(true));
        CompletableFuture<Void> closing =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                driver.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        try {
            assertThrows(Exception.class, () -> allocation.get(2, TimeUnit.SECONDS));
        } finally {
            accepted.countDown();
        }
        closing.get(3, TimeUnit.SECONDS);
        assertTrue(created.get());
        assertTrue(deleted.get());
    }

    private static ApplicationSpecification specification() {
        return SeatunnelApplicationConfig.parse("env { job.mode = BATCH }", options());
    }

    private static Map<String, String> options() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return options;
    }

    private static KubernetesJob job() {
        KubernetesJob job =
                KubernetesResourceFactory.job(
                        "app",
                        SeatunnelKubernetesApplicationCli.class.getName(),
                        KubernetesApplicationParameters.from(
                                specification(), ReadonlyConfig.fromMap(new HashMap<>(options()))));
        job.getInternalResource().getMetadata().setUid("uid-1");
        return job;
    }

    private static KubernetesPod pod(String name, String phase) {
        return new KubernetesPod(
                new V1Pod()
                        .metadata(new V1ObjectMeta().name(name))
                        .status(new V1PodStatus().phase(phase)));
    }
}
