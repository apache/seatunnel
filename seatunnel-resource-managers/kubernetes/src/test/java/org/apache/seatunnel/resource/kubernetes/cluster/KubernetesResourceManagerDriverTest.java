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

package org.apache.seatunnel.resource.kubernetes.cluster;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.ApplicationId;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KubernetesResourceManagerDriverTest {
    @Test
    void reportsWorkerFailureAndCleansEveryPod() throws Exception {
        KubernetesClient api = mock(KubernetesClient.class);
        ResourceManagerContext context = context();
        when(api.getJob("app")).thenReturn(job());
        AtomicBoolean failed = new AtomicBoolean();
        when(api.listPods(anyString()))
                .thenAnswer(
                        invocation ->
                                Arrays.asList(
                                        pod("app-worker-0", failed.get() ? "Failed" : "Running"),
                                        pod("app-worker-1", "Running")));
        KubernetesResourceManagerDriver driver =
                new KubernetesResourceManagerDriver(
                        api, KubernetesApplicationParameters.from(specification()));
        driver.initialize(context);
        WorkerRegistration first =
                driver.requestWorker(specification().getWorkerSpecification()).get();
        driver.requestWorker(specification().getWorkerSpecification()).get();
        failed.set(true);
        driver.checkWorkers();
        verify(context).onWorkerTerminated(eq(first.getWorkerId()), anyString());
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
                        api, KubernetesApplicationParameters.from(specification()));
        driver.initialize(context());
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
                        api, KubernetesApplicationParameters.from(specification()));
        driver.initialize(context());
        CompletableFuture<WorkerRegistration> allocation =
                driver.requestWorker(specification().getWorkerSpecification());
        assertTrue(creating.await(2, TimeUnit.SECONDS));
        assertFalse(allocation.isDone());
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

    private static ResourceManagerContext context() {
        ResourceManagerContext context = mock(ResourceManagerContext.class);
        when(context.getApplicationId())
                .thenReturn(new ApplicationId(DeployType.KUBERNETES, "app"));
        when(context.getClusterName()).thenReturn("isolated-app");
        when(context.getMasterAddress()).thenReturn("10.0.0.1:5801");
        return context;
    }

    private static ApplicationSpecification specification() {
        Map<String, String> options = new HashMap<>();
        options.put(KubernetesOptions.IMAGE.key(), "seatunnel:application");
        options.put(KubernetesOptions.KUBE_CONFIG.key(), "/submitter-kubeconfig");
        options.put(ApplicationOptions.WORKER_COUNT.key(), "2");
        return ApplicationSpecification.fromOptions(
                DeployType.KUBERNETES, "env { job.mode = BATCH }", options);
    }

    private static KubernetesJob job() {
        KubernetesJob job =
                KubernetesResourceFactory.job(
                        "app", KubernetesApplicationParameters.from(specification()));
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
