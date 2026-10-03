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

package org.apache.seatunnel.resource.kubernetes.kubeclient;

import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Namespace-scoped SDK connection shared by submission, status and resource management.
 *
 * <p>Operations are synchronous and may run concurrently on the SDK's thread-safe HTTP client. The
 * deployer or driver that opens this connection owns its lifetime and must close it after pending
 * requests and resource cleanup complete.
 */
final class DefaultKubernetesClient implements KubernetesClient {
    private final ApiClient client;
    private final CoreV1Api core;
    private final BatchV1Api batch;
    private final String namespace;

    DefaultKubernetesClient(ApiClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
        this.core = new CoreV1Api(client);
        this.batch = new BatchV1Api(client);
    }

    @Override
    public KubernetesConfigMap getConfigMap(String name) throws ApiException {
        return new KubernetesConfigMap(core.readNamespacedConfigMap(name, namespace, null));
    }

    @Override
    public KubernetesJob createJob(KubernetesJob job) throws ApiException {
        return new KubernetesJob(
                batch.createNamespacedJob(
                        namespace, job.getInternalResource(), null, null, null, null));
    }

    /**
     * Reads the owner Job without changing its lifecycle.
     *
     * @param name application Job name in this connection's namespace
     * @return current Job object, including server-assigned ownership metadata
     * @throws ApiException if the Job is absent or the API request fails
     */
    @Override
    public KubernetesJob getJob(String name) throws ApiException {
        return new KubernetesJob(batch.readNamespacedJob(name, namespace, null));
    }

    /** Starts only the suspended Job field without overwriting concurrent controller updates. */
    @Override
    public void startJob(String name) throws ApiException {
        batch.patchNamespacedJob(
                name,
                namespace,
                new V1Patch("[{\"op\":\"replace\",\"path\":\"/spec/suspend\",\"value\":false}]"),
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    public void createSecret(KubernetesSecret value) throws ApiException {
        core.createNamespacedSecret(namespace, value.getInternalResource(), null, null, null, null);
    }

    @Override
    public void createService(KubernetesService value) throws ApiException {
        core.createNamespacedService(
                namespace, value.getInternalResource(), null, null, null, null);
    }

    /**
     * Requests a worker pod whose owner reference is supplied by the caller.
     *
     * @param pod desired pod metadata and container specification
     * @throws ApiException on admission or transport failure; an ambiguous request may have created
     *     the pod and the caller remains responsible for compensating cleanup
     */
    @Override
    public void createPod(KubernetesPod pod) throws ApiException {
        core.createNamespacedPod(namespace, pod.getInternalResource(), null, null, null, null);
    }

    /**
     * Retrieves a snapshot of pods matching an application or role label selector.
     *
     * @param selector Kubernetes label selector restricted to this connection's namespace
     * @return current matching pods; a missing pod is not synthesized
     * @throws ApiException if the API request fails
     */
    @Override
    public List<KubernetesPod> listPods(String selector) throws ApiException {
        List<KubernetesPod> pods = new ArrayList<>();
        for (V1Pod pod :
                core.listNamespacedPod(
                                namespace, null, null, null, null, selector, null, null, null, null,
                                null, null)
                        .getItems()) {
            pods.add(new KubernetesPod(pod));
        }
        return pods;
    }

    @Override
    public KubernetesWatch watchPods(
            String selector,
            long intervalMillis,
            Consumer<List<KubernetesPod>> listener,
            Consumer<Exception> errorHandler) {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-kubernetes-pod-watch");
                            thread.setDaemon(true);
                            return thread;
                        });
        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        listener.accept(listPods(selector));
                    } catch (Exception failure) {
                        executor.shutdown();
                        errorHandler.accept(failure);
                    }
                },
                0,
                intervalMillis,
                TimeUnit.MILLISECONDS);
        return executor::shutdownNow;
    }

    /**
     * Requests immediate deletion of one worker, accepting an already absent pod.
     *
     * @param name worker pod name in this connection's namespace
     * @throws ApiException if deletion fails for any reason other than an absent pod
     */
    @Override
    public void deletePod(String name) throws ApiException {
        try {
            core.deleteNamespacedPod(name, namespace, null, null, 0, null, "Background", null);
        } catch (ApiException failure) {
            rethrowUnlessMissing(failure);
        }
    }

    /** Attempts every cleanup even when another resource deletion fails. */
    @Override
    public void deleteApplication(String id) throws ApiException {
        ApiException failure = null;
        try {
            batch.deleteNamespacedJob(id, namespace, null, null, 0, null, "Foreground", null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                failure = e;
            }
        }
        try {
            deletePods(KubernetesResourceFactory.selector(id));
        } catch (ApiException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            core.deleteNamespacedService(id, namespace, null, null, null, null, null, null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        try {
            core.deleteNamespacedSecret(id, namespace, null, null, null, null, null, null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Deletes all worker pods for an application while preserving its Job and master pod.
     *
     * @param id application Job name used by the worker ownership labels
     * @throws ApiException if collection deletion fails for a reason other than absence
     */
    @Override
    public void deleteWorkers(String id) throws ApiException {
        deletePods(
                KubernetesResourceFactory.selector(id)
                        + ","
                        + KubernetesResourceFactory.ROLE_LABEL
                        + "=worker");
    }

    private void deletePods(String selector) throws ApiException {
        try {
            core.deleteCollectionNamespacedPod(
                    namespace,
                    null,
                    null,
                    null,
                    null,
                    0,
                    selector,
                    null,
                    null,
                    "Background",
                    null,
                    null,
                    null,
                    10,
                    null);
        } catch (ApiException failure) {
            rethrowUnlessMissing(failure);
        }
    }

    private static void rethrowUnlessMissing(ApiException failure) throws ApiException {
        if (failure.getCode() != 404) {
            throw failure;
        }
    }

    /**
     * Releases HTTP resources without deleting Kubernetes objects.
     *
     * <p>The owner must first stop admission, drain pending requests and complete resource cleanup;
     * callers must not issue requests after this method returns.
     */
    @Override
    public void close() {
        client.getHttpClient().dispatcher().executorService().shutdown();
        client.getHttpClient().connectionPool().evictAll();
    }
}
