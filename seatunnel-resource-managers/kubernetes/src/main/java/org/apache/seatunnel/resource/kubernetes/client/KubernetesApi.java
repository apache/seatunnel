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

package org.apache.seatunnel.resource.kubernetes.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.resource.kubernetes.cluster.KubernetesResources;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesOptions;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.util.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Namespace-scoped SDK connection shared by submission, status and resource management.
 *
 * <p>Operations are synchronous and may run concurrently on the SDK's thread-safe HTTP client. The
 * deployer or driver that opens this connection owns its lifetime and must close it after pending
 * requests and resource cleanup complete.
 */
public class KubernetesApi implements AutoCloseable {
    private final ApiClient client;
    private final CoreV1Api core;
    private final BatchV1Api batch;
    private final String namespace;

    /**
     * Opens a namespace-scoped connection with bounded API request timeouts.
     *
     * @param options deployment namespace and optional submitter kubeconfig
     * @param inCluster whether only the running pod's service-account credentials may be used
     * @return connection owned by the caller, which must close it after resource cleanup
     * @throws IOException if credentials or kubeconfig cannot be loaded
     */
    public static KubernetesApi connect(Map<String, String> options, boolean inCluster)
            throws IOException {
        ReadonlyConfig config = ReadonlyConfig.fromMap(new HashMap<String, Object>(options));
        String kubeconfig = config.get(KubernetesOptions.KUBE_CONFIG);
        ApiClient client =
                inCluster
                        ? Config.fromCluster()
                        : kubeconfig == null
                                ? Config.defaultClient()
                                : Config.fromConfig(kubeconfig);
        client.setConnectTimeout(10000);
        client.setReadTimeout(10000);
        client.setHttpClient(
                client.getHttpClient().newBuilder().callTimeout(10, TimeUnit.SECONDS).build());
        return new KubernetesApi(client, config.get(KubernetesOptions.NAMESPACE));
    }

    KubernetesApi(ApiClient client, String namespace) {
        this.client = client;
        this.namespace = namespace;
        this.core = new CoreV1Api(client);
        this.batch = new BatchV1Api(client);
    }

    V1Job createJob(V1Job job) throws ApiException {
        return batch.createNamespacedJob(namespace, job, null, null, null, null);
    }

    /**
     * Reads the owner Job without changing its lifecycle.
     *
     * @param name application Job name in this connection's namespace
     * @return current Job object, including server-assigned ownership metadata
     * @throws ApiException if the Job is absent or the API request fails
     */
    public V1Job getJob(String name) throws ApiException {
        return batch.readNamespacedJob(name, namespace, null);
    }

    /** Starts only the suspended Job field without overwriting concurrent controller updates. */
    void startJob(String name) throws ApiException {
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

    void createConfigMap(V1ConfigMap value) throws ApiException {
        core.createNamespacedConfigMap(namespace, value, null, null, null, null);
    }

    void createService(V1Service value) throws ApiException {
        core.createNamespacedService(namespace, value, null, null, null, null);
    }

    /**
     * Requests a worker pod whose owner reference is supplied by the caller.
     *
     * @param pod desired pod metadata and container specification
     * @throws ApiException on admission or transport failure; an ambiguous request may have created
     *     the pod and the caller remains responsible for compensating cleanup
     */
    public void createPod(V1Pod pod) throws ApiException {
        core.createNamespacedPod(namespace, pod, null, null, null, null);
    }

    /**
     * Retrieves a snapshot of pods matching an application or role label selector.
     *
     * @param selector Kubernetes label selector restricted to this connection's namespace
     * @return current matching pods; a missing pod is not synthesized
     * @throws ApiException if the API request fails
     */
    public List<V1Pod> listPods(String selector) throws ApiException {
        return core.listNamespacedPod(
                        namespace, null, null, null, null, selector, null, null, null, null, null,
                        null)
                .getItems();
    }

    /**
     * Requests immediate deletion of one worker, accepting an already absent pod.
     *
     * @param name worker pod name in this connection's namespace
     * @throws ApiException if deletion fails for any reason other than an absent pod
     */
    public void deletePod(String name) throws ApiException {
        ignoreMissing(
                () ->
                        core.deleteNamespacedPod(
                                name, namespace, null, null, 0, null, "Background", null));
    }

    /** Attempts every cleanup even when another resource deletion fails. */
    void deleteApplication(String id) throws ApiException {
        ApiException failure = null;
        try {
            ignoreMissing(
                    () ->
                            batch.deleteNamespacedJob(
                                    id, namespace, null, null, 0, null, "Foreground", null));
        } catch (ApiException e) {
            failure = e;
        }
        try {
            deletePods(KubernetesResources.selector(id));
        } catch (ApiException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            ignoreMissing(
                    () ->
                            core.deleteNamespacedService(
                                    id, namespace, null, null, null, null, null, null));
        } catch (ApiException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            ignoreMissing(
                    () ->
                            core.deleteNamespacedConfigMap(
                                    id, namespace, null, null, null, null, null, null));
        } catch (ApiException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
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
    public void deleteWorkers(String id) throws ApiException {
        deletePods(
                KubernetesResources.selector(id)
                        + ","
                        + KubernetesResources.ROLE_LABEL
                        + "=worker");
    }

    private void deletePods(String selector) throws ApiException {
        ignoreMissing(
                () ->
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
                                null));
    }

    private static void ignoreMissing(ApiAction action) throws ApiException {
        try {
            action.run();
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                throw e;
            }
        }
    }

    private interface ApiAction {
        void run() throws ApiException;
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
