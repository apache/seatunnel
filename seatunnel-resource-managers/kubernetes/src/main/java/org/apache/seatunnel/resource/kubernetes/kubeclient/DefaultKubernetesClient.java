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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import io.kubernetes.client.custom.V1Patch;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Secret;
import io.kubernetes.client.openapi.models.V1Service;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Namespace-scoped SDK connection shared by submission, status and resource management.
 *
 * <p>Operations are synchronous and may run concurrently on the SDK's thread-safe HTTP client. The
 * deployer or driver that opens this connection owns its lifetime and must close it after pending
 * requests and resource cleanup complete.
 */
@Slf4j
final class DefaultKubernetesClient implements KubernetesClient {

    private final ApiClient client;
    private final ApiClient watchClient;
    private final CoreV1Api core;
    private final BatchV1Api batch;
    private final String namespace;

    DefaultKubernetesClient(ApiClient client, String namespace) {
        this(client, client, namespace);
    }

    DefaultKubernetesClient(ApiClient client, ApiClient watchClient, String namespace) {
        this.client = client;
        this.watchClient = watchClient;
        this.namespace = namespace;
        this.core = new CoreV1Api(client);
        this.batch = new BatchV1Api(client);
    }

    @Override
    public KubernetesConfigMap getConfigMap(String name) {
        try {
            return new KubernetesConfigMap(core.readNamespacedConfigMap(name, namespace, null));
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    @Override
    public KubernetesSecret getSecret(String name) {
        try {
            return new KubernetesSecret(core.readNamespacedSecret(name, namespace, null));
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    @Override
    public KubernetesJob createJob(KubernetesJob job) {
        try {
            return new KubernetesJob(
                    batch.createNamespacedJob(
                            namespace, job.getInternalResource(), null, null, null, null));
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    /**
     * Reads the owner Job without changing its lifecycle.
     *
     * @param name application Job name in this connection's namespace
     * @return current Job object, including server-assigned ownership metadata
     * @throws KubernetesClientException if the Job is absent or the API request fails
     */
    @Override
    public KubernetesJob getJob(String name) {
        try {
            return new KubernetesJob(batch.readNamespacedJob(name, namespace, null));
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    /** Starts only the suspended Job field without overwriting concurrent controller updates. */
    @Override
    public void startJob(String name) {
        try {
            batch.patchNamespacedJob(
                    name,
                    namespace,
                    new V1Patch(
                            "[{\"op\":\"replace\",\"path\":\"/spec/suspend\",\"value\":false}]"),
                    null,
                    null,
                    null,
                    null,
                    null);
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    /**
     * Creates every accompanying resource with the owner Job's server-assigned reference.
     *
     * <p>Resources created earlier in the list stay behind when a later resource fails; the caller
     * compensates them by deleting the owner Job, so Kubernetes removes exactly the objects created
     * for this application and never a pre-existing object with the same name.
     */
    @Override
    public void createAccompanyingResources(
            KubernetesJob owner, List<KubernetesResource<?>> resources) {
        try {
            V1OwnerReference ownerReference = ownerReference(owner);
            for (KubernetesResource<?> resource : resources) {
                if (resource instanceof KubernetesSecret) {
                    V1Secret secret = ((KubernetesSecret) resource).getInternalResource();
                    secret.getMetadata()
                            .setOwnerReferences(Collections.singletonList(ownerReference));
                    core.createNamespacedSecret(namespace, secret, null, null, null, null);
                } else if (resource instanceof KubernetesService) {
                    V1Service service = ((KubernetesService) resource).getInternalResource();
                    service.getMetadata()
                            .setOwnerReferences(Collections.singletonList(ownerReference));
                    core.createNamespacedService(namespace, service, null, null, null, null);
                } else if (resource instanceof KubernetesConfigMap) {
                    V1ConfigMap configMap = ((KubernetesConfigMap) resource).getInternalResource();
                    configMap
                            .getMetadata()
                            .setOwnerReferences(Collections.singletonList(ownerReference));
                    core.createNamespacedConfigMap(namespace, configMap, null, null, null, null);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported accompanying Kubernetes resource " + resource.getName());
                }
            }
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    /**
     * Requests a worker pod whose owner reference is supplied by the caller.
     *
     * @param pod desired pod metadata and container specification
     * @throws KubernetesClientException on admission or transport failure; an ambiguous request may
     *     have created the pod and the caller remains responsible for compensating cleanup
     */
    @Override
    public void createPod(KubernetesPod pod) {
        try {
            core.createNamespacedPod(namespace, pod.getInternalResource(), null, null, null, null);
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    /**
     * Retrieves a snapshot of pods matching an application or role label selector.
     *
     * @param selector Kubernetes label selector restricted to this connection's namespace
     * @return current matching pods; a missing pod is not synthesized
     * @throws KubernetesClientException if the API request fails
     */
    @Override
    public List<KubernetesPod> listPods(String selector) {
        try {
            List<KubernetesPod> pods = new ArrayList<>();
            for (V1Pod pod :
                    core.listNamespacedPod(
                                    namespace, null, null, null, null, selector, null, null, null,
                                    null, null, null)
                            .getItems()) {
                pods.add(new KubernetesPod(pod));
            }
            return pods;
        } catch (ApiException failure) {
            throw wrap(failure);
        }
    }

    @Override
    public KubernetesWatch watchPods(String selector, PodWatchCallback callback) {
        InformerPodWatch watch = new InformerPodWatch(watchClient, namespace, selector, callback);
        watch.start();
        return watch;
    }

    /**
     * Requests immediate deletion of one worker, accepting an already absent pod.
     *
     * @param name worker pod name in this connection's namespace
     * @throws KubernetesClientException if deletion fails for any reason other than an absent pod
     */
    @Override
    public void deletePod(String name) {
        try {
            core.deleteNamespacedPod(name, namespace, null, null, 0, null, "Background", null);
        } catch (ApiException failure) {
            rethrowUnlessMissing(failure);
        }
    }

    /**
     * Deletes the owner Job with foreground propagation so Kubernetes removes the Pods, Secret and
     * Service created for this application, then removes any remaining labeled Pods.
     *
     * <p>Workers are disposable and are deleted immediately. The master Pod keeps its own
     * termination grace period so its shutdown hook and checkpoint retention can finish before the
     * container is killed.
     */
    @Override
    public void deleteApplication(String id) {
        KubernetesClientException failure = null;
        try {
            batch.deleteNamespacedJob(id, namespace, null, null, 0, null, "Foreground", null);
        } catch (ApiException e) {
            if (e.getCode() != 404) {
                failure = wrap(e);
            }
        }
        try {
            // Workers are disposable, so they are deleted immediately.
            deletePods(KubernetesResourceIdentity.selector(id, KubernetesConstants.WORKER_ROLE), 0);
        } catch (KubernetesClientException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        try {
            // The master keeps its own termination grace period so its shutdown hook and
            // checkpoint retention can finish before the container is killed.
            deletePods(
                    KubernetesResourceIdentity.selector(id, KubernetesConstants.MASTER_ROLE), null);
        } catch (KubernetesClientException e) {
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

    /** Builds the owner reference used by every resource created for one application Job. */
    private static V1OwnerReference ownerReference(KubernetesJob owner) {
        return new V1OwnerReference()
                .apiVersion(KubernetesConstants.BATCH_API_VERSION)
                .kind(KubernetesConstants.JOB_KIND)
                .name(owner.getName())
                .uid(owner.getUid())
                .controller(false)
                .blockOwnerDeletion(false);
    }

    /**
     * Deletes all worker pods for an application while preserving its Job and master pod.
     *
     * @param id application Job name used by the worker ownership labels
     * @throws KubernetesClientException if collection deletion fails for a reason other than
     *     absence
     */
    @Override
    public void deleteWorkers(String id) {
        deletePods(KubernetesResourceIdentity.selector(id, KubernetesConstants.WORKER_ROLE), 0);
    }

    /**
     * Deletes the Pods selected by the given label selector without waiting for their termination.
     *
     * <p>Deletion uses {@code Background} propagation, so Kubernetes removes the Pods on its own
     * schedule and a missing collection is tolerated.
     *
     * @param selector label selector of the Pods to delete
     * @param gracePeriodSeconds termination grace period in seconds, or {@code null} to keep the
     *     {@code terminationGracePeriodSeconds} declared by each Pod
     * @throws KubernetesClientException if the request fails for a reason other than absence
     */
    private void deletePods(String selector, Integer gracePeriodSeconds) {
        try {
            core.deleteCollectionNamespacedPod(
                    namespace,
                    null,
                    null,
                    null,
                    null,
                    gracePeriodSeconds,
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

    /**
     * Returns one SDK failure as the provider's own exception type.
     *
     * <p>Shared with {@link InformerPodWatch} so failures that the informer observes through the
     * SDK keep the provider's exception type and the per-code watch policy.
     */
    static KubernetesClientException wrap(ApiException failure) {
        String detail = failure.getMessage();
        String message =
                detail == null || detail.isEmpty()
                        ? "Kubernetes API request failed (HTTP " + failure.getCode() + ")"
                        : detail;
        String body = responseBody(failure);
        if (!body.isEmpty()) {
            message = message + " - " + body;
        }
        return new KubernetesClientException(failure.getCode(), message, failure);
    }

    /**
     * Returns the response body of one failed request for diagnosis.
     *
     * <p>RBAC, quota and admission failures explain themselves in the Kubernetes `Status` body,
     * which the SDK message does not include. The body is collapsed to a single bounded line so it
     * cannot break log formatting or flood the output.
     *
     * @param failure failed SDK request
     * @return the collapsed body, or an empty string when the response carried none
     */
    private static String responseBody(ApiException failure) {
        String body = failure.getResponseBody();
        if (body == null || body.trim().isEmpty()) {
            return "";
        }
        return StringUtils.abbreviate(body.trim().replaceAll("\\s+", " "), 512);
    }

    private static void rethrowUnlessMissing(ApiException failure) {
        if (failure.getCode() != 404) {
            throw wrap(failure);
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
        closeClient(client);
        if (watchClient != client) {
            closeClient(watchClient);
        }
    }

    private static void closeClient(ApiClient apiClient) {
        apiClient.getHttpClient().dispatcher().executorService().shutdown();
        apiClient.getHttpClient().connectionPool().evictAll();
    }
}
