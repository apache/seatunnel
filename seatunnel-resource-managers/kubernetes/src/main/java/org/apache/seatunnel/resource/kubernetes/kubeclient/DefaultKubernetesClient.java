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

import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesResource;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesSecret;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Namespace-scoped SDK connection shared by submission, status and resource management.
 *
 * <p>Operations are synchronous and may run concurrently on the SDK's thread-safe HTTP client. The
 * deployer or driver that opens this connection owns its lifetime and must close it after pending
 * requests and resource cleanup complete.
 */
final class DefaultKubernetesClient implements KubernetesClient {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultKubernetesClient.class);

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

    /**
     * Creates every accompanying resource with the owner Job's server-assigned reference.
     *
     * <p>Resources created earlier in the list stay behind when a later resource fails; the caller
     * compensates them by deleting the owner Job, so Kubernetes removes exactly the objects created
     * for this application and never a pre-existing object with the same name.
     */
    @Override
    public void createAccompanyingResources(
            KubernetesJob owner, List<KubernetesResource<?>> resources) throws ApiException {
        V1OwnerReference ownerReference = ownerReference(owner);
        for (KubernetesResource<?> resource : resources) {
            if (resource instanceof KubernetesSecret) {
                V1Secret secret = ((KubernetesSecret) resource).getInternalResource();
                secret.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
                core.createNamespacedSecret(namespace, secret, null, null, null, null);
            } else if (resource instanceof KubernetesService) {
                V1Service service = ((KubernetesService) resource).getInternalResource();
                service.getMetadata().setOwnerReferences(Collections.singletonList(ownerReference));
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

        AtomicBoolean stopped = new AtomicBoolean();
        AtomicLong firstFailureNanos = new AtomicLong();

        Runnable runnable =
                () -> {
                    if (stopped.get()) {
                        return;
                    }
                    List<KubernetesPod> pods;
                    try {
                        pods = listPods(selector);
                    } catch (Exception failure) {
                        handleListFailure(
                                executor,
                                stopped,
                                errorHandler,
                                firstFailureNanos,
                                selector,
                                failure);
                        return;
                    }
                    // A successful poll clears the accumulated transient failure budget.
                    firstFailureNanos.set(0L);
                    // The listener is the driver's observation callback. A failure here is terminal
                    // for the watch and must be reported directly: treating it as a list failure
                    // would clear the budget on the next successful poll and never escalate.
                    try {
                        listener.accept(pods);
                    } catch (Exception failure) {
                        stopAndReport(executor, stopped, errorHandler, failure);
                    }
                };
        executor.scheduleWithFixedDelay(runnable, 0, intervalMillis, TimeUnit.MILLISECONDS);
        return () -> {
            stopped.set(true);
            executor.shutdownNow();
        };
    }

    /**
     * Applies the transient-failure policy to one failed pod listing. A non-retryable failure or an
     * exhausted budget stops the watch and reports the failure; otherwise it is logged and the
     * budget keeps accumulating across polls until the next successful listing.
     */
    private static void handleListFailure(
            ScheduledExecutorService executor,
            AtomicBoolean stopped,
            Consumer<Exception> errorHandler,
            AtomicLong firstFailureNanos,
            String selector,
            Exception failure) {
        if (isNonRetryable(failure)) {
            stopAndReport(executor, stopped, errorHandler, failure);
            return;
        }
        long started = firstFailureNanos.get();
        if (started == 0L) {
            firstFailureNanos.compareAndSet(0L, System.nanoTime());
            started = firstFailureNanos.get();
        }
        LOG.warn(
                "Could not list application pods for selector {}; will keep polling",
                selector,
                failure);
        if (transientFailureBudgetExhausted(started)) {
            stopAndReport(
                    executor,
                    stopped,
                    errorHandler,
                    new IllegalStateException(
                            "Kubernetes pod watch exceeded the transient failure budget for selector "
                                    + selector,
                            failure));
        }
    }

    /**
     * @param startedNanos {@link System#nanoTime()} of the first failure in the current streak
     * @return whether the tolerated transient pod-listing failure budget has elapsed
     */
    private static boolean transientFailureBudgetExhausted(long startedNanos) {
        return System.nanoTime() - startedNanos
                >= TimeUnit.MILLISECONDS.toNanos(
                        KubernetesConstants.WATCH_TRANSIENT_FAILURE_BUDGET_MILLIS);
    }

    private static void stopAndReport(
            ScheduledExecutorService executor,
            AtomicBoolean stopped,
            Consumer<Exception> errorHandler,
            Exception failure) {
        stopped.set(true);
        executor.shutdown();
        errorHandler.accept(failure);
    }

    private static boolean isNonRetryable(Exception failure) {
        if (failure instanceof ApiException) {
            int code = ((ApiException) failure).getCode();
            return code == 401 || code == 403 || code == 404;
        }
        return false;
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

    /**
     * Deletes the owner Job with foreground propagation so Kubernetes removes the Pods, Secret and
     * Service created for this application, then removes any remaining labeled Pods.
     */
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
            deletePods(KubernetesResourceIdentity.selector(id));
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
     * @throws ApiException if collection deletion fails for a reason other than absence
     */
    @Override
    public void deleteWorkers(String id) throws ApiException {
        deletePods(KubernetesResourceIdentity.selector(id, KubernetesConstants.WORKER_ROLE));
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
