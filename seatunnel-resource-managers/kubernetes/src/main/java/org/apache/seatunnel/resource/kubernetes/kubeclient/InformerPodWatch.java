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

import org.apache.seatunnel.resource.kubernetes.exception.KubernetesClientException;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.CallGenerator;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Watches one application's Pods through the Kubernetes watch API.
 *
 * <p>The shared informer opens one list and one watch stream per application, keeps the stream
 * reconciled with the API server and delivers the watch actions to a {@link PodWatchCallback}. The
 * API server therefore no longer sees a list request per poll, and the resource manager reacts to a
 * Pod change immediately instead of waiting for the next snapshot.
 *
 * <p>Failures keep the policy of the previous polling implementation: unrecoverable API errors stop
 * the watch immediately, transient errors are tolerated for {@link
 * KubernetesConstants#WATCH_TRANSIENT_FAILURE_BUDGET_MILLIS} while the informer reconnects, and an
 * exception from a callback is terminal because it comes from the resource manager driver.
 */
@Slf4j
final class InformerPodWatch implements KubernetesWatch {

    private final SharedInformerFactory factory;
    private final ExecutorService informerExecutor;
    private final String selector;
    private final PodWatchCallback callback;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicLong firstFailureNanos = new AtomicLong();

    InformerPodWatch(
            ApiClient client, String namespace, String selector, PodWatchCallback callback) {
        this.selector = selector;
        this.callback = callback;
        this.informerExecutor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-pod-informer");
                            thread.setDaemon(true);
                            return thread;
                        });
        this.factory = new SharedInformerFactory(client, informerExecutor);
        SharedIndexInformer<V1Pod> informer =
                factory.sharedIndexInformerFor(
                        callGenerator(client, namespace, selector),
                        V1Pod.class,
                        V1PodList.class,
                        0L,
                        (type, failure) -> handleWatchFailure(failure));
        informer.addEventHandler(
                new ResourceEventHandler<>() {
                    @Override
                    public void onAdd(V1Pod pod) {
                        deliver(() -> callback.onPodAdded(new KubernetesPod(pod)));
                    }

                    @Override
                    public void onUpdate(V1Pod oldPod, V1Pod newPod) {
                        deliver(() -> callback.onPodModified(new KubernetesPod(newPod)));
                    }

                    @Override
                    public void onDelete(V1Pod pod, boolean deletedFinalStateUnknown) {
                        deliver(() -> callback.onPodDeleted(new KubernetesPod(pod)));
                    }
                });
    }

    /** Starts the informer's list and watch requests. */
    void start() {
        factory.startAllRegisteredInformers();
    }

    @Override
    public void close() {
        stop();
    }

    private static CallGenerator callGenerator(
            ApiClient client, String namespace, String selector) {
        return params ->
                new CoreV1Api(client)
                        .listNamespacedPodCall(
                                namespace,
                                null,
                                null,
                                null,
                                null,
                                selector,
                                null,
                                params.resourceVersion,
                                null,
                                null,
                                params.timeoutSeconds,
                                params.watch,
                                null);
    }

    private void deliver(Runnable delivery) {
        if (stopped.get()) {
            return;
        }
        // A watch event proves the stream is healthy, so it clears the transient failure streak.
        firstFailureNanos.set(0L);
        try {
            delivery.run();
        } catch (Exception failure) {
            stopAndReport(failure);
        }
    }

    /**
     * Applies the transient-failure policy to one informer error. A non-retryable failure or an
     * exhausted budget stops the watch and reports the failure; otherwise it is logged and the
     * informer keeps reconnecting.
     */
    private void handleWatchFailure(Throwable failure) {
        if (stopped.get()) {
            return;
        }
        Exception reportable = reportable(failure);
        if (isNonRetryable(reportable)) {
            stopAndReport(reportable);
            return;
        }
        long started = firstFailureNanos.get();
        if (started == 0L) {
            firstFailureNanos.compareAndSet(0L, System.nanoTime());
            started = firstFailureNanos.get();
        }
        log.warn(
                "Could not watch application pods for selector {}; the informer will reconnect",
                selector,
                reportable);
        if (transientFailureBudgetExhausted(started)) {
            stopAndReport(
                    new IllegalStateException(
                            "Kubernetes pod watch exceeded the transient failure budget for selector "
                                    + selector,
                            reportable));
        }
    }

    /** Returns one informer failure as the provider's exception type where possible. */
    private static Exception reportable(Throwable failure) {
        if (failure instanceof ApiException) {
            return DefaultKubernetesClient.wrap((ApiException) failure);
        }
        if (failure instanceof Exception) {
            return (Exception) failure;
        }
        return new IllegalStateException(failure);
    }

    private void stopAndReport(Exception failure) {
        stop();
        callback.onWatchError(failure);
    }

    private void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        factory.stopAllRegisteredInformers();
        informerExecutor.shutdownNow();
    }

    /**
     * @param startedNanos {@link System#nanoTime()} of the first failure in the current streak
     * @return whether the tolerated transient pod-watch failure budget has elapsed
     */
    private static boolean transientFailureBudgetExhausted(long startedNanos) {
        return System.nanoTime() - startedNanos
                >= TimeUnit.MILLISECONDS.toNanos(
                        KubernetesConstants.WATCH_TRANSIENT_FAILURE_BUDGET_MILLIS);
    }

    private static boolean isNonRetryable(Exception failure) {
        if (failure instanceof KubernetesClientException) {
            int code = ((KubernetesClientException) failure).getCode();
            return code == 401 || code == 403 || code == 404;
        }
        return false;
    }
}
