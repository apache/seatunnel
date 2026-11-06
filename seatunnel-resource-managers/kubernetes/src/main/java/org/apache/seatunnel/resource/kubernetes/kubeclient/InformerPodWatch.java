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

import com.google.gson.reflect.TypeToken;
import io.kubernetes.client.informer.ListerWatcher;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.CallGenerator;
import io.kubernetes.client.util.CallGeneratorParams;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watchable;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Call;

import java.io.IOException;
import java.net.ConnectException;
import java.util.Iterator;
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
 *
 * <p>The reflector retries a refused watch connection internally, with a one second pause and
 * without calling the informer's exception handler, so those attempts would never reach the failure
 * policy above. {@link ReportingListerWatcher} reports them instead, which keeps the budget
 * meaningful while the API server is unreachable.
 */
@Slf4j
final class InformerPodWatch implements KubernetesWatch {

    private final SharedInformerFactory factory;
    private final ExecutorService informerExecutor;
    private final String selector;
    private final PodWatchCallback callback;
    private final long transientFailureBudgetMillis;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicLong firstFailureNanos = new AtomicLong();

    InformerPodWatch(
            ApiClient client, String namespace, String selector, PodWatchCallback callback) {
        this(
                client,
                podListerWatcher(client, namespace, selector),
                selector,
                callback,
                KubernetesConstants.WATCH_TRANSIENT_FAILURE_BUDGET_MILLIS);
    }

    /**
     * Runs the informer against an injected lister/watcher and failure budget so tests can drive
     * reconnects without an API server.
     */
    InformerPodWatch(
            ApiClient client,
            ListerWatcher<V1Pod, V1PodList> listerWatcher,
            String selector,
            PodWatchCallback callback,
            long transientFailureBudgetMillis) {
        this.selector = selector;
        this.callback = callback;
        this.transientFailureBudgetMillis = transientFailureBudgetMillis;
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
                        new ReportingListerWatcher(listerWatcher),
                        V1Pod.class,
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

    /**
     * Binds list and watch calls the same way the shared informer factory does, but keeps the watch
     * accessible to {@link ReportingListerWatcher}.
     *
     * @param client informer client
     * @param namespace watched namespace
     * @param selector label selector of the application's worker Pods
     * @return informer lister/watcher for this application's Pods
     */
    private static ListerWatcher<V1Pod, V1PodList> podListerWatcher(
            ApiClient client, String namespace, String selector) {
        CallGenerator callGenerator = callGenerator(client, namespace, selector);
        // A watch response stays open until the API server ends it, so it must not inherit the
        // bounded read timeout of the ordinary API client.
        client.setReadTimeout(0);
        return new ListerWatcher<>() {
            @Override
            public V1PodList list(CallGeneratorParams params) throws ApiException {
                return (V1PodList)
                        client.execute(callGenerator.generate(params), V1PodList.class).getData();
            }

            @Override
            public Watchable<V1Pod> watch(CallGeneratorParams params) throws ApiException {
                Call call =
                        client.getHttpClient().newCall(callGenerator.generate(params).request());
                return Watch.createWatch(
                        client,
                        call,
                        TypeToken.getParameterized(Watch.Response.class, V1Pod.class).getType());
            }
        };
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
            log.warn(
                    "Could not watch application pods for selector {}; the informer will reconnect",
                    selector,
                    reportable);
        } else {
            log.debug(
                    "Could not watch application pods for selector {}; the informer will reconnect",
                    selector,
                    reportable);
        }
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
    private boolean transientFailureBudgetExhausted(long startedNanos) {
        return System.nanoTime() - startedNanos
                >= TimeUnit.MILLISECONDS.toNanos(transientFailureBudgetMillis);
    }

    private static boolean isNonRetryable(Exception failure) {
        if (failure instanceof KubernetesClientException) {
            int code = ((KubernetesClientException) failure).getCode();
            return code == 401 || code == 403 || code == 404;
        }
        return false;
    }

    /**
     * Feeds every reconnect the reflector swallows into the shared failure policy.
     *
     * <p>The reflector calls {@code watch} again after a refused connection instead of reporting
     * the failure, so without this wrapper a permanently unreachable API server would keep the
     * watch silently retrying and the application would never observe the worker Pods it can no
     * longer track. Failures the reflector reports itself (every type other than a connect
     * exception) are left to it so the budget counts each failure exactly once.
     */
    private final class ReportingListerWatcher implements ListerWatcher<V1Pod, V1PodList> {

        private final ListerWatcher<V1Pod, V1PodList> delegate;

        private ReportingListerWatcher(ListerWatcher<V1Pod, V1PodList> delegate) {
            this.delegate = delegate;
        }

        @Override
        public V1PodList list(CallGeneratorParams params) throws ApiException {
            V1PodList pods = delegate.list(params);
            // A completed list proves the API server is reachable again.
            firstFailureNanos.set(0L);
            return pods;
        }

        @Override
        public Watchable<V1Pod> watch(CallGeneratorParams params) throws ApiException {
            try {
                Watchable<V1Pod> watch = delegate.watch(params);
                // The watch call is executed eagerly, so a returned stream is an open connection.
                firstFailureNanos.set(0L);
                return new ReportingWatch(watch);
            } catch (ApiException failure) {
                // Report first, then rethrow so the reflector keeps its retry and re-list
                // behaviour.
                reportSwallowedFailure(failure);
                throw failure;
            }
        }

        /**
         * Reports a connection failure that the informer reflector retries silently.
         *
         * <p>{@code ReflectorRunnable} catches a refused watch connection, sleeps for one second
         * and calls {@code watch} again without notifying the informer's exception handler (see the
         * {@code isConnectException} branch of its run loop). Without this report the transient
         * failure budget would never start, so an unreachable API server would leave the
         * application running while it has already lost sight of its worker Pods.
         *
         * <p>Only connect exceptions are reported here: every other failure reaches {@link
         * #handleWatchFailure(Throwable)} through the informer's own handler, and reporting those a
         * second time would exhaust the shared budget after half the intended time.
         *
         * @param failure failed watch attempt as it left the delegate
         */
        private void reportSwallowedFailure(Throwable failure) {
            if (isConnectException(failure)) {
                handleWatchFailure(failure);
            }
        }
    }

    /** Reports stream failures that the reflector retries without notifying the informer. */
    private final class ReportingWatch implements Watchable<V1Pod> {

        private final Watchable<V1Pod> delegate;

        private ReportingWatch(Watchable<V1Pod> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            try {
                return delegate.hasNext();
            } catch (RuntimeException failure) {
                reportSwallowed(failure);
                throw failure;
            }
        }

        @Override
        public Watch.Response<V1Pod> next() {
            try {
                return delegate.next();
            } catch (RuntimeException failure) {
                reportSwallowed(failure);
                throw failure;
            }
        }

        @Override
        public Iterator<Watch.Response<V1Pod>> iterator() {
            return this;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        /**
         * Reports a connection failure of an already established stream.
         *
         * <p>The reflector treats a connection loss while reading the stream exactly like a failed
         * reconnect: it sleeps and retries without notifying the informer, so the failure has to
         * reach the budget from here for the same reason as the watch call above. Failures the
         * reflector reports itself are left to it, which keeps every failure counted once.
         *
         * @param failure failure thrown while reading the stream
         */
        private void reportSwallowed(RuntimeException failure) {
            if (isConnectException(failure)) {
                handleWatchFailure(failure);
            }
        }
    }

    /**
     * @param failure watch failure
     * @return whether the reflector retries this failure without reporting it
     */
    private static boolean isConnectException(Throwable failure) {
        if (failure instanceof ConnectException) {
            return true;
        }
        return failure.getCause() instanceof ConnectException;
    }
}
