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

import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;
import org.apache.seatunnel.resource.kubernetes.config.KubernetesSessionConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.PodWatchCallback;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.SessionWorkerPodFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Creates and adopts bare worker Pods for one session cluster.
 *
 * <p>This driver deliberately differs from the application-mode driver in two ways:
 *
 * <ul>
 *   <li>workers are owned by the session root ConfigMap, so they survive active-master failover;
 *   <li>{@link #close()} only stops observation and cancels reconnects. It never deletes workers,
 *       because a new active master may still need to adopt them, and it does not close the shared
 *       Kubernetes client because the process-level entrypoint owns that lifecycle.
 * </ul>
 */
@Slf4j
public final class KubernetesSessionResourceManagerDriver
        implements ResourceManagerDriver<KubernetesWorkerNode> {

    private final KubernetesClient api;
    private final KubernetesSessionConfig config;
    private final String sessionId;
    private final Set<String> workers = ConcurrentHashMap.newKeySet();
    private final Set<String> releasing = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<Void>> launches = ConcurrentHashMap.newKeySet();
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> pending =
            new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean reconnecting = new AtomicBoolean();

    private ResourceEventHandler<KubernetesWorkerNode> resourceEventHandler;
    private ScheduledExecutorService mainThreadExecutor;
    private Executor ioExecutor;
    private KubernetesConfigMap root;
    private KubernetesWatch watch;
    private volatile long reconnectDelayMillis = 1_000L;
    private volatile ScheduledFuture<?> reconnectTask;

    private final PodWatchCallback watchCallback =
            new PodWatchCallback() {
                @Override
                public void onPodAdded(KubernetesPod pod) {
                    // Workers are tracked when the API accepts the create request.
                }

                @Override
                public void onPodModified(KubernetesPod pod) {
                    if (pod.isTerminating() || pod.isTerminated() || pod.isUnknown()) {
                        onWorkerGone(pod.getName(), "Worker pod phase " + pod.getPhase());
                    }
                }

                @Override
                public void onPodDeleted(KubernetesPod pod) {
                    onWorkerGone(pod.getName(), "Worker pod deleted");
                }

                @Override
                public void onWatchError(Exception failure) {
                    handleWatchError(failure);
                }
            };

    public KubernetesSessionResourceManagerDriver(
            KubernetesClient api, KubernetesSessionConfig config, String sessionId) {
        this.api = api;
        this.config = config;
        this.sessionId = sessionId;
    }

    @Override
    public synchronized void initialize(
            ResourceEventHandler<KubernetesWorkerNode> resourceEventHandler,
            ScheduledExecutorService mainThreadExecutor,
            Executor ioExecutor,
            Supplier<String> masterAddress)
            throws Exception {
        if (closed.get() || this.resourceEventHandler != null) {
            throw new IllegalStateException(
                    "Session Kubernetes driver already initialized or closed");
        }
        this.resourceEventHandler = resourceEventHandler;
        this.mainThreadExecutor = mainThreadExecutor;
        this.ioExecutor = ioExecutor;
        this.root = api.getConfigMap(config.getRootConfigMap());
        if (root.getInternalResource().getMetadata() == null
                || root.getInternalResource().getMetadata().getUid() == null) {
            throw new IllegalStateException(
                    "Session root ConfigMap "
                            + config.getRootConfigMap()
                            + " has no server-assigned UID");
        }
        running.set(true);
        startWatch();
        log.info(
                "Initialized Kubernetes session driver for {} in namespace {}, root={}",
                sessionId,
                config.getNamespace(),
                config.getRootConfigMap());
    }

    private synchronized void startWatch() throws Exception {
        if (closed.get()) {
            return;
        }
        KubernetesWatch next = api.watchPods(selector(), watchCallback);
        KubernetesWatch previous = watch;
        watch = next;
        if (previous != null) {
            previous.close();
        }
        running.set(true);
        reconnectDelayMillis = 1_000L;
        if (ioExecutor != null) {
            ioExecutor.execute(this::resyncWorkers);
        }
    }

    private void resyncWorkers() {
        if (closed.get() || !running.get()) {
            return;
        }
        try {
            Set<String> live = ConcurrentHashMap.newKeySet();
            for (KubernetesPod pod : api.listPods(selector())) {
                if (!pod.isTerminated() && !pod.isTerminating()) {
                    live.add(pod.getName());
                }
            }
            for (String name : new ArrayList<>(workers)) {
                if (!live.contains(name)) {
                    onWorkerGone(name, "Worker pod missing after watch reconnect");
                }
            }
        } catch (Exception failure) {
            log.warn("Failed to resynchronize session worker Pods for {}", sessionId, failure);
        }
    }

    private void handleWatchError(Exception failure) {
        if (closed.get()) {
            return;
        }
        running.set(false);
        if (resourceEventHandler != null) {
            try {
                mainThreadExecutor.execute(() -> resourceEventHandler.onError(failure));
            } catch (RuntimeException rejected) {
                log.debug("Could not publish session driver watch error", rejected);
            }
        }
        scheduleWatchReconnect();
    }

    private void scheduleWatchReconnect() {
        if (closed.get() || !reconnecting.compareAndSet(false, true)) {
            return;
        }
        long delay = reconnectDelayMillis;
        reconnectDelayMillis = Math.min(reconnectDelayMillis * 2, 30_000L);
        try {
            reconnectTask =
                    mainThreadExecutor.schedule(
                            () -> {
                                reconnecting.set(false);
                                if (closed.get()) {
                                    return;
                                }
                                try {
                                    startWatch();
                                    log.info(
                                            "Reconnected session worker Pod watch for {}",
                                            sessionId);
                                } catch (Exception failure) {
                                    log.warn(
                                            "Failed to reconnect session worker Pod watch for {}",
                                            sessionId,
                                            failure);
                                    scheduleWatchReconnect();
                                }
                            },
                            delay,
                            TimeUnit.MILLISECONDS);
        } catch (RuntimeException failure) {
            reconnecting.set(false);
            log.warn("Could not schedule session worker Pod watch reconnect", failure);
        }
    }

    @Override
    public CompletableFuture<KubernetesWorkerNode> requestWorker(WorkerSpecification resources) {
        CompletableFuture<KubernetesWorkerNode> future = new CompletableFuture<>();
        if (!running.get() || closed.get()) {
            future.completeExceptionally(
                    new IllegalStateException("Session Kubernetes driver is not running"));
            return future;
        }
        String name = sessionId + "-worker-" + UUID.randomUUID().toString().substring(0, 8);
        KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(name));
        workers.add(name);
        pending.put(name, future);
        CompletableFuture<Void> launch =
                CompletableFuture.runAsync(
                        () -> createWorker(name, resources, worker, future), ioExecutor);
        launches.add(launch);
        launch.whenComplete(
                (ignored, failure) -> {
                    launches.remove(launch);
                    if (failure != null) {
                        future.completeExceptionally(failure);
                    }
                });
        return future;
    }

    private void createWorker(
            String name,
            WorkerSpecification resources,
            KubernetesWorkerNode worker,
            CompletableFuture<KubernetesWorkerNode> future) {
        try {
            api.createPod(
                    new KubernetesPod(
                            SessionWorkerPodFactory.createWorkerPod(
                                    root, name, config, resources)));
            pending.remove(name);
            future.complete(worker);
            log.info("Created session worker Pod {}", name);
        } catch (Exception failure) {
            pending.remove(name);
            workers.remove(name);
            future.completeExceptionally(failure);
            log.warn("Failed to create session worker Pod {}", name, failure);
        }
    }

    @Override
    public List<KubernetesWorkerNode> listWorkers() {
        List<KubernetesWorkerNode> result = new ArrayList<>();
        for (KubernetesPod pod : api.listPods(selector())) {
            if (!pod.isTerminated() && !pod.isTerminating()) {
                result.add(new KubernetesWorkerNode(new ResourceID(pod.getName())));
            }
        }
        return result;
    }

    @Override
    public void adoptWorkers(List<KubernetesWorkerNode> adopted) {
        for (KubernetesWorkerNode worker : adopted) {
            workers.add(worker.getResourceID().getResourceIdString());
        }
    }

    @Override
    public CompletableFuture<Void> releaseWorker(KubernetesWorkerNode worker) {
        String name = worker.getResourceID().getResourceIdString();
        workers.remove(name);
        releasing.add(name);
        return CompletableFuture.runAsync(
                () -> {
                    try {
                        api.deletePod(name);
                    } catch (RuntimeException failure) {
                        throw new CompletionException(failure);
                    } finally {
                        releasing.remove(name);
                    }
                },
                ioExecutor);
    }

    private void onWorkerGone(String name, String diagnostics) {
        if (!running.get() || releasing.contains(name) || !workers.remove(name)) {
            return;
        }
        KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(name));
        mainThreadExecutor.execute(
                () -> resourceEventHandler.onWorkerTerminated(worker, diagnostics));
    }

    private String selector() {
        return KubernetesResourceIdentity.selector(sessionId, KubernetesConstants.WORKER_ROLE);
    }

    /** Stops observation without deleting workers; the shared Kubernetes client stays open. */
    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        if (reconnectTask != null) {
            reconnectTask.cancel(false);
            reconnectTask = null;
        }
        for (CompletableFuture<KubernetesWorkerNode> future : pending.values()) {
            future.completeExceptionally(
                    new CancellationException("Session master is stepping down"));
        }
        pending.clear();
        if (watch != null) {
            watch.close();
        }
        for (CompletableFuture<Void> launch : launches) {
            try {
                launch.get(25, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception failure) {
                log.warn("Timed out draining session worker launches", failure);
                break;
            }
        }
    }
}
