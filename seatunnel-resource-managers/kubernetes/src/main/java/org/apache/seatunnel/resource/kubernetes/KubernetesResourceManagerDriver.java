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
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceEventHandler;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceID;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.KubernetesResourceFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.parameters.KubernetesApplicationParameters;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Fixed-size worker provisioning with lifecycle failure detection and explicit cleanup. */
public final class KubernetesResourceManagerDriver
        implements ResourceManagerDriver<KubernetesWorkerNode> {
    private static final long WORKER_WATCH_INTERVAL_MILLIS = 1_000;

    private final KubernetesClient api;
    private final KubernetesApplicationParameters parameters;
    private final String applicationId;
    private final String clusterName;
    private final Map<String, KubernetesWorkerNode> workers = new HashMap<>();
    private final Set<String> releasing = new HashSet<>();
    private final Set<String> missingWorkers = new HashSet<>();
    private final Set<CompletableFuture<Void>> launches = new HashSet<>();
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> pending = new HashMap<>();
    private Supplier<String> masterAddress;
    private ResourceEventHandler<KubernetesWorkerNode> resourceEventHandler;
    private ScheduledExecutorService mainThreadExecutor;
    private Executor ioExecutor;
    private KubernetesJob job;
    private KubernetesWatch workerWatch;
    private final IdGenerator idGenerator;
    private boolean running;
    private boolean closed;
    private boolean workersStopped;

    /**
     * Takes ownership of the client and fixed application identity/cluster name; initialize starts
     * observation and close releases the client.
     */
    public KubernetesResourceManagerDriver(
            KubernetesClient api,
            KubernetesApplicationParameters parameters,
            String applicationId,
            String clusterName) {
        this.api = api;
        this.parameters = parameters;
        this.applicationId = applicationId;
        this.clusterName = clusterName;
        this.idGenerator = new IdGenerator();
    }

    /**
     * Resolves the owner Job and starts asynchronous observation before admitting workers.
     *
     * @param resourceEventHandler receives unexpected worker exits and driver failures
     * @param mainThreadExecutor serializes resource callbacks
     * @param ioExecutor executes worker creation; owned by the caller
     * @param masterAddress supplies the bound master endpoint for each worker launch
     * @throws Exception if the owner cannot be read or the driver is already initialized
     */
    @Override
    public synchronized void initialize(
            ResourceEventHandler<KubernetesWorkerNode> resourceEventHandler,
            ScheduledExecutorService mainThreadExecutor,
            Executor ioExecutor,
            Supplier<String> masterAddress)
            throws Exception {
        if (closed || this.resourceEventHandler != null) {
            throw new IllegalStateException(
                    "Kubernetes driver has already been initialized or closed");
        }
        this.masterAddress = masterAddress;
        this.resourceEventHandler = resourceEventHandler;
        this.mainThreadExecutor = mainThreadExecutor;
        this.ioExecutor = ioExecutor;
        this.job = api.getJob(applicationId);
        this.running = true;
        this.workerWatch =
                api.watchPods(
                        KubernetesResourceFactory.selector(
                                job.getName(), KubernetesConstants.WORKER_ROLE),
                        WORKER_WATCH_INTERVAL_MILLIS,
                        this::checkWorkers,
                        this::onWatchFailure);
    }

    /**
     * Enqueues allocation without blocking the runtime's startup deadline on an SDK request.
     *
     * @param resources fixed worker CPU, memory and slot requirements
     * @return allocation future; completion means the Pod was created, not registered with Zeta
     */
    @Override
    public synchronized CompletableFuture<KubernetesWorkerNode> requestWorker(
            WorkerSpecification resources) {
        CompletableFuture<KubernetesWorkerNode> future = new CompletableFuture<>();
        if (!running) {
            future.completeExceptionally(
                    new IllegalStateException("Kubernetes driver is not running"));
            return future;
        }
        String name = job.getName() + "-worker-" + idGenerator.getNextId();
        // Register before creation: ambiguous HTTP failures must still be deleted on close.
        KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(name));
        workers.put(name, worker);
        pending.put(name, future);
        try {
            CompletableFuture<Void> launch =
                    CompletableFuture.runAsync(
                            () -> launchWorker(worker, resources, future), ioExecutor);
            launches.add(launch);
            launch.whenComplete(
                    (ignored, failure) -> {
                        synchronized (this) {
                            launches.remove(launch);
                        }
                        if (failure != null) {
                            future.completeExceptionally(failure);
                        }
                    });
        } catch (RuntimeException failure) {
            future.completeExceptionally(failure);
        }
        return future;
    }

    private void launchWorker(
            KubernetesWorkerNode worker,
            WorkerSpecification resources,
            CompletableFuture<KubernetesWorkerNode> future) {
        String name = worker.getResourceID().getResourceIdString();
        synchronized (this) {
            if (!running) {
                future.completeExceptionally(new CancellationException("Application is stopping"));
                return;
            }
        }
        try {
            api.createPod(
                    KubernetesResourceFactory.worker(
                            job, name, parameters, resources, clusterName, masterAddress.get()));
            synchronized (this) {
                if (running && future.complete(worker)) {
                    pending.remove(name);
                    return;
                }
            }
            // A request accepted just before close must not leave a Pod behind after cleanup.
            api.deletePod(name);
            future.completeExceptionally(
                    new CancellationException("Application stopped during allocation"));
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    }

    /**
     * Removes a worker and stops tracking it only after the API acknowledges deletion.
     *
     * @param worker previously allocated worker
     * @return completion of the Pod deletion; close will retry workers whose deletion fails
     */
    @Override
    public CompletableFuture<Void> releaseWorker(KubernetesWorkerNode worker) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        String name = worker.getResourceID().getResourceIdString();
        synchronized (this) {
            if (!workers.containsKey(name)) {
                return CompletableFuture.completedFuture(null);
            }
            releasing.add(name);
        }
        try {
            api.deletePod(name);
            synchronized (this) {
                workers.remove(name);
            }
            result.complete(null);
        } catch (Exception e) {
            result.completeExceptionally(e);
        } finally {
            synchronized (this) {
                releasing.remove(name);
            }
        }
        return result;
    }

    void checkWorkers() {
        try {
            String selector =
                    KubernetesResourceFactory.selector(
                            job.getName(), KubernetesConstants.WORKER_ROLE);
            checkWorkers(api.listPods(selector));
        } catch (Exception failure) {
            onWatchFailure(failure);
        }
    }

    private void checkWorkers(List<KubernetesPod> pods) {
        Set<String> observed;
        Set<String> missingNow = new HashSet<>();
        synchronized (this) {
            if (!running) {
                return;
            }
            observed = new HashSet<>(workers.keySet());
            observed.removeAll(pending.keySet());
            observed.removeAll(releasing);
            if (observed.isEmpty()) {
                missingWorkers.clear();
                return;
            }
        }
        Map<String, KubernetesPod> current = new HashMap<>();
        for (KubernetesPod pod : pods) {
            current.put(pod.getName(), pod);
        }
        KubernetesWorkerNode terminatedWorker = null;
        String diagnostics = null;
        synchronized (this) {
            if (!running) {
                return;
            }
            for (String name : observed) {
                KubernetesWorkerNode worker = workers.get(name);
                if (worker == null || releasing.contains(name)) {
                    continue;
                }
                KubernetesPod pod = current.get(name);
                if (pod != null && pod.isSucceeded()) {
                    workers.remove(name);
                    missingWorkers.remove(name);
                    continue;
                }
                if (pod == null || pod.isTerminating() || pod.isTerminated()) {
                    if (missingWorkers.contains(name)) {
                        terminatedWorker = worker;
                        diagnostics =
                                pod == null
                                        ? "Worker pod disappeared"
                                        : "Worker pod terminated with phase " + pod.getPhase();
                        break;
                    }
                    missingNow.add(name);
                }
            }
            if (terminatedWorker != null) {
                running = false;
                missingWorkers.clear();
            } else {
                missingWorkers.clear();
                missingWorkers.addAll(missingNow);
            }
        }
        if (terminatedWorker != null) {
            KubernetesWorkerNode worker = terminatedWorker;
            String reason = diagnostics;
            mainThreadExecutor.execute(
                    () -> resourceEventHandler.onWorkerTerminated(worker, reason));
        }
    }

    private void onWatchFailure(Exception failure) {
        synchronized (this) {
            if (running) {
                running = false;
                mainThreadExecutor.execute(() -> resourceEventHandler.onError(failure));
            }
        }
    }

    /**
     * Stops admission and monitoring, drains bounded in-flight creates, then deletes all workers.
     * The SDK remains usable for final application reporting until close. Repeated calls are safe.
     *
     * @throws Exception if allocation cannot terminate or the bulk deletion fails
     */
    @Override
    public void stopWorkers() throws Exception {
        CompletableFuture<?>[] pendingLaunches;
        synchronized (this) {
            if (workersStopped) {
                return;
            }
            workersStopped = true;
            running = false;
            pendingLaunches = launches.toArray(new CompletableFuture[0]);
            for (CompletableFuture<KubernetesWorkerNode> future : pending.values()) {
                future.completeExceptionally(new CancellationException("Application is stopping"));
            }
        }
        if (workerWatch != null) {
            workerWatch.close();
        }
        Exception failure = null;
        try {
            // SDK calls have a 10s total timeout; a racing create may issue one compensating
            // delete.
            CompletableFuture.allOf(pendingLaunches).get(25, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = e;
        } catch (Exception e) {
            failure = e;
        }
        if (job != null) {
            try {
                api.deleteWorkers(job.getName());
            } catch (Exception e) {
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
     * Stops remaining workers as a fallback and releases this driver's SDK connection.
     *
     * @throws Exception if worker cleanup fails; the SDK is still closed
     */
    @Override
    public void close() throws Exception {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        try {
            stopWorkers();
        } finally {
            api.close();
        }
    }
}
