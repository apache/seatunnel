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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Fixed-size worker provisioning with lifecycle failure detection and explicit cleanup. */
public final class KubernetesResourceManagerDriver
        implements ResourceManagerDriver<KubernetesWorkerNode> {
    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesResourceManagerDriver.class);

    private static final long WORKER_WATCH_INTERVAL_MILLIS = 1_000;

    private final KubernetesClient api;
    private final KubernetesApplicationParameters parameters;
    private final String applicationId;
    private final String clusterName;
    /**
     * Worker nodes currently owned by this driver, keyed by Pod name. Entries are removed on
     * explicit release or successful completion; anything still present is expected to be running.
     */
    private final Map<String, KubernetesWorkerNode> workers = new ConcurrentHashMap<>();
    /**
     * Workers whose deletion request is in flight. The watch must ignore them because a Pod can
     * briefly appear as terminating after release has already removed it from {@link #workers}.
     */
    private final Set<String> releasing = ConcurrentHashMap.newKeySet();
    /**
     * Workers absent in the previous poll. A worker is reported as terminated only after two
     * consecutive misses, avoiding a false failure when listPods races a concurrent create.
     */
    private final Set<String> missingWorkers = ConcurrentHashMap.newKeySet();
    /**
     * In-flight Pod creation tasks. Shutdown waits for these because a create accepted just before
     * close can still produce a Pod that must be compensated with a delete.
     */
    private final Set<CompletableFuture<Void>> launches = ConcurrentHashMap.newKeySet();
    /**
     * Pod creations whose result has not yet been published to the caller. Startup cancellation
     * completes these exceptionally, and successful launch removes the corresponding entry.
     */
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> pending =
            new ConcurrentHashMap<>();

    private Supplier<String> masterAddress;
    private ResourceEventHandler<KubernetesWorkerNode> resourceEventHandler;
    private ScheduledExecutorService mainThreadExecutor;
    private Executor ioExecutor;
    private KubernetesJob job;
    private KubernetesWatch workerWatch;
    private final IdGenerator idGenerator;
    /**
     * Admission and observation gate. Once false, requestWorker rejects new workers and the watch
     * stops publishing failures or terminations.
     */
    private final AtomicBoolean running = new AtomicBoolean();
    /** Set by close() so repeated lifecycle calls do not reopen or double-close the SDK. */
    private final AtomicBoolean closed = new AtomicBoolean();
    /** Guards stopWorkers() so late calls cannot start a second drain after cleanup is underway. */
    private final AtomicBoolean workersStopped = new AtomicBoolean();

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
        if (closed.get() || this.resourceEventHandler != null) {
            throw new IllegalStateException(
                    "Kubernetes driver has already been initialized or closed");
        }
        this.masterAddress = masterAddress;
        this.resourceEventHandler = resourceEventHandler;
        this.mainThreadExecutor = mainThreadExecutor;
        this.ioExecutor = ioExecutor;
        this.job = api.getJob(applicationId);
        this.running.set(true);
        this.workerWatch =
                api.watchPods(
                        KubernetesResourceFactory.selector(
                                job.getName(), KubernetesConstants.WORKER_ROLE),
                        WORKER_WATCH_INTERVAL_MILLIS,
                        this::checkWorkers,
                        this::onWatchFailure);
        LOG.info(
                "Initialized Kubernetes driver for application {}, watching worker pods of job {}",
                applicationId,
                job.getName());
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
        if (!running.get()) {
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
                        launches.remove(launch);
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
        if (!running.get()) {
            future.completeExceptionally(new CancellationException("Application is stopping"));
            return;
        }
        try {
            api.createPod(
                    KubernetesResourceFactory.worker(
                            job, name, parameters, resources, clusterName, masterAddress.get()));
            LOG.debug("Created worker pod {}", name);
            if (running.get() && future.complete(worker)) {
                pending.remove(name);
                return;
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
        if (workers.remove(name) == null) {
            return CompletableFuture.completedFuture(null);
        }
        releasing.add(name);
        try {
            api.deletePod(name);
            result.complete(null);
        } catch (Exception e) {
            result.completeExceptionally(e);
        } finally {
            releasing.remove(name);
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
        Set<String> missingNow = new HashSet<>();
        Set<String> observed = observedWorkers();
        if (observed.isEmpty()) {
            return;
        }
        Map<String, KubernetesPod> current = new HashMap<>();
        for (KubernetesPod pod : pods) {
            current.put(pod.getName(), pod);
        }
        KubernetesWorkerNode terminatedWorker = null;
        String diagnostics = null;
        // Require two consecutive misses before failing: listPods can race with a concurrent
        // create.
        if (!running.get()) {
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
            running.set(false);
            missingWorkers.clear();
        } else {
            missingWorkers.clear();
            missingWorkers.addAll(missingNow);
        }
        // Publish outside the lock so a slow event handler cannot block worker observation.
        if (terminatedWorker != null) {
            publishWorkerTermination(terminatedWorker, diagnostics);
        }
    }

    /**
     * Snapshots the workers that should be present, excluding in-flight and releasing ones.
     *
     * @return worker names to observe, or an empty set when no worker needs checking
     */
    private Set<String> observedWorkers() {
        // Snapshot the workers that are expected to exist, excluding in-flight and releasing ones.
        if (!running.get()) {
            return Collections.emptySet();
        }
        Set<String> observed = new HashSet<>(workers.keySet());
        observed.removeAll(pending.keySet());
        observed.removeAll(releasing);
        if (observed.isEmpty()) {
            missingWorkers.clear();
        }
        return observed;
    }

    /**
     * Publishes a worker termination event on the main-thread executor outside the driver lock.
     *
     * @param worker worker that unexpectedly disappeared or terminated
     * @param diagnostics platform-provided termination reason
     */
    private void publishWorkerTermination(KubernetesWorkerNode worker, String diagnostics) {
        LOG.warn(
                "Worker pod {} failed: {}",
                worker.getResourceID().getResourceIdString(),
                diagnostics);
        mainThreadExecutor.execute(
                () -> resourceEventHandler.onWorkerTerminated(worker, diagnostics));
    }

    private void onWatchFailure(Exception failure) {
        LOG.warn("Kubernetes worker watch failed", failure);
        if (running.compareAndSet(true, false)) {
            mainThreadExecutor.execute(() -> resourceEventHandler.onError(failure));
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
        if (!workersStopped.compareAndSet(false, true)) {
            return;
        }
        running.set(false);
        pendingLaunches = launches.toArray(new CompletableFuture[0]);
        for (CompletableFuture<KubernetesWorkerNode> future : pending.values()) {
            future.completeExceptionally(new CancellationException("Application is stopping"));
        }
        // Stop the poll loop before draining creates so a late snapshot cannot fail the shutdown.
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
            // Bulk delete is the final safety net for Pods that were never tracked or raced close.
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
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            stopWorkers();
        } finally {
            api.close();
        }
    }
}
