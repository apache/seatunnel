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
import org.apache.seatunnel.resource.kubernetes.config.KubernetesApplicationConfig;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesClient;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesConstants;
import org.apache.seatunnel.resource.kubernetes.kubeclient.KubernetesResourceIdentity;
import org.apache.seatunnel.resource.kubernetes.kubeclient.PodWatchCallback;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.ConfigMapDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.EnvironmentDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.KubernetesStepDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.decorators.SecretDecorator;
import org.apache.seatunnel.resource.kubernetes.kubeclient.factory.WorkerPodFactory;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesJob;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.seatunnel.resource.kubernetes.kubeclient.resources.KubernetesWatch;

import io.kubernetes.client.openapi.models.V1Pod;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/** Fixed-size worker provisioning with lifecycle failure detection and explicit cleanup. */
@Slf4j
public final class KubernetesResourceManagerDriver
        implements ResourceManagerDriver<KubernetesWorkerNode> {

    private final KubernetesClient api;
    private final KubernetesApplicationConfig parameters;
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
            KubernetesApplicationConfig parameters,
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
                        KubernetesResourceIdentity.selector(
                                job.getName(), KubernetesConstants.WORKER_ROLE),
                        new PodWatchCallback() {
                            @Override
                            public void onPodAdded(KubernetesPod pod) {
                                // Workers are tracked when the API server accepts their creation.
                            }

                            @Override
                            public void onPodModified(KubernetesPod pod) {
                                onWorkerPodModified(pod);
                            }

                            @Override
                            public void onPodDeleted(KubernetesPod pod) {
                                onWorkerPodGone(pod.getName(), "Worker pod deleted");
                            }

                            @Override
                            public void onWatchError(Exception failure) {
                                onWatchFailure(failure);
                            }
                        });
        log.info(
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
            api.createPod(assembleWorkerPod(name, resources));
            log.debug("Created worker pod {}", name);
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
     * Assembles one worker Pod through the ordered decorator list.
     *
     * <p>The first step creates the Pod and every later step adds the features shared with the
     * master, so worker and master Pods cannot drift apart.
     *
     * @param name generated worker Pod name
     * @param resources per-worker resource and slot settings
     * @return worker Pod ready to create
     */
    KubernetesPod assembleWorkerPod(String name, WorkerSpecification resources) {
        V1Pod pod =
                WorkerPodFactory.createWorkerPod(
                        job, name, parameters, resources, clusterName, masterAddress.get());
        List<KubernetesStepDecorator> decorators =
                Arrays.asList(
                        new EnvironmentDecorator(parameters),
                        new SecretDecorator(job.getName(), parameters),
                        new ConfigMapDecorator(job.getName(), parameters));
        for (KubernetesStepDecorator decorator : decorators) {
            pod = decorator.decorate(pod);
        }
        return new KubernetesPod(pod);
    }

    /**
     * Removes a worker and stops tracking it only after the API acknowledges deletion.
     *
     * @param worker previously allocated worker
     * @return completion of the Pod deletion; close will retry workers whose deletion fails
     */
    @Override
    public CompletableFuture<Void> releaseWorker(KubernetesWorkerNode worker) {
        String name = worker.getResourceID().getResourceIdString();
        if (workers.remove(name) == null) {
            return CompletableFuture.completedFuture(null);
        }
        releasing.add(name);
        // Delete asynchronously so the runtime can release every worker in parallel within a single
        // cleanup deadline; blocking the caller per worker would serialize the whole stop budget.
        return CompletableFuture.runAsync(() -> deleteReleasedWorker(name), ioExecutor);
    }

    /** Deletes one released pod on the IO executor and stops ignoring it when the call returns. */
    private void deleteReleasedWorker(String name) {
        try {
            api.deletePod(name);
        } catch (Exception failure) {
            throw new CompletionException(failure);
        } finally {
            releasing.remove(name);
        }
    }

    /**
     * Reports a worker as lost when the watch sees its Pod terminate or start terminating.
     *
     * @param pod changed worker Pod
     */
    private void onWorkerPodModified(KubernetesPod pod) {
        if (!running.get()) {
            return;
        }
        if (pod.isSucceeded()) {
            // The Pod completed on its own; nothing is lost and it needs no further observation.
            workers.remove(pod.getName());
            return;
        }
        if (pod.isTerminating() || pod.isTerminated() || pod.isUnknown()) {
            onWorkerPodGone(pod.getName(), "Worker pod terminated with phase " + pod.getPhase());
        }
    }

    /**
     * Publishes one worker loss; because the watch event is authoritative, the first loss stops
     * observation and later events are ignored.
     *
     * @param name worker Pod name
     * @param diagnostics platform-provided termination reason
     */
    private void onWorkerPodGone(String name, String diagnostics) {
        if (!running.get() || releasing.contains(name)) {
            return;
        }
        KubernetesWorkerNode worker = workers.get(name);
        if (worker == null) {
            return;
        }
        if (running.compareAndSet(true, false)) {
            publishWorkerTermination(worker, diagnostics);
        }
    }

    /**
     * Publishes a worker termination event on the main-thread executor outside the driver lock.
     *
     * @param worker worker that unexpectedly disappeared or terminated
     * @param diagnostics platform-provided termination reason
     */
    private void publishWorkerTermination(KubernetesWorkerNode worker, String diagnostics) {
        log.warn(
                "Worker pod {} failed: {}",
                worker.getResourceID().getResourceIdString(),
                diagnostics);
        mainThreadExecutor.execute(
                () -> resourceEventHandler.onWorkerTerminated(worker, diagnostics));
    }

    private void onWatchFailure(Exception failure) {
        log.warn("Kubernetes worker watch failed", failure);
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
        // Stop the watch before draining creates so a late event cannot fail the shutdown.
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
