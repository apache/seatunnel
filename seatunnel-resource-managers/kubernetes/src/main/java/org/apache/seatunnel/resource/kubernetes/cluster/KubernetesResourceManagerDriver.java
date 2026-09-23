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

package org.apache.seatunnel.resource.kubernetes.cluster;

import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerDriver;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerRegistration;
import org.apache.seatunnel.resource.core.application.ApplicationSpecification;
import org.apache.seatunnel.resource.core.application.WorkerSpecification;
import org.apache.seatunnel.resource.kubernetes.client.KubernetesApi;

import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1Pod;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Fixed-size worker provisioning with lifecycle failure detection and explicit cleanup. */
final class KubernetesResourceManagerDriver implements ResourceManagerDriver {
    private final KubernetesApi api;
    private final ApplicationSpecification specification;
    private final Set<String> workers = new HashSet<>();
    private final Set<String> releasing = new HashSet<>();
    private final ScheduledExecutorService monitor =
            Executors.newSingleThreadScheduledExecutor(
                    r -> {
                        Thread thread = new Thread(r, "seatunnel-kubernetes-worker-monitor");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final ExecutorService launches =
            Executors.newSingleThreadExecutor(
                    r -> {
                        Thread thread = new Thread(r, "seatunnel-kubernetes-worker-launch");
                        thread.setDaemon(true);
                        return thread;
                    });
    private final Map<String, CompletableFuture<WorkerRegistration>> pending = new HashMap<>();
    private ResourceManagerContext context;
    private V1Job job;
    private int nextWorker;
    private boolean running;
    private boolean closed;
    private boolean workersStopped;

    KubernetesResourceManagerDriver(KubernetesApi api, ApplicationSpecification specification) {
        this.api = api;
        this.specification = specification;
    }

    /**
     * Resolves the owner Job and starts asynchronous observation before admitting workers.
     *
     * @param context native runtime callbacks and the isolated application identity
     * @throws Exception if the owner cannot be read or the driver is already initialized
     */
    @Override
    public synchronized void initialize(ResourceManagerContext context) throws Exception {
        if (closed || this.context != null) {
            throw new IllegalStateException(
                    "Kubernetes driver has already been initialized or closed");
        }
        this.context = context;
        this.job = api.getJob(context.getApplicationId().getId());
        this.running = true;
        monitor.scheduleWithFixedDelay(this::checkWorkers, 1, 1, TimeUnit.SECONDS);
    }

    /**
     * Enqueues allocation without blocking the runtime's startup deadline on an SDK request.
     *
     * @param resources fixed worker CPU, memory and slot requirements
     * @return allocation future; completion means the Pod was created, not registered with Zeta
     */
    @Override
    public synchronized CompletableFuture<WorkerRegistration> requestWorker(
            WorkerSpecification resources) {
        CompletableFuture<WorkerRegistration> future = new CompletableFuture<>();
        if (!running) {
            future.completeExceptionally(
                    new IllegalStateException("Kubernetes driver is not running"));
            return future;
        }
        String name = job.getMetadata().getName() + "-worker-" + nextWorker++;
        // Register before creation: ambiguous HTTP failures must still be deleted on close.
        workers.add(name);
        pending.put(name, future);
        launches.execute(() -> launchWorker(name, resources, future));
        return future;
    }

    private void launchWorker(
            String name,
            WorkerSpecification resources,
            CompletableFuture<WorkerRegistration> future) {
        synchronized (this) {
            if (!running) {
                future.completeExceptionally(new CancellationException("Application is stopping"));
                return;
            }
        }
        try {
            api.createPod(
                    KubernetesResources.worker(
                            job,
                            name,
                            specification,
                            resources,
                            context.getClusterName(),
                            context.getMasterAddress()));
            synchronized (this) {
                if (running) {
                    pending.remove(name);
                    future.complete(new WorkerRegistration(name));
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
     * @throws Exception when deletion fails; close will retry tracked workers
     */
    @Override
    public void releaseWorker(WorkerRegistration worker) throws Exception {
        String name = worker.getWorkerId();
        synchronized (this) {
            releasing.add(name);
        }
        try {
            api.deletePod(name);
            synchronized (this) {
                workers.remove(name);
            }
        } finally {
            synchronized (this) {
                releasing.remove(name);
            }
        }
    }

    void checkWorkers() {
        Set<String> observed;
        synchronized (this) {
            if (!running) {
                return;
            }
            observed = new HashSet<>(workers);
            observed.removeAll(pending.keySet());
            observed.removeAll(releasing);
            if (observed.isEmpty()) {
                return;
            }
        }
        try {
            Map<String, V1Pod> current = new HashMap<>();
            for (V1Pod pod :
                    api.listPods(
                            KubernetesResources.selector(job.getMetadata().getName())
                                    + ","
                                    + KubernetesResources.ROLE_LABEL
                                    + "=worker")) {
                current.put(pod.getMetadata().getName(), pod);
            }
            synchronized (this) {
                if (!running) {
                    return;
                }
                for (String name : observed) {
                    if (!workers.contains(name) || releasing.contains(name)) {
                        continue;
                    }
                    V1Pod pod = current.get(name);
                    String phase =
                            pod == null || pod.getStatus() == null
                                    ? null
                                    : pod.getStatus().getPhase();
                    if (pod == null
                            || pod.getMetadata().getDeletionTimestamp() != null
                            || "Failed".equals(phase)
                            || "Succeeded".equals(phase)) {
                        running = false;
                        context.onWorkerTerminated(
                                name,
                                pod == null
                                        ? "Worker pod disappeared"
                                        : "Worker pod terminated with phase " + phase);
                        return;
                    }
                }
            }
        } catch (Exception e) {
            synchronized (this) {
                if (running) {
                    running = false;
                    context.onError(e);
                }
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
        synchronized (this) {
            if (workersStopped) {
                return;
            }
            workersStopped = true;
            running = false;
            for (CompletableFuture<WorkerRegistration> future : pending.values()) {
                future.completeExceptionally(new CancellationException("Application is stopping"));
            }
        }
        monitor.shutdownNow();
        launches.shutdownNow();
        Exception failure = null;
        try {
            // SDK calls have a 10s total timeout; a racing create may issue one compensating
            // delete.
            if (!launches.awaitTermination(25, TimeUnit.SECONDS)) {
                failure =
                        new IllegalStateException(
                                "Timed out draining Kubernetes worker allocations");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            failure = e;
        }
        if (job != null) {
            try {
                api.deleteWorkers(job.getMetadata().getName());
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
