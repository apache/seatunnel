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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.spec.SessionSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceIDRetrievable;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Session-scoped worker lifecycle and slot registry.
 *
 * <p>The active master owns this manager. It creates the configured minimum number of workers,
 * provisions additional workers when a pending job needs more slots, and adopts existing workers
 * after a master failover. Closing this manager must never delete workers: the next active master
 * continues to own them.
 */
@Slf4j
public class SessionResourceManager<WorkerType extends ResourceIDRetrievable>
        extends AbstractResourceManager implements ResourceEventHandler<WorkerType> {

    private final SessionSpecification specification;
    private final ResourceManagerDriver<WorkerType> driver;
    private final Map<String, WorkerType> workers = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<WorkerType>> pending = new ConcurrentHashMap<>();
    private final ScheduledExecutorService mainThreadExecutor;
    private final ExecutorService ioExecutor;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicInteger consecutiveRequestFailures = new AtomicInteger();
    private volatile ScheduledFuture<?> reconcileTask;

    private static final long MAX_RECONCILE_DELAY_MILLIS = 60_000L;

    public SessionResourceManager(
            NodeEngine nodeEngine,
            EngineConfig engineConfig,
            SessionSpecification specification,
            ResourceManagerDriver<WorkerType> driver) {
        super(nodeEngine, engineConfig);
        this.specification = specification;
        this.driver = driver;
        this.mainThreadExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "seatunnel-session-resource-events");
                            thread.setDaemon(true);
                            return thread;
                        });
        this.ioExecutor =
                Executors.newFixedThreadPool(
                        4,
                        runnable -> {
                            Thread thread = new Thread(runnable, "seatunnel-session-resource-io");
                            thread.setDaemon(true);
                            return thread;
                        });
    }

    @Override
    public void init() {
        if (closed.get()) {
            throw new IllegalStateException("Session resource manager has already been closed");
        }
        if (!initialized.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "Session resource manager has already been initialized");
        }
        super.init();
        try {
            driver.initialize(this, mainThreadExecutor, ioExecutor, this::masterAddress);
            List<WorkerType> existing = driver.listWorkers();
            driver.adoptWorkers(existing);
            synchronized (this) {
                for (WorkerType worker : existing) {
                    workers.put(workerId(worker), worker);
                }
            }
            ensureMinWorkers();
            rescheduleReconcile(0);
        } catch (Exception failure) {
            close();
            throw new IllegalStateException(
                    "Could not initialize session resource manager", failure);
        }
    }

    @Override
    public boolean supportDynamicWorker() {
        return specification.isDynamicWorkerEnabled();
    }

    /**
     * Adds the session-specific "no workers at all" bootstrap to the common allocation path.
     *
     * <p>The common manager cannot create platform workers, so a session with no registered worker
     * must provision the first worker set before delegating to {@link
     * AbstractResourceManager#applyResources}. Once a worker exists, partial slot shortages are
     * handled by {@link ResourceRequestHandler}, which calls {@link #findNewWorker} after its
     * normal retries are exhausted.
     */
    @Override
    public CompletableFuture<List<SlotProfile>> applyResources(
            long jobId, List<ResourceProfile> resourceProfiles, Map<String, String> tagFilter)
            throws NoEnoughResourceException {
        if (supportDynamicWorker()) {
            awaitInitialWorkerRegistration();
            if (workers.isEmpty() && pending.isEmpty()) {
                findNewWorker(resourceProfiles, tagFilter);
            }
        }
        return super.applyResources(jobId, resourceProfiles, tagFilter);
    }

    /**
     * Waits for the initially created worker set to register before a job can be rejected.
     *
     * <p>The coordinator activates as soon as the worker Pods are launched, without waiting for
     * their Hazelcast heartbeats. A job submitted in that window would otherwise be rejected even
     * though the minimum worker set is healthy and about to register. This wait is bounded by
     * {@code startup-timeout-millis} and runs on the job scheduling thread, not under the manager
     * monitor or on the coordinator activation path.
     */
    private void awaitInitialWorkerRegistration() {
        int targetPods;
        synchronized (this) {
            targetPods = Math.min(specification.getMinWorkers(), currentWorkerCount());
        }
        if (targetPods <= 0) {
            return;
        }
        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(specification.getStartupTimeoutMillis());
        while (workers.size() < targetPods || workerCount(Collections.emptyMap()) < targetPods) {
            if (closed.get()) {
                throw new NoEnoughResourceException(
                        "Session resource manager closed while waiting for the initial workers");
            }
            if (System.nanoTime() >= deadline) {
                throw new NoEnoughResourceException(
                        String.format(
                                "Timed out waiting for initial session workers: expected=%d, pods=%d, registered=%d, timeoutMs=%d",
                                targetPods,
                                workers.size(),
                                workerCount(Collections.emptyMap()),
                                specification.getStartupTimeoutMillis()));
            }
            try {
                TimeUnit.MILLISECONDS.sleep(Math.min(200L, specification.getRetryIntervalMillis()));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NoEnoughResourceException(
                        "Interrupted while waiting for initial session workers", interrupted);
            }
        }
    }

    /**
     * Creates enough workers to cover the slots that the pending job could not acquire.
     *
     * <p>The worker budget is based on live or in-flight Pods rather than registered heartbeats, so
     * a slow or crash-looping Pod still consumes the {@code max-workers} budget. The reservation is
     * made under the manager monitor, but the registration wait runs after the monitor is released
     * so worker replacement and other jobs are not blocked behind one pending request.
     */
    @Override
    protected void findNewWorker(
            List<ResourceProfile> resourceProfiles, Map<String, String> tagFilter) {
        if (!specification.isDynamicWorkerEnabled()) {
            throw new NoEnoughResourceException(
                    "Dynamic worker provisioning is disabled for this session");
        }
        if (tagFilter != null && !tagFilter.isEmpty()) {
            throw new NoEnoughResourceException(
                    "Dynamic worker provisioning does not support tagged worker templates yet");
        }

        int slotsPerWorker = specification.getWorkerSpecification().getSlots();
        int registeredBefore = workerCount(Collections.emptyMap());
        int launched;
        synchronized (this) {
            int create =
                    calculateWorkers(
                            resourceProfiles.size(),
                            slotsPerWorker,
                            currentWorkerCount(),
                            specification.getMaxWorkers());
            if (create <= 0) {
                throw new NoEnoughResourceException(
                        String.format(
                                "Session cluster reached max-workers=%d before satisfying %d missing slots",
                                specification.getMaxWorkers(), resourceProfiles.size()));
            }
            log.info(
                    "Provisioning {} worker(s) for {} missing slot(s); live-or-pending workers={}, max-workers={}",
                    create,
                    resourceProfiles.size(),
                    currentWorkerCount(),
                    specification.getMaxWorkers());
            launched = requestWorkers(create);
        }
        if (launched <= 0) {
            throw new NoEnoughResourceException(
                    "No session worker could be launched for the pending request");
        }
        awaitAnyRegistration(registeredBefore);
    }

    /** Creates at least the configured minimum worker set. */
    private synchronized void ensureMinWorkers() {
        int missing = specification.getMinWorkers() - currentWorkerCount();
        if (missing > 0) {
            log.info("Creating {} worker(s) to reach min-workers", missing);
            requestWorkers(missing);
        }
    }

    /**
     * Periodic desired-state check for the minimum worker set.
     *
     * <p>Creation failures are retried with exponential backoff. The task is scheduled on the
     * manager's single event thread, so reconciliation cannot run concurrently with itself.
     */
    private void reconcile() {
        if (closed.get()) {
            return;
        }
        try {
            int missing;
            synchronized (this) {
                missing = specification.getMinWorkers() - currentWorkerCount();
            }
            if (missing > 0) {
                requestWorkers(missing);
            }
        } finally {
            rescheduleReconcile(reconcileDelayMillis());
        }
    }

    private long reconcileDelayMillis() {
        int failures = Math.min(consecutiveRequestFailures.get(), 6);
        long delay = Math.max(1L, specification.getRetryIntervalMillis());
        return Math.min(delay << failures, MAX_RECONCILE_DELAY_MILLIS);
    }

    private synchronized void rescheduleReconcile(long delayMillis) {
        if (closed.get()) {
            return;
        }
        if (reconcileTask != null) {
            reconcileTask.cancel(false);
        }
        try {
            reconcileTask =
                    mainThreadExecutor.schedule(
                            this::reconcile, Math.max(0L, delayMillis), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException failure) {
            if (!closed.get()) {
                log.warn("Could not schedule session worker reconciliation", failure);
            }
        }
    }

    /**
     * Reserves and launches up to {@code requested} workers within the configured budget.
     *
     * <p>Reservations are recorded in {@code pending} before the asynchronous launch starts, so
     * concurrent callers cannot exceed {@code max-workers} while Pods are being created.
     *
     * @param requested desired worker count
     * @return number of launch requests accepted
     */
    private synchronized int requestWorkers(int requested) {
        if (closed.get()) {
            return 0;
        }
        int available = specification.getMaxWorkers() - currentWorkerCount();
        int create = Math.min(Math.max(requested, 0), Math.max(available, 0));
        int launched = 0;
        for (int index = 0; index < create; index++) {
            String token = specification.getName() + "-request-" + System.nanoTime() + "-" + index;
            try {
                CompletableFuture<WorkerType> future =
                        driver.requestWorker(specification.getWorkerSpecification());
                pending.put(token, future);
                launched++;
                future.whenComplete(
                        (worker, failure) -> {
                            synchronized (SessionResourceManager.this) {
                                pending.remove(token);
                                if (failure != null) {
                                    consecutiveRequestFailures.incrementAndGet();
                                } else if (worker != null) {
                                    workers.put(workerId(worker), worker);
                                    consecutiveRequestFailures.set(0);
                                }
                            }
                            if (failure != null) {
                                log.warn(
                                        "Failed to create session worker request {}",
                                        token,
                                        failure);
                                rescheduleReconcile(reconcileDelayMillis());
                            } else if (worker != null) {
                                log.info("Session worker created: {}", workerId(worker));
                            }
                        });
            } catch (RuntimeException failure) {
                consecutiveRequestFailures.incrementAndGet();
                log.warn("Failed to submit session worker request {}", token, failure);
            }
        }
        return launched;
    }

    /**
     * Waits until at least one worker created after the supplied registered count has registered.
     *
     * <p>The caller retries slot allocation after this method returns. Waiting for a fixed target
     * equal to every requested worker would never complete when one Pod dies during startup; the
     * live Pod budget already prevents a retry from creating a replacement for the same slot.
     */
    private void awaitAnyRegistration(int registeredBefore) {
        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(specification.getStartupTimeoutMillis());
        while (workerCount(Collections.emptyMap()) <= registeredBefore) {
            if (closed.get()) {
                throw new NoEnoughResourceException(
                        "Session resource manager closed while waiting for workers to register");
            }
            if (System.nanoTime() >= deadline) {
                throw new NoEnoughResourceException(
                        String.format(
                                "Timed out waiting for session workers to register: baseline=%d, current=%d, timeoutMs=%d",
                                registeredBefore,
                                workerCount(Collections.emptyMap()),
                                specification.getStartupTimeoutMillis()));
            }
            try {
                TimeUnit.MILLISECONDS.sleep(Math.min(200L, specification.getRetryIntervalMillis()));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new NoEnoughResourceException(
                        "Interrupted while waiting for session workers to register", interrupted);
            }
        }
    }

    /**
     * Counts workers this manager has adopted or launched, plus launches still in flight.
     *
     * <p>This is deliberately not the heartbeat registry: a Pod that exists but has not joined
     * Hazelcast must still consume the {@code max-workers} budget.
     */
    private int currentWorkerCount() {
        return workers.size() + pending.size();
    }

    private static String workerId(ResourceIDRetrievable worker) {
        return worker.getResourceID().getResourceIdString();
    }

    private String masterAddress() {
        Address address = nodeEngine.getThisAddress();
        String host = address.getHost();
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + address.getPort();
    }

    /**
     * Removes a terminated worker, deletes its failed Pod, and lets the reconcile loop restore the
     * minimum worker count.
     *
     * <p>Replacement is intentionally asynchronous: a crash-looping image must not turn Pod
     * creation into a hot loop. The reconcile delay grows with consecutive request failures.
     */
    @Override
    public void onWorkerTerminated(WorkerType worker, String diagnostics) {
        String id = workerId(worker);
        synchronized (this) {
            workers.remove(id);
        }
        log.warn("Session worker {} terminated: {}", id, diagnostics);
        if (closed.get()) {
            return;
        }
        consecutiveRequestFailures.incrementAndGet();
        try {
            driver.releaseWorker(worker)
                    .whenComplete(
                            (ignored, failure) -> {
                                if (failure != null) {
                                    log.warn(
                                            "Failed to delete terminated session worker {}",
                                            id,
                                            failure);
                                }
                            });
        } catch (RuntimeException failure) {
            log.warn("Failed to delete terminated session worker {}", id, failure);
        }
        rescheduleReconcile(reconcileDelayMillis());
    }

    /**
     * A successful heartbeat proves the replacement is healthy enough to reset crash-loop backoff.
     */
    @Override
    public void heartbeat(WorkerProfile workerProfile) {
        super.heartbeat(workerProfile);
        consecutiveRequestFailures.set(0);
    }

    /**
     * Keeps the manager alive when the driver reports an observation failure.
     *
     * <p>The Kubernetes driver reconnects its watch with its own backoff; this callback also
     * schedules a minimum-worker reconciliation so a transient error cannot permanently leave the
     * pool below {@code min-workers}.
     */
    @Override
    public void onError(Throwable exception) {
        log.error("Session resource manager reported an error", exception);
        if (!closed.get()) {
            rescheduleReconcile(reconcileDelayMillis());
        }
    }

    @Override
    protected void closeResourceManager() {
        closed.set(true);
        ScheduledFuture<?> task = reconcileTask;
        if (task != null) {
            task.cancel(false);
        }
        for (CompletableFuture<WorkerType> future : pending.values()) {
            future.completeExceptionally(
                    new CancellationException("Session resource manager is closing"));
        }
        pending.clear();
        try {
            driver.close();
        } catch (Exception failure) {
            log.warn("Failed to close session resource manager driver", failure);
        } finally {
            mainThreadExecutor.shutdownNow();
            ioExecutor.shutdownNow();
        }
    }

    /**
     * Calculates how many homogeneous workers are needed for a slot shortage.
     *
     * @param missingSlots slots that could not be allocated
     * @param slotsPerWorker fixed slots offered by one worker
     * @param currentWorkers workers already ready or being created
     * @param maxWorkers session upper bound
     * @return number of workers to create, capped by the remaining worker budget
     */
    public static int calculateWorkers(
            int missingSlots, int slotsPerWorker, int currentWorkers, int maxWorkers) {
        if (missingSlots <= 0 || slotsPerWorker <= 0 || maxWorkers <= currentWorkers) {
            return 0;
        }
        int needed = (missingSlots + slotsPerWorker - 1) / slotsPerWorker;
        int available = maxWorkers - currentWorkers;
        return Math.min(needed, available);
    }

    /** @return worker ids currently adopted or created by this manager */
    public List<String> getWorkerIds() {
        return new ArrayList<>(workers.keySet());
    }

    /** @return worker requests that have not completed yet */
    public Set<String> getInFlightTokens() {
        return new ConcurrentHashMap<>(pending).keySet();
    }

    /** @return immutable session specification used by this manager */
    public SessionSpecification getSpecification() {
        return specification;
    }

    /** @return the homogeneous worker resource envelope */
    public WorkerSpecification getWorkerSpecification() {
        return specification.getWorkerSpecification();
    }
}
