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

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean closed = new AtomicBoolean();

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
    public synchronized void init() {
        if (closed.get()) {
            throw new IllegalStateException("Session resource manager has already been closed");
        }
        super.init();
        try {
            driver.initialize(this, mainThreadExecutor, ioExecutor, this::masterAddress);
            List<WorkerType> existing = driver.listWorkers();
            driver.adoptWorkers(existing);
            for (WorkerType worker : existing) {
                workers.put(workerId(worker), worker);
            }
            ensureMinWorkers();
            awaitMinWorkers();
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
        if (supportDynamicWorker() && workerCount(tagFilter) == 0) {
            findNewWorker(resourceProfiles, tagFilter);
        }
        return super.applyResources(jobId, resourceProfiles, tagFilter);
    }

    /**
     * Creates enough workers to cover the slots that the pending job could not acquire.
     *
     * <p>The worker count is derived from the missing slot count and the fixed slots per worker.
     * The method waits for the requested workers to register before returning so the caller can
     * retry slot allocation immediately. It never exceeds {@code max-workers}.
     */
    @Override
    protected synchronized void findNewWorker(
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
        int currentRegistered = workerCount(Collections.emptyMap());
        int currentTotal = currentRegistered + pending.size();
        int create =
                calculateWorkers(
                        resourceProfiles.size(),
                        slotsPerWorker,
                        currentTotal,
                        specification.getMaxWorkers());
        if (create <= 0) {
            throw new NoEnoughResourceException(
                    String.format(
                            "Session cluster reached max-workers=%d before satisfying %d missing slots",
                            specification.getMaxWorkers(), resourceProfiles.size()));
        }

        int targetRegistered = currentRegistered + create;
        log.info(
                "Provisioning {} worker(s) for {} missing slot(s); target registered workers={}, max-workers={}",
                create,
                resourceProfiles.size(),
                targetRegistered,
                specification.getMaxWorkers());
        requestWorkers(create);
        awaitRegistration(targetRegistered);
    }

    /** Creates at least the configured minimum worker set. */
    private void ensureMinWorkers() {
        int missing = specification.getMinWorkers() - currentWorkerCount();
        if (missing > 0) {
            log.info("Creating {} worker(s) to reach min-workers", missing);
            requestWorkers(missing);
        }
    }

    /**
     * Waits for the initial worker set before scheduling can observe an empty cluster.
     *
     * <p>Without this wait a job submitted immediately after the master starts can see zero
     * registered workers and provision extra workers that are no longer needed once the min-workers
     * finish starting. A failed initial set is logged and left to the normal provisioning/retry
     * path rather than failing coordinator startup.
     */
    private void awaitMinWorkers() {
        if (workerCount(Collections.emptyMap()) >= specification.getMinWorkers()) {
            return;
        }
        try {
            awaitRegistration(specification.getMinWorkers());
        } catch (NoEnoughResourceException failure) {
            log.warn(
                    "Session min-workers did not register within {} ms; leaving provisioning to the pending-job path",
                    specification.getStartupTimeoutMillis(),
                    failure);
        }
    }

    private synchronized void requestWorkers(int requested) {
        if (closed.get()) {
            return;
        }
        int available = specification.getMaxWorkers() - currentWorkerCount();
        int create = Math.min(Math.max(requested, 0), Math.max(available, 0));
        for (int index = 0; index < create; index++) {
            String token = specification.getName() + "-request-" + System.nanoTime() + "-" + index;
            try {
                CompletableFuture<WorkerType> future =
                        driver.requestWorker(specification.getWorkerSpecification());
                pending.put(token, future);
                future.whenComplete(
                        (worker, failure) -> {
                            pending.remove(token);
                            if (failure != null) {
                                log.warn(
                                        "Failed to create session worker request {}",
                                        token,
                                        failure);
                            } else if (worker != null) {
                                workers.put(workerId(worker), worker);
                                log.info("Session worker created: {}", workerId(worker));
                            }
                        });
            } catch (RuntimeException failure) {
                log.warn("Failed to submit session worker request {}", token, failure);
            }
        }
    }

    private void awaitRegistration(int target) {
        long deadline =
                System.nanoTime()
                        + TimeUnit.MILLISECONDS.toNanos(specification.getStartupTimeoutMillis());
        while (workerCount(Collections.emptyMap()) < target) {
            if (System.nanoTime() >= deadline) {
                throw new NoEnoughResourceException(
                        String.format(
                                "Timed out waiting for session workers to register: expected=%d, current=%d, timeoutMs=%d",
                                target,
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

    private int currentWorkerCount() {
        return workerCount(Collections.emptyMap()) + pending.size();
    }

    private static String workerId(ResourceIDRetrievable worker) {
        return worker.getResourceID().getResourceIdString();
    }

    private String masterAddress() {
        Address address = nodeEngine.getThisAddress();
        String host = address.getHost();
        return (host.contains(":") ? "[" + host + "]" : host) + ":" + address.getPort();
    }

    @Override
    public void onWorkerTerminated(WorkerType worker, String diagnostics) {
        String id = workerId(worker);
        workers.remove(id);
        log.warn("Session worker {} terminated: {}", id, diagnostics);
        if (!closed.get()) {
            requestWorkers(1);
        }
    }

    @Override
    public void onError(Throwable exception) {
        log.error("Session resource manager reported an error", exception);
    }

    @Override
    protected void closeResourceManager() {
        closed.set(true);
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
