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
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.RandomStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SlotAllocationStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SlotRatioStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SystemLoadStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ReleaseSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ResetResourceOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.SyncWorkerProfileOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.RequestSlotOperationStats;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractResourceManager implements ResourceManager {

    private static final long DEFAULT_WORKER_CHECK_INTERVAL = 500;

    @Getter public final ConcurrentMap<Address, WorkerProfile> registerWorker;

    private final NodeEngine nodeEngine;

    private final ExecutionMode mode;

    @Getter private final EngineConfig engineConfig;

    private volatile boolean isRunning = true;

    @Getter private final SlotAllocationStrategy slotAllocationStrategy;

    // Track master-side slot request cost without changing allocation behavior.
    private final AtomicLong requestSlotOperationSuccessCount = new AtomicLong();
    private final AtomicLong requestSlotOperationNoSlotCount = new AtomicLong();
    private final AtomicLong requestSlotOperationFailureCount = new AtomicLong();
    private final AtomicLong requestSlotOperationLastInvocationLatencyMs = new AtomicLong();
    private final AtomicLong requestSlotOperationMaxInvocationLatencyMs = new AtomicLong();

    public AbstractResourceManager(NodeEngine nodeEngine, EngineConfig engineConfig) {
        this.registerWorker = new ConcurrentHashMap<>();
        this.nodeEngine = nodeEngine;
        this.engineConfig = engineConfig;
        this.mode = engineConfig.getMode();

        switch (engineConfig.getSlotServiceConfig().getAllocateStrategy()) {
            case SYSTEM_LOAD:
                this.slotAllocationStrategy = new SystemLoadStrategy();
                break;
            case SLOT_RATIO:
                this.slotAllocationStrategy = new SlotRatioStrategy();
                break;
            case RANDOM:
            default:
                this.slotAllocationStrategy = new RandomStrategy();
                break;
        }
    }

    @Override
    public void init() {
        log.info("Init ResourceManager");
        initWorker();
    }

    private void initWorker() {
        log.info("initWorker... ");
        List<Address> aliveNode =
                nodeEngine.getClusterService().getMembers().stream()
                        .map(Member::getAddress)
                        .collect(Collectors.toList());
        log.info("init live nodes: {}", aliveNode);
        List<CompletableFuture<Void>> futures =
                aliveNode.stream()
                        .map(
                                node ->
                                        sendToMember(new SyncWorkerProfileOperation(), node)
                                                .thenAccept(
                                                        p -> {
                                                            if (p != null) {
                                                                registerWorker.put(
                                                                        node, (WorkerProfile) p);
                                                                log.info(
                                                                        "received new worker register: "
                                                                                + ((WorkerProfile)
                                                                                                p)
                                                                                        .getAddress());
                                                            }
                                                        }))
                        .collect(Collectors.toList());
        futures.forEach(CompletableFuture::join);

        log.info("registerWorker: {}", registerWorker);
    }

    @Override
    public CompletableFuture<SlotProfile> applyResource(
            long jobId, ResourceProfile resourceProfile, Map<String, String> tagFilter)
            throws NoEnoughResourceException {
        CompletableFuture<SlotProfile> completableFuture = new CompletableFuture<>();
        applyResources(jobId, Collections.singletonList(resourceProfile), tagFilter)
                .whenComplete(
                        (profile, error) -> {
                            if (error != null) {
                                completableFuture.completeExceptionally(error);
                            } else {
                                completableFuture.complete(profile.get(0));
                            }
                        });
        return completableFuture;
    }

    private void waitingWorkerRegister() {
        if (ExecutionMode.LOCAL.equals(mode)) {
            // Local mode, should wait worker(master node) register.
            try {
                while (registerWorker.isEmpty() && isRunning) {
                    log.info("waiting current worker register to resource manager...");
                    Thread.sleep(DEFAULT_WORKER_CHECK_INTERVAL);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        log.warn(
                "Node heartbeat timeout, disconnected for resource manager. "
                        + "Node Address: "
                        + event.getMember().getAddress());
        registerWorker.remove(event.getMember().getAddress());
    }

    @Override
    public CompletableFuture<List<SlotProfile>> applyResources(
            long jobId, List<ResourceProfile> resourceProfile, Map<String, String> tagFilter)
            throws NoEnoughResourceException {
        waitingWorkerRegister();
        ConcurrentMap<Address, WorkerProfile> matchedWorker = filterWorkerByTag(tagFilter);
        if (matchedWorker.isEmpty()) {
            log.error("No matched worker with tag filter {}.", tagFilter);
            throw new NoEnoughResourceException();
        }
        return new ResourceRequestHandler(
                        jobId, resourceProfile, matchedWorker, this, slotAllocationStrategy)
                .request(tagFilter);
    }

    protected boolean supportDynamicWorker() {
        return false;
    }

    /**
     * find new worker in third party resource manager, it returned after worker register successes.
     *
     * @param resourceProfiles the worker should have resource profile list
     */
    protected void findNewWorker(
            List<ResourceProfile> resourceProfiles, Map<String, String> tagFilter) {
        throw new UnsupportedOperationException(
                "Unsupported operation to find new worker in " + this.getClass().getName());
    }

    @Override
    public void close() {
        isRunning = false;
    }

    protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
        return new CompletableFuture<>(
                NodeEngineUtil.sendOperationToMemberNode(nodeEngine, operation, address));
    }

    void recordRequestSlotOperationSuccess(long elapsedMillis) {
        updateRequestSlotOperationLatency(elapsedMillis);
        requestSlotOperationSuccessCount.incrementAndGet();
    }

    void recordRequestSlotOperationNoSlot(long elapsedMillis) {
        updateRequestSlotOperationLatency(elapsedMillis);
        requestSlotOperationNoSlotCount.incrementAndGet();
    }

    void recordRequestSlotOperationFailure(long elapsedMillis) {
        updateRequestSlotOperationLatency(elapsedMillis);
        requestSlotOperationFailureCount.incrementAndGet();
    }

    private void updateRequestSlotOperationLatency(long elapsedMillis) {
        requestSlotOperationLastInvocationLatencyMs.set(elapsedMillis);
        requestSlotOperationMaxInvocationLatencyMs.accumulateAndGet(elapsedMillis, Math::max);
    }

    /** Returns the latest master-side RequestSlotOperation observability snapshot. */
    @Override
    public RequestSlotOperationStats getRequestSlotOperationStats() {
        return new RequestSlotOperationStats(
                requestSlotOperationSuccessCount.get(),
                requestSlotOperationNoSlotCount.get(),
                requestSlotOperationFailureCount.get(),
                requestSlotOperationLastInvocationLatencyMs.get(),
                requestSlotOperationMaxInvocationLatencyMs.get());
    }

    @Override
    public CompletableFuture<Void> releaseResources(long jobId, List<SlotProfile> profiles) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        Set<Address> releaseWorkers =
                profiles.stream()
                        .map(SlotProfile::getWorker)
                        .filter(
                                address ->
                                        nodeEngine.getClusterService().getMember(address) != null)
                        .collect(Collectors.toSet());
        for (SlotProfile profile : profiles) {
            futures.add(releaseResource(jobId, profile, false));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete(
                        (r, e) -> {
                            if (e != null) {
                                completableFuture.completeExceptionally(e);
                            } else {
                                refreshWorkerProfiles(releaseWorkers)
                                        .whenComplete(
                                                (unused, refreshError) -> {
                                                    if (refreshError != null) {
                                                        completableFuture.completeExceptionally(
                                                                refreshError);
                                                    } else {
                                                        completableFuture.complete(null);
                                                    }
                                                });
                            }
                        });
        return completableFuture;
    }

    /**
     * Refreshes the master-side worker profile cache for the given workers.
     *
     * <p>This is invoked from the resource-release critical path AFTER the slot release future
     * has already completed on the worker side. We deduplicate by worker so a batch release that
     * targets several slots on the same worker only triggers one profile sync, and we apply the
     * refreshed profile to {@code registerWorker} via {@link #heartbeat(WorkerProfile)}.
     *
     * <p>Note: refresh failures are intentionally propagated to the caller. The slot itself has
     * already been released, so the master cache will be temporarily stale; surfacing the failure
     * lets the outer release future fail loudly rather than silently diverging from the worker
     * state until the next heartbeat corrects it.
     */
    private CompletableFuture<Void> refreshWorkerProfiles(Collection<Address> workers) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (workers.isEmpty()) {
            result.complete(null);
            return result;
        }

        List<CompletableFuture<WorkerProfile>> futures =
                workers.stream()
                        .map(
                                worker ->
                                        this.<WorkerProfile>sendToMember(
                                                new SyncWorkerProfileOperation(), worker))
                        .collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete(
                        (unused, error) -> {
                            if (error != null) {
                                result.completeExceptionally(error);
                                return;
                            }
                            futures.forEach(
                                    future -> {
                                        WorkerProfile workerProfile = future.join();
                                        if (workerProfile != null) {
                                            heartbeat(workerProfile);
                                        }
                                    });
                            result.complete(null);
                        });
        return result;
    }

    @Override
    public CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile) {
        return releaseResource(jobId, profile, true);
    }

    private CompletableFuture<Void> releaseResource(
            long jobId, SlotProfile profile, boolean refreshWorkerProfile) {
        if (nodeEngine.getClusterService().getMember(profile.getWorker()) != null) {
            CompletableFuture<Object> releaseFuture =
                    sendToMember(new ReleaseSlotOperation(jobId, profile), profile.getWorker());
            CompletableFuture<Void> result = new CompletableFuture<>();
            releaseFuture.whenComplete(
                    (unused, error) -> {
                        if (error != null) {
                            result.completeExceptionally(error);
                            return;
                        }
                        if (!refreshWorkerProfile) {
                            result.complete(null);
                            return;
                        }
                        refreshWorkerProfiles(Collections.singleton(profile.getWorker()))
                                .whenComplete(
                                        (refreshUnused, refreshError) -> {
                                            if (refreshError != null) {
                                                result.completeExceptionally(refreshError);
                                            } else {
                                                result.complete(null);
                                            }
                                        });
                    });
            return result;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public boolean slotActiveCheck(SlotProfile profile) {
        boolean active = false;
        if (registerWorker.containsKey(profile.getWorker())) {
            active =
                    Arrays.stream(registerWorker.get(profile.getWorker()).getAssignedSlots())
                            .anyMatch(
                                    s ->
                                            s.getSlotID() == profile.getSlotID()
                                                    && s.getSequence()
                                                            .equals(profile.getSequence()));
        }

        if (!active) {
            log.info("received slot active check failed, profile: " + profile);
        } else {
            log.info("received slot active check success, profile: " + profile);
        }
        return active;
    }

    @Override
    public void heartbeat(WorkerProfile workerProfile) {
        if (!registerWorker.containsKey(workerProfile.getAddress())) {
            log.info("received new worker register: " + workerProfile.getAddress());
            sendToMember(new ResetResourceOperation(), workerProfile.getAddress()).join();
        } else {
            log.debug("received worker heartbeat from: " + workerProfile.getAddress());
        }
        registerWorker.put(workerProfile.getAddress(), workerProfile);

        this.updateWorkerLoad(workerProfile);
    }

    /** Update worker load info. */
    private void updateWorkerLoad(WorkerProfile workerProfile) {
        if (slotAllocationStrategy instanceof SystemLoadStrategy
                && Objects.nonNull(workerProfile.getSystemLoadInfo())) {
            ((SystemLoadStrategy) slotAllocationStrategy)
                    .updateWorkerLoad(
                            workerProfile.getAddress(), workerProfile.getSystemLoadInfo());
        }
    }

    @Override
    public List<SlotProfile> getUnassignedSlots(Map<String, String> tags) {
        return filterWorkerByTag(tags).values().stream()
                .flatMap(workerProfile -> Arrays.stream(workerProfile.getUnassignedSlots()))
                .collect(Collectors.toList());
    }

    @Override
    public List<SlotProfile> getAssignedSlots(Map<String, String> tags) {
        return filterWorkerByTag(tags).values().stream()
                .flatMap(workerProfile -> Arrays.stream(workerProfile.getAssignedSlots()))
                .collect(Collectors.toList());
    }

    @Override
    public int workerCount(Map<String, String> tags) {
        return filterWorkerByTag(tags).size();
    }

    private ConcurrentMap<Address, WorkerProfile> filterWorkerByTag(Map<String, String> tagFilter) {
        if (tagFilter == null || tagFilter.isEmpty()) {
            return registerWorker;
        }
        return registerWorker.entrySet().stream()
                .filter(
                        e -> {
                            Map<String, String> workerAttr = e.getValue().getAttributes();
                            if (workerAttr == null || workerAttr.isEmpty()) {
                                return false;
                            }
                            boolean match = true;
                            for (Map.Entry<String, String> entry : tagFilter.entrySet()) {
                                if (!workerAttr.containsKey(entry.getKey())
                                        || !workerAttr
                                                .get(entry.getKey())
                                                .equals(entry.getValue())) {
                                    return false;
                                }
                            }
                            return match;
                        })
                .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
