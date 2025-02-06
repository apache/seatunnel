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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SlotAllocationStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.RequestSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotAndWorkerProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

/** Handle each slot request from resource manager */
public class ResourceRequestHandler {

    private static final ILogger LOGGER = Logger.getLogger(ResourceRequestHandler.class);
    private final CompletableFuture<List<SlotProfile>> completableFuture;
    /*
     * Cache the slot already request successes, and not request success or not request finished will be null.
     * The key match with {@link resourceProfile} index. Meaning which value in resultSlotProfiles index is null, the
     * resourceProfile with same index in resourceProfile haven't requested successes yet.
     */
    private final ConcurrentMap<Integer, SlotProfile> resultSlotProfiles;
    private final ConcurrentMap<Address, WorkerProfile> registerWorker;

    private static final int MAX_RETRY_TIMES = 3;

    private final long jobId;

    private final List<ResourceProfile> resourceProfile;

    private final AbstractResourceManager resourceManager;

    private final AllocateStrategy allocateStrategy;

    private final SlotAllocationStrategy slotAllocationStrategy;

    public ResourceRequestHandler(
            long jobId,
            List<ResourceProfile> resourceProfile,
            ConcurrentMap<Address, WorkerProfile> registerWorker,
            AbstractResourceManager resourceManager,
            SlotAllocationStrategy slotAllocationStrategy) {
        this.completableFuture = new CompletableFuture<>();
        this.resultSlotProfiles = new ConcurrentHashMap<>();
        this.jobId = jobId;
        this.resourceProfile = resourceProfile;
        this.registerWorker = registerWorker;
        this.resourceManager = resourceManager;
        this.allocateStrategy =
                resourceManager.getEngineConfig().getSlotServiceConfig().getAllocateStrategy();
        this.slotAllocationStrategy = slotAllocationStrategy;
    }

    public CompletableFuture<List<SlotProfile>> request(Map<String, String> tags) {
        requestSlotWithRetry(resourceProfile, MAX_RETRY_TIMES, tags);
        return completableFuture;
    }

    private CompletableFuture<SlotAndWorkerProfile> requestSlotWithRetry(
            List<ResourceProfile> request, int retryTimes, Map<String, String> tags) {
        if (retryTimes <= 0) {
            LOGGER.fine("can't apply resource request with retry times: " + MAX_RETRY_TIMES);
            return CompletableFuture.supplyAsync(
                    () -> {
                        throw new NoEnoughResourceException(
                                "can't apply resource request with retry times: "
                                        + MAX_RETRY_TIMES);
                    });
        }
        List<CompletableFuture<SlotAndWorkerProfile>> allRequestFuture = requestSlots(request);
        // all resource preCheck done, also had sent request to worker
        return getAllOfFuture(allRequestFuture)
                .whenComplete(
                        withTryCatch(
                                LOGGER,
                                (unused, error) -> {
                                    if (error != null) {
                                        completeRequestWithException(error);
                                    } else {
                                        List<ResourceProfile> needRequestResource =
                                                stillNeedRequestResource();
                                        if (!needRequestResource.isEmpty()) {
                                            Exception requestSlotWithRetryError = null;
                                            try {
                                                requestSlotWithRetry(
                                                                needRequestResource,
                                                                retryTimes - 1,
                                                                tags)
                                                        .get();
                                            } catch (Exception e) {
                                                LOGGER.warning(
                                                        "request slot with retry error: "
                                                                + e.getMessage());
                                                requestSlotWithRetryError = e;
                                            }
                                            if (requestSlotWithRetryError != null) {
                                                // meaning have some slot not request success
                                                if (resourceManager.supportDynamicWorker()) {
                                                    applyByDynamicWorker(tags);
                                                } else {
                                                    completeRequestWithException(
                                                            requestSlotWithRetryError);
                                                }
                                            }
                                        }
                                    }
                                }));
    }

    private List<ResourceProfile> stillNeedRequestResource() {
        List<ResourceProfile> needRequestResource = new ArrayList<>();
        for (int i = 0; i < resourceProfile.size(); i++) {
            if (!resultSlotProfiles.containsKey(i)) {
                needRequestResource.add(resourceProfile.get(i));
            }
        }
        return needRequestResource;
    }

    private List<CompletableFuture<SlotAndWorkerProfile>> requestSlots(
            List<ResourceProfile> requestProfile) {
        List<CompletableFuture<SlotAndWorkerProfile>> allRequestFuture = new ArrayList<>();

        for (int i = 0; i < requestProfile.size(); i++) {
            ResourceProfile r = requestProfile.get(i);
            Optional<WorkerProfile> workerProfile = preCheckWorkerResource(r);
            if (workerProfile.isPresent()) {
                // request slot to member
                CompletableFuture<SlotAndWorkerProfile> internalCompletableFuture =
                        singleResourceRequestToMember(i, r, workerProfile.get());
                allRequestFuture.add(internalCompletableFuture);
            } else {
                // if no worker can provide the resource, we should return a failed future
                LOGGER.fine("pre check worker resource failed, can't apply resource request: " + r);
                allRequestFuture.add(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    throw new NoEnoughResourceException(
                                            "can't apply resource request: " + r);
                                }));
            }
        }
        return allRequestFuture;
    }

    private void completeRequestWithException(Throwable e) {
        releaseAllResourceInternal();
        completableFuture.completeExceptionally(e);
    }

    private void addSlotToCacheMap(int index, SlotProfile slotProfile) {
        // null value means the slot request failed, no suitable slot found
        if (null != slotProfile) {
            resultSlotProfiles.put(index, slotProfile);
            if (resultSlotProfiles.size() == resourceProfile.size()) {
                List<SlotProfile> value = new ArrayList<>();
                for (int i = 0; i < resultSlotProfiles.size(); i++) {
                    value.add(resultSlotProfiles.get(i));
                }
                completableFuture.complete(value);
            }
        } else {
            LOGGER.fine("no suitable slot found for resource: " + resourceProfile.get(index));
        }
    }

    private CompletableFuture<SlotAndWorkerProfile> singleResourceRequestToMember(
            int i, ResourceProfile r, WorkerProfile workerProfile) {
        CompletableFuture<SlotAndWorkerProfile> future =
                resourceManager.sendToMember(
                        new RequestSlotOperation(jobId, r), workerProfile.getAddress());
        return future.whenComplete(
                withTryCatch(
                        LOGGER,
                        (slotAndWorkerProfile, error) -> {
                            if (error != null) {
                                throw new RuntimeException(error);
                            } else {
                                resourceManager.heartbeat(slotAndWorkerProfile.getWorkerProfile());
                                addSlotToCacheMap(i, slotAndWorkerProfile.getSlotProfile());
                            }
                        }));
    }

    @VisibleForTesting
    public Optional<WorkerProfile> preCheckWorkerResource(ResourceProfile r) {
        List<WorkerProfile> workerProfiles =
                Arrays.asList(registerWorker.values().toArray(new WorkerProfile[0]));

        List<WorkerProfile> availableWorkers =
                workerProfiles.stream()
                        .filter(
                                worker ->
                                        Arrays.stream(worker.getUnassignedSlots())
                                                .anyMatch(
                                                        slot ->
                                                                slot.getResourceProfile()
                                                                        .enoughThan(r)))
                        .collect(Collectors.toList());

        Optional<WorkerProfile> workerProfile =
                slotAllocationStrategy.selectWorker(availableWorkers);

        if (!workerProfile.isPresent()) {
            // Check if there are still unassigned resources
            if (allocateStrategy == AllocateStrategy.RANDOM) {
                Collections.shuffle(workerProfiles);
            }
            workerProfile =
                    workerProfiles.stream()
                            .filter(WorkerProfile::isDynamicSlot)
                            .filter(worker -> worker.getUnassignedResource().enoughThan(r))
                            .findAny();
        }

        return workerProfile;
    }

    /**
     * When the {@link DeployType} supports dynamic workers and the resources of the current worker
     * cannot meet the requirements of resource application, we can dynamically request the
     * third-party resource management to create a new worker, and then complete the resource
     * application
     */
    private void applyByDynamicWorker(Map<String, String> tags) {
        List<ResourceProfile> needApplyResource = new ArrayList<>();
        List<Integer> needApplyIndex = new ArrayList<>();
        for (int i = 0; i < resultSlotProfiles.size(); i++) {
            if (!resultSlotProfiles.containsKey(i)) {
                needApplyResource.add(resourceProfile.get(i));
                needApplyIndex.add(i);
            }
        }
        resourceManager.findNewWorker(needApplyResource, tags);
        resourceManager
                .applyResources(jobId, needApplyResource, tags)
                .whenComplete(
                        withTryCatch(
                                LOGGER,
                                (s, e) -> {
                                    if (e != null) {
                                        completeRequestWithException(e);
                                        return;
                                    }
                                    for (int i = 0; i < s.size(); i++) {
                                        addSlotToCacheMap(needApplyIndex.get(i), s.get(i));
                                    }
                                }));
    }

    private void releaseAllResourceInternal() {
        LOGGER.warning("apply resource not success, release all already applied resource");
        new ArrayList<>(resultSlotProfiles.keySet())
                .forEach(
                        index -> {
                            SlotProfile profile = resultSlotProfiles.remove(index);
                            if (profile != null) {
                                resourceManager.releaseResource(jobId, profile);
                            }
                        });
    }

    private <T> CompletableFuture<T> getAllOfFuture(List<CompletableFuture<T>> allRequestFuture) {
        return (CompletableFuture<T>)
                CompletableFuture.allOf(allRequestFuture.toArray(new CompletableFuture[0]));
    }
}
