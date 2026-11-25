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

import org.apache.seatunnel.engine.common.Constant;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private final AtomicBoolean completionStarted = new AtomicBoolean(false);

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
        List<Integer> requestIndexes = new ArrayList<>();
        for (int i = 0; i < resourceProfile.size(); i++) {
            requestIndexes.add(i);
        }
        requestSlotWithRetry(requestIndexes, MAX_RETRY_TIMES, tags);
        return completableFuture;
    }

    private CompletableFuture<SlotAndWorkerProfile> requestSlotWithRetry(
            List<Integer> requestIndexes, int retryTimes, Map<String, String> tags) {
        if (retryTimes <= 0) {
            LOGGER.fine("can't apply resource request with retry times: " + MAX_RETRY_TIMES);
            return CompletableFuture.supplyAsync(
                    () -> {
                        throw new NoEnoughResourceException(
                                "can't apply resource request with retry times: "
                                        + MAX_RETRY_TIMES);
                    });
        }
        List<CompletableFuture<SlotAndWorkerProfile>> allRequestFuture =
                requestSlots(requestIndexes);
        // all resource preCheck done, also had sent request to worker
        return getAllOfFuture(allRequestFuture)
                .whenComplete(
                        withTryCatch(
                                LOGGER,
                                (unused, error) -> {
                                    if (error != null) {
                                        completeRequestWithException(toExternalException(error));
                                        return;
                                    } else {
                                        List<Integer> needRequestIndexes =
                                                stillNeedRequestIndexes();
                                        if (!needRequestIndexes.isEmpty()) {
                                            Exception requestSlotWithRetryError = null;
                                            try {
                                                requestSlotWithRetry(
                                                                needRequestIndexes,
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
                                                    int failedIndex =
                                                            findNullIndexInResultSlotProfiles();
                                                    String failureMessage =
                                                            String.format(
                                                                    "Apply resource not success for job: %d, required: %d slots, obtained: %d slots, "
                                                                            + "first unassigned resource at index %d: %s",
                                                                    jobId,
                                                                    resourceProfile.size(),
                                                                    resultSlotProfiles.size(),
                                                                    failedIndex,
                                                                    resourceProfile.get(
                                                                            failedIndex));
                                                    LOGGER.warning(failureMessage);
                                                    completeRequestWithException(
                                                            toExternalException(
                                                                    failureMessage,
                                                                    requestSlotWithRetryError));
                                                }
                                            }
                                        }
                                    }
                                }));
    }

    private List<Integer> stillNeedRequestIndexes() {
        List<Integer> needRequestIndexes = new ArrayList<>();
        for (int i = 0; i < resourceProfile.size(); i++) {
            if (!resultSlotProfiles.containsKey(i)) {
                needRequestIndexes.add(i);
            }
        }
        return needRequestIndexes;
    }

    private int findNullIndexInResultSlotProfiles() {
        for (int i = 0; i < resourceProfile.size(); i++) {
            if (!resultSlotProfiles.containsKey(i)) {
                return i;
            }
        }
        return 0;
    }

    private List<CompletableFuture<SlotAndWorkerProfile>> requestSlots(
            List<Integer> requestIndexes) {
        List<CompletableFuture<SlotAndWorkerProfile>> allRequestFuture = new ArrayList<>();

        for (int index : requestIndexes) {
            ResourceProfile r = resourceProfile.get(index);
            Optional<WorkerProfile> workerProfile = preCheckWorkerResource(r);
            if (workerProfile.isPresent()) {
                // request slot to member
                CompletableFuture<SlotAndWorkerProfile> internalCompletableFuture =
                        singleResourceRequestToMember(index, r, workerProfile.get());
                allRequestFuture.add(internalCompletableFuture);
            } else {
                // Leave the original index unassigned so retry or dynamic-worker provisioning can
                // handle it after all in-flight slot requests finish.
                LOGGER.fine("pre check worker resource failed, can't apply resource request: " + r);
            }
        }
        return allRequestFuture;
    }

    private void completeRequestWithException(Throwable e) {
        if (!completionStarted.compareAndSet(false, true)) {
            return;
        }
        releaseAllResourceInternal()
                .whenComplete(
                        (ignored, releaseError) -> {
                            if (releaseError != null) {
                                e.addSuppressed(releaseError);
                            }
                            completableFuture.completeExceptionally(e);
                        });
    }

    private void addSlotToCacheMap(int index, SlotProfile slotProfile) {
        // null value means the slot request failed, no suitable slot found
        if (null != slotProfile) {
            resultSlotProfiles.put(index, slotProfile);
            if (resultSlotProfiles.size() == resourceProfile.size()
                    && completionStarted.compareAndSet(false, true)) {
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
        long invocationStartNanos = System.nanoTime();
        CompletableFuture<SlotAndWorkerProfile> future =
                resourceManager.sendToMember(
                        new RequestSlotOperation(jobId, r), workerProfile.getAddress());
        return future.whenComplete(
                withTryCatch(
                        LOGGER,
                        (slotAndWorkerProfile, error) -> {
                            long elapsedMillis = elapsedMillisSince(invocationStartNanos);
                            if (error != null) {
                                resourceManager.recordRequestSlotOperationFailure(elapsedMillis);
                                throw new RuntimeException(error);
                            } else {
                                if (slotAndWorkerProfile.getSlotProfile() == null) {
                                    resourceManager.recordRequestSlotOperationNoSlot(elapsedMillis);
                                } else {
                                    resourceManager.recordRequestSlotOperationSuccess(
                                            elapsedMillis);
                                }
                                resourceManager.heartbeat(slotAndWorkerProfile.getWorkerProfile());
                                addSlotToCacheMap(i, slotAndWorkerProfile.getSlotProfile());
                            }
                        }));
    }

    private long elapsedMillisSince(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
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
        for (int i = 0; i < resourceProfile.size(); i++) {
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

    private CompletableFuture<Void> releaseAllResourceInternal() {
        int appliedCount = resultSlotProfiles.size();
        int requiredCount = resourceProfile.size();

        String slotIds =
                resultSlotProfiles.values().stream()
                        .filter(Objects::nonNull)
                        .map(
                                slot ->
                                        String.format(
                                                "Slot-%d@%s", slot.getSlotID(), slot.getWorker()))
                        .collect(Collectors.joining(", "));

        LOGGER.warning(
                String.format(
                        "Apply resource not success for job: %d, required: %d slots, applied: %d slots, "
                                + "releasing slots: [%s], remaining: %d slots not assigned",
                        jobId, requiredCount, appliedCount, slotIds, requiredCount - appliedCount));

        List<SlotProfile> acquiredSlots =
                resultSlotProfiles.values().stream()
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        acquiredSlots.forEach(
                profile ->
                        LOGGER.fine(
                                String.format(
                                        "Releasing slot %d for job %d from worker %s",
                                        profile.getSlotID(), jobId, profile.getWorker())));
        List<CompletableFuture<Void>> releaseFutures =
                acquiredSlots.stream()
                        .map(
                                profile ->
                                        releaseResourceWithRetry(
                                                profile, Constant.OPERATION_RETRY_TIME))
                        .collect(Collectors.toList());
        return getAllOfFuture(releaseFutures);
    }

    private CompletableFuture<Void> releaseResourceWithRetry(
            SlotProfile slotProfile, int remainingAttempts) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        CompletableFuture<Void> releaseFuture;
        try {
            releaseFuture = resourceManager.releaseResource(jobId, slotProfile);
        } catch (Exception e) {
            releaseFuture = new CompletableFuture<>();
            releaseFuture.completeExceptionally(e);
        }
        releaseFuture.whenComplete(
                (ignored, error) -> {
                    if (error == null) {
                        result.complete(null);
                    } else if (remainingAttempts > 1) {
                        LOGGER.warning(
                                String.format(
                                        "Release slot %d for job %d failed, retrying (%d attempts left)",
                                        slotProfile.getSlotID(), jobId, remainingAttempts - 1),
                                error);
                        releaseResourceWithRetry(slotProfile, remainingAttempts - 1)
                                .whenComplete(
                                        (retryIgnored, retryError) -> {
                                            if (retryError == null) {
                                                result.complete(null);
                                            } else {
                                                result.completeExceptionally(retryError);
                                            }
                                        });
                    } else {
                        result.completeExceptionally(error);
                    }
                });
        return result;
    }

    private NoEnoughResourceException toExternalException(Throwable error) {
        return toExternalException(buildFailureMessage(), error);
    }

    private NoEnoughResourceException toExternalException(String message, Throwable error) {
        Throwable root = unwrapCompletionException(error);
        if (root instanceof NoEnoughResourceException) {
            return (NoEnoughResourceException) root;
        }
        return new NoEnoughResourceException(message, root);
    }

    private String buildFailureMessage() {
        int failedIndex = findNullIndexInResultSlotProfiles();
        String failedResource =
                failedIndex >= 0 && failedIndex < resourceProfile.size()
                        ? String.valueOf(resourceProfile.get(failedIndex))
                        : "unknown";
        return String.format(
                "Apply resource not success for job: %d, required: %d slots, obtained: %d slots, "
                        + "first unassigned resource at index %d: %s",
                jobId,
                resourceProfile.size(),
                resultSlotProfiles.size(),
                failedIndex,
                failedResource);
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        Throwable current = error;
        while (current instanceof ExecutionException || current instanceof CompletionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }
        return current;
    }

    private <T> CompletableFuture<T> getAllOfFuture(List<CompletableFuture<T>> allRequestFuture) {
        return (CompletableFuture<T>)
                CompletableFuture.allOf(allRequestFuture.toArray(new CompletableFuture[0]));
    }
}
