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

import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.heartbeat.HeartbeatListener;
import org.apache.seatunnel.engine.server.resourcemanager.heartbeat.HeartbeatManager;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ReleaseSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.RequestSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotAndWorkerProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractResourceManager implements ResourceManager {

    private static final long DEFAULT_WORKER_CHECK_INTERVAL = 500;
    private static final ILogger LOGGER = Logger.getLogger(AbstractResourceManager.class);

    protected final Map<String, WorkerProfile> registerWorker;

    private final HeartbeatManager heartbeatManager;
    private final NodeEngine nodeEngine;

    private final ExecutionMode mode = ExecutionMode.LOCAL;

    public AbstractResourceManager(NodeEngine nodeEngine) {
        this.registerWorker = new ConcurrentHashMap<>();
        this.nodeEngine = nodeEngine;
        this.heartbeatManager = new HeartbeatManager(new WorkerHeartbeatListener());
    }

    @Override
    public void init() {
        heartbeatManager.start(Executors.newSingleThreadScheduledExecutor());
    }

    @Override
    public CompletableFuture<SlotProfile> applyResource(long jobId, ResourceProfile resourceProfile) throws NoEnoughResourceException {
        waitingWorkerRegister();
        CompletableFuture<SlotProfile> completableFuture = new CompletableFuture<>();
        applyResources(jobId, Collections.singletonList(resourceProfile)).whenComplete((profile, error) -> {
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
                while (registerWorker.isEmpty()) {
                    Thread.sleep(DEFAULT_WORKER_CHECK_INTERVAL);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public CompletableFuture<List<SlotProfile>> applyResources(long jobId,
                                                               List<ResourceProfile> resourceProfile) throws NoEnoughResourceException {
        // TODO Apply for the profile with the largest resources first, and then decrease in order. In this
        //  way, the success rate of resource application will be higher.
        waitingWorkerRegister();
        CompletableFuture<List<SlotProfile>> completableFuture = new CompletableFuture<>();
        CompletableFuture<Void> requestSlotFuture = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger();
        List<SlotProfile> slotProfiles = new CopyOnWriteArrayList<>();
        List<CompletableFuture<SlotAndWorkerProfile>> allRequestFuture = new ArrayList<>();
        for (ResourceProfile r : resourceProfile) {
            Optional<WorkerProfile> workerProfile =
                registerWorker.values().stream().filter(worker -> worker.getUnassignedResource().enoughThan(r)).findAny();

            if (!workerProfile.isPresent()) {
                workerProfile =
                    registerWorker.values().stream().filter(worker -> Arrays.stream(worker.getUnassignedSlots()).anyMatch(slot -> slot.getResourceProfile().enoughThan(r))).findAny();
            }

            if (workerProfile.isPresent()) {
                InvocationFuture<SlotAndWorkerProfile> future = sendToMember(new RequestSlotOperation(jobId, r), workerProfile.get().getAddress());
                CompletableFuture<SlotAndWorkerProfile> internalCompletableFuture = future.whenComplete(
                    (slotAndWorkerProfile, error) -> {
                        if (error != null) {
                            throw new RuntimeException(error);
                        } else {
                            workerTouch(slotAndWorkerProfile.getWorkerProfile());
                            if (null != slotAndWorkerProfile.getSlotProfile()) {
                                slotProfiles.add(slotAndWorkerProfile.getSlotProfile());
                                if (successCount.incrementAndGet() == resourceProfile.size()) {
                                    completableFuture.complete(slotProfiles);
                                }
                            } else {
                                slotProfiles.add(null);
                            }
                        }
                        if (slotProfiles.size() == resourceProfile.size()) {
                            requestSlotFuture.complete(null);
                        }
                    }
                );
                allRequestFuture.add(internalCompletableFuture);
            } else {
                CompletableFuture.allOf(allRequestFuture.toArray(new CompletableFuture[0])).whenComplete((unused, error) -> {
                        releaseAllResourceInternal(jobId, slotProfiles);
                        if (error != null) {
                            completableFuture.completeExceptionally(error);
                        } else {
                            completableFuture.completeExceptionally(new NoEnoughResourceException("can't apply resource request: " + r));
                        }
                    }
                );
                return completableFuture;
            }
        }
        CompletableFuture.allOf(allRequestFuture.toArray(new CompletableFuture[0])).whenComplete((unused, error) -> {
            if (error != null) {
                releaseAllResourceInternal(jobId, slotProfiles);
                completableFuture.completeExceptionally(error);
            }
            if (slotProfiles.contains(null)) {
                if (supportDynamicWorker()) {
                    List<ResourceProfile> needApplyResource = new ArrayList<>();
                    List<Integer> needApplyIndex = new ArrayList<>();
                    for (int i = 0; i < slotProfiles.size(); i++) {
                        if (slotProfiles.get(i) == null) {
                            needApplyResource.add(resourceProfile.get(i));
                            needApplyIndex.add(i);
                        }
                    }
                    findNewWorker(needApplyResource);
                    applyResources(jobId, needApplyResource).whenComplete((s, e) -> {
                        if (e != null) {
                            releaseAllResourceInternal(jobId, slotProfiles);
                            completableFuture.completeExceptionally(e);
                            return;
                        }
                        for (int i = 0; i < s.size(); i++) {
                            slotProfiles.set(needApplyIndex.get(i), s.get(i));
                            if (successCount.incrementAndGet() == resourceProfile.size()) {
                                completableFuture.complete(slotProfiles);
                            }
                        }

                    });
                } else {
                    releaseAllResourceInternal(jobId, slotProfiles);
                    completableFuture.completeExceptionally(new NoEnoughResourceException("can't apply resource request: " + resourceProfile.get(slotProfiles.indexOf(null))));
                }
            }
        });
        return completableFuture;
    }

    protected boolean supportDynamicWorker() {
        return false;
    }

    private void releaseAllResourceInternal(long jobId, List<SlotProfile> slotProfiles) {
        LOGGER.warning("apply resource not success, release all already applied resource");
        slotProfiles.stream().filter(Objects::nonNull).forEach(profile -> {
            releaseResource(jobId, profile);
        });
    }

    /**
     * find new worker in third party resource manager, it returned after worker register successes.
     *
     * @param resourceProfiles the worker should have resource profile list
     */
    protected void findNewWorker(List<ResourceProfile> resourceProfiles) {
        throw new UnsupportedOperationException("Unsupported operation to find new worker in " + this.getClass().getName());
    }

    private <E> InvocationFuture<E> sendToMember(Operation operation, Address address) {
        InvocationBuilder invocationBuilder =
                nodeEngine.getOperationService().createInvocationBuilder(SeaTunnelServer.SERVICE_NAME,
                        operation, address);
        return invocationBuilder.invoke();
    }

    @Override
    public CompletableFuture<Void> releaseResources(long jobId, List<SlotProfile> profiles) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        AtomicInteger releaseCount = new AtomicInteger();
        for (SlotProfile profile : profiles) {
            releaseResource(jobId, profile).whenComplete((r, e) -> {
                if (e != null) {
                    completableFuture.completeExceptionally(e);
                } else {
                    if (releaseCount.incrementAndGet() == profiles.size()) {
                        completableFuture.complete(null);
                    }
                }
            });
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile) {
        return sendToMember(new ReleaseSlotOperation(jobId, profile), profile.getWorker());
    }

    @Override
    public void workerTouch(WorkerProfile workerProfile) {
        if (!registerWorker.containsKey(workerProfile.getWorkerID())) {
            LOGGER.info("received new worker register: " + workerProfile.getAddress());
        } else {
            LOGGER.fine("received worker heartbeat from: " + workerProfile.getAddress());
        }
        registerWorker.put(workerProfile.getWorkerID(), workerProfile);
        heartbeatFromWorker(workerProfile.getWorkerID());
    }

    @Override
    public void heartbeatFromWorker(String workerID) {
        heartbeatManager.heartbeat(workerID);
    }

    private class WorkerHeartbeatListener implements HeartbeatListener {
        @Override
        public void nodeDisconnected(String nodeID) {
            heartbeatManager.removeNode(nodeID);
            registerWorker.remove(nodeID);
        }
    }
}
