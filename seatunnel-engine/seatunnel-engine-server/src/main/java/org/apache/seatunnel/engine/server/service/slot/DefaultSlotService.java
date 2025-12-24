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

package org.apache.seatunnel.engine.server.service.slot;

import org.apache.seatunnel.engine.common.config.server.AllocateStrategy;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.server.TaskExecutionService;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.WorkerHeartbeatOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.SneakyThrows;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** The slot service of seatunnel server, used for manage slot in worker. */
public class DefaultSlotService implements SlotService {

    private static final ILogger LOGGER = Logger.getLogger(DefaultSlotService.class);
    private static final long DEFAULT_HEARTBEAT_TIMEOUT = 5000;
    private static final int SYSTEM_LOAD_SEND_INTERVAL = 2;
    private final NodeEngineImpl nodeEngine;

    private AtomicReference<ResourceProfile> unassignedResource;

    private AtomicReference<ResourceProfile> assignedResource;

    private ConcurrentMap<Integer, SlotProfile> assignedSlots;

    private ConcurrentMap<Integer, SlotProfile> unassignedSlots;
    private ScheduledExecutorService scheduledExecutorService;
    private final SlotServiceConfig config;
    private volatile boolean initStatus;
    private final IdGenerator idGenerator;
    private final TaskExecutionService taskExecutionService;
    private ConcurrentMap<Integer, SlotContext> contexts;
    private String slotServiceSequence;

    public DefaultSlotService(
            NodeEngineImpl nodeEngine,
            TaskExecutionService taskExecutionService,
            SlotServiceConfig config) {
        this.nodeEngine = nodeEngine;
        this.config = config;
        this.taskExecutionService = taskExecutionService;
        this.idGenerator = new IdGenerator();
    }

    @Override
    public void init() {
        initStatus = true;
        slotServiceSequence = UUID.randomUUID().toString();
        contexts = new ConcurrentHashMap<>();
        assignedSlots = new ConcurrentHashMap<>();
        unassignedSlots = new ConcurrentHashMap<>();
        unassignedResource = new AtomicReference<>(new ResourceProfile());
        assignedResource = new AtomicReference<>(new ResourceProfile());
        scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor(
                        r ->
                                new Thread(
                                        r,
                                        String.format(
                                                "hz.%s.seaTunnel.slotService.thread",
                                                nodeEngine.getHazelcastInstance().getName())));
        if (!config.isDynamicSlot()) {
            initFixedSlots();
        }
        unassignedResource.set(getNodeResource());
        AtomicInteger systemLoadSendCountDown = new AtomicInteger(SYSTEM_LOAD_SEND_INTERVAL);
        scheduledExecutorService.scheduleAtFixedRate(
                () -> {
                    try {
                        LOGGER.fine(
                                "start send heartbeat to resource manager, this address: "
                                        + nodeEngine.getClusterService().getThisAddress());
                        // Must first obtain SYSTEM_LOAD and then obtain workProfile. If you obtain
                        // workProfile first and then obtain SYSTEM_LOAD, resource information will
                        // be reported inaccurately.
                        SystemLoadInfo systemLoadInfo =
                                Optional.of(systemLoadSendCountDown.decrementAndGet())
                                        .filter(
                                                count ->
                                                        count == 0
                                                                && config.getAllocateStrategy()
                                                                        == AllocateStrategy
                                                                                .SYSTEM_LOAD)
                                        .map(
                                                count -> {
                                                    systemLoadSendCountDown.set(
                                                            SYSTEM_LOAD_SEND_INTERVAL);
                                                    SystemLoadInfo info = new SystemLoadInfo();
                                                    info.setCpuPercentage(getCpuPercentage());
                                                    info.setMemPercentage(getMemPercentage());
                                                    LOGGER.fine("send system load info to master");
                                                    return info;
                                                })
                                        .orElse(null);

                        WorkerProfile workerProfile = getWorkerProfile();
                        Optional.ofNullable(systemLoadInfo)
                                .ifPresent(workerProfile::setSystemLoadInfo);

                        sendToMaster(new WorkerHeartbeatOperation(workerProfile)).join();
                    } catch (Exception e) {
                        LOGGER.warning(
                                "failed send heartbeat to resource manager, will retry later. this address: "
                                        + nodeEngine.getClusterService().getThisAddress());
                    }
                },
                0,
                DEFAULT_HEARTBEAT_TIMEOUT,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void reset() {
        if (!initStatus) {
            synchronized (this) {
                if (!initStatus) {
                    this.close();
                    init();
                }
            }
        }
    }

    @Override
    public synchronized SlotAndWorkerProfile requestSlot(
            long jobId, ResourceProfile resourceProfile) {
        initStatus = false;
        SlotProfile profile = selectBestMatchSlot(resourceProfile);
        if (profile != null) {
            profile.assign(jobId);
            assignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::merge);
            unassignedResource.accumulateAndGet(
                    profile.getResourceProfile(), ResourceProfile::subtract);
            unassignedSlots.remove(profile.getSlotID());
            assignedSlots.put(profile.getSlotID(), profile);
            contexts.computeIfAbsent(
                    profile.getSlotID(),
                    p -> new SlotContext(profile.getSlotID(), taskExecutionService));
        }
        LOGGER.fine(
                String.format(
                        "received slot request, jobID: %d, resource profile: %s, return: %s",
                        jobId, resourceProfile, profile));
        return new SlotAndWorkerProfile(getWorkerProfile(), profile);
    }

    @Override
    public SlotContext getSlotContext(SlotProfile slotProfile) {
        if (!contexts.containsKey(slotProfile.getSlotID())) {
            throw new WrongTargetSlotException(
                    "Unknown slot in slot service, slot profile: " + slotProfile);
        }
        return contexts.get(slotProfile.getSlotID());
    }

    @Override
    public synchronized void releaseSlot(long jobId, SlotProfile profile) {
        LOGGER.info(
                String.format(
                        "received slot release request, jobID: %d, slot: %s", jobId, profile));
        if (!assignedSlots.containsKey(profile.getSlotID())) {
            throw new WrongTargetSlotException(
                    "Not exist this slot in slot service, slot profile: " + profile);
        }

        if (!assignedSlots.get(profile.getSlotID()).getSequence().equals(profile.getSequence())) {
            throw new WrongTargetSlotException(
                    "Wrong slot sequence in profile, slot profile: " + profile);
        }

        if (assignedSlots.get(profile.getSlotID()).getOwnerJobID() != jobId) {
            throw new WrongTargetSlotException(
                    String.format(
                            "The profile %s not belong with job %d",
                            assignedSlots.get(profile.getSlotID()), jobId));
        }

        assignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::subtract);
        unassignedResource.accumulateAndGet(profile.getResourceProfile(), ResourceProfile::merge);
        profile.unassigned();
        if (!config.isDynamicSlot()) {
            unassignedSlots.put(profile.getSlotID(), profile);
        }
        assignedSlots.remove(profile.getSlotID());
        contexts.remove(profile.getSlotID());
    }

    @Override
    public void close() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    /**
     * Select the best match slot for the profile.
     *
     * @return the best match slot, null if no suitable slot found.
     */
    private SlotProfile selectBestMatchSlot(ResourceProfile profile) {
        if (unassignedSlots.isEmpty() && !config.isDynamicSlot()) {
            return null;
        }
        if (config.isDynamicSlot()) {
            if (unassignedResource.get().enoughThan(profile)) {
                return new SlotProfile(
                        nodeEngine.getThisAddress(),
                        (int) idGenerator.getNextId(),
                        profile,
                        slotServiceSequence);
            }
        } else {
            Optional<SlotProfile> result =
                    unassignedSlots.values().stream()
                            .filter(slot -> slot.getResourceProfile().enoughThan(profile))
                            .min(
                                    (slot1, slot2) -> {
                                        if (slot1.getResourceProfile().getHeapMemory().getBytes()
                                                != slot2.getResourceProfile()
                                                        .getHeapMemory()
                                                        .getBytes()) {
                                            return slot1.getResourceProfile()
                                                                            .getHeapMemory()
                                                                            .getBytes()
                                                                    - slot2.getResourceProfile()
                                                                            .getHeapMemory()
                                                                            .getBytes()
                                                            >= 0
                                                    ? 1
                                                    : -1;
                                        } else {
                                            return slot1.getResourceProfile().getCpu().getCore()
                                                    - slot2.getResourceProfile().getCpu().getCore();
                                        }
                                    });
            return result.orElse(null);
        }
        return null;
    }

    private void initFixedSlots() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        for (int i = 0; i < config.getSlotNum(); i++) {
            unassignedSlots.put(
                    i,
                    new SlotProfile(
                            nodeEngine.getThisAddress(),
                            i,
                            new ResourceProfile(
                                    CPU.of(0), Memory.of(maxMemory / config.getSlotNum())),
                            slotServiceSequence));
        }
    }

    @Override
    public synchronized WorkerProfile getWorkerProfile() {
        WorkerProfile workerProfile = new WorkerProfile(nodeEngine.getThisAddress());
        workerProfile.setProfile(getNodeResource());
        workerProfile.setAssignedSlots(assignedSlots.values().toArray(new SlotProfile[0]));
        workerProfile.setUnassignedSlots(unassignedSlots.values().toArray(new SlotProfile[0]));
        workerProfile.setUnassignedResource(unassignedResource.get());
        workerProfile.setAttributes(nodeEngine.getLocalMember().getAttributes());
        workerProfile.setDynamicSlot(config.isDynamicSlot());
        return workerProfile;
    }

    private ResourceProfile getNodeResource() {
        return new ResourceProfile(CPU.of(0), Memory.of(Runtime.getRuntime().maxMemory()));
    }

    public <E> InvocationFuture<E> sendToMaster(Operation operation) {
        return NodeEngineUtil.sendOperationToMasterNode(nodeEngine, operation);
    }

    public double getMemPercentage() {
        MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryMxBean.getHeapMemoryUsage();
        return ((double) heapMemoryUsage.getUsed() / (double) heapMemoryUsage.getMax());
    }

    @SneakyThrows
    public double getCpuPercentage() {
        // Create a SystemInfo object to access hardware information
        SystemInfo si = new SystemInfo();
        // Get the hardware abstraction layer
        HardwareAbstractionLayer hal = si.getHardware();
        // Get the central processor
        CentralProcessor processor = hal.getProcessor();
        // Get the previous CPU load ticks
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        // Sleep for 1 second to measure the CPU load over time
        Thread.sleep(1000);
        // Get the current CPU load ticks
        long[] ticks = processor.getSystemCpuLoadTicks();

        // Calculate the difference in CPU ticks for each type
        long user =
                ticks[CentralProcessor.TickType.USER.getIndex()]
                        - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long nice =
                ticks[CentralProcessor.TickType.NICE.getIndex()]
                        - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long sys =
                ticks[CentralProcessor.TickType.SYSTEM.getIndex()]
                        - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long idle =
                ticks[CentralProcessor.TickType.IDLE.getIndex()]
                        - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        // Calculate the total CPU ticks
        long totalCpu = user + nice + sys + idle;

        // Calculate and return the CPU usage percentage
        return ((double) (totalCpu - idle) / (double) totalCpu);
    }
}
