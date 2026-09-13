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

package org.apache.seatunnel.engine.server.autoscale;

import org.apache.seatunnel.engine.common.config.server.AutoscalerConfig;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;

import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

/**
 * Builds immutable autoscaler snapshots from ResourceManager state.
 *
 * <p>The collector normalizes worker samples, slot mode, pending jobs, and scheduler shortage
 * deltas; it does not evaluate policy.
 */
public final class DefaultAutoscalerSignalCollector implements AutoscalerSignalCollector {

    private final ResourceManager resourceManager;
    private final AutoscalerConfig config;
    private final IntSupplier pendingJobCountSupplier;
    private final LongSupplier oldestPendingDurationMillisSupplier;
    private final LongSupplier currentTimeMillisSupplier;
    private final AtomicLong lastShortageSequence = new AtomicLong();

    public DefaultAutoscalerSignalCollector(
            ResourceManager resourceManager,
            AutoscalerConfig config,
            IntSupplier pendingJobCountSupplier,
            LongSupplier oldestPendingDurationMillisSupplier,
            LongSupplier currentTimeMillisSupplier) {
        this.resourceManager = Objects.requireNonNull(resourceManager, "resourceManager");
        this.config = Objects.requireNonNull(config, "config");
        this.pendingJobCountSupplier =
                Objects.requireNonNull(pendingJobCountSupplier, "pendingJobCountSupplier");
        this.oldestPendingDurationMillisSupplier =
                Objects.requireNonNull(
                        oldestPendingDurationMillisSupplier, "oldestPendingDurationMillisSupplier");
        this.currentTimeMillisSupplier =
                Objects.requireNonNull(currentTimeMillisSupplier, "currentTimeMillisSupplier");
    }

    @Override
    public AutoscalerMetricsSnapshot collect() {
        long nowMillis = currentTimeMillisSupplier.getAsLong();
        HashSet<Address> currentWorkers =
                new HashSet<>(resourceManager.getRegisterWorker().keySet());
        resourceManager.getAutoscalerWorkerSampleStore().retainWorkers(currentWorkers);
        WorkerSampleSummary workerSampleSummary =
                resourceManager
                        .getAutoscalerWorkerSampleStore()
                        .summarize(
                                currentWorkers,
                                nowMillis,
                                TimeUnit.SECONDS.toMillis(config.getMetricsFreshnessSeconds()));
        ResourceShortageSnapshot shortageSnapshot =
                resourceManager
                        .getResourceShortageStats()
                        .snapshotSince(lastShortageSequence.get());
        lastShortageSequence.set(shortageSnapshot.getSequence());
        SlotSummary slotSummary = summarizeSlots();

        return AutoscalerMetricsSnapshot.builder()
                .evaluationTimeMillis(nowMillis)
                .currentWorkers(currentWorkers.size())
                .minWorkers(config.getMinWorkers())
                .maxWorkers(config.getMaxWorkers())
                .slotMode(slotSummary.slotMode)
                .assignedSlots(slotSummary.assignedSlots)
                .unassignedSlots(slotSummary.unassignedSlots)
                .fixedSlotUtilization(slotSummary.fixedSlotUtilization)
                .cpu(workerSampleSummary.getCpu())
                .jvmMemory(workerSampleSummary.getJvmMemory())
                .totalWorkerSamples(workerSampleSummary.getTotalSamples())
                .validWorkerSamples(workerSampleSummary.getValidSamples())
                .missingWorkerSamples(workerSampleSummary.getMissingSamples())
                .staleWorkerSamples(workerSampleSummary.getStaleSamples())
                .futureWorkerSamples(workerSampleSummary.getFutureSamples())
                .pendingJobCount(pendingJobCountSupplier.getAsInt())
                .oldestPendingDurationMillis(oldestPendingDurationMillisSupplier.getAsLong())
                .resourceShortageCount(shortageSnapshot.getShortageCount())
                .waitShortageCount(shortageSnapshot.getWaitCount())
                .rejectShortageCount(shortageSnapshot.getRejectCount())
                .waitShortage(shortageSnapshot.isLatestWait())
                .rejectShortage(shortageSnapshot.isLatestReject())
                .scaleInMetricsValid(workerSampleSummary.isScaleInMetricsValid())
                .build();
    }

    private SlotSummary summarizeSlots() {
        int workerCount = resourceManager.getRegisterWorker().size();
        if (workerCount == 0) {
            return new SlotSummary(SlotMode.UNKNOWN, 0, 0, MetricValue.unknown());
        }

        int dynamicWorkers = 0;
        int fixedWorkers = 0;
        int assignedSlots = 0;
        int unassignedSlots = 0;
        for (WorkerProfile workerProfile : resourceManager.getRegisterWorker().values()) {
            if (workerProfile.isDynamicSlot()) {
                dynamicWorkers++;
            } else {
                fixedWorkers++;
                assignedSlots += safeLength(workerProfile.getAssignedSlots());
                unassignedSlots += safeLength(workerProfile.getUnassignedSlots());
            }
        }

        if (fixedWorkers == workerCount) {
            int totalSlots = assignedSlots + unassignedSlots;
            MetricValue utilization =
                    totalSlots == 0
                            ? MetricValue.unknown()
                            : MetricValue.valid((double) assignedSlots / (double) totalSlots);
            return new SlotSummary(SlotMode.FIXED, assignedSlots, unassignedSlots, utilization);
        }
        if (dynamicWorkers == workerCount) {
            return new SlotSummary(
                    SlotMode.DYNAMIC, assignedSlots, unassignedSlots, MetricValue.unknown());
        }
        return new SlotSummary(
                SlotMode.MIXED, assignedSlots, unassignedSlots, MetricValue.unknown());
    }

    private static int safeLength(Object[] values) {
        return values == null ? 0 : values.length;
    }

    private static final class SlotSummary {
        private final SlotMode slotMode;
        private final int assignedSlots;
        private final int unassignedSlots;
        private final MetricValue fixedSlotUtilization;

        private SlotSummary(
                SlotMode slotMode,
                int assignedSlots,
                int unassignedSlots,
                MetricValue fixedSlotUtilization) {
            this.slotMode = slotMode;
            this.assignedSlots = assignedSlots;
            this.unassignedSlots = unassignedSlots;
            this.fixedSlotUtilization = fixedSlotUtilization;
        }
    }
}
