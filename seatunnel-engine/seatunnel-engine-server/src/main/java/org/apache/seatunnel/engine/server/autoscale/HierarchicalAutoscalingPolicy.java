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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Deterministic Phase 1 policy for autoscaling recommendations.
 *
 * <p>Scheduler shortage and CPU/JVM-memory pressure may trigger scale-out; slot pressure is only
 * auxiliary.
 */
public final class HierarchicalAutoscalingPolicy implements AutoscalingPolicy {

    private final AutoscalerPolicyConfig config;

    public HierarchicalAutoscalingPolicy(AutoscalerPolicyConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public AutoscaleEvaluation evaluate(AutoscalerMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");

        List<String> triggers = new ArrayList<>();
        List<String> blockers = new ArrayList<>();

        if (snapshot.hasSchedulerShortage()) {
            triggers.add("scheduler_resource_shortage");
            if (snapshot.isWaitShortage()) {
                triggers.add("scheduler_wait_shortage");
            }
            if (snapshot.isRejectShortage()) {
                triggers.add("scheduler_reject_shortage");
            }
            return new AutoscaleEvaluation(ScalingAction.SCALE_OUT, triggers, blockers);
        }

        if (snapshot.getCpu().isGreaterThanOrEqualTo(config.getScaleOutCpuThreshold())) {
            triggers.add("cpu_utilization_high");
        }
        if (snapshot.getJvmMemory()
                .isGreaterThanOrEqualTo(config.getScaleOutJvmMemoryThreshold())) {
            triggers.add("jvm_memory_utilization_high");
        }
        if (!triggers.isEmpty()) {
            return new AutoscaleEvaluation(ScalingAction.SCALE_OUT, triggers, blockers);
        }

        if (snapshot.getFixedSlotUtilization()
                .isGreaterThanOrEqualTo(config.getFixedSlotScaleOutThreshold())) {
            blockers.add("slot_pressure_auxiliary_only");
        }

        if (snapshot.getCurrentWorkers() <= snapshot.getMinWorkers()) {
            blockers.add("min_workers_reached");
            return new AutoscaleEvaluation(ScalingAction.NO_ACTION, triggers, blockers);
        }

        if (!snapshot.isScaleInMetricsValid()) {
            blockers.add("scale_in_metrics_incomplete");
            return new AutoscaleEvaluation(ScalingAction.SCALE_IN_BLOCKED, triggers, blockers);
        }

        boolean lowCpu = snapshot.getCpu().isLessThan(config.getScaleInCpuThreshold());
        boolean lowJvmMemory =
                snapshot.getJvmMemory().isLessThan(config.getScaleInJvmMemoryThreshold());
        boolean lowSlot = isLowSlot(snapshot);

        if (lowCpu && lowJvmMemory && lowSlot) {
            triggers.add("resource_utilization_low");
            return new AutoscaleEvaluation(ScalingAction.SCALE_IN_CANDIDATE, triggers, blockers);
        }

        return new AutoscaleEvaluation(ScalingAction.NO_ACTION, triggers, blockers);
    }

    private boolean isLowSlot(AutoscalerMetricsSnapshot snapshot) {
        if (snapshot.getSlotMode() != SlotMode.FIXED) {
            return true;
        }
        return snapshot.getFixedSlotUtilization().isLessThan(config.getFixedSlotScaleInThreshold());
    }
}
