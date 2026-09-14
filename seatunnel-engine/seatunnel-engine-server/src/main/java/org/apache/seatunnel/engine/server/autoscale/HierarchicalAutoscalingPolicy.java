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
 * Evaluates autoscaling signals in priority order and produces a scaling recommendation.
 *
 * <p>Scheduler shortages and high CPU or JVM memory utilization can trigger scale-out. Fixed-slot
 * utilization is considered as an auxiliary signal when evaluating scale-in decisions.
 */
public final class HierarchicalAutoscalingPolicy implements AutoscalingPolicy {

    private final AutoscalerConfig config;

    public HierarchicalAutoscalingPolicy(AutoscalerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public AutoscaleEvaluation evaluate(AutoscalerMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");

        List<String> reasons = new ArrayList<>();

        if (snapshot.hasSchedulerShortage()) {
            reasons.add("scheduler_resource_shortage");
            if (snapshot.isWaitShortage()) {
                reasons.add("scheduler_wait_shortage");
            }
            if (snapshot.isRejectShortage()) {
                reasons.add("scheduler_reject_shortage");
            }
            return new AutoscaleEvaluation(EvaluationAction.SCALE_OUT, reasons);
        }

        if (snapshot.getCpu().isGreaterThanOrEqualTo(config.getScaleOutCpuThreshold())) {
            reasons.add("cpu_utilization_high");
        }
        if (snapshot.getJvmMemory()
                .isGreaterThanOrEqualTo(config.getScaleOutJvmMemoryThreshold())) {
            reasons.add("jvm_memory_utilization_high");
        }
        if (!reasons.isEmpty()) {
            return new AutoscaleEvaluation(EvaluationAction.SCALE_OUT, reasons);
        }

        // Slot pressure alone is not sufficient to trigger scale-out.
        boolean slotPressure =
                snapshot.getFixedSlotUtilization()
                        .isGreaterThanOrEqualTo(config.getFixedSlotScaleOutThreshold());
        // Combine slot pressure with scheduler waiting evidence to confirm capacity demand.
        boolean schedulingPressure =
                snapshot.getPendingJobCount() > 0
                        || snapshot.getLongestPendingDurationMillis() > 0L;
        if (slotPressure && schedulingPressure) {
            reasons.add("slot_pressure_with_scheduling_pressure");
            return new AutoscaleEvaluation(EvaluationAction.SCALE_OUT, reasons);
        }

        if (snapshot.getCurrentWorkers() <= snapshot.getMinWorkers()) {
            reasons.add("min_workers_reached");
            return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, reasons);
        }

        if (!snapshot.isScaleInMetricsValid()) {
            reasons.add("scale_in_metrics_incomplete");
            return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, reasons);
        }

        boolean lowCpu = snapshot.getCpu().isLessThan(config.getScaleInCpuThreshold());
        boolean lowJvmMemory =
                snapshot.getJvmMemory().isLessThan(config.getScaleInJvmMemoryThreshold());
        boolean lowSlot = isLowSlot(snapshot);

        if (lowCpu && lowJvmMemory && lowSlot) {
            reasons.add("resource_utilization_low");
            return new AutoscaleEvaluation(EvaluationAction.SCALE_IN, reasons);
        }

        reasons.add("no_scaling_condition_met");
        return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, reasons);
    }

    private boolean isLowSlot(AutoscalerMetricsSnapshot snapshot) {
        if (snapshot.isDynamicSlot()) {
            return true;
        }
        return snapshot.getFixedSlotUtilization().isLessThan(config.getFixedSlotScaleInThreshold());
    }
}
