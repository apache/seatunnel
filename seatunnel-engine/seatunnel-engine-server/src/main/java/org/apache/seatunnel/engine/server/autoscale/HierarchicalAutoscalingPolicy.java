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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Evaluates autoscaling signals in priority order and produces a scaling recommendation.
 *
 * <p>The evaluation rules are applied in the following order:
 *
 * <ol>
 *   <li>Any scheduler resource shortage, high CPU/JVM memory utilization, or fixed-slot pressure
 *       combined with scheduling pressure triggers scale-out unless the maximum worker count has
 *       been reached.
 *   <li>When the minimum worker count is reached, no scaling action is taken.
 *   <li>Incomplete worker metrics block scale-in for safety.
 *   <li>Scale-in is recommended only when there are no pending jobs and CPU, JVM memory, and slot
 *       utilization are all below their configured thresholds.
 *   <li>If none of the conditions is met, no scaling action is taken.
 * </ol>
 */
public final class HierarchicalAutoscalingPolicy implements AutoscalingPolicy {

    private final AutoscalerConfig config;

    public HierarchicalAutoscalingPolicy(AutoscalerConfig config) {
        this.config = Objects.requireNonNull(config, "config");
    }

    @Override
    public AutoscaleEvaluation evaluate(AutoscalerMetricsSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");

        List<String> scaleOutReasons = evaluateScaleOut(snapshot);
        if (!scaleOutReasons.isEmpty()) {
            if (snapshot.getCurrentWorkers() >= snapshot.getMaxWorkers()) {
                scaleOutReasons.add("scale_out_blocked_by_max_workers");
                return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, scaleOutReasons);
            }
            return new AutoscaleEvaluation(EvaluationAction.SCALE_OUT, scaleOutReasons);
        }

        List<String> scaleInReasons = evaluateScaleIn(snapshot);
        if (!scaleInReasons.isEmpty()) {
            if (snapshot.getCurrentWorkers() <= snapshot.getMinWorkers()) {
                scaleInReasons.add("scale_in_blocked_by_min_workers");
                return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, scaleInReasons);
            }

            // Pending work indicates unmet scheduling demand, so scaling in could reduce capacity.
            if (hasSchedulingPressure(snapshot)) {
                scaleInReasons.add("scale_in_blocked_by_pending_jobs");
                return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, scaleInReasons);
            }

            if (!snapshot.isAllWorkerMetricsValid()) {
                scaleInReasons.add("scale_in_blocked_by_incomplete_metrics");
                return new AutoscaleEvaluation(EvaluationAction.NO_ACTION, scaleInReasons);
            }

            return new AutoscaleEvaluation(EvaluationAction.SCALE_IN, scaleInReasons);
        }

        return new AutoscaleEvaluation(
                EvaluationAction.NO_ACTION, Collections.singletonList("no_scaling_condition_met"));
    }

    private List<String> evaluateScaleIn(AutoscalerMetricsSnapshot snapshot) {
        boolean lowCpu = snapshot.getCpu().isLessThan(config.getScaleInCpuThreshold());
        boolean lowJvmMemory =
                snapshot.getJvmMemory().isLessThan(config.getScaleInJvmMemoryThreshold());
        boolean lowSlot = isLowSlot(snapshot);

        if (!(lowCpu && lowJvmMemory && lowSlot)) {
            return new ArrayList<>();
        }

        List<String> reasons = new ArrayList<>();
        reasons.add("cpu_utilization_low");
        reasons.add("jvm_memory_utilization_low");
        reasons.add("slot_utilization_low");
        return reasons;
    }

    /** Returns the reasons for any scale-out condition, or an empty list when none is present. */
    private List<String> evaluateScaleOut(AutoscalerMetricsSnapshot snapshot) {
        List<String> reasons = new ArrayList<>();

        if (snapshot.hasSchedulerShortage()) {
            reasons.add("scheduler_resource_shortage");
            if (snapshot.hasNewWaitShortage()) {
                reasons.add("scheduler_wait_shortage");
            }
            if (snapshot.hasNewRejectShortage()) {
                reasons.add("scheduler_reject_shortage");
            }
            return reasons;
        }

        if (snapshot.getCpu().isGreaterThanOrEqualTo(config.getScaleOutCpuThreshold())) {
            reasons.add("cpu_utilization_high");
        }
        if (snapshot.getJvmMemory()
                .isGreaterThanOrEqualTo(config.getScaleOutJvmMemoryThreshold())) {
            reasons.add("jvm_memory_utilization_high");
        }
        if (!reasons.isEmpty()) {
            return reasons;
        }

        // Slot pressure alone is not sufficient to trigger scale-out.
        boolean slotPressure =
                snapshot.getFixedSlotUtilization()
                        .isGreaterThanOrEqualTo(config.getFixedSlotScaleOutThreshold());
        // Combine slot pressure with scheduler waiting evidence to confirm capacity demand.
        if (slotPressure && hasSchedulingPressure(snapshot)) {
            reasons.add("slot_pressure_with_scheduling_pressure");
            return reasons;
        }

        return reasons;
    }

    private boolean hasSchedulingPressure(AutoscalerMetricsSnapshot snapshot) {
        return snapshot.getPendingJobCount() > 0 || snapshot.getLongestPendingDurationMillis() > 0L;
    }

    private boolean isLowSlot(AutoscalerMetricsSnapshot snapshot) {
        if (snapshot.isDynamicSlot()) {
            return true;
        }
        return snapshot.getFixedSlotUtilization().isLessThan(config.getFixedSlotScaleInThreshold());
    }
}
