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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class HierarchicalAutoscalingPolicyTest {

    private final HierarchicalAutoscalingPolicy policy =
            new HierarchicalAutoscalingPolicy(
                    AutoscalerPolicyConfig.builder()
                            .scaleOutCpuThreshold(0.8d)
                            .scaleOutJvmMemoryThreshold(0.8d)
                            .scaleInCpuThreshold(0.3d)
                            .scaleInJvmMemoryThreshold(0.3d)
                            .fixedSlotScaleInThreshold(0.3d)
                            .build());

    @Test
    void schedulerShortageTriggersScaleOutWithLowUtilization() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .resourceShortageCount(1L)
                                .waitShortage(true)
                                .cpu(MetricValue.valid(0.1d))
                                .jvmMemory(MetricValue.valid(0.1d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_OUT, evaluation.getAction());
        Assertions.assertTrue(
                evaluation.getTriggerReasons().contains("scheduler_resource_shortage"));
    }

    @Test
    void highCpuTriggersScaleOutWhenSlotsAreLow() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.8d))
                                .jvmMemory(MetricValue.valid(0.1d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_OUT, evaluation.getAction());
        Assertions.assertTrue(evaluation.getTriggerReasons().contains("cpu_utilization_high"));
    }

    @Test
    void highJvmMemoryTriggersScaleOutWhenSlotsAreLow() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.1d))
                                .jvmMemory(MetricValue.valid(0.81d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_OUT, evaluation.getAction());
        Assertions.assertTrue(
                evaluation.getTriggerReasons().contains("jvm_memory_utilization_high"));
    }

    @Test
    void slotPressureAloneDoesNotTriggerScaleOut() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.2d))
                                .jvmMemory(MetricValue.valid(0.2d))
                                .fixedSlotUtilization(MetricValue.valid(0.95d))
                                .build());

        Assertions.assertEquals(ScalingAction.NO_ACTION, evaluation.getAction());
        Assertions.assertTrue(
                evaluation.getBlockingReasons().contains("slot_pressure_auxiliary_only"));
    }

    @Test
    void dynamicSlotModeHasUnknownSlotPressureAndCanScaleInWhenUtilizationIsLow() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .slotMode(SlotMode.DYNAMIC)
                                .fixedSlotUtilization(MetricValue.unknown())
                                .cpu(MetricValue.valid(0.29d))
                                .jvmMemory(MetricValue.valid(0.29d))
                                .scaleInMetricsValid(true)
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_IN_CANDIDATE, evaluation.getAction());
    }

    @Test
    void equalScaleInThresholdDoesNotScaleIn() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.3d))
                                .jvmMemory(MetricValue.valid(0.29d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .scaleInMetricsValid(true)
                                .build());

        Assertions.assertEquals(ScalingAction.NO_ACTION, evaluation.getAction());
    }

    @Test
    void invalidCurrentWorkerSampleBlocksScaleIn() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.1d))
                                .jvmMemory(MetricValue.valid(0.1d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .missingWorkerSamples(1)
                                .scaleInMetricsValid(false)
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_IN_BLOCKED, evaluation.getAction());
        Assertions.assertTrue(
                evaluation.getBlockingReasons().contains("scale_in_metrics_incomplete"));
    }

    @Test
    void cpuJustBelowScaleOutThresholdDoesNotTriggerScaleOut() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.79d))
                                .jvmMemory(MetricValue.valid(0.1d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .build());

        Assertions.assertEquals(ScalingAction.NO_ACTION, evaluation.getAction());
    }

    @Test
    void jvmMemoryAtExactScaleOutThresholdTriggersScaleOut() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.1d))
                                .jvmMemory(MetricValue.valid(0.8d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_OUT, evaluation.getAction());
        Assertions.assertTrue(
                evaluation.getTriggerReasons().contains("jvm_memory_utilization_high"));
    }

    @Test
    void justBelowScaleInThresholdTriggersScaleInCandidate() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .cpu(MetricValue.valid(0.29d))
                                .jvmMemory(MetricValue.valid(0.29d))
                                .fixedSlotUtilization(MetricValue.valid(0.29d))
                                .scaleInMetricsValid(true)
                                .build());

        Assertions.assertEquals(ScalingAction.SCALE_IN_CANDIDATE, evaluation.getAction());
    }

    @Test
    void minWorkerCountBlocksScaleIn() {
        AutoscaleEvaluation evaluation =
                policy.evaluate(
                        baseSnapshot()
                                .currentWorkers(1)
                                .minWorkers(1)
                                .cpu(MetricValue.valid(0.1d))
                                .jvmMemory(MetricValue.valid(0.1d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .scaleInMetricsValid(true)
                                .build());

        Assertions.assertEquals(ScalingAction.NO_ACTION, evaluation.getAction());
        Assertions.assertTrue(evaluation.getBlockingReasons().contains("min_workers_reached"));
    }

    private AutoscalerMetricsSnapshot.Builder baseSnapshot() {
        return AutoscalerMetricsSnapshot.builder()
                .evaluationTimeMillis(1000L)
                .currentWorkers(3)
                .minWorkers(1)
                .maxWorkers(10)
                .slotMode(SlotMode.FIXED)
                .assignedSlots(1)
                .unassignedSlots(9)
                .fixedSlotUtilization(MetricValue.valid(0.1d))
                .cpu(MetricValue.valid(0.5d))
                .jvmMemory(MetricValue.valid(0.5d))
                .totalWorkerSamples(3)
                .validWorkerSamples(3)
                .missingWorkerSamples(0)
                .staleWorkerSamples(0)
                .futureWorkerSamples(0)
                .pendingJobCount(0)
                .oldestPendingDurationMillis(0L)
                .resourceShortageCount(0L)
                .waitShortage(false)
                .rejectShortage(false)
                .scaleInMetricsValid(true);
    }
}
