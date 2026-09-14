/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package org.apache.seatunnel.engine.server.autoscale;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class DefaultAutoScalerTest {
    @Test
    void publishesOnlyWhenFiringStartsOrItsRepeatIntervalElapses() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder()
                        .scaleOutStabilizationSeconds(1)
                        .recommendationRepeatSeconds(10)
                        .build();
        FakeTime time = new FakeTime();
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10, 10);
        EvaluationAction[] action = {EvaluationAction.SCALE_OUT};
        DefaultAutoScaler scaler = scaler(config, time, store, action);

        scaler.evaluateOnce();
        Assertions.assertNull(store.view(true, true).getLatestRecommendation());
        time.monotonicMillis = 1_000L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());
        Assertions.assertEquals(
                EvaluationAction.SCALE_OUT,
                store.view(true, true).getLatestRecommendation().getAction());
        time.monotonicMillis = 5_000L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());
        time.monotonicMillis = 11_000L;
        scaler.evaluateOnce();
        Assertions.assertEquals(2, store.view(true, true).getRecommendationHistory().size());
        Assertions.assertEquals(
                1L, store.view(true, true).getLatestRecommendation().getGeneration());
        action[0] = EvaluationAction.NO_ACTION;
        time.monotonicMillis = 12_000L;
        scaler.evaluateOnce();
        AutoscalerView view = store.view(true, true);
        Assertions.assertEquals(2, view.getRecommendationHistory().size());
        Assertions.assertEquals(
                AutoscalingState.FIRING,
                view.getLatestEvaluationRecord().getStateTransition().getPreviousState());
        Assertions.assertEquals(
                AutoscalingState.NORMAL,
                view.getLatestEvaluationRecord().getStateTransition().getCurrentState());
        Assertions.assertEquals(5, view.getEvaluationHistory().size());
    }

    @Test
    void recoveryDoesNotPublishRecommendations() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder()
                        .scaleOutStabilizationSeconds(1)
                        .keepFiringSeconds(10)
                        .recommendationRepeatSeconds(100)
                        .build();
        FakeTime time = new FakeTime();
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10, 10);
        EvaluationAction[] action = {EvaluationAction.SCALE_OUT};
        DefaultAutoScaler scaler = scaler(config, time, store, action);
        scaler.evaluateOnce();
        time.monotonicMillis = 1_000L;
        scaler.evaluateOnce();
        action[0] = EvaluationAction.NO_ACTION;
        time.monotonicMillis = 2_000L;
        scaler.evaluateOnce();
        action[0] = EvaluationAction.SCALE_OUT;
        time.monotonicMillis = 3_000L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());
        action[0] = EvaluationAction.NO_ACTION;
        time.monotonicMillis = 4_000L;
        scaler.evaluateOnce();
        time.monotonicMillis = 14_000L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());
        Assertions.assertEquals(
                AutoscalingState.NORMAL,
                store.view(true, true)
                        .getLatestEvaluationRecord()
                        .getStateTransition()
                        .getCurrentState());
    }

    @Test
    void returnsToNormalBeforeFiringAnOppositeRecommendation() {
        AutoscalerRuntimeConfig config =
                AutoscalerRuntimeConfig.builder()
                        .scaleOutStabilizationSeconds(1)
                        .scaleInStabilizationSeconds(1)
                        .build();
        FakeTime time = new FakeTime();
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10, 10);
        EvaluationAction[] action = {EvaluationAction.SCALE_OUT};
        DefaultAutoScaler scaler = scaler(config, time, store, action);

        scaler.evaluateOnce();
        time.monotonicMillis = 1_000L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());

        action[0] = EvaluationAction.SCALE_IN;
        time.monotonicMillis = 1_001L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());
        Assertions.assertEquals(
                AutoscalingState.NORMAL,
                store.view(true, true)
                        .getLatestEvaluationRecord()
                        .getStateTransition()
                        .getCurrentState());

        time.monotonicMillis = 1_002L;
        scaler.evaluateOnce();
        Assertions.assertEquals(1, store.view(true, true).getRecommendationHistory().size());
        Assertions.assertEquals(
                AutoscalingState.PENDING,
                store.view(true, true)
                        .getLatestEvaluationRecord()
                        .getStateTransition()
                        .getCurrentState());

        time.monotonicMillis = 2_002L;
        scaler.evaluateOnce();
        Assertions.assertEquals(2, store.view(true, true).getRecommendationHistory().size());
        Assertions.assertEquals(
                EvaluationAction.SCALE_IN,
                store.view(true, true).getLatestRecommendation().getAction());
    }

    private static DefaultAutoScaler scaler(
            AutoscalerRuntimeConfig config,
            FakeTime time,
            InMemoryAutoscalerStateStore store,
            EvaluationAction[] action) {
        return new DefaultAutoScaler(
                1L,
                config,
                () ->
                        AutoscalerMetricsSnapshot.builder()
                                .currentWorkers(3)
                                .minWorkers(1)
                                .maxWorkers(10)
                                .cpu(MetricValue.valid(0.1d))
                                .jvmMemory(MetricValue.valid(0.1d))
                                .fixedSlotUtilization(MetricValue.valid(0.1d))
                                .scaleInMetricsValid(true)
                                .build(),
                snapshot -> new AutoscaleEvaluation(action[0], Collections.singletonList("test")),
                DefaultAutoScaler.stateTracker(config),
                store,
                time);
    }

    private static final class FakeTime implements AutoscalerTimeSource {
        private long monotonicMillis;

        @Override
        public long currentTimeMillis() {
            return monotonicMillis;
        }

        @Override
        public long monotonicTimeMillis() {
            return monotonicMillis;
        }
    }
}
