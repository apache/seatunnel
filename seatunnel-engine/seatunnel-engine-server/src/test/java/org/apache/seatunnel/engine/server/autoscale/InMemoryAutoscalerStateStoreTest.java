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

class InMemoryAutoscalerStateStoreTest {
    @Test
    void retainsEvaluationRecordsAndRecommendationsInSeparateBoundedHistories() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(2, 1);
        AutoscalingEvaluationRecord first =
                record(EvaluationAction.NO_ACTION, AutoscalingState.NORMAL, 1L);
        AutoscalingEvaluationRecord second =
                record(EvaluationAction.SCALE_OUT, AutoscalingState.PENDING, 2L);
        store.saveEvaluation(first);
        store.saveEvaluation(second);
        ScalingRecommendation recommendation = recommendation(1L, 0L);
        store.saveRecommendation(recommendation);

        AutoscalerView view = store.view(true, true);
        Assertions.assertEquals(second, view.getLatestEvaluationRecord());
        Assertions.assertEquals(1, view.getEvaluationHistory().size());
        Assertions.assertEquals(second, view.getEvaluationHistory().get(0));
        Assertions.assertEquals(recommendation, view.getLatestRecommendation());
        Assertions.assertEquals(1, view.getRecommendationHistory().size());
        Assertions.assertFalse(view.getLatestRecommendation().isValidAt(1L));
    }

    @Test
    void storesRecommendationsWithoutCoalescingEvaluationRecords() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(2, 3);
        ScalingRecommendation recommendation = recommendation(1L, 0L);
        store.saveRecommendation(recommendation);
        store.saveRecommendation(recommendation);
        store.saveEvaluation(record(EvaluationAction.NO_ACTION, AutoscalingState.NORMAL, 1L));
        store.saveEvaluation(record(EvaluationAction.NO_ACTION, AutoscalingState.NORMAL, 2L));
        Assertions.assertEquals(2, store.view(true, true).getEvaluationHistory().size());
        Assertions.assertEquals(2, store.view(true, true).getRecommendationHistory().size());
    }

    private static AutoscalingEvaluationRecord record(
            EvaluationAction action, AutoscalingState currentState, long evaluatedAtMillis) {
        EvaluationAction trackedAction = currentState == AutoscalingState.NORMAL ? null : action;
        return new AutoscalingEvaluationRecord(
                new AutoscaleEvaluation(action, Collections.emptyList()),
                new AutoscalingStateTransition(
                        currentState, trackedAction, currentState, trackedAction),
                evaluatedAtMillis);
    }

    private static ScalingRecommendation recommendation(long epoch, long generation) {
        return ScalingRecommendation.builder()
                .masterEpoch(epoch)
                .generation(generation)
                .action(EvaluationAction.SCALE_OUT)
                .currentWorkers(3)
                .recommendedWorkers(4)
                .observedAtMillis(0L)
                .validUntilMillis(0L)
                .decisionReasons(Collections.emptyList())
                .build();
    }
}
