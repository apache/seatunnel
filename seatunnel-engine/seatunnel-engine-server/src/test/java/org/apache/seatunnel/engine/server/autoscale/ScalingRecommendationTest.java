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

import java.util.Collections;

class ScalingRecommendationTest {

    @Test
    void acceptsOnlyScalingActionsAndUsesAnInclusiveValidityDeadline() {
        ScalingRecommendation recommendation =
                ScalingRecommendation.builder()
                        .masterEpoch(2L)
                        .generation(3L)
                        .action(EvaluationAction.SCALE_OUT)
                        .currentWorkers(3)
                        .recommendedWorkers(4)
                        .observedAtMillis(100L)
                        .validUntilMillis(200L)
                        .decisionReasons(Collections.singletonList("cpu_high"))
                        .build();

        Assertions.assertTrue(recommendation.isValidAt(199L));
        Assertions.assertTrue(recommendation.isValidAt(200L));
        Assertions.assertFalse(recommendation.isValidAt(201L));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        ScalingRecommendation.builder()
                                .action(EvaluationAction.NO_ACTION)
                                .observedAtMillis(100L)
                                .validUntilMillis(200L)
                                .build());
    }

    @Test
    void rejectsAValidityDeadlineBeforeObservation() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        ScalingRecommendation.builder()
                                .action(EvaluationAction.SCALE_IN)
                                .observedAtMillis(200L)
                                .validUntilMillis(199L)
                                .build());
    }
}
