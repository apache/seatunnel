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

class DefaultRecommendationPublisherTest {
    @Test
    void savesOnlyNewerRecommendations() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10, 10);
        DefaultRecommendationPublisher publisher = new DefaultRecommendationPublisher(store);

        ScalingRecommendation first = recommendation(1L, 0L);
        ScalingRecommendation duplicate = recommendation(1L, 0L);
        ScalingRecommendation older = recommendation(1L, -1L);
        ScalingRecommendation newer = recommendation(1L, 1L);

        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.ACCEPTED, publisher.publish(first));
        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.DUPLICATE, publisher.publish(duplicate));
        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.REJECTED, publisher.publish(older));
        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.ACCEPTED, publisher.publish(newer));
        Assertions.assertEquals(2, store.view(true, true).getRecommendationHistory().size());
    }

    @Test
    void rejectsRecommendationsFromAnOlderMasterAndResetsForANewIncarnation() {
        InMemoryAutoscalerStateStore store = new InMemoryAutoscalerStateStore(10, 10);
        DefaultRecommendationPublisher publisher = new DefaultRecommendationPublisher(store);

        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.ACCEPTED,
                publisher.publish(recommendation(2L, 0L)));
        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.REJECTED,
                publisher.publish(recommendation(1L, 100L)));

        publisher.reset();

        Assertions.assertEquals(
                RecommendationPublisher.PublicationResult.ACCEPTED,
                publisher.publish(recommendation(1L, 0L)));
        Assertions.assertEquals(2, store.view(true, true).getRecommendationHistory().size());
    }

    private static ScalingRecommendation recommendation(long epoch, long generation) {
        return ScalingRecommendation.builder()
                .masterEpoch(epoch)
                .generation(generation)
                .action(EvaluationAction.SCALE_OUT)
                .currentWorkers(3)
                .recommendedWorkers(4)
                .observedAtMillis(0L)
                .validUntilMillis(1L)
                .decisionReasons(Collections.emptyList())
                .build();
    }
}
