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

class RecommendationFenceTest {

    @Test
    void acceptsNewerIdentityAndTreatsDuplicateAsNoop() {
        RecommendationFence fence = new RecommendationFence();

        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, fence.tryPublish(1L, 0L));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.DUPLICATE, fence.tryPublish(1L, 0L));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, fence.tryPublish(1L, 1L));
    }

    @Test
    void rejectsLowerEpochAndNonIncreasingGeneration() {
        RecommendationFence fence = new RecommendationFence();

        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, fence.tryPublish(2L, 3L));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.REJECTED, fence.tryPublish(1L, 100L));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.REJECTED, fence.tryPublish(2L, 2L));
        Assertions.assertEquals(
                RecommendationFence.PublicationResult.ACCEPTED, fence.tryPublish(3L, 0L));
    }
}
