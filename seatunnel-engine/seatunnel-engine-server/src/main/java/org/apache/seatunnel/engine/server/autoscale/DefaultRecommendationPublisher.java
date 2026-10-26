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

import java.util.Objects;

/** Publishes recommendations after enforcing active-master epoch and generation ordering. */
public final class DefaultRecommendationPublisher implements RecommendationPublisher {
    private final AutoscalerStateStore stateStore;
    private long lastMasterEpoch = Long.MIN_VALUE;
    private long lastGeneration = Long.MIN_VALUE;

    public DefaultRecommendationPublisher(AutoscalerStateStore stateStore) {
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
    }

    @Override
    public synchronized PublicationResult publish(ScalingRecommendation recommendation) {
        Objects.requireNonNull(recommendation, "recommendation");
        PublicationResult result = validateVersion(recommendation);
        if (result == PublicationResult.ACCEPTED) {
            stateStore.saveRecommendation(recommendation);
            lastMasterEpoch = recommendation.getMasterEpoch();
            lastGeneration = recommendation.getGeneration();
        }
        return result;
    }

    @Override
    public synchronized void reset() {
        lastMasterEpoch = Long.MIN_VALUE;
        lastGeneration = Long.MIN_VALUE;
    }

    private PublicationResult validateVersion(ScalingRecommendation recommendation) {
        long masterEpoch = recommendation.getMasterEpoch();
        long generation = recommendation.getGeneration();
        if (masterEpoch == lastMasterEpoch && generation == lastGeneration) {
            return PublicationResult.DUPLICATE;
        }
        if (masterEpoch < lastMasterEpoch
                || (masterEpoch == lastMasterEpoch && generation < lastGeneration)) {
            return PublicationResult.REJECTED;
        }
        return PublicationResult.ACCEPTED;
    }
}
