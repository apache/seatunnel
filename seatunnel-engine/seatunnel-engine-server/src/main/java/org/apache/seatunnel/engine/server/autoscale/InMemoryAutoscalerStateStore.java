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
import java.util.EnumMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded in-memory store for the latest autoscaler recommendation and recent history.
 *
 * <p>The store applies recommendation fencing and maintains scrape-safe cumulative counts by
 * action.
 */
public final class InMemoryAutoscalerStateStore implements AutoscalerStateStore {

    private final int historySize;
    private final RecommendationFence fence = new RecommendationFence();
    private final LinkedList<ScalingRecommendation> history = new LinkedList<>();
    private final EnumMap<ScalingAction, Long> recommendationCounts =
            new EnumMap<>(ScalingAction.class);
    private ScalingRecommendation latest;

    public InMemoryAutoscalerStateStore(int historySize) {
        if (historySize <= 0) {
            throw new IllegalArgumentException("historySize must be > 0");
        }
        this.historySize = historySize;
        for (ScalingAction action : ScalingAction.values()) {
            recommendationCounts.put(action, 0L);
        }
    }

    @Override
    public synchronized RecommendationFence.PublicationResult publish(
            ScalingRecommendation recommendation) {
        Objects.requireNonNull(recommendation, "recommendation");
        RecommendationFence.PublicationResult result =
                fence.tryPublish(recommendation.getMasterEpoch(), recommendation.getGeneration());
        if (result != RecommendationFence.PublicationResult.ACCEPTED) {
            return result;
        }
        latest = recommendation;
        recommendationCounts.compute(
                recommendation.getAction(), (action, count) -> count == null ? 1L : count + 1L);
        history.add(recommendation);
        while (history.size() > historySize) {
            history.removeFirst();
        }
        return result;
    }

    @Override
    public synchronized AutoscalerView view(
            boolean enabled,
            boolean running,
            long currentMasterEpoch,
            long nextGeneration,
            int scaleOutStabilizationSeconds,
            int scaleInStabilizationSeconds) {
        List<ScalingRecommendation> historyCopy =
                Collections.unmodifiableList(new ArrayList<>(history));
        Map<ScalingAction, Long> countsCopy = new EnumMap<>(recommendationCounts);
        AutoscalerMetricsSnapshot snapshot = latest == null ? null : latest.getSnapshot();
        return new AutoscalerView(
                enabled,
                running,
                currentMasterEpoch,
                nextGeneration,
                scaleOutStabilizationSeconds,
                scaleInStabilizationSeconds,
                latest,
                snapshot,
                historyCopy,
                countsCopy);
    }

    public synchronized AutoscalerView view(boolean enabled, boolean running) {
        return view(enabled, running, 0L, 0L, 0, 0);
    }
}
