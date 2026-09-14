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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/** Bounded in-memory histories for evaluation records and published recommendations. */
public final class InMemoryAutoscalerStateStore implements AutoscalerStateStore {
    private final int recommendationHistorySize;
    private final int evaluationHistorySize;
    private final LinkedList<AutoscalingEvaluationRecord> evaluationHistory = new LinkedList<>();
    private final LinkedList<ScalingRecommendation> recommendationHistory = new LinkedList<>();
    private AutoscalingEvaluationRecord latestEvaluationRecord;
    private ScalingRecommendation latestRecommendation;
    private AutoscalerMetricsSnapshot currentSnapshot;

    public InMemoryAutoscalerStateStore(int recommendationHistorySize, int evaluationHistorySize) {
        if (recommendationHistorySize <= 0 || evaluationHistorySize <= 0) {
            throw new IllegalArgumentException("history sizes must be > 0");
        }
        this.recommendationHistorySize = recommendationHistorySize;
        this.evaluationHistorySize = evaluationHistorySize;
    }

    @Override
    public synchronized void clear() {
        latestEvaluationRecord = null;
        latestRecommendation = null;
        currentSnapshot = null;
        evaluationHistory.clear();
        recommendationHistory.clear();
    }

    @Override
    public synchronized void updateCurrentSnapshot(AutoscalerMetricsSnapshot snapshot) {
        currentSnapshot = Objects.requireNonNull(snapshot, "snapshot");
    }

    @Override
    public synchronized void recordEvaluation(AutoscalingEvaluationRecord record) {
        latestEvaluationRecord = Objects.requireNonNull(record, "record");
        evaluationHistory.add(record);
        trim(evaluationHistory, evaluationHistorySize);
    }

    @Override
    public synchronized void saveRecommendation(ScalingRecommendation recommendation) {
        latestRecommendation = Objects.requireNonNull(recommendation, "recommendation");
        recommendationHistory.add(recommendation);
        trim(recommendationHistory, recommendationHistorySize);
    }

    @Override
    public synchronized AutoscalerView view(
            boolean enabled,
            boolean running,
            long currentMasterEpoch,
            long nextGeneration,
            int scaleOutStabilizationSeconds,
            int scaleInStabilizationSeconds) {
        return new AutoscalerView(
                enabled,
                running,
                currentMasterEpoch,
                nextGeneration,
                scaleOutStabilizationSeconds,
                scaleInStabilizationSeconds,
                latestEvaluationRecord,
                latestRecommendation,
                currentSnapshot,
                immutableCopy(evaluationHistory),
                immutableCopy(recommendationHistory));
    }

    public synchronized AutoscalerView view(boolean enabled, boolean running) {
        return view(enabled, running, 0L, 0L, 0, 0);
    }

    private static <T> List<T> immutableCopy(List<T> source) {
        return Collections.unmodifiableList(new ArrayList<>(source));
    }

    private static <T> void trim(LinkedList<T> history, int maxSize) {
        while (history.size() > maxSize) {
            history.removeFirst();
        }
    }
}
