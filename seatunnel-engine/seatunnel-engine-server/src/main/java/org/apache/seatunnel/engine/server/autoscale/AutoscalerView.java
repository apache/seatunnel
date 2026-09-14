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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Read model for autoscaler evaluation/state and recommendation histories. */
public final class AutoscalerView implements Serializable {
    private static final long serialVersionUID = 1L;
    private final boolean enabled;
    private final boolean running;
    private final long currentMasterEpoch;
    private final long nextGeneration;
    private final int scaleOutStabilizationSeconds;
    private final int scaleInStabilizationSeconds;
    private final AutoscalingEvaluationRecord latestEvaluationRecord;
    private final ScalingRecommendation latestRecommendation;
    private final AutoscalerMetricsSnapshot currentSnapshot;
    private final List<AutoscalingEvaluationRecord> evaluationHistory;
    private final List<ScalingRecommendation> recommendationHistory;

    public AutoscalerView(
            boolean enabled,
            boolean running,
            long currentMasterEpoch,
            long nextGeneration,
            int scaleOutStabilizationSeconds,
            int scaleInStabilizationSeconds,
            AutoscalingEvaluationRecord latestEvaluationRecord,
            ScalingRecommendation latestRecommendation,
            AutoscalerMetricsSnapshot currentSnapshot,
            List<AutoscalingEvaluationRecord> evaluationHistory,
            List<ScalingRecommendation> recommendationHistory) {
        this.enabled = enabled;
        this.running = running;
        this.currentMasterEpoch = currentMasterEpoch;
        this.nextGeneration = nextGeneration;
        this.scaleOutStabilizationSeconds = scaleOutStabilizationSeconds;
        this.scaleInStabilizationSeconds = scaleInStabilizationSeconds;
        this.latestEvaluationRecord = latestEvaluationRecord;
        this.latestRecommendation = latestRecommendation;
        this.currentSnapshot = currentSnapshot;
        this.evaluationHistory = Collections.unmodifiableList(new ArrayList<>(evaluationHistory));
        this.recommendationHistory =
                Collections.unmodifiableList(new ArrayList<>(recommendationHistory));
    }

    public boolean isEnabled() {
        return enabled;
    }

    public boolean isRunning() {
        return running;
    }

    public long getCurrentMasterEpoch() {
        return currentMasterEpoch;
    }

    public long getNextGeneration() {
        return nextGeneration;
    }

    public int getScaleOutStabilizationSeconds() {
        return scaleOutStabilizationSeconds;
    }

    public int getScaleInStabilizationSeconds() {
        return scaleInStabilizationSeconds;
    }

    public AutoscalingEvaluationRecord getLatestEvaluationRecord() {
        return latestEvaluationRecord;
    }

    public ScalingRecommendation getLatestRecommendation() {
        return latestRecommendation;
    }

    public AutoscalerMetricsSnapshot getCurrentSnapshot() {
        return currentSnapshot;
    }

    public List<AutoscalingEvaluationRecord> getEvaluationHistory() {
        return evaluationHistory;
    }

    public List<ScalingRecommendation> getRecommendationHistory() {
        return recommendationHistory;
    }
}
