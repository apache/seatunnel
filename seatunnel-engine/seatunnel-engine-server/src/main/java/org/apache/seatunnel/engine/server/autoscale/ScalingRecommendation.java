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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable autoscaler output for one evaluation generation.
 *
 * <p>Phase 1 recommendations are diagnostic only and include the input snapshot plus trigger and
 * blocking reasons.
 */
public final class ScalingRecommendation implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long masterEpoch;
    private final long generation;
    private final ScalingAction action;
    private final int currentWorkers;
    private final int recommendedWorkers;
    private final long observedAtMillis;
    private final long validUntilMillis;
    private final List<String> triggerReasons;
    private final List<String> blockingReasons;
    private final AutoscalerMetricsSnapshot snapshot;
    private final boolean recommendationOnly;

    private ScalingRecommendation(Builder builder) {
        this.masterEpoch = builder.masterEpoch;
        this.generation = builder.generation;
        this.action = Objects.requireNonNull(builder.action, "action");
        this.currentWorkers = builder.currentWorkers;
        this.recommendedWorkers = builder.recommendedWorkers;
        this.observedAtMillis = builder.observedAtMillis;
        this.validUntilMillis = builder.validUntilMillis;
        this.triggerReasons = immutableCopy(builder.triggerReasons);
        this.blockingReasons = immutableCopy(builder.blockingReasons);
        this.snapshot = Objects.requireNonNull(builder.snapshot, "snapshot");
        this.recommendationOnly = builder.recommendationOnly;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getMasterEpoch() {
        return masterEpoch;
    }

    public long getGeneration() {
        return generation;
    }

    public ScalingAction getAction() {
        return action;
    }

    public int getCurrentWorkers() {
        return currentWorkers;
    }

    public int getRecommendedWorkers() {
        return recommendedWorkers;
    }

    public long getObservedAtMillis() {
        return observedAtMillis;
    }

    public long getValidUntilMillis() {
        return validUntilMillis;
    }

    public List<String> getTriggerReasons() {
        return triggerReasons;
    }

    public List<String> getBlockingReasons() {
        return blockingReasons;
    }

    public AutoscalerMetricsSnapshot getSnapshot() {
        return snapshot;
    }

    public boolean isRecommendationOnly() {
        return recommendationOnly;
    }

    private static List<String> immutableCopy(List<String> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    public static final class Builder {

        private long masterEpoch;
        private long generation;
        private ScalingAction action = ScalingAction.NO_ACTION;
        private int currentWorkers;
        private int recommendedWorkers;
        private long observedAtMillis;
        private long validUntilMillis;
        private List<String> triggerReasons = Collections.emptyList();
        private List<String> blockingReasons = Collections.emptyList();
        private AutoscalerMetricsSnapshot snapshot = AutoscalerMetricsSnapshot.builder().build();
        private boolean recommendationOnly = true;

        public Builder masterEpoch(long masterEpoch) {
            this.masterEpoch = masterEpoch;
            return this;
        }

        public Builder generation(long generation) {
            this.generation = generation;
            return this;
        }

        public Builder action(ScalingAction action) {
            this.action = action;
            return this;
        }

        public Builder currentWorkers(int currentWorkers) {
            this.currentWorkers = currentWorkers;
            return this;
        }

        public Builder recommendedWorkers(int recommendedWorkers) {
            this.recommendedWorkers = recommendedWorkers;
            return this;
        }

        public Builder observedAtMillis(long observedAtMillis) {
            this.observedAtMillis = observedAtMillis;
            return this;
        }

        public Builder validUntilMillis(long validUntilMillis) {
            this.validUntilMillis = validUntilMillis;
            return this;
        }

        public Builder triggerReasons(List<String> triggerReasons) {
            this.triggerReasons = triggerReasons;
            return this;
        }

        public Builder blockingReasons(List<String> blockingReasons) {
            this.blockingReasons = blockingReasons;
            return this;
        }

        public Builder snapshot(AutoscalerMetricsSnapshot snapshot) {
            this.snapshot = snapshot;
            return this;
        }

        public Builder recommendationOnly(boolean recommendationOnly) {
            this.recommendationOnly = recommendationOnly;
            return this;
        }

        public ScalingRecommendation build() {
            return new ScalingRecommendation(this);
        }
    }
}
