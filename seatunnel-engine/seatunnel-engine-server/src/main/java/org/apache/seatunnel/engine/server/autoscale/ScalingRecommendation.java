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

/** A time-bounded, externally consumable absolute worker scaling target. */
public final class ScalingRecommendation implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long masterEpoch;
    private final long generation;
    private final EvaluationAction action;
    private final int currentWorkers;
    private final int recommendedWorkers;
    private final long observedAtMillis;
    private final long validUntilMillis;
    private final List<String> decisionReasons;

    private ScalingRecommendation(Builder builder) {
        this.masterEpoch = builder.masterEpoch;
        this.generation = builder.generation;
        this.action = Objects.requireNonNull(builder.action, "action");
        this.currentWorkers = builder.currentWorkers;
        this.recommendedWorkers = builder.recommendedWorkers;
        this.observedAtMillis = builder.observedAtMillis;
        this.validUntilMillis = builder.validUntilMillis;
        this.decisionReasons =
                Collections.unmodifiableList(new ArrayList<>(builder.decisionReasons));
        if (validUntilMillis < observedAtMillis) {
            throw new IllegalArgumentException(
                    "validUntilMillis must not precede observedAtMillis");
        }
        if (action == EvaluationAction.NO_ACTION) {
            throw new IllegalArgumentException("action must be a scaling action");
        }
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

    public EvaluationAction getAction() {
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

    public List<String> getDecisionReasons() {
        return decisionReasons;
    }

    public boolean isValidAt(long currentTimeMillis) {
        return currentTimeMillis <= validUntilMillis;
    }

    public static final class Builder {
        private long masterEpoch;
        private long generation;
        private EvaluationAction action;
        private int currentWorkers;
        private int recommendedWorkers;
        private long observedAtMillis;
        private long validUntilMillis;
        private List<String> decisionReasons = Collections.emptyList();

        public Builder masterEpoch(long value) {
            masterEpoch = value;
            return this;
        }

        public Builder generation(long value) {
            generation = value;
            return this;
        }

        public Builder action(EvaluationAction value) {
            action = value;
            return this;
        }

        public Builder currentWorkers(int value) {
            currentWorkers = value;
            return this;
        }

        public Builder recommendedWorkers(int value) {
            recommendedWorkers = value;
            return this;
        }

        public Builder observedAtMillis(long value) {
            observedAtMillis = value;
            return this;
        }

        public Builder validUntilMillis(long value) {
            validUntilMillis = value;
            return this;
        }

        public Builder decisionReasons(List<String> value) {
            decisionReasons = value;
            return this;
        }

        public ScalingRecommendation build() {
            return new ScalingRecommendation(this);
        }
    }
}
