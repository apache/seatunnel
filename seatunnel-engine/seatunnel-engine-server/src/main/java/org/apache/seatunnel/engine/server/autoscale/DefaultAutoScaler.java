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
import java.util.concurrent.TimeUnit;

/** Runs policy evaluations, records state transitions, and publishes stable scaling targets. */
public final class DefaultAutoScaler {
    private final AutoscalerConfig config;
    private final AutoscalerSignalCollector signalCollector;
    private final AutoscalingPolicy policy;
    private final AutoscalingStateTracker stateTracker;
    private final AutoscalerStateStore stateStore;
    private final RecommendationPublisher recommendationPublisher;
    private final AutoscalerTimeSource timeSource;
    private long masterEpoch;
    private long generation;
    private volatile boolean closed;
    private long lastPublishedTimeMillis = -1L;

    public DefaultAutoScaler(
            long masterEpoch,
            AutoscalerConfig config,
            AutoscalerSignalCollector signalCollector,
            AutoscalingPolicy policy,
            AutoscalingStateTracker stateTracker,
            AutoscalerStateStore stateStore,
            RecommendationPublisher recommendationPublisher,
            AutoscalerTimeSource timeSource) {
        this.masterEpoch = masterEpoch;
        this.config = Objects.requireNonNull(config, "config");
        this.signalCollector = Objects.requireNonNull(signalCollector, "signalCollector");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.stateTracker = Objects.requireNonNull(stateTracker, "stateTracker");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
        this.recommendationPublisher =
                Objects.requireNonNull(recommendationPublisher, "recommendationPublisher");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    public static AutoscalingStateTracker stateTracker(AutoscalerConfig config) {
        return new AutoscalingStateTracker(
                TimeUnit.SECONDS.toMillis(config.getScaleOutStabilizationSeconds()),
                TimeUnit.SECONDS.toMillis(config.getScaleInStabilizationSeconds()),
                TimeUnit.SECONDS.toMillis(config.getKeepFiringSeconds()));
    }

    /** Collects metrics, records the resulting state transition, and publishes stable targets. */
    public synchronized void evaluateOnce() {
        if (closed) {
            return;
        }
        AutoscalerMetricsSnapshot snapshot = signalCollector.collect();
        stateStore.updateCurrentSnapshot(snapshot);
        AutoscaleEvaluation evaluation = policy.evaluate(snapshot);
        long monotonicTimeMillis = timeSource.monotonicTimeMillis();
        AutoscalingStateTransition transition =
                stateTracker.evaluate(evaluation.getEvaluationAction(), monotonicTimeMillis);
        long evaluatedAtMillis = timeSource.currentTimeMillis();
        stateStore.recordEvaluation(
                new AutoscalingEvaluationRecord(evaluation, transition, evaluatedAtMillis));
        if (!shouldPublishRecommendation(transition, monotonicTimeMillis)) {
            return;
        }
        publishRecommendation(
                transition.getCurrentStateAction(), snapshot, evaluation, evaluatedAtMillis);
    }

    public synchronized void close() {
        closed = true;
    }

    public synchronized void reset(long masterEpoch) {
        this.masterEpoch = masterEpoch;
        generation = 0L;
        lastPublishedTimeMillis = -1L;
        stateTracker.reset();
        recommendationPublisher.reset();
    }

    public synchronized long getMasterEpoch() {
        return masterEpoch;
    }

    public synchronized long getNextGeneration() {
        return generation;
    }

    private RecommendationPublisher.PublicationResult publishRecommendation(
            EvaluationAction action,
            AutoscalerMetricsSnapshot snapshot,
            AutoscaleEvaluation evaluation,
            long observedAtMillis) {
        ScalingRecommendation recommendation =
                ScalingRecommendation.builder()
                        .masterEpoch(masterEpoch)
                        .generation(generation++)
                        .action(action)
                        .currentWorkers(snapshot.getCurrentWorkers())
                        .recommendedWorkers(recommendedWorkers(action, snapshot))
                        .observedAtMillis(observedAtMillis)
                        .validUntilMillis(
                                observedAtMillis
                                        + TimeUnit.SECONDS.toMillis(
                                                config.getEvaluationIntervalSeconds()))
                        .decisionReasons(evaluation.getDecisionReasons())
                        .build();
        RecommendationPublisher.PublicationResult result =
                recommendationPublisher.publish(recommendation);
        if (result == RecommendationPublisher.PublicationResult.ACCEPTED) {
            lastPublishedTimeMillis = timeSource.monotonicTimeMillis();
        }
        return result;
    }

    private boolean isRepeatDue(long currentTimeMillis) {
        return lastPublishedTimeMillis >= 0
                && currentTimeMillis - lastPublishedTimeMillis
                        >= TimeUnit.SECONDS.toMillis(config.getRecommendationRepeatSeconds());
    }

    private boolean shouldPublishRecommendation(
            AutoscalingStateTransition transition, long currentMonotonicMillis) {
        return transition.startsFiring()
                || (transition.remainsFiring() && isRepeatDue(currentMonotonicMillis));
    }

    private int recommendedWorkers(EvaluationAction action, AutoscalerMetricsSnapshot snapshot) {
        int currentWorkers = snapshot.getCurrentWorkers();
        return action == EvaluationAction.SCALE_OUT
                ? Math.min(config.getMaxWorkers(), currentWorkers + config.getScaleStep())
                : Math.max(config.getMinWorkers(), currentWorkers - config.getScaleStep());
    }
}
