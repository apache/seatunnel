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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Runs one advisory autoscaler evaluation at a time for the active master.
 *
 * <p>It collects signals, applies policy and stabilization, then publishes a fenced
 * recommendation-only result.
 */
public final class DefaultAutoScaler {

    private final AutoscalerRuntimeConfig config;
    private final AutoscalerSignalCollector signalCollector;
    private final AutoscalingPolicy policy;
    private final StabilizationTracker stabilizationTracker;
    private final AutoscalerStateStore stateStore;
    private final AutoscalerTimeSource timeSource;

    private long masterEpoch;
    private long generation;
    private volatile boolean closed;

    public DefaultAutoScaler(
            long masterEpoch,
            AutoscalerRuntimeConfig config,
            AutoscalerSignalCollector signalCollector,
            AutoscalingPolicy policy,
            StabilizationTracker stabilizationTracker,
            AutoscalerStateStore stateStore,
            AutoscalerTimeSource timeSource) {
        this.masterEpoch = masterEpoch;
        this.config = Objects.requireNonNull(config, "config");
        this.signalCollector = Objects.requireNonNull(signalCollector, "signalCollector");
        this.policy = Objects.requireNonNull(policy, "policy");
        this.stabilizationTracker =
                Objects.requireNonNull(stabilizationTracker, "stabilizationTracker");
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
    }

    public static AutoscalerPolicyConfig policyConfig(AutoscalerRuntimeConfig config) {
        return AutoscalerPolicyConfig.builder()
                .scaleOutCpuThreshold(config.getScaleOutCpuThreshold())
                .scaleOutJvmMemoryThreshold(config.getScaleOutJvmMemoryThreshold())
                .scaleInCpuThreshold(config.getScaleInCpuThreshold())
                .scaleInJvmMemoryThreshold(config.getScaleInJvmMemoryThreshold())
                .fixedSlotScaleOutThreshold(config.getFixedSlotScaleOutThreshold())
                .fixedSlotScaleInThreshold(config.getFixedSlotScaleInThreshold())
                .build();
    }

    public static StabilizationTracker stabilizationTracker(AutoscalerRuntimeConfig config) {
        return new StabilizationTracker(
                TimeUnit.SECONDS.toNanos(config.getScaleOutStabilizationSeconds()),
                TimeUnit.SECONDS.toNanos(config.getScaleInStabilizationSeconds()));
    }

    public synchronized RecommendationFence.PublicationResult evaluateOnce() {
        if (closed) {
            return RecommendationFence.PublicationResult.REJECTED;
        }
        AutoscalerMetricsSnapshot snapshot = signalCollector.collect();
        AutoscaleEvaluation evaluation = policy.evaluate(snapshot);
        boolean stabilized =
                stabilizationTracker.isStabilized(evaluation.getAction(), timeSource.nanoTime());
        ScalingAction publishedAction = evaluation.getAction();
        ArrayList<String> blockingReasons = new ArrayList<>(evaluation.getBlockingReasons());
        if (!stabilized) {
            blockingReasons.add("stabilization_window_not_satisfied");
            publishedAction = ScalingAction.NO_ACTION;
        }

        ScalingRecommendation recommendation =
                ScalingRecommendation.builder()
                        .masterEpoch(masterEpoch)
                        .generation(generation++)
                        .action(publishedAction)
                        .currentWorkers(snapshot.getCurrentWorkers())
                        .recommendedWorkers(recommendedWorkers(publishedAction, snapshot))
                        .observedAtMillis(timeSource.currentTimeMillis())
                        .validUntilMillis(
                                timeSource.currentTimeMillis()
                                        + TimeUnit.SECONDS.toMillis(
                                                config.getEvaluationIntervalSeconds()))
                        .triggerReasons(evaluation.getTriggerReasons())
                        .blockingReasons(blockingReasons)
                        .snapshot(snapshot)
                        .recommendationOnly(true)
                        .build();
        return stateStore.publish(recommendation);
    }

    public synchronized void close() {
        closed = true;
    }

    public synchronized void reset(long masterEpoch) {
        this.masterEpoch = masterEpoch;
        this.generation = 0L;
        this.stabilizationTracker.reset();
    }

    public synchronized long getMasterEpoch() {
        return masterEpoch;
    }

    public synchronized long getNextGeneration() {
        return generation;
    }

    private int recommendedWorkers(
            ScalingAction publishedAction, AutoscalerMetricsSnapshot snapshot) {
        int currentWorkers = snapshot.getCurrentWorkers();
        if (publishedAction == ScalingAction.SCALE_OUT) {
            return Math.min(config.getMaxWorkers(), currentWorkers + config.getScaleStep());
        }
        if (publishedAction == ScalingAction.SCALE_IN_CANDIDATE) {
            return Math.max(config.getMinWorkers(), currentWorkers - config.getScaleStep());
        }
        return currentWorkers;
    }
}
