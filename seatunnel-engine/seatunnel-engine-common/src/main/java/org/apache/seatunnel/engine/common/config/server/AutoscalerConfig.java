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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Configuration for Zeta Phase 1 autoscaling recommendations.
 *
 * <p>The feature is disabled by default and setters validate user-facing safety bounds before
 * runtime use.
 */
@Data
public class AutoscalerConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean enabled = ServerConfigOptions.AUTOSCALER_ENABLED.defaultValue();
    private int evaluationIntervalSeconds =
            ServerConfigOptions.AUTOSCALER_EVALUATION_INTERVAL_SECONDS.defaultValue();
    private int metricsFreshnessSeconds =
            ServerConfigOptions.AUTOSCALER_METRICS_FRESHNESS_SECONDS.defaultValue();
    private int maxFutureSkewSeconds =
            ServerConfigOptions.AUTOSCALER_MAX_FUTURE_SKEW_SECONDS.defaultValue();
    private int scaleOutStabilizationSeconds =
            ServerConfigOptions.AUTOSCALER_SCALE_OUT_STABILIZATION_SECONDS.defaultValue();
    private int scaleInStabilizationSeconds =
            ServerConfigOptions.AUTOSCALER_SCALE_IN_STABILIZATION_SECONDS.defaultValue();
    private double scaleOutCpuThreshold =
            ServerConfigOptions.AUTOSCALER_SCALE_OUT_CPU_THRESHOLD.defaultValue();
    private double scaleOutJvmMemoryThreshold =
            ServerConfigOptions.AUTOSCALER_SCALE_OUT_JVM_MEMORY_THRESHOLD.defaultValue();
    private double scaleInCpuThreshold =
            ServerConfigOptions.AUTOSCALER_SCALE_IN_CPU_THRESHOLD.defaultValue();
    private double scaleInJvmMemoryThreshold =
            ServerConfigOptions.AUTOSCALER_SCALE_IN_JVM_MEMORY_THRESHOLD.defaultValue();
    private double fixedSlotScaleOutThreshold =
            ServerConfigOptions.AUTOSCALER_FIXED_SLOT_SCALE_OUT_THRESHOLD.defaultValue();
    private double fixedSlotScaleInThreshold =
            ServerConfigOptions.AUTOSCALER_FIXED_SLOT_SCALE_IN_THRESHOLD.defaultValue();
    private int scaleStep = ServerConfigOptions.AUTOSCALER_SCALE_STEP.defaultValue();
    private int minWorkers = ServerConfigOptions.AUTOSCALER_MIN_WORKERS.defaultValue();
    private int maxWorkers = ServerConfigOptions.AUTOSCALER_MAX_WORKERS.defaultValue();
    private int historySize = ServerConfigOptions.AUTOSCALER_HISTORY_SIZE.defaultValue();

    public void setEvaluationIntervalSeconds(int evaluationIntervalSeconds) {
        checkPositive(
                evaluationIntervalSeconds,
                ServerConfigOptions.AUTOSCALER_EVALUATION_INTERVAL_SECONDS.key() + " must be > 0");
        this.evaluationIntervalSeconds = evaluationIntervalSeconds;
    }

    public void setMetricsFreshnessSeconds(int metricsFreshnessSeconds) {
        checkPositive(
                metricsFreshnessSeconds,
                ServerConfigOptions.AUTOSCALER_METRICS_FRESHNESS_SECONDS.key() + " must be > 0");
        this.metricsFreshnessSeconds = metricsFreshnessSeconds;
    }

    public void setMaxFutureSkewSeconds(int maxFutureSkewSeconds) {
        if (maxFutureSkewSeconds < 0) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_MAX_FUTURE_SKEW_SECONDS.key() + " must be >= 0");
        }
        this.maxFutureSkewSeconds = maxFutureSkewSeconds;
    }

    public void setScaleOutStabilizationSeconds(int scaleOutStabilizationSeconds) {
        if (scaleOutStabilizationSeconds < 0) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_SCALE_OUT_STABILIZATION_SECONDS.key()
                            + " must be >= 0");
        }
        this.scaleOutStabilizationSeconds = scaleOutStabilizationSeconds;
    }

    public void setScaleInStabilizationSeconds(int scaleInStabilizationSeconds) {
        if (scaleInStabilizationSeconds < 0) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_SCALE_IN_STABILIZATION_SECONDS.key()
                            + " must be >= 0");
        }
        this.scaleInStabilizationSeconds = scaleInStabilizationSeconds;
    }

    public void setScaleOutCpuThreshold(double scaleOutCpuThreshold) {
        checkThreshold(
                ServerConfigOptions.AUTOSCALER_SCALE_OUT_CPU_THRESHOLD.key(), scaleOutCpuThreshold);
        this.scaleOutCpuThreshold = scaleOutCpuThreshold;
    }

    public void setScaleOutJvmMemoryThreshold(double scaleOutJvmMemoryThreshold) {
        checkThreshold(
                ServerConfigOptions.AUTOSCALER_SCALE_OUT_JVM_MEMORY_THRESHOLD.key(),
                scaleOutJvmMemoryThreshold);
        this.scaleOutJvmMemoryThreshold = scaleOutJvmMemoryThreshold;
    }

    public void setScaleInCpuThreshold(double scaleInCpuThreshold) {
        checkThreshold(
                ServerConfigOptions.AUTOSCALER_SCALE_IN_CPU_THRESHOLD.key(), scaleInCpuThreshold);
        this.scaleInCpuThreshold = scaleInCpuThreshold;
    }

    public void setScaleInJvmMemoryThreshold(double scaleInJvmMemoryThreshold) {
        checkThreshold(
                ServerConfigOptions.AUTOSCALER_SCALE_IN_JVM_MEMORY_THRESHOLD.key(),
                scaleInJvmMemoryThreshold);
        this.scaleInJvmMemoryThreshold = scaleInJvmMemoryThreshold;
    }

    public void setFixedSlotScaleOutThreshold(double fixedSlotScaleOutThreshold) {
        checkThreshold(
                ServerConfigOptions.AUTOSCALER_FIXED_SLOT_SCALE_OUT_THRESHOLD.key(),
                fixedSlotScaleOutThreshold);
        this.fixedSlotScaleOutThreshold = fixedSlotScaleOutThreshold;
    }

    public void setFixedSlotScaleInThreshold(double fixedSlotScaleInThreshold) {
        checkThreshold(
                ServerConfigOptions.AUTOSCALER_FIXED_SLOT_SCALE_IN_THRESHOLD.key(),
                fixedSlotScaleInThreshold);
        this.fixedSlotScaleInThreshold = fixedSlotScaleInThreshold;
    }

    public void setScaleStep(int scaleStep) {
        checkPositive(scaleStep, ServerConfigOptions.AUTOSCALER_SCALE_STEP.key() + " must be > 0");
        this.scaleStep = scaleStep;
    }

    public void setMinWorkers(int minWorkers) {
        checkPositive(
                minWorkers, ServerConfigOptions.AUTOSCALER_MIN_WORKERS.key() + " must be > 0");
        this.minWorkers = minWorkers;
    }

    public void setMaxWorkers(int maxWorkers) {
        checkPositive(
                maxWorkers, ServerConfigOptions.AUTOSCALER_MAX_WORKERS.key() + " must be > 0");
        this.maxWorkers = maxWorkers;
    }

    public void setHistorySize(int historySize) {
        checkPositive(
                historySize, ServerConfigOptions.AUTOSCALER_HISTORY_SIZE.key() + " must be > 0");
        this.historySize = historySize;
    }

    public void validate() {
        checkThresholdOrdering();
        checkWorkerRange();
    }

    private void checkThresholdOrdering() {
        if (scaleInCpuThreshold >= scaleOutCpuThreshold) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_SCALE_IN_CPU_THRESHOLD.key()
                            + " must be lower than "
                            + ServerConfigOptions.AUTOSCALER_SCALE_OUT_CPU_THRESHOLD.key());
        }
        if (scaleInJvmMemoryThreshold >= scaleOutJvmMemoryThreshold) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_SCALE_IN_JVM_MEMORY_THRESHOLD.key()
                            + " must be lower than "
                            + ServerConfigOptions.AUTOSCALER_SCALE_OUT_JVM_MEMORY_THRESHOLD.key());
        }
        if (fixedSlotScaleInThreshold >= fixedSlotScaleOutThreshold) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_FIXED_SLOT_SCALE_IN_THRESHOLD.key()
                            + " must be lower than "
                            + ServerConfigOptions.AUTOSCALER_FIXED_SLOT_SCALE_OUT_THRESHOLD.key());
        }
    }

    private void checkWorkerRange() {
        if (maxWorkers < minWorkers) {
            throw new IllegalArgumentException(
                    ServerConfigOptions.AUTOSCALER_MAX_WORKERS.key()
                            + " must be >= "
                            + ServerConfigOptions.AUTOSCALER_MIN_WORKERS.key());
        }
    }

    private static void checkThreshold(String key, double value) {
        if (!Double.isFinite(value) || value < 0.0d || value > 1.0d) {
            throw new IllegalArgumentException(key + " must be in [0.0, 1.0]");
        }
    }
}
