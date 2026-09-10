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

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * Internal runtime configuration for the advisory autoscaler.
 *
 * <p>This class intentionally contains no YAML or public option parsing. Public configuration
 * adapters may map their values to this model in a later integration layer.
 */
public final class AutoscalerRuntimeConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Whether the advisory autoscaler evaluation loop is enabled. Disabled by default. */
    private final boolean enabled;

    /** Interval between two autoscaler evaluations, in seconds. */
    private final int evaluationIntervalSeconds;

    /**
     * Maximum allowed age of Worker metrics during an autoscaler evaluation, in seconds. Metrics
     * older than this threshold are treated as stale and excluded from the evaluation.
     */
    private final int maxMetricStalenessSeconds;

    /**
     * Maximum amount by which a worker sample timestamp may lead the receiver clock when their
     * clocks are out of sync, in seconds.
     */
    private final int futureTimestampToleranceSeconds;

    /**
     * Required continuous duration of scale-out pressure before publishing scale-out, in seconds.
     */
    private final int scaleOutStabilizationSeconds;

    /**
     * Required continuous duration of scale-in conditions before publishing scale-in, in seconds.
     */
    private final int scaleInStabilizationSeconds;

    /** CPU utilization at or above which the policy considers scaling out. */
    private final double scaleOutCpuThreshold;

    /** JVM memory utilization at or above which the policy considers scaling out. */
    private final double scaleOutJvmMemoryThreshold;

    /** CPU utilization below which the policy may consider scaling in. */
    private final double scaleInCpuThreshold;

    /** JVM memory utilization below which the policy may consider scaling in. */
    private final double scaleInJvmMemoryThreshold;

    /**
     * Fixed-slot utilization at or above which slot pressure can combine with scheduling pressure.
     */
    private final double fixedSlotScaleOutThreshold;

    /** Fixed-slot utilization below which the policy may consider scaling in. */
    private final double fixedSlotScaleInThreshold;

    /** Number of workers by which one advisory recommendation changes the target. */
    private final int scaleStep;

    /** Lower bound for the recommended worker count. */
    private final int minWorkers;

    /** Upper bound for the recommended worker count. */
    private final int maxWorkers;

    /** Maximum number of recent recommendations retained in the in-memory state store. */
    private final int historySize;

    private AutoscalerRuntimeConfig(Builder builder) {
        this.enabled = builder.enabled;
        this.evaluationIntervalSeconds = builder.evaluationIntervalSeconds;
        this.maxMetricStalenessSeconds = builder.maxMetricStalenessSeconds;
        this.futureTimestampToleranceSeconds = builder.futureTimestampToleranceSeconds;
        this.scaleOutStabilizationSeconds = builder.scaleOutStabilizationSeconds;
        this.scaleInStabilizationSeconds = builder.scaleInStabilizationSeconds;
        this.scaleOutCpuThreshold = builder.scaleOutCpuThreshold;
        this.scaleOutJvmMemoryThreshold = builder.scaleOutJvmMemoryThreshold;
        this.scaleInCpuThreshold = builder.scaleInCpuThreshold;
        this.scaleInJvmMemoryThreshold = builder.scaleInJvmMemoryThreshold;
        this.fixedSlotScaleOutThreshold = builder.fixedSlotScaleOutThreshold;
        this.fixedSlotScaleInThreshold = builder.fixedSlotScaleInThreshold;
        this.scaleStep = builder.scaleStep;
        this.minWorkers = builder.minWorkers;
        this.maxWorkers = builder.maxWorkers;
        this.historySize = builder.historySize;
        validate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static AutoscalerRuntimeConfig defaults() {
        return builder().build();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getEvaluationIntervalSeconds() {
        return evaluationIntervalSeconds;
    }

    public int getMaxMetricStalenessSeconds() {
        return maxMetricStalenessSeconds;
    }

    public int getFutureTimestampToleranceSeconds() {
        return futureTimestampToleranceSeconds;
    }

    public int getScaleOutStabilizationSeconds() {
        return scaleOutStabilizationSeconds;
    }

    public int getScaleInStabilizationSeconds() {
        return scaleInStabilizationSeconds;
    }

    public double getScaleOutCpuThreshold() {
        return scaleOutCpuThreshold;
    }

    public double getScaleOutJvmMemoryThreshold() {
        return scaleOutJvmMemoryThreshold;
    }

    public double getScaleInCpuThreshold() {
        return scaleInCpuThreshold;
    }

    public double getScaleInJvmMemoryThreshold() {
        return scaleInJvmMemoryThreshold;
    }

    public double getFixedSlotScaleOutThreshold() {
        return fixedSlotScaleOutThreshold;
    }

    public double getFixedSlotScaleInThreshold() {
        return fixedSlotScaleInThreshold;
    }

    public int getScaleStep() {
        return scaleStep;
    }

    public int getMinWorkers() {
        return minWorkers;
    }

    public int getMaxWorkers() {
        return maxWorkers;
    }

    public int getHistorySize() {
        return historySize;
    }

    public void validate() {
        if (scaleInCpuThreshold >= scaleOutCpuThreshold
                || scaleInJvmMemoryThreshold >= scaleOutJvmMemoryThreshold
                || fixedSlotScaleInThreshold >= fixedSlotScaleOutThreshold) {
            throw new IllegalArgumentException(
                    "scale-in thresholds must be lower than scale-out thresholds");
        }
        if (maxWorkers < minWorkers) {
            throw new IllegalArgumentException(
                    "maxWorkers must be greater than or equal to minWorkers");
        }
    }

    public static final class Builder {
        private boolean enabled = false;
        private int evaluationIntervalSeconds = 30;
        private int maxMetricStalenessSeconds = 120;
        private int futureTimestampToleranceSeconds = 5;
        private int scaleOutStabilizationSeconds = 300;
        private int scaleInStabilizationSeconds = 600;
        private double scaleOutCpuThreshold = 0.8d;
        private double scaleOutJvmMemoryThreshold = 0.8d;
        private double scaleInCpuThreshold = 0.3d;
        private double scaleInJvmMemoryThreshold = 0.3d;
        private double fixedSlotScaleOutThreshold = 0.8d;
        private double fixedSlotScaleInThreshold = 0.3d;
        private int scaleStep = 1;
        private int minWorkers = 1;
        private int maxWorkers = Integer.MAX_VALUE;
        private int historySize = 20;

        public Builder enabled(boolean value) {
            enabled = value;
            return this;
        }

        public Builder evaluationIntervalSeconds(int value) {
            evaluationIntervalSeconds = value;
            return this;
        }

        public Builder maxMetricStalenessSeconds(int value) {
            maxMetricStalenessSeconds = value;
            return this;
        }

        public Builder futureTimestampToleranceSeconds(int value) {
            futureTimestampToleranceSeconds = value;
            return this;
        }

        public Builder scaleOutStabilizationSeconds(int value) {
            scaleOutStabilizationSeconds = value;
            return this;
        }

        public Builder scaleInStabilizationSeconds(int value) {
            scaleInStabilizationSeconds = value;
            return this;
        }

        public Builder scaleOutCpuThreshold(double value) {
            scaleOutCpuThreshold = value;
            return this;
        }

        public Builder scaleOutJvmMemoryThreshold(double value) {
            scaleOutJvmMemoryThreshold = value;
            return this;
        }

        public Builder scaleInCpuThreshold(double value) {
            scaleInCpuThreshold = value;
            return this;
        }

        public Builder scaleInJvmMemoryThreshold(double value) {
            scaleInJvmMemoryThreshold = value;
            return this;
        }

        public Builder fixedSlotScaleOutThreshold(double value) {
            fixedSlotScaleOutThreshold = value;
            return this;
        }

        public Builder fixedSlotScaleInThreshold(double value) {
            fixedSlotScaleInThreshold = value;
            return this;
        }

        public Builder scaleStep(int value) {
            scaleStep = value;
            return this;
        }

        public Builder minWorkers(int value) {
            minWorkers = value;
            return this;
        }

        public Builder maxWorkers(int value) {
            maxWorkers = value;
            return this;
        }

        public Builder historySize(int value) {
            historySize = value;
            return this;
        }

        public AutoscalerRuntimeConfig build() {
            validate();
            return new AutoscalerRuntimeConfig(this);
        }

        private void validate() {
            checkPositive(evaluationIntervalSeconds, "evaluationIntervalSeconds must be > 0");
            checkPositive(maxMetricStalenessSeconds, "maxMetricStalenessSeconds must be > 0");
            if (futureTimestampToleranceSeconds < 0
                    || scaleOutStabilizationSeconds < 0
                    || scaleInStabilizationSeconds < 0) {
                throw new IllegalArgumentException("time windows and skew must be >= 0");
            }
            checkPositive(scaleStep, "scaleStep must be > 0");
            checkPositive(minWorkers, "minWorkers must be > 0");
            checkPositive(maxWorkers, "maxWorkers must be > 0");
            checkPositive(historySize, "historySize must be > 0");
            checkThreshold(scaleOutCpuThreshold);
            checkThreshold(scaleOutJvmMemoryThreshold);
            checkThreshold(scaleInCpuThreshold);
            checkThreshold(scaleInJvmMemoryThreshold);
            checkThreshold(fixedSlotScaleOutThreshold);
            checkThreshold(fixedSlotScaleInThreshold);
        }

        private static void checkThreshold(double value) {
            if (!Double.isFinite(value) || value < 0.0d || value > 1.0d) {
                throw new IllegalArgumentException("threshold must be in [0.0, 1.0]");
            }
        }
    }
}
