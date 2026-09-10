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

/**
 * Threshold configuration used by the autoscaling policy to evaluate scale-out and scale-in
 * conditions.
 *
 * <p>All utilization thresholds are ratios in the range {@code [0, 1]}.
 */
public final class AutoscalerPolicyConfig {

    /** CPU utilization at or above which scale-out may be triggered. */
    private final double scaleOutCpuThreshold;

    /** JVM memory utilization at or above which scale-out may be triggered. */
    private final double scaleOutJvmMemoryThreshold;

    /** CPU utilization below which the CPU scale-in condition is satisfied. */
    private final double scaleInCpuThreshold;

    /** JVM memory utilization below which the memory scale-in condition is satisfied. */
    private final double scaleInJvmMemoryThreshold;

    /**
     * Fixed-slot utilization at or above which slot pressure can combine with scheduling pressure.
     */
    private final double fixedSlotScaleOutThreshold;

    /** Fixed-slot utilization below which the slot scale-in condition is satisfied. */
    private final double fixedSlotScaleInThreshold;

    private AutoscalerPolicyConfig(Builder builder) {
        this.scaleOutCpuThreshold = builder.scaleOutCpuThreshold;
        this.scaleOutJvmMemoryThreshold = builder.scaleOutJvmMemoryThreshold;
        this.scaleInCpuThreshold = builder.scaleInCpuThreshold;
        this.scaleInJvmMemoryThreshold = builder.scaleInJvmMemoryThreshold;
        this.fixedSlotScaleOutThreshold = builder.fixedSlotScaleOutThreshold;
        this.fixedSlotScaleInThreshold = builder.fixedSlotScaleInThreshold;
    }

    public static Builder builder() {
        return new Builder();
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

    public static final class Builder {

        private double scaleOutCpuThreshold = 0.8d;
        private double scaleOutJvmMemoryThreshold = 0.8d;
        private double scaleInCpuThreshold = 0.3d;
        private double scaleInJvmMemoryThreshold = 0.3d;
        private double fixedSlotScaleOutThreshold = 0.8d;
        private double fixedSlotScaleInThreshold = 0.3d;

        public Builder scaleOutCpuThreshold(double scaleOutCpuThreshold) {
            this.scaleOutCpuThreshold = scaleOutCpuThreshold;
            return this;
        }

        public Builder scaleOutJvmMemoryThreshold(double scaleOutJvmMemoryThreshold) {
            this.scaleOutJvmMemoryThreshold = scaleOutJvmMemoryThreshold;
            return this;
        }

        public Builder scaleInCpuThreshold(double scaleInCpuThreshold) {
            this.scaleInCpuThreshold = scaleInCpuThreshold;
            return this;
        }

        public Builder scaleInJvmMemoryThreshold(double scaleInJvmMemoryThreshold) {
            this.scaleInJvmMemoryThreshold = scaleInJvmMemoryThreshold;
            return this;
        }

        public Builder fixedSlotScaleOutThreshold(double fixedSlotScaleOutThreshold) {
            this.fixedSlotScaleOutThreshold = fixedSlotScaleOutThreshold;
            return this;
        }

        public Builder fixedSlotScaleInThreshold(double fixedSlotScaleInThreshold) {
            this.fixedSlotScaleInThreshold = fixedSlotScaleInThreshold;
            return this;
        }

        public AutoscalerPolicyConfig build() {
            return new AutoscalerPolicyConfig(this);
        }
    }
}
