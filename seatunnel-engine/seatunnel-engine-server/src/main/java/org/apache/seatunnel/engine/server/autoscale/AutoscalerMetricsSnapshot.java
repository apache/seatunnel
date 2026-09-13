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
import java.util.Objects;

/**
 * Immutable input snapshot evaluated by the autoscaling policy.
 *
 * <p>Metric values carry explicit validity status so missing, stale, future, and unknown signals
 * are not confused with numeric zero.
 */
public final class AutoscalerMetricsSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long evaluationTimeMillis;
    private final int currentWorkers;
    private final int minWorkers;
    private final int maxWorkers;
    private final boolean dynamicSlot;
    private final int assignedSlots;
    private final int unassignedSlots;
    private final MetricValue fixedSlotUtilization;
    private final MetricValue cpu;
    private final MetricValue jvmMemory;
    private final int totalWorkerSamples;
    private final int validWorkerSamples;
    private final int missingWorkerSamples;
    private final int staleWorkerSamples;
    private final int futureWorkerSamples;
    private final int pendingJobCount;
    private final long longestPendingDurationMillis;
    private final long resourceShortageCount;
    private final long waitShortageCount;
    private final long rejectShortageCount;
    private final boolean waitShortage;
    private final boolean rejectShortage;
    private final boolean scaleInMetricsValid;

    private AutoscalerMetricsSnapshot(Builder builder) {
        this.evaluationTimeMillis = builder.evaluationTimeMillis;
        this.currentWorkers = builder.currentWorkers;
        this.minWorkers = builder.minWorkers;
        this.maxWorkers = builder.maxWorkers;
        this.dynamicSlot = builder.dynamicSlot;
        this.assignedSlots = builder.assignedSlots;
        this.unassignedSlots = builder.unassignedSlots;
        this.fixedSlotUtilization =
                Objects.requireNonNull(builder.fixedSlotUtilization, "fixedSlotUtilization");
        this.cpu = Objects.requireNonNull(builder.cpu, "cpu");
        this.jvmMemory = Objects.requireNonNull(builder.jvmMemory, "jvmMemory");
        this.totalWorkerSamples = builder.totalWorkerSamples;
        this.validWorkerSamples = builder.validWorkerSamples;
        this.missingWorkerSamples = builder.missingWorkerSamples;
        this.staleWorkerSamples = builder.staleWorkerSamples;
        this.futureWorkerSamples = builder.futureWorkerSamples;
        this.pendingJobCount = builder.pendingJobCount;
        this.longestPendingDurationMillis = builder.longestPendingDurationMillis;
        this.resourceShortageCount = builder.resourceShortageCount;
        this.waitShortageCount = builder.waitShortageCount;
        this.rejectShortageCount = builder.rejectShortageCount;
        this.waitShortage = builder.waitShortage;
        this.rejectShortage = builder.rejectShortage;
        this.scaleInMetricsValid = builder.scaleInMetricsValid;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getEvaluationTimeMillis() {
        return evaluationTimeMillis;
    }

    public int getCurrentWorkers() {
        return currentWorkers;
    }

    public int getMinWorkers() {
        return minWorkers;
    }

    public int getMaxWorkers() {
        return maxWorkers;
    }

    public boolean isDynamicSlot() {
        return dynamicSlot;
    }

    public int getAssignedSlots() {
        return assignedSlots;
    }

    public int getUnassignedSlots() {
        return unassignedSlots;
    }

    public MetricValue getFixedSlotUtilization() {
        return fixedSlotUtilization;
    }

    public MetricValue getCpu() {
        return cpu;
    }

    public MetricValue getJvmMemory() {
        return jvmMemory;
    }

    public int getTotalWorkerSamples() {
        return totalWorkerSamples;
    }

    public int getValidWorkerSamples() {
        return validWorkerSamples;
    }

    public int getMissingWorkerSamples() {
        return missingWorkerSamples;
    }

    public int getStaleWorkerSamples() {
        return staleWorkerSamples;
    }

    public int getFutureWorkerSamples() {
        return futureWorkerSamples;
    }

    public int getPendingJobCount() {
        return pendingJobCount;
    }

    public long getLongestPendingDurationMillis() {
        return longestPendingDurationMillis;
    }

    public long getResourceShortageCount() {
        return resourceShortageCount;
    }

    public long getWaitShortageCount() {
        return waitShortageCount;
    }

    public long getRejectShortageCount() {
        return rejectShortageCount;
    }

    public boolean isWaitShortage() {
        return waitShortage;
    }

    public boolean isRejectShortage() {
        return rejectShortage;
    }

    public boolean isScaleInMetricsValid() {
        return scaleInMetricsValid;
    }

    public boolean hasSchedulerShortage() {
        return resourceShortageCount > 0L || waitShortage || rejectShortage;
    }

    public static final class Builder {

        private long evaluationTimeMillis;
        private int currentWorkers;
        private int minWorkers = 1;
        private int maxWorkers = Integer.MAX_VALUE;
        private boolean dynamicSlot = true;
        private int assignedSlots;
        private int unassignedSlots;
        private MetricValue fixedSlotUtilization = MetricValue.unknown();
        private MetricValue cpu = MetricValue.missing();
        private MetricValue jvmMemory = MetricValue.missing();
        private int totalWorkerSamples;
        private int validWorkerSamples;
        private int missingWorkerSamples;
        private int staleWorkerSamples;
        private int futureWorkerSamples;
        private int pendingJobCount;
        private long longestPendingDurationMillis;
        private long resourceShortageCount;
        private long waitShortageCount;
        private long rejectShortageCount;
        private boolean waitShortage;
        private boolean rejectShortage;
        private boolean scaleInMetricsValid;

        public Builder evaluationTimeMillis(long evaluationTimeMillis) {
            this.evaluationTimeMillis = evaluationTimeMillis;
            return this;
        }

        public Builder currentWorkers(int currentWorkers) {
            this.currentWorkers = currentWorkers;
            return this;
        }

        public Builder minWorkers(int minWorkers) {
            this.minWorkers = minWorkers;
            return this;
        }

        public Builder maxWorkers(int maxWorkers) {
            this.maxWorkers = maxWorkers;
            return this;
        }

        public Builder dynamicSlot(boolean dynamicSlot) {
            this.dynamicSlot = dynamicSlot;
            return this;
        }

        public Builder assignedSlots(int assignedSlots) {
            this.assignedSlots = assignedSlots;
            return this;
        }

        public Builder unassignedSlots(int unassignedSlots) {
            this.unassignedSlots = unassignedSlots;
            return this;
        }

        public Builder fixedSlotUtilization(MetricValue fixedSlotUtilization) {
            this.fixedSlotUtilization = fixedSlotUtilization;
            return this;
        }

        public Builder cpu(MetricValue cpu) {
            this.cpu = cpu;
            return this;
        }

        public Builder jvmMemory(MetricValue jvmMemory) {
            this.jvmMemory = jvmMemory;
            return this;
        }

        public Builder totalWorkerSamples(int totalWorkerSamples) {
            this.totalWorkerSamples = totalWorkerSamples;
            return this;
        }

        public Builder validWorkerSamples(int validWorkerSamples) {
            this.validWorkerSamples = validWorkerSamples;
            return this;
        }

        public Builder missingWorkerSamples(int missingWorkerSamples) {
            this.missingWorkerSamples = missingWorkerSamples;
            return this;
        }

        public Builder staleWorkerSamples(int staleWorkerSamples) {
            this.staleWorkerSamples = staleWorkerSamples;
            return this;
        }

        public Builder futureWorkerSamples(int futureWorkerSamples) {
            this.futureWorkerSamples = futureWorkerSamples;
            return this;
        }

        public Builder pendingJobCount(int pendingJobCount) {
            this.pendingJobCount = pendingJobCount;
            return this;
        }

        public Builder longestPendingDurationMillis(long longestPendingDurationMillis) {
            this.longestPendingDurationMillis = longestPendingDurationMillis;
            return this;
        }

        public Builder resourceShortageCount(long resourceShortageCount) {
            this.resourceShortageCount = resourceShortageCount;
            return this;
        }

        public Builder waitShortageCount(long waitShortageCount) {
            this.waitShortageCount = waitShortageCount;
            return this;
        }

        public Builder rejectShortageCount(long rejectShortageCount) {
            this.rejectShortageCount = rejectShortageCount;
            return this;
        }

        public Builder waitShortage(boolean waitShortage) {
            this.waitShortage = waitShortage;
            return this;
        }

        public Builder rejectShortage(boolean rejectShortage) {
            this.rejectShortage = rejectShortage;
            return this;
        }

        public Builder scaleInMetricsValid(boolean scaleInMetricsValid) {
            this.scaleInMetricsValid = scaleInMetricsValid;
            return this;
        }

        public AutoscalerMetricsSnapshot build() {
            return new AutoscalerMetricsSnapshot(this);
        }
    }
}
