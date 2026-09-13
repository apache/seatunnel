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

public final class WorkerSampleSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int totalSamples;
    private final int validSamples;
    private final int missingSamples;
    private final int staleSamples;
    private final int futureSamples;
    private final MetricValue cpu;
    private final MetricValue jvmMemory;
    private final boolean scaleInMetricsValid;

    public WorkerSampleSummary(
            int totalSamples,
            int validSamples,
            int missingSamples,
            int staleSamples,
            int futureSamples,
            MetricValue cpu,
            MetricValue jvmMemory,
            boolean scaleInMetricsValid) {
        this.totalSamples = totalSamples;
        this.validSamples = validSamples;
        this.missingSamples = missingSamples;
        this.staleSamples = staleSamples;
        this.futureSamples = futureSamples;
        this.cpu = Objects.requireNonNull(cpu, "cpu");
        this.jvmMemory = Objects.requireNonNull(jvmMemory, "jvmMemory");
        this.scaleInMetricsValid = scaleInMetricsValid;
    }

    public int getTotalSamples() {
        return totalSamples;
    }

    public int getValidSamples() {
        return validSamples;
    }

    public int getMissingSamples() {
        return missingSamples;
    }

    public int getStaleSamples() {
        return staleSamples;
    }

    public int getFutureSamples() {
        return futureSamples;
    }

    public MetricValue getCpu() {
        return cpu;
    }

    public MetricValue getJvmMemory() {
        return jvmMemory;
    }

    public boolean isScaleInMetricsValid() {
        return scaleInMetricsValid;
    }
}
