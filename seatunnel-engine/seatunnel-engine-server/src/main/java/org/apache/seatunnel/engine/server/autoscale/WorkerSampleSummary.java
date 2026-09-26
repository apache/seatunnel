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

/** Summarizes the freshness and aggregate values of the latest samples from registered workers. */
public final class WorkerSampleSummary implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Number of registered workers considered when building this summary. */
    private final int totalSamples;

    /** Number of worker samples that are present, fresh, and valid for evaluation. */
    private final int validSamples;

    /** Number of registered workers without an accepted sample. */
    private final int missingSamples;

    /** Number of samples older than the configured freshness window. */
    private final int staleSamples;

    /** Number of samples whose event time is beyond the allowed future timestamp tolerance. */
    private final int futureSamples;

    /** Aggregated CPU utilization of valid worker samples. */
    private final MetricValue cpu;

    /** Aggregated JVM memory utilization of valid worker samples. */
    private final MetricValue jvmMemory;

    /** Whether every current worker has a complete and valid metric sample. */
    private final boolean allWorkerMetricsValid;

    /**
     * Creates a summary of the latest worker samples.
     *
     * @param totalSamples number of registered workers considered
     * @param validSamples number of present, fresh, and valid samples
     * @param missingSamples number of registered workers without an accepted sample
     * @param staleSamples number of samples older than the freshness window
     * @param futureSamples number of samples whose event time exceeds the future timestamp
     *     tolerance
     * @param cpu aggregated CPU utilization of valid samples
     * @param jvmMemory aggregated JVM memory utilization of valid samples
     * @param allWorkerMetricsValid whether every current worker has a complete and valid sample
     */
    public WorkerSampleSummary(
            int totalSamples,
            int validSamples,
            int missingSamples,
            int staleSamples,
            int futureSamples,
            MetricValue cpu,
            MetricValue jvmMemory,
            boolean allWorkerMetricsValid) {
        this.totalSamples = totalSamples;
        this.validSamples = validSamples;
        this.missingSamples = missingSamples;
        this.staleSamples = staleSamples;
        this.futureSamples = futureSamples;
        this.cpu = Objects.requireNonNull(cpu, "cpu");
        this.jvmMemory = Objects.requireNonNull(jvmMemory, "jvmMemory");
        this.allWorkerMetricsValid = allWorkerMetricsValid;
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

    public boolean isAllWorkerMetricsValid() {
        return allWorkerMetricsValid;
    }
}
