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

package org.apache.seatunnel.benchmark.connector.sink;

/** Fixed-size millisecond histogram used without adding a benchmark runtime dependency. */
final class LatencyHistogram {

    private final int maxTrackedMillis;
    private final long[] counts;
    private long totalCount;
    private long maximum;

    LatencyHistogram(int maxTrackedMillis) {
        this.maxTrackedMillis = maxTrackedMillis;
        this.counts = new long[maxTrackedMillis + 2];
    }

    void record(long latencyMillis) {
        long nonNegativeLatency = Math.max(0L, latencyMillis);
        int bucket =
                nonNegativeLatency > maxTrackedMillis
                        ? maxTrackedMillis + 1
                        : (int) nonNegativeLatency;
        counts[bucket]++;
        totalCount++;
        maximum = Math.max(maximum, nonNegativeLatency);
    }

    void merge(LatencyHistogram other) {
        if (maxTrackedMillis != other.maxTrackedMillis) {
            throw new IllegalArgumentException("Cannot merge different latency histogram ranges");
        }
        for (int index = 0; index < counts.length; index++) {
            counts[index] += other.counts[index];
        }
        totalCount += other.totalCount;
        maximum = Math.max(maximum, other.maximum);
    }

    PercentileResult percentile(double percentile) {
        if (totalCount == 0) {
            return new PercentileResult(0L, false);
        }
        long rank = Math.max(1L, (long) Math.ceil(percentile * totalCount));
        long seen = 0L;
        for (int index = 0; index < counts.length; index++) {
            seen += counts[index];
            if (seen >= rank) {
                return new PercentileResult(index, index == counts.length - 1);
            }
        }
        throw new IllegalStateException("Percentile rank exceeds histogram count");
    }

    long getTotalCount() {
        return totalCount;
    }

    long getMaximum() {
        return maximum;
    }

    long getOverflowCount() {
        return counts[counts.length - 1];
    }
}
