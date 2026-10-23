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

package org.apache.seatunnel.trace.analyzer;

import org.apache.seatunnel.trace.analyzer.model.BottleneckPoint;
import org.apache.seatunnel.trace.analyzer.model.TraceEntry;
import org.apache.seatunnel.trace.analyzer.model.TraceRecord;
import org.apache.seatunnel.trace.analyzer.model.TraceStatistics;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Computes latency statistics and bottleneck candidates from parsed stain trace records. */
@Slf4j
public class TraceDataAggregator {

    /** Aggregates overall latency metrics plus the slowest traces from the parsed record set. */
    public TraceStatistics aggregate(List<TraceRecord> records) {
        if (records == null || records.isEmpty()) {
            return new TraceStatistics(0, 0, 0, 0, 0, 0, new HashMap<>(), new ArrayList<>());
        }

        long totalCount = records.size();
        List<Long> latencies =
                records.stream()
                        .map(TraceRecord::getE2ELatencyMs)
                        .filter(l -> l > 0)
                        .sorted()
                        .collect(Collectors.toList());

        double avgLatency =
                latencies.isEmpty()
                        ? 0
                        : latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long maxLatency = latencies.isEmpty() ? 0 : latencies.get(latencies.size() - 1);
        long minLatency = latencies.isEmpty() ? 0 : latencies.get(0);
        long p95Latency = percentile(latencies, 95);
        long p99Latency = percentile(latencies, 99);

        Map<String, Double> stageAvgDurations = calculateStageAvgDurations(records);

        List<TraceRecord> topSlowTraces =
                records.stream()
                        .sorted(Comparator.comparing(TraceRecord::getE2ELatencyMs).reversed())
                        .limit(10)
                        .collect(Collectors.toList());

        log.info(
                "Aggregated {} traces: avg={} ms, p95={} ms, p99={} ms, max={} ms, min={} ms",
                totalCount,
                String.format("%.2f", avgLatency),
                p95Latency,
                p99Latency,
                maxLatency,
                minLatency);

        return new TraceStatistics(
                totalCount,
                avgLatency,
                maxLatency,
                minLatency,
                p95Latency,
                p99Latency,
                stageAvgDurations,
                topSlowTraces);
    }

    /** Finds large gaps between consecutive stage entries and keeps the slowest transitions. */
    public List<BottleneckPoint> analyzeBottlenecks(List<TraceRecord> records) {
        List<BottleneckPoint> bottlenecks = new ArrayList<>();

        for (TraceRecord record : records) {
            List<TraceEntry> entries = record.getEntries();
            for (int i = 1; i < entries.size(); i++) {
                long gap = entries.get(i).getTimestampMs() - entries.get(i - 1).getTimestampMs();
                if (gap > 10) {
                    bottlenecks.add(
                            new BottleneckPoint(
                                    record.getTraceId(),
                                    entries.get(i - 1).getStageName(),
                                    entries.get(i).getStageName(),
                                    gap));
                }
            }
        }

        List<BottleneckPoint> topBottlenecks =
                bottlenecks.stream()
                        .sorted(Comparator.comparing(BottleneckPoint::getGapMs).reversed())
                        .limit(20)
                        .collect(Collectors.toList());

        log.info("Found {} bottleneck points (showing top 20)", bottlenecks.size());
        return topBottlenecks;
    }

    /**
     * Returns the value at the given percentile (1–100) from a pre-sorted list. Uses the
     * nearest-rank method: index = ceil(p/100 * n) - 1.
     */
    private static long percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) {
            return 0L;
        }
        int idx = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.min(idx, sorted.size() - 1));
    }

    private Map<String, Double> calculateStageAvgDurations(List<TraceRecord> records) {
        Map<String, List<Long>> stageDurations = new HashMap<>();

        for (TraceRecord record : records) {
            List<TraceEntry> entries = record.getEntries();
            for (int i = 1; i < entries.size(); i++) {
                String stageName = entries.get(i).getStageName();
                long duration =
                        entries.get(i).getTimestampMs() - entries.get(i - 1).getTimestampMs();
                stageDurations.computeIfAbsent(stageName, k -> new ArrayList<>()).add(duration);
            }
        }

        Map<String, Double> avgDurations = new HashMap<>();
        for (Map.Entry<String, List<Long>> entry : stageDurations.entrySet()) {
            double avg = entry.getValue().stream().mapToLong(Long::longValue).average().orElse(0);
            avgDurations.put(entry.getKey(), avg);
        }

        return avgDurations;
    }
}
