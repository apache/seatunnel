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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link TraceDataAggregator}. */
class TraceDataAggregatorTest {

    private final TraceDataAggregator aggregator = new TraceDataAggregator();

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    /**
     * Build a TraceRecord with exactly two entries: SOURCE_EMIT at startMs, SINK_WRITE_DONE at
     * endMs.
     */
    private TraceRecord record(long startMs, long endMs) {
        List<TraceEntry> entries =
                Arrays.asList(
                        new TraceEntry(1, 1L, startMs, "SOURCE_EMIT"),
                        new TraceEntry(6, 2L, endMs, "SINK_WRITE_DONE"));
        return new TraceRecord(startMs, 1L, "job1", "table1", startMs, entries);
    }

    // -----------------------------------------------------------------------
    // aggregate – basic stats
    // -----------------------------------------------------------------------

    @Test
    void testAggregateEmptyReturnsZeroStats() {
        TraceStatistics stats = aggregator.aggregate(Collections.emptyList());

        assertEquals(0, stats.getTotalCount());
        assertEquals(0.0, stats.getAvgLatencyMs(), 0.001);
        assertEquals(0, stats.getMaxLatencyMs());
        assertEquals(0, stats.getMinLatencyMs());
        assertEquals(0, stats.getP95LatencyMs());
        assertEquals(0, stats.getP99LatencyMs());
        assertTrue(stats.getTopSlowTraces().isEmpty());
    }

    @Test
    void testAggregateSingleRecord() {
        // E2E latency = 200 – 100 = 100 ms
        TraceStatistics stats = aggregator.aggregate(Collections.singletonList(record(100L, 200L)));

        assertEquals(1, stats.getTotalCount());
        assertEquals(100.0, stats.getAvgLatencyMs(), 0.001);
        assertEquals(100, stats.getMaxLatencyMs());
        assertEquals(100, stats.getMinLatencyMs());
        assertEquals(100, stats.getP95LatencyMs());
        assertEquals(100, stats.getP99LatencyMs());
        assertEquals(1, stats.getTopSlowTraces().size());
    }

    @Test
    void testAggregateMultipleRecords() {
        // Latencies: 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 ms
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            records.add(record(0L, i * 10L));
        }

        TraceStatistics stats = aggregator.aggregate(records);

        assertEquals(10, stats.getTotalCount());
        assertEquals(55.0, stats.getAvgLatencyMs(), 0.001);
        assertEquals(100, stats.getMaxLatencyMs());
        assertEquals(10, stats.getMinLatencyMs());

        // P95 of [10,20,...,100]: ceil(0.95*10)-1 = 9  → 100 ms
        assertEquals(100, stats.getP95LatencyMs());
        // P99 of [10,20,...,100]: ceil(0.99*10)-1 = 9  → 100 ms
        assertEquals(100, stats.getP99LatencyMs());
    }

    @Test
    void testAggregateP95P99LargerSample() {
        // 100 records with latencies 1..100 ms
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            records.add(record(0L, i));
        }

        TraceStatistics stats = aggregator.aggregate(records);

        assertEquals(100, stats.getTotalCount());
        // P95: ceil(0.95 * 100) - 1 = 94  → latency[94] = 95 ms
        assertEquals(95, stats.getP95LatencyMs());
        // P99: ceil(0.99 * 100) - 1 = 98  → latency[98] = 99 ms
        assertEquals(99, stats.getP99LatencyMs());
    }

    @Test
    void testAggregateRecordsWithZeroLatencyAreExcluded() {
        // Single-entry records produce latency = 0 and should be ignored in the stats
        List<TraceEntry> singleEntry =
                Collections.singletonList(new TraceEntry(1, 1L, 1000L, "SOURCE_EMIT"));
        TraceRecord zeroLatency = new TraceRecord(1L, 1L, "job1", "tbl", 1000L, singleEntry);
        TraceRecord valid = record(0L, 50L);

        TraceStatistics stats = aggregator.aggregate(Arrays.asList(zeroLatency, valid));

        // totalCount includes all records
        assertEquals(2, stats.getTotalCount());
        // but latency stats are computed only from records with latency > 0
        assertEquals(50.0, stats.getAvgLatencyMs(), 0.001);
        assertEquals(50, stats.getMaxLatencyMs());
        assertEquals(50, stats.getMinLatencyMs());
    }

    @Test
    void testAggregateTopSlowTracesLimit() {
        // 15 records – topSlowTraces should be capped at 10
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 1; i <= 15; i++) {
            records.add(record(0L, i * 10L));
        }

        TraceStatistics stats = aggregator.aggregate(records);

        assertEquals(10, stats.getTopSlowTraces().size());
        // Slowest should be first
        assertEquals(150, stats.getTopSlowTraces().get(0).getE2ELatencyMs());
    }

    // -----------------------------------------------------------------------
    // analyzeBottlenecks
    // -----------------------------------------------------------------------

    @Test
    void testAnalyzeBottlenecksEmptyList() {
        List<BottleneckPoint> bottlenecks = aggregator.analyzeBottlenecks(Collections.emptyList());
        assertTrue(bottlenecks.isEmpty());
    }

    @Test
    void testAnalyzeBottlenecksGapBelowThresholdIsIgnored() {
        // Gap = 1 ms (< 10 ms threshold)
        TraceRecord r = record(1000L, 1001L);
        List<BottleneckPoint> bottlenecks =
                aggregator.analyzeBottlenecks(Collections.singletonList(r));
        assertTrue(bottlenecks.isEmpty());
    }

    @Test
    void testAnalyzeBottlenecksGapAboveThreshold() {
        // Gap = 500 ms (> 10 ms threshold)
        TraceRecord r = record(1000L, 1500L);
        List<BottleneckPoint> bottlenecks =
                aggregator.analyzeBottlenecks(Collections.singletonList(r));

        assertEquals(1, bottlenecks.size());
        BottleneckPoint bp = bottlenecks.get(0);
        assertEquals("SOURCE_EMIT", bp.getFromStage());
        assertEquals("SINK_WRITE_DONE", bp.getToStage());
        assertEquals(500L, bp.getGapMs());
    }

    @Test
    void testAnalyzeBottlenecksTopNLimit() {
        // Create 25 records each with one bottleneck gap of increasing size
        List<TraceRecord> records = new ArrayList<>();
        for (int i = 1; i <= 25; i++) {
            records.add(record(0L, i * 100L)); // gaps: 100, 200, ..., 2500 ms
        }

        List<BottleneckPoint> bottlenecks = aggregator.analyzeBottlenecks(records);

        // Result is limited to top 20 and sorted descending by gap
        assertEquals(20, bottlenecks.size());
        assertTrue(bottlenecks.get(0).getGapMs() >= bottlenecks.get(1).getGapMs());
    }

    @Test
    void testAnalyzeBottlenecksMultipleGapsInOneRecord() {
        // Three-entry record: two inter-stage gaps
        List<TraceEntry> entries =
                Arrays.asList(
                        new TraceEntry(1, 1L, 0L, "SOURCE_EMIT"),
                        new TraceEntry(2, 1L, 500L, "QUEUE_IN"), // gap 500 ms
                        new TraceEntry(6, 2L, 600L, "SINK_WRITE_DONE") // gap 100 ms
                        );
        TraceRecord r = new TraceRecord(1L, 1L, "job1", "tbl", 0L, entries);

        List<BottleneckPoint> bottlenecks =
                aggregator.analyzeBottlenecks(Collections.singletonList(r));

        assertEquals(2, bottlenecks.size());
        // Sorted descending: 500 ms first
        assertEquals(500L, bottlenecks.get(0).getGapMs());
        assertEquals(100L, bottlenecks.get(1).getGapMs());
    }
}
