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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Black-hole sink that records whole-pipeline throughput and event-time latency. */
public final class BenchmarkSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private static final ConcurrentMap<String, ResultAccumulator> RUNS = new ConcurrentHashMap<>();

    private final String accumulatorKey;
    private final ResultAccumulator accumulator;
    private final long expectedRows;
    private final LatencyHistogram allLatencies;
    private final LatencyHistogram firstHalfLatencies;
    private final LatencyHistogram secondHalfLatencies;
    private long rowCount;
    private long checksum;
    private long firstReceivedMillis = Long.MAX_VALUE;
    private long lastReceivedMillis;
    private boolean closed;

    public BenchmarkSinkWriter(
            Context context,
            String resultPath,
            String runId,
            long expectedRows,
            long offeredRate,
            int payloadSize,
            int transformOperations,
            int maxTrackedLatencyMillis,
            long maxP99LatencyMillis,
            double maxLatencyGrowthRatio) {
        this.expectedRows = expectedRows;
        this.allLatencies = new LatencyHistogram(maxTrackedLatencyMillis);
        this.firstHalfLatencies = new LatencyHistogram(maxTrackedLatencyMillis);
        this.secondHalfLatencies = new LatencyHistogram(maxTrackedLatencyMillis);
        this.accumulatorKey = Paths.get(resultPath).toAbsolutePath() + "::" + runId;
        this.accumulator =
                RUNS.computeIfAbsent(
                        accumulatorKey,
                        ignored ->
                                new ResultAccumulator(
                                        context.getNumberOfParallelSubtasks(),
                                        resultPath,
                                        runId,
                                        expectedRows,
                                        offeredRate,
                                        payloadSize,
                                        transformOperations,
                                        maxTrackedLatencyMillis,
                                        maxP99LatencyMillis,
                                        maxLatencyGrowthRatio));
    }

    @Override
    public void write(SeaTunnelRow element) {
        long nowMillis = System.currentTimeMillis();
        long sequence = (Long) element.getField(0);
        long scheduledAtMillis = (Long) element.getField(1);
        long rowChecksum = (Long) element.getField(3);
        long latencyMillis = Math.max(0L, nowMillis - scheduledAtMillis);

        allLatencies.record(latencyMillis);
        if (sequence < expectedRows / 2L) {
            firstHalfLatencies.record(latencyMillis);
        } else {
            secondHalfLatencies.record(latencyMillis);
        }
        rowCount++;
        checksum += rowChecksum;
        firstReceivedMillis = Math.min(firstReceivedMillis, nowMillis);
        lastReceivedMillis = nowMillis;
    }

    @Override
    @Deprecated
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        boolean complete =
                accumulator.merge(
                        rowCount,
                        checksum,
                        firstReceivedMillis,
                        lastReceivedMillis,
                        allLatencies,
                        firstHalfLatencies,
                        secondHalfLatencies);
        if (complete) {
            try {
                accumulator.writeResult();
            } finally {
                RUNS.remove(accumulatorKey, accumulator);
            }
        }
    }

    private static final class ResultAccumulator {

        private final int expectedWriters;
        private final Path resultDirectory;
        private final String runId;
        private final long expectedRows;
        private final long offeredRate;
        private final int payloadSize;
        private final int transformOperations;
        private final long maxP99LatencyMillis;
        private final double maxLatencyGrowthRatio;
        private final LatencyHistogram allLatencies;
        private final LatencyHistogram firstHalfLatencies;
        private final LatencyHistogram secondHalfLatencies;
        private int closedWriters;
        private long rowCount;
        private long checksum;
        private long firstReceivedMillis = Long.MAX_VALUE;
        private long lastReceivedMillis;

        private ResultAccumulator(
                int expectedWriters,
                String resultPath,
                String runId,
                long expectedRows,
                long offeredRate,
                int payloadSize,
                int transformOperations,
                int maxTrackedLatencyMillis,
                long maxP99LatencyMillis,
                double maxLatencyGrowthRatio) {
            this.expectedWriters = expectedWriters;
            this.resultDirectory = Paths.get(resultPath);
            this.runId = runId;
            this.expectedRows = expectedRows;
            this.offeredRate = offeredRate;
            this.payloadSize = payloadSize;
            this.transformOperations = transformOperations;
            this.maxP99LatencyMillis = maxP99LatencyMillis;
            this.maxLatencyGrowthRatio = maxLatencyGrowthRatio;
            this.allLatencies = new LatencyHistogram(maxTrackedLatencyMillis);
            this.firstHalfLatencies = new LatencyHistogram(maxTrackedLatencyMillis);
            this.secondHalfLatencies = new LatencyHistogram(maxTrackedLatencyMillis);
        }

        private synchronized boolean merge(
                long writerRows,
                long writerChecksum,
                long writerFirstReceivedMillis,
                long writerLastReceivedMillis,
                LatencyHistogram writerAllLatencies,
                LatencyHistogram writerFirstHalfLatencies,
                LatencyHistogram writerSecondHalfLatencies) {
            rowCount += writerRows;
            checksum += writerChecksum;
            firstReceivedMillis = Math.min(firstReceivedMillis, writerFirstReceivedMillis);
            lastReceivedMillis = Math.max(lastReceivedMillis, writerLastReceivedMillis);
            allLatencies.merge(writerAllLatencies);
            firstHalfLatencies.merge(writerFirstHalfLatencies);
            secondHalfLatencies.merge(writerSecondHalfLatencies);
            closedWriters++;
            return closedWriters == expectedWriters;
        }

        private synchronized void writeResult() throws IOException {
            Files.createDirectories(resultDirectory);
            Path result = resultDirectory.resolve(runId + ".json");
            Path temporary = resultDirectory.resolve(runId + ".json.tmp");
            Files.write(temporary, toJson().getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(
                        temporary,
                        result,
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, result, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        private String toJson() {
            long durationMillis =
                    rowCount == 0 ? 0L : Math.max(1L, lastReceivedMillis - firstReceivedMillis);
            double throughput = durationMillis == 0 ? 0D : rowCount * 1_000D / durationMillis;
            PercentileResult p50 = allLatencies.percentile(0.50D);
            PercentileResult p95 = allLatencies.percentile(0.95D);
            PercentileResult p99 = allLatencies.percentile(0.99D);
            PercentileResult firstHalfP99 = firstHalfLatencies.percentile(0.99D);
            PercentileResult secondHalfP99 = secondHalfLatencies.percentile(0.99D);
            boolean percentilesClamped =
                    p50.isClamped()
                            || p95.isClamped()
                            || p99.isClamped()
                            || firstHalfP99.isClamped()
                            || secondHalfP99.isClamped();
            double growthRatio =
                    (secondHalfP99.getValueMillis() + 1D) / (firstHalfP99.getValueMillis() + 1D);
            boolean sustainable =
                    rowCount == expectedRows
                            && !percentilesClamped
                            && p99.getValueMillis() <= maxP99LatencyMillis
                            && growthRatio <= maxLatencyGrowthRatio;

            return String.format(
                    Locale.ROOT,
                    "{\n"
                            + "  \"run_id\": \"%s\",\n"
                            + "  \"offered_rate_rows_per_second\": %d,\n"
                            + "  \"parallelism\": %d,\n"
                            + "  \"payload_size\": %d,\n"
                            + "  \"transform_operations\": %d,\n"
                            + "  \"expected_rows\": %d,\n"
                            + "  \"processed_rows\": %d,\n"
                            + "  \"duration_seconds\": %.3f,\n"
                            + "  \"throughput_rows_per_second\": %.3f,\n"
                            + "  \"event_time_latency_p50_ms\": %d,\n"
                            + "  \"event_time_latency_p95_ms\": %d,\n"
                            + "  \"event_time_latency_p99_ms\": %d,\n"
                            + "  \"event_time_latency_max_ms\": %d,\n"
                            + "  \"first_half_p99_ms\": %d,\n"
                            + "  \"second_half_p99_ms\": %d,\n"
                            + "  \"latency_growth_ratio\": %.4f,\n"
                            + "  \"latency_percentiles_clamped\": %s,\n"
                            + "  \"latency_overflow_rows\": %d,\n"
                            + "  \"checksum\": %d,\n"
                            + "  \"sustainable\": %s\n"
                            + "}\n",
                    runId,
                    offeredRate,
                    expectedWriters,
                    payloadSize,
                    transformOperations,
                    expectedRows,
                    rowCount,
                    durationMillis / 1_000D,
                    throughput,
                    p50.getValueMillis(),
                    p95.getValueMillis(),
                    p99.getValueMillis(),
                    allLatencies.getMaximum(),
                    firstHalfP99.getValueMillis(),
                    secondHalfP99.getValueMillis(),
                    growthRatio,
                    percentilesClamped,
                    allLatencies.getOverflowCount(),
                    checksum,
                    sustainable);
        }
    }
}
