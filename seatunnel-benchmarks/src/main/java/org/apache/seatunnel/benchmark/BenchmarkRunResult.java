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

package org.apache.seatunnel.benchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Correctness and latency summary emitted by {@code BenchmarkSink}. */
public final class BenchmarkRunResult {

    private final long expectedRows;
    private final long processedRows;
    private final double throughputRowsPerSecond;
    private final long p99LatencyMillis;
    private final long checksum;
    private final boolean sustainable;

    private BenchmarkRunResult(
            long expectedRows,
            long processedRows,
            double throughputRowsPerSecond,
            long p99LatencyMillis,
            long checksum,
            boolean sustainable) {
        this.expectedRows = expectedRows;
        this.processedRows = processedRows;
        this.throughputRowsPerSecond = throughputRowsPerSecond;
        this.p99LatencyMillis = p99LatencyMillis;
        this.checksum = checksum;
        this.sustainable = sustainable;
    }

    public static BenchmarkRunResult read(Path resultFile) throws IOException {
        String json = new String(Files.readAllBytes(resultFile), StandardCharsets.UTF_8);
        return new BenchmarkRunResult(
                longValue(json, "expected_rows"),
                longValue(json, "processed_rows"),
                doubleValue(json, "throughput_rows_per_second"),
                longValue(json, "event_time_latency_p99_ms"),
                longValue(json, "checksum"),
                booleanValue(json, "sustainable"));
    }

    private static long longValue(String json, String key) {
        return Long.parseLong(value(json, key, "-?[0-9]+"));
    }

    private static double doubleValue(String json, String key) {
        return Double.parseDouble(value(json, key, "-?[0-9]+(?:\\.[0-9]+)?"));
    }

    private static boolean booleanValue(String json, String key) {
        return Boolean.parseBoolean(value(json, key, "true|false"));
    }

    private static String value(String json, String key, String valuePattern) {
        Pattern pattern =
                Pattern.compile(
                        "\\\"" + Pattern.quote(key) + "\\\"\\s*:\\s*(" + valuePattern + ")");
        Matcher matcher = pattern.matcher(json);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Missing benchmark result field: " + key);
        }
        return matcher.group(1);
    }

    public long getExpectedRows() {
        return expectedRows;
    }

    public long getProcessedRows() {
        return processedRows;
    }

    public double getThroughputRowsPerSecond() {
        return throughputRowsPerSecond;
    }

    public long getP99LatencyMillis() {
        return p99LatencyMillis;
    }

    public long getChecksum() {
        return checksum;
    }

    public boolean isSustainable() {
        return sustainable;
    }
}
