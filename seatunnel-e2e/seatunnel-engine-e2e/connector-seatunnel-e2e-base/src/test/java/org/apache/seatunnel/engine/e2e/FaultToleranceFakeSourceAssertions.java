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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.common.utils.FileUtils;

import org.junit.jupiter.api.Assertions;

import java.util.concurrent.TimeUnit;

/**
 * Helper assertions for fault-tolerance tests backed by the FakeSource templates.
 *
 * <p>These restore scenarios use FakeSource with five splits per reader. If a failure happens after
 * split assignment but before the next completed checkpoint, the recovered job can replay whole
 * splits. The LocalFile sink output is therefore expected to stay split-aligned and contain at
 * least the logical row count, but its physical line count is not guaranteed to equal that logical
 * row count exactly.
 */
final class FaultToleranceFakeSourceAssertions {

    private static final int DEFAULT_FAKE_SOURCE_SPLIT_NUM = 5;
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 2_000L;
    private static final long DEFAULT_STABLE_WINDOW_MILLIS = 10_000L;

    private FaultToleranceFakeSourceAssertions() {}

    /**
     * Waits until restore output reaches the expected logical size and stops growing.
     *
     * @param outputDir LocalFile sink target directory
     * @param minExpectedRows logical rows that must be present after recovery
     * @param sourceRowNumPerParallelism FakeSource row.num configured per reader
     * @param timeoutMillis maximum wait time
     */
    static void assertOutputRecoveredAndStable(
            String outputDir,
            long minExpectedRows,
            long sourceRowNumPerParallelism,
            long timeoutMillis) {
        long splitRowCount = calculateSplitRowCount(sourceRowNumPerParallelism);
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        long previousLineCount = -1L;
        long stableSinceNanos = -1L;
        long finalLineCount = 0L;
        AssertionError lastAssertionError = null;

        while (System.nanoTime() < deadlineNanos) {
            finalLineCount = FileUtils.getFileLineNumberFromDir(outputDir);
            try {
                assertReplayAlignedOutput(finalLineCount, minExpectedRows, splitRowCount);
                lastAssertionError = null;
                long now = System.nanoTime();
                if (finalLineCount != previousLineCount) {
                    previousLineCount = finalLineCount;
                    stableSinceNanos = now;
                } else if (stableSinceNanos > 0L
                        && TimeUnit.NANOSECONDS.toMillis(now - stableSinceNanos)
                                >= DEFAULT_STABLE_WINDOW_MILLIS) {
                    return;
                }
            } catch (AssertionError assertionError) {
                lastAssertionError = assertionError;
                previousLineCount = finalLineCount;
                stableSinceNanos = -1L;
            }

            sleep(DEFAULT_POLL_INTERVAL_MILLIS);
        }

        if (lastAssertionError != null) {
            throw lastAssertionError;
        }
        Assertions.fail(
                String.format(
                        "Output under %s did not stabilize within %d ms, final line count: %d",
                        outputDir, timeoutMillis, finalLineCount));
    }

    private static long calculateSplitRowCount(long sourceRowNumPerParallelism) {
        return (long)
                Math.ceil((double) sourceRowNumPerParallelism / DEFAULT_FAKE_SOURCE_SPLIT_NUM);
    }

    private static void assertReplayAlignedOutput(
            long currentLineCount, long minExpectedRows, long splitRowCount) {
        Assertions.assertTrue(
                currentLineCount >= minExpectedRows,
                String.format(
                        "Expected at least %d rows after recovery, but found %d",
                        minExpectedRows, currentLineCount));
        Assertions.assertEquals(
                0L,
                currentLineCount % splitRowCount,
                String.format(
                        "Expected replayed output to stay aligned to split size %d, but found %d rows",
                        splitRowCount, currentLineCount));
    }

    private static void sleep(long sleepMillis) {
        try {
            // Intentional polling interval: the bounded stability loop must observe elapsed stable
            // time without continuously reading the sink directory.
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new AssertionError(
                    "Interrupted while waiting for FakeSource output to stabilize",
                    interruptedException);
        }
    }
}
