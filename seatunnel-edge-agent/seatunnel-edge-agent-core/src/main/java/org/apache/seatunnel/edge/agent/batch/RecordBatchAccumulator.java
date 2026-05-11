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

package org.apache.seatunnel.edge.agent.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Accumulates polled payloads until {@code bulk-max-size} or {@code flush-interval-ms}, whichever
 * triggers first.
 */
public final class RecordBatchAccumulator {

    private final int bulkMaxSize;
    private final long flushIntervalMs;
    private final List<AccumulatedRecord> buffer = new ArrayList<>();
    private long windowStartMs;

    public RecordBatchAccumulator(int bulkMaxSize, long flushIntervalMs) {
        if (bulkMaxSize < 1) {
            throw new IllegalArgumentException("bulkMaxSize must be >= 1.");
        }
        if (flushIntervalMs < 1L) {
            throw new IllegalArgumentException("flushIntervalMs must be >= 1.");
        }
        this.bulkMaxSize = bulkMaxSize;
        this.flushIntervalMs = flushIntervalMs;
        this.windowStartMs = System.currentTimeMillis();
    }

    public void offer(String payload, String sourceInputId) {
        if (buffer.isEmpty()) {
            windowStartMs = System.currentTimeMillis();
        }
        buffer.add(new AccumulatedRecord(payload, sourceInputId));
    }

    /** Returns {@code true} when size threshold is reached. */
    public boolean shouldFlushBySize() {
        return buffer.size() >= bulkMaxSize;
    }

    /** Returns {@code true} when time threshold is reached with buffered data. */
    public boolean shouldFlushByTime(long nowMs) {
        return !buffer.isEmpty() && nowMs - windowStartMs >= flushIntervalMs;
    }

    /**
     * Returns and clears the buffer when either flush predicate matches; otherwise returns empty
     * list.
     */
    public List<AccumulatedRecord> drainIfReady(long nowMs) {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }
        if (!shouldFlushBySize() && !shouldFlushByTime(nowMs)) {
            return Collections.emptyList();
        }
        List<AccumulatedRecord> copy = new ArrayList<>(buffer);
        buffer.clear();
        windowStartMs = nowMs;
        return copy;
    }

    /** Forces a flush of everything buffered (used during shutdown). */
    public List<AccumulatedRecord> drainAll(long nowMs) {
        if (buffer.isEmpty()) {
            return Collections.emptyList();
        }
        List<AccumulatedRecord> copy = new ArrayList<>(buffer);
        buffer.clear();
        windowStartMs = nowMs;
        return copy;
    }
}
