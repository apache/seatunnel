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

package org.apache.seatunnel.benchmark.connector.source;

import org.apache.seatunnel.api.source.SourceSplit;

/** One interleaved partition of the global open-loop event sequence. */
public final class BenchmarkSourceSplit implements SourceSplit {

    private static final long serialVersionUID = 1L;

    private final int subtaskId;
    private final int stride;
    private final long totalRows;
    private final long startEpochMillis;
    private final long ratePerSecond;
    private final int payloadSize;
    private final int emitBatchSize;
    private long nextSequence;

    public BenchmarkSourceSplit(
            int subtaskId,
            int stride,
            long totalRows,
            long startEpochMillis,
            long ratePerSecond,
            int payloadSize,
            int emitBatchSize,
            long nextSequence) {
        this.subtaskId = subtaskId;
        this.stride = stride;
        this.totalRows = totalRows;
        this.startEpochMillis = startEpochMillis;
        this.ratePerSecond = ratePerSecond;
        this.payloadSize = payloadSize;
        this.emitBatchSize = emitBatchSize;
        this.nextSequence = nextSequence;
    }

    @Override
    public String splitId() {
        return "benchmark-" + subtaskId;
    }

    public BenchmarkSourceSplit copy() {
        return new BenchmarkSourceSplit(
                subtaskId,
                stride,
                totalRows,
                startEpochMillis,
                ratePerSecond,
                payloadSize,
                emitBatchSize,
                nextSequence);
    }

    public int getSubtaskId() {
        return subtaskId;
    }

    public int getStride() {
        return stride;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getStartEpochMillis() {
        return startEpochMillis;
    }

    public long getRatePerSecond() {
        return ratePerSecond;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public int getEmitBatchSize() {
        return emitBatchSize;
    }

    public long getNextSequence() {
        return nextSequence;
    }

    public void advance() {
        nextSequence += stride;
    }
}
