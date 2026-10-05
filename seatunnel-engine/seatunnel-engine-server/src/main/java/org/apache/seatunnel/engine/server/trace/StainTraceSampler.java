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

package org.apache.seatunnel.engine.server.trace;

import org.apache.seatunnel.api.common.metrics.Counter;

import java.util.concurrent.atomic.AtomicLong;

/** Generates trace ids under sampling-rate and per-worker budget limits. */
public class StainTraceSampler {
    private static final StainTraceBudget WORKER_BUDGET = new StainTraceBudget();

    private final boolean enabled;
    private final int sampleRate;
    private final int maxTracesPerSecondPerWorker;
    private final int maxEntriesPerTrace;

    private final AtomicLong sequence = new AtomicLong(0);

    private final Counter samplesGeneratedTotal;
    private final Counter budgetThrottledTotal;

    public StainTraceSampler(
            boolean enabled,
            int sampleRate,
            int maxTracesPerSecondPerWorker,
            int maxEntriesPerTrace,
            Counter samplesGeneratedTotal,
            Counter budgetThrottledTotal) {
        this.enabled = enabled;
        this.sampleRate = sampleRate;
        this.maxTracesPerSecondPerWorker = maxTracesPerSecondPerWorker;
        this.maxEntriesPerTrace = maxEntriesPerTrace;
        this.samplesGeneratedTotal = samplesGeneratedTotal;
        this.budgetThrottledTotal = budgetThrottledTotal;
    }

    public int getMaxEntriesPerTrace() {
        return maxEntriesPerTrace;
    }

    /**
     * Returns a deterministic trace id for the sampled row or {@link
     * StainTraceConstants#NO_TRACE_ID}.
     */
    public long tryGenerateTraceId(long taskId, long nowMs) {
        if (!enabled) {
            return StainTraceConstants.NO_TRACE_ID;
        }
        if (sampleRate <= 0) {
            return StainTraceConstants.NO_TRACE_ID;
        }
        long seq = sequence.incrementAndGet();
        if (seq % sampleRate != 0) {
            return StainTraceConstants.NO_TRACE_ID;
        }
        if (!WORKER_BUDGET.tryAcquire(maxTracesPerSecondPerWorker, nowMs)) {
            if (budgetThrottledTotal != null) {
                budgetThrottledTotal.inc();
            }
            return StainTraceConstants.NO_TRACE_ID;
        }
        if (samplesGeneratedTotal != null) {
            samplesGeneratedTotal.inc();
        }
        return mixTraceId(taskId, seq, nowMs);
    }

    private long mixTraceId(long taskId, long seq, long nowMs) {
        long x = taskId * 0x9E3779B97F4A7C15L;
        x ^= seq * 0xC2B2AE3D27D4EB4FL;
        x ^= nowMs;
        x ^= (x >>> 33);
        x *= 0xFF51AFD7ED558CCDL;
        x ^= (x >>> 33);
        return x & Long.MAX_VALUE;
    }
}
