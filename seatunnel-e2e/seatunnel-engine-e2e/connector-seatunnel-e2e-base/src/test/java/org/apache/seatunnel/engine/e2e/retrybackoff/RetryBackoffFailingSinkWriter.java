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

package org.apache.seatunnel.engine.e2e.retrybackoff;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sink writer that always fails a task on its first record, so the pipeline reaches {@code FAILED}
 * and enters the engine's restore backoff.
 *
 * <p>It exists only as a deterministic failure fixture: no connector, format or transform semantics
 * are under test here. {@link #OPENED} counts how many writers the engine actually created, which
 * is how the test tells "the authorized restore never deployed anything" apart from "another
 * attempt was deployed and then torn down".
 *
 * <p>{@link #firstFailedWriteNanos()} is the fixture's second observation point: the instant its
 * first write failed. The engine cannot start the retry backoff before that failure, so the test
 * uses it as a lower bound for the real backoff start and measures the cancellation and retry
 * latencies from it instead of from a later, arbitrary poll of the pipeline state. The engine
 * executes this sink in the same JVM as the test (in-JVM master/worker cluster), so the static
 * timestamps are comparable across the test thread and the task threads.
 */
public class RetryBackoffFailingSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    /** Number of writers the engine has created across all attempts of the job. */
    public static final AtomicInteger OPENED = new AtomicInteger();

    /**
     * {@link System#nanoTime()} of the first failed write since {@link #resetFailureSignal()}, or
     * {@code 0} while no write has failed yet.
     */
    private static final AtomicLong FIRST_FAILED_WRITE_NANOS = new AtomicLong();

    public RetryBackoffFailingSinkWriter() {
        OPENED.incrementAndGet();
    }

    @Override
    public void write(SeaTunnelRow element) {
        // Only the first failure of an attempt matters: it is the earliest moment the pipeline can
        // begin failing, and every later write of the same attempt happens before the backoff too.
        FIRST_FAILED_WRITE_NANOS.compareAndSet(0L, System.nanoTime());
        throw new IllegalStateException("RetryBackoffFail sink always fails on write");
    }

    @Override
    public void close() {}

    /** Start of the failure clock: the first failed write after the last reset, or {@code 0}. */
    public static long firstFailedWriteNanos() {
        return FIRST_FAILED_WRITE_NANOS.get();
    }

    /**
     * Restarts the failure clock, so a second job is anchored to its own failure, not a stale one.
     */
    public static void resetFailureSignal() {
        FIRST_FAILED_WRITE_NANOS.set(0L);
    }

    public static void reset() {
        OPENED.set(0);
        resetFailureSignal();
    }
}
