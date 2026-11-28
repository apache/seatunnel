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

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Sink writer that always fails a task on its first record, so the pipeline reaches {@code FAILED}
 * and enters the engine's restore backoff.
 *
 * <p>It exists only as a deterministic failure fixture: no connector, format or transform semantics
 * are under test here. Everything the test needs to know about a run is recorded here, on the
 * engine threads, as {@link System#nanoTime()} timestamps:
 *
 * <ul>
 *   <li>{@link #OPENED} and the per-attempt entry of {@link #attemptOpenNanos(int)} count and time
 *       the sink writers the engine actually created. That is how the test tells "the authorized
 *       restore never deployed anything" apart from "another attempt was deployed and then tore
 *       down", and it is how the control measures the real retry delay from the writer open of
 *       attempt 2 instead of from the moment a poll happened to notice it.
 *   <li>The per-attempt entry of {@link #attemptFirstFailedWriteNanos(int)} is the instant that
 *       attempt's first write failed. The engine cannot start the retry backoff before that
 *       failure, so it is a lower bound for the real backoff start and the test measures the
 *       cancellation and retry latencies from it instead of from a later, arbitrary poll of the
 *       pipeline state.
 * </ul>
 *
 * <p>Both timestamps are recorded by the fixture on the engine side, so reading them late cannot
 * change them: a delayed observer reports the same delay as an observer that looked immediately.
 * The engine executes this sink in the same JVM as the test (in-JVM master/worker cluster), so the
 * static timestamps are comparable across the test thread and the task threads.
 *
 * <p>The other half of the fixture is the optional first-write gate ({@link #armFirstWriteGate()}):
 * when armed, the attempt-1 write announces itself, then blocks until the test calls {@link
 * #releaseFirstWrite()}. That handshake gives the test a controlled release, so the failure that
 * starts the backoff cannot happen while the test is not yet watching it, and the observation of
 * "the restore was authorized" never has to win a race against a transient pipeline state. The gate
 * is bounded and released from a {@code finally} block by the test, so it can never park an engine
 * task forever.
 *
 * <p>The fixture is process-wide static state on purpose (the sink instance is created per task by
 * the engine); {@link #reset()} restores a clean slate between jobs. See {@link
 * RetryBackoffFailingSinkFactory} for why this fixture needs a factory at all.
 */
@Slf4j
public class RetryBackoffFailingSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    /** Number of writers the engine has created across all attempts since the last reset. */
    public static final AtomicInteger OPENED = new AtomicInteger();

    /** How many attempts (initial run plus restores) the fixture keeps timestamps for. */
    private static final int MAX_RECORDED_ATTEMPTS = 8;

    /** {@link System#nanoTime()} at which the n-th attempt's writer was created, 0 when unset. */
    private static final AtomicLongArray OPEN_NANOS = new AtomicLongArray(MAX_RECORDED_ATTEMPTS);

    /** {@link System#nanoTime()} of the n-th attempt's first failed write, 0 when unset. */
    private static final AtomicLongArray FIRST_FAILED_WRITE_NANOS =
            new AtomicLongArray(MAX_RECORDED_ATTEMPTS);

    /** Whether the attempt-1 write waits for {@link #releaseFirstWrite()} before it fails. */
    private static final AtomicBoolean FIRST_WRITE_GATE_ARMED = new AtomicBoolean();

    /**
     * Upper bound on how long the gated write blocks the engine. The test releases it immediately,
     * so this only matters when the test itself dies, in which case failing the write is better
     * than parking the task thread forever.
     */
    private static final long FIRST_WRITE_GATE_TIMEOUT_SECONDS = 120L;

    private static volatile CountDownLatch firstWriteReached = new CountDownLatch(1);
    private static volatile CountDownLatch firstWriteReleased = new CountDownLatch(1);

    /** 0-based index of this writer among the writers created since the last {@link #reset()}. */
    private final int attemptIndex;

    public RetryBackoffFailingSinkWriter() {
        this.attemptIndex = OPENED.getAndIncrement();
        if (attemptIndex < MAX_RECORDED_ATTEMPTS) {
            OPEN_NANOS.set(attemptIndex, System.nanoTime());
        }
    }

    @Override
    public void write(SeaTunnelRow element) {
        if (attemptIndex == 0 && FIRST_WRITE_GATE_ARMED.get()) {
            // Let the test get ready before the failure that starts the backoff exists.
            awaitTestRelease();
        }
        // Only the first failure of an attempt matters: it is the earliest moment the pipeline can
        // begin failing, and every later write of the same attempt happens before the backoff too.
        recordFailedWrite();
        throw new IllegalStateException("RetryBackoffFail sink always fails on write");
    }

    @Override
    public void close() {}

    /**
     * Signals the test that the failing write is at hand, then waits for the test to let the engine
     * continue. The failure timestamp is taken after the release, so it is the real failure instant
     * and the cancel the test issues next is measured from it.
     */
    private void awaitTestRelease() {
        CountDownLatch reached = firstWriteReached;
        CountDownLatch released = firstWriteReleased;
        reached.countDown();
        try {
            if (!released.await(FIRST_WRITE_GATE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn(
                        "The test did not release the gated first write within {}s, failing it"
                                + " anyway",
                        FIRST_WRITE_GATE_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void recordFailedWrite() {
        if (attemptIndex < MAX_RECORDED_ATTEMPTS) {
            FIRST_FAILED_WRITE_NANOS.compareAndSet(attemptIndex, 0L, System.nanoTime());
        }
    }

    /**
     * Makes the attempt-1 write wait for {@link #releaseFirstWrite()} before it fails. Not armed by
     * default: the control job must fail immediately, only the cancelling job needs the handshake.
     */
    public static void armFirstWriteGate() {
        FIRST_WRITE_GATE_ARMED.set(true);
    }

    /**
     * Waits until the armed attempt-1 write reached the gate, i.e. the engine is about to fail the
     * attempt and start its backoff.
     *
     * @return {@code false} if the write did not reach the gate within the timeout
     */
    public static boolean awaitFirstWriteReached(long timeout, TimeUnit unit)
            throws InterruptedException {
        return firstWriteReached.await(timeout, unit);
    }

    /** Lets the gated attempt-1 write fail. Idempotent, so it is safe to call from a finally. */
    public static void releaseFirstWrite() {
        firstWriteReleased.countDown();
    }

    /** {@link System#nanoTime()} of the given 1-based attempt's writer creation, or 0 if unset. */
    public static long attemptOpenNanos(int attemptNumber) {
        return recordedTimestamp(OPEN_NANOS, attemptNumber);
    }

    /**
     * {@link System#nanoTime()} of the given 1-based attempt's first failed write, or 0 if that
     * attempt never failed a write.
     */
    public static long attemptFirstFailedWriteNanos(int attemptNumber) {
        return recordedTimestamp(FIRST_FAILED_WRITE_NANOS, attemptNumber);
    }

    private static long recordedTimestamp(AtomicLongArray timestamps, int attemptNumber) {
        if (attemptNumber < 1 || attemptNumber > MAX_RECORDED_ATTEMPTS) {
            throw new IllegalArgumentException(
                    "attemptNumber must be within 1.."
                            + MAX_RECORDED_ATTEMPTS
                            + " but was "
                            + attemptNumber);
        }
        return timestamps.get(attemptNumber - 1);
    }

    /**
     * Restores a clean slate for the next job: attempt numbering, timestamps and the first-write
     * gate. Call it before submitting a job whose attempts must be observed from attempt 1.
     */
    public static void reset() {
        FIRST_WRITE_GATE_ARMED.set(false);
        // Release a straggler still waiting on the previous latch before replacing it.
        firstWriteReleased.countDown();
        firstWriteReached = new CountDownLatch(1);
        firstWriteReleased = new CountDownLatch(1);
        for (int attempt = 0; attempt < MAX_RECORDED_ATTEMPTS; attempt++) {
            OPEN_NANOS.set(attempt, 0L);
            FIRST_FAILED_WRITE_NANOS.set(attempt, 0L);
        }
        OPENED.set(0);
    }
}
