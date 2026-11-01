/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.future;

import org.apache.seatunnel.engine.imap.storage.file.IMapFileStorage;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Completion handle for one WAL append published through the disruptor.
 *
 * <p>{@link #isDone()} reports completion, not success. Callers must read {@link #get()} (or the
 * timed variant) to learn whether the durable write succeeded.
 */
public class RequestFuture implements Future<Boolean> {

    private final CountDownLatch latch = new CountDownLatch(1);

    /**
     * Cap for {@link #get()}. {@link Future} requires the untimed overload, so it cannot be
     * removed; this stops a killed WAL worker or a never-dispatched event from blocking the caller
     * forever. Same default as {@code IMapFileStorage#DEFAULT_WRITE_DATA_TIMEOUT_MILLISECONDS}.
     * Production callers pass the configured timeout to {@link #get(long, TimeUnit)} instead.
     */
    private final long untimedGetTimeoutMillis;

    // Written in done() before countDown(); get() already observes it via the latch
    // happens-before. volatile keeps the result bit safely published across threads.
    private volatile boolean success = false;

    public RequestFuture() {
        this(IMapFileStorage.DEFAULT_WRITE_DATA_TIMEOUT_MILLISECONDS);
    }

    /**
     * @param untimedGetTimeoutMillis cap applied by {@link #get()}; must be positive. Timed {@link
     *     #get(long, TimeUnit)} uses the timeout its caller passes and ignores this value.
     */
    public RequestFuture(long untimedGetTimeoutMillis) {
        if (untimedGetTimeoutMillis <= 0L) {
            throw new IllegalArgumentException(
                    "untimedGetTimeoutMillis must be positive, got " + untimedGetTimeoutMillis);
        }
        this.untimedGetTimeoutMillis = untimedGetTimeoutMillis;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return latch.getCount() == 0L;
    }

    /**
     * Waits for the WAL append, capped by {@code writDataTimeoutMilliseconds}.
     *
     * <p>{@link Future} requires this untimed overload, so it cannot be removed. It is not an
     * unbounded block: expiry throws {@link TimeoutException} (wrapped in {@link
     * ExecutionException}, because this signature cannot throw {@link TimeoutException} directly).
     * A worker killed by an {@link Error}, or an event that was never dispatched, therefore cannot
     * hang the caller forever. Production callers use {@link #get(long, TimeUnit)} with the
     * configured write timeout instead of this overload.
     *
     * @throws ExecutionException if the wait reaches {@code writDataTimeoutMilliseconds} before
     *     completion; the cause is a {@link TimeoutException}
     */
    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        try {
            return get(untimedGetTimeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new ExecutionException(
                    "Timed out waiting for WAL append completion after "
                            + untimedGetTimeoutMillis
                            + " ms",
                    e);
        }
    }

    /**
     * Waits up to {@code timeout} for the WAL append to complete.
     *
     * <p>On expiry this throws {@link TimeoutException} instead of returning {@code false},
     * matching {@link Future#get(long, TimeUnit)}. A timeout does not cancel the in-flight append;
     * the future remains incomplete until {@link #done(boolean)} runs. Callers must treat timeout
     * as failure for the waiting key.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return whether the durable write succeeded
     * @throws TimeoutException if the wait times out before completion
     * @throws InterruptedException if the waiting thread is interrupted
     */
    @Override
    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        if (!latch.await(timeout, unit)) {
            throw new TimeoutException("Timed out waiting for WAL append completion");
        }
        return success;
    }

    public void done(boolean success) {
        this.success = success;
        latch.countDown();
    }
}
