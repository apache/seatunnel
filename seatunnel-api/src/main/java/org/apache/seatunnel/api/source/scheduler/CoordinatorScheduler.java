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

package org.apache.seatunnel.api.source.scheduler;

import org.apache.seatunnel.api.annotation.Experimental;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

/**
 * Engine-owned scheduler for Source coordinator discovery and other blocking control work.
 *
 * <p>The API intentionally has no synchronous cross-thread call. Callers snapshot immutable input
 * on the coordinator event loop, execute it in {@link #callAsync}, and apply the result from the
 * result handler, which the engine runs on the same coordinator event loop.
 */
@Experimental
public interface CoordinatorScheduler {

    /**
     * Executes blocking or CPU-heavy work outside the coordinator event loop.
     *
     * <p>{@code callable} runs on a shared engine worker thread. It must not read or write
     * enumerator state: no connector fields, no collections the coordinator owns, nothing a
     * checkpoint can observe. Snapshot whatever it needs on the coordinator event loop before
     * submitting, and return everything it discovers as a value. {@code resultHandler} then runs on
     * the coordinator event loop and is the only place that may apply the result to enumerator
     * state.
     *
     * <p>Results from an obsolete coordinator epoch are discarded by the engine.
     */
    <T> Cancellable callAsync(
            AsyncTaskKey key,
            Callable<T> callable,
            BiConsumer<T, Throwable> resultHandler,
            AsyncTaskOptions options);

    /**
     * Schedules a non-blocking callback to run on the coordinator event loop after the delay.
     *
     * <p>The callback must only snapshot state, apply an async result, or submit work through
     * {@link #callAsync}. A pending timer with the same key is cancelled and replaced atomically
     * from the coordinator event loop.
     */
    Cancellable scheduleInCoordinatorThread(AsyncTaskKey key, Duration delay, Runnable task);
}
