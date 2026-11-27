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

package org.apache.seatunnel.lineage;

/** A transport-independent extension point for lineage event delivery. */
public interface LineageBackend {

    /** Returns the name used by {@link LineageConfig#transport()}. */
    String getName();

    /**
     * Emits one already-built lineage event.
     *
     * <p>May block the calling thread synchronously. HTTP implementations typically apply {@code
     * config.timeoutMs()} as per-phase inactivity limits (connection request, connect, and socket
     * read), not as one wall-clock deadline for the whole send. Retries and backoff sleeps add
     * further delay, so {@code (retryTimes + 1) * timeoutMs} is not a hard total abort bound.
     * Callers on a latency-sensitive thread should isolate delivery and treat these settings as
     * best-effort limits.
     *
     * <p>Delivery failures are not required to escape this method. Callers that must skip the rest
     * of a multi-output event set use {@link #tryEmit(LineageConfig, LineageEvent)}.
     */
    void emit(LineageConfig config, LineageEvent event) throws Exception;

    /**
     * Emits one event and reports whether it was delivered.
     *
     * <p>The default delegates to {@link #emit(LineageConfig, LineageEvent)} and reports success
     * when that call returns. A backend that swallows a failed send must override this and return
     * {@code false}, so a multi-output loop can stop after the first exhausted attempt instead of
     * paying the same retry cycle for every remaining output.
     *
     * @param config immutable lineage configuration
     * @param event event to send
     * @return {@code false} when the event was not delivered
     */
    default boolean tryEmit(LineageConfig config, LineageEvent event) throws Exception {
        emit(config, event);
        return true;
    }
}
