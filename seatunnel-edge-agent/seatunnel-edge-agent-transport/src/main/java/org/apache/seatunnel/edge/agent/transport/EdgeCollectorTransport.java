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

package org.apache.seatunnel.edge.agent.transport;

import java.io.IOException;

public interface EdgeCollectorTransport extends AutoCloseable {

    /**
     * Ensures a live session exists (TCP connect + {@code __AUTH__}).
     *
     * <p>Called from bootstrap before the input reader opens. Idempotent when already connected.
     * Implementations may throw {@code EdgeSocketCollectorRejectedException} when another collector
     * is already connected ({@code REJECTED}); callers must not auto-reconnect in that case.
     */
    void open();

    /**
     * Sends one batch and returns when the source replies {@code RECEIVED}.
     *
     * <p>Delegates to {@code sendUntilReceived}.
     *
     * @param batchId EdgeSocket batch id (from WAL row {@code batch_id}, via {@code
     *     edge_agent_meta})
     * @param payload serialized wire payload (RAW line or PACKET JSON envelope)
     * @throws IOException on I/O or protocol errors
     * @throws InterruptedException if interrupted while waiting for backpressure sleep
     */
    default void send(long batchId, String payload) throws IOException, InterruptedException {
        sendUntilReceived(batchId, payload);
    }

    /**
     * Sends one batch and blocks until the ingress returns {@code RECEIVED}.
     *
     * <p>Called from the scheduler for each claimed WAL row. Handles {@code RETRY} and {@code
     * QUEUE_FULL:<ms>} with backoff/resend per EdgeSocket collector protocol. Does not wait for
     * engine checkpoint {@code __COMMIT__} / {@code ACK:<watermark>}.
     *
     * @param batchId monotonic WAL id (must match {@code __BATCH__:<batchId>:...})
     * @param payload wire-ready payload string
     * @throws IOException on auth failure, decrypt failure, or exhausted retries
     * @throws InterruptedException if interrupted during backoff
     */
    void sendUntilReceived(long batchId, String payload) throws IOException, InterruptedException;

    /**
     * Performs a lightweight TCP reachability check.
     *
     * <p>Connect-only probe; does not keep a session. Used for health checks without affecting the
     * active collector session.
     *
     * @return {@code true} if any configured endpoint accepts a connection
     * @throws IOException on unexpected probe errors
     */
    boolean probeReachable() throws IOException;

    /**
     * Closes the active session and releases sockets.
     *
     * <p>Called on graceful shutdown. Safe to call when not connected.
     *
     * @throws IOException if closing the socket fails
     */
    @Override
    void close();
}
