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

package org.apache.seatunnel.edge.agent.connector;

import java.util.List;

public interface EdgeInputReader extends AutoCloseable {

    /**
     * Initializes the reader before the scheduler loop starts.
     *
     * <p>Called once from {@link
     * org.apache.seatunnel.edge.agent.starter.runtime.EdgeAgentRuntimeBootstrap} after transport
     * {@code open()} and before polling. Implementations should load resume positions from the
     * injected {@link EdgeSourcePositionStore} when provided at factory {@code create} time.
     *
     * @throws Exception if initialization fails (startup aborts)
     */
    default void open() throws Exception {}

    /**
     * Polls up to {@code maxRecords} new events from the input source.
     *
     * <p>Called on every scheduler iteration. An empty list means no new data right now; the
     * scheduler may idle-sleep. Implementations must be non-blocking or honor interrupt on long
     * waits.
     *
     * @param maxRecords maximum events to return in one poll
     * @return zero or more events; never {@code null}
     * @throws Exception on unrecoverable read errors (may fail the agent loop)
     */
    List<EdgeEvent> poll(int maxRecords) throws Exception;

    /**
     * Releases resources held by the reader.
     *
     * <p>Called from bootstrap or scheduler shutdown. Idempotent implementations are encouraged.
     *
     * @throws Exception if cleanup fails (may be combined with other close failures)
     */
    @Override
    default void close() throws Exception {}
}
