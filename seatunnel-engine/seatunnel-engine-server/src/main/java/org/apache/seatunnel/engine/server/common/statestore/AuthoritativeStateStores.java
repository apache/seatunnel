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

package org.apache.seatunnel.engine.server.common.statestore;

import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

/**
 * Bundle of authoritative control state that a new leader must trust during leader handoff.
 *
 * <p>This layer groups states that are likely to live on top of a consensus layer. The actual
 * storage backend may still be a local store, but the responsibility for deciding what is
 * authoritative belongs more strongly to this group.
 */
public interface AuthoritativeStateStores extends AutoCloseable {

    /**
     * Returns the checkpoint ID counter store.
     *
     * <p>The current key format is a string composed from {@code jobId + pipelineId}.
     *
     * @return checkpoint counter store
     */
    CounterStateStore<String> checkpointCounterStore();

    /**
     * Returns the store for engine-level row error counters.
     *
     * <p>The current key format is versioned and scoped by job, pipeline, action, stage and counter
     * name.
     *
     * @return error handler counter store
     */
    CounterStateStore<String> errorHandlerCounterStore();

    /**
     * Releases resources owned by authoritative stores.
     *
     * <p>Implementations that only wrap non-closeable stores may keep the default no-op behavior.
     */
    @Override
    default void close() {
        // no-op
    }
}
