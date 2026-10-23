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

package org.apache.seatunnel.api.sink;

import java.util.Optional;

/**
 * Manages a shared resource (e.g., a connection pool) across multiple per-table sink writers in a
 * multi-table sink scenario.
 *
 * <p>Created by {@link SupportResourceShare#initMultiTableResourceManager(int, int)} and closed by
 * the multi-table sink runtime (e.g., {@link
 * org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter#close()} or {@link
 * org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkAggregatedCommitter#close()}) after
 * all participating components are closed.
 *
 * @param <T> the type of shared resource
 * @see SupportResourceShare
 */
public interface MultiTableResourceManager<T> {

    /**
     * Returns the shared resource held by this manager.
     *
     * <p>Implementations that manage a shared object (e.g., a connection pool) should return it
     * wrapped in an {@link Optional}. Return {@link Optional#empty()} only when the sink does not
     * require a shared resource.
     *
     * <p>This method must be thread-safe, as multiple per-table writers may call it concurrently
     * from different write queues.
     *
     * @return an {@link Optional} containing the shared resource, or {@link Optional#empty()} if no
     *     shared resource is needed
     */
    default Optional<T> getSharedResource() {
        return Optional.empty();
    }

    /**
     * Releases the shared resource held by this manager.
     *
     * <p>Called by the multi-table sink runtime (e.g., {@link
     * org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter#close()} or {@link
     * org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkAggregatedCommitter#close()})
     * after all participating components have been closed. Implementations should release pooled
     * connections or other resources here.
     */
    default void close() {}
}
