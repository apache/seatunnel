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

/**
 * SPI interface for sink writers that need to share resources (e.g., connection pools) across
 * multiple tables in a multi-table sink scenario.
 *
 * <p>The methods in this interface are called by {@link
 * org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter} during initialization in the
 * following order:
 *
 * <ol>
 *   <li>{@link #initMultiTableResourceManager(int, int)} is called once to create the shared
 *       resource manager.
 *   <li>{@link #setMultiTableResourceManager(MultiTableResourceManager, int)} is called on each
 *       per-table writer to inject the shared resource manager.
 * </ol>
 *
 * @param <T> the type of shared resource (e.g., a connection pool)
 */
public interface SupportResourceShare<T> {

    /**
     * Creates and returns a {@link MultiTableResourceManager} that holds the shared resource.
     *
     * <p>Called once per subtask by the {@link
     * org.apache.seatunnel.api.sink.multitablesink.MultiTableSinkWriter} constructor. The first
     * writer in the map is used to create the resource manager.
     *
     * @param tableSize the number of target tables managed by this sink
     * @param queueSize the number of parallel write queues (thread pool size = queueSize * 2)
     * @return a resource manager instance, or {@code null} if resource sharing is not needed
     */
    default MultiTableResourceManager<T> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        return null;
    }

    /**
     * Injects the shared {@link MultiTableResourceManager} into the implementing writer.
     *
     * <p>Called after {@link #initMultiTableResourceManager(int, int)} on each per-table writer.
     * Implementations should store the resource manager and use {@code queueIndex} to retrieve the
     * appropriate shared resource.
     *
     * @param multiTableResourceManager the shared resource manager created by {@link
     *     #initMultiTableResourceManager(int, int)}
     * @param queueIndex the index of the write queue this writer belongs to
     */
    default void setMultiTableResourceManager(
            MultiTableResourceManager<T> multiTableResourceManager, int queueIndex) {}
}
