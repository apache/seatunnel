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

package org.apache.seatunnel.api.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;

import java.util.List;

/**
 * A {@link Collector} is used to collect data from {@link SourceReader}.
 *
 * @param <T> data type.
 */
public interface Collector<T> {

    void collect(T record);

    default void markSchemaChangeBeforeCheckpoint() {}

    default void collect(SchemaChangeEvent event) {}

    /**
     * Restores the collector's local schema before records are emitted from restored source state.
     *
     * <p>This method only refreshes local runtime state. It must not emit schema change events to
     * downstream operators because they restore their own checkpoint state independently.
     *
     * <p>Only the Zeta engine collector restores this state today. Flink and Spark retain the
     * default no-op implementation until their translation-layer collectors support schema
     * restoration.
     */
    default void restoreSchema(List<CatalogTable> catalogTables) {}

    default void markSchemaChangeAfterCheckpoint() {}

    /**
     * Returns the checkpoint lock.
     *
     * @return The object to use as the lock
     */
    Object getCheckpointLock();

    default boolean isEmptyThisPollNext() {
        return false;
    }

    default void resetEmptyThisPollNext() {}
}
