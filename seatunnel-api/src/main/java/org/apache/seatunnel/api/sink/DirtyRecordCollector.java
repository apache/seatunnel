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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.Serializable;

/**
 * Base interface for dirty records collector. This interface is used to collect and record all
 * dirty data content and exception information.
 */
public interface DirtyRecordCollector extends Serializable {

    void collect(
            final int subTaskIndex,
            final Object dirtyRecord,
            final Throwable exception,
            final String errorMessage,
            final CatalogTable catalogTable);

    default void collect(
            final int subTaskIndex,
            final Object dirtyRecord,
            final Throwable exception,
            final String errorMessage) {
        collect(subTaskIndex, dirtyRecord, exception, errorMessage, null);
    }

    default void collect(
            final int subTaskIndex,
            final Object dirtyRecord,
            final Throwable exception,
            final CatalogTable catalogTable) {
        collect(subTaskIndex, dirtyRecord, exception, "", catalogTable);
    }

    default void collect(
            final int subTaskIndex, final Object dirtyRecord, final Throwable exception) {
        collect(subTaskIndex, dirtyRecord, exception, "", null);
    }

    default void init(Config config) throws Exception {}

    default void init(Config config, CatalogTable catalogTable) throws Exception {
        init(config);
    }

    default void collectFromUserRule(
            int subTaskIndex, Object record, String errorMessage, CatalogTable catalogTable) {
        collect(subTaskIndex, record, null, errorMessage, catalogTable);
    }

    default boolean validateAndCollectIfDirty(
            int subTaskIndex, SeaTunnelRow record, CatalogTable catalogTable) {
        return false;
    }

    default void close() throws Exception {}

    default long getDirtyRecordCount() {
        return 0L;
    }

    default void checkThreshold() throws Exception {}

    default void setDistributedCounter(DistributedCounter counter) {}

    default void incrementDistributedCounter() {}
}
