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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.util.Optional;

/** The Sink Connector Writer which support multi table should implement this interface */
public interface SupportMultiTableSinkWriter<T> extends SupportResourceShare<T> {

    /**
     * The primary key index of the table in SeaTunnelRow, use it to make sure the same key value
     * will be written to the same sink writer
     */
    default Optional<Integer> primaryKey() {
        return Optional.empty();
    }

    /**
     * Returns whether the writer can create a new per-table writer during runtime.
     *
     * <p>This is required when a multi-table source starts emitting a table that was not part of
     * the startup sink writer map.
     */
    default boolean supportsNewlyCreatedTable() {
        return false;
    }

    /**
     * Creates a sink writer for a newly discovered upstream table.
     *
     * <p>The provided {@link CatalogTable} is the upstream logical table. Implementations may map
     * it to a different physical target table using their own configuration rules.
     */
    default SinkWriter<SeaTunnelRow, ?, ?> createSinkWriter(
            CatalogTable catalogTable, SinkWriter.Context context) throws IOException {
        throw new UnsupportedOperationException(
                String.format(
                        "Sink writer %s does not support runtime newly created tables",
                        getClass().getName()));
    }
}
