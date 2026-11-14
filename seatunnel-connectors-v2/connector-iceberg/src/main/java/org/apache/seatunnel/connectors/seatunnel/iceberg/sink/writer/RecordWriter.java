/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.List;
import java.util.Optional;

public interface RecordWriter extends Cloneable {

    void write(SeaTunnelRow seaTunnelRow, SeaTunnelRowType rowType);

    void applySchemaChange(SeaTunnelRowType afterRowType, SchemaChangeEvent event);

    /**
     * Whether this writer applies deletes and upserts by key (a delta writer). Then all changes of
     * one key must reach the same writer, in order.
     */
    default boolean writesDeltas() {
        return true;
    }

    /**
     * Returns the position in {@code rowType} of a key column the writer applies changes by, for
     * routing rows of one key to one writer. Empty when there is none.
     */
    default Optional<Integer> routingKeyIndex(SeaTunnelRowType rowType) {
        return Optional.empty();
    }

    default List<WriteResult> complete() {
        return ImmutableList.of();
    }

    default void close() {}
}
