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

package org.apache.seatunnel.api.table.operation.event;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.operation.TableOperationType;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Emitted when a captured source table is truncated. The table schema is unchanged. */
@Getter
@ToString
public class TruncateTableEvent implements TableOperationEvent {

    private static final long serialVersionUID = 1L;

    private final TableIdentifier tableIdentifier;
    private long createdTime = System.currentTimeMillis();
    /**
     * Filled by {@link org.apache.seatunnel.api.event.EventListener} when the event is reported
     * (same path as {@code SchemaChangeEvent}), not by the CDC resolver.
     */
    @Setter private String jobId;

    @Setter private String statement;
    @Setter private String sourceDialectName;

    public TruncateTableEvent(TableIdentifier tableIdentifier) {
        this.tableIdentifier = tableIdentifier;
    }

    public static TruncateTableEvent of(TableIdentifier tableIdentifier) {
        return new TruncateTableEvent(tableIdentifier);
    }

    @Override
    public TableIdentifier tableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public TableOperationType operationType() {
        return TableOperationType.TRUNCATE_TABLE;
    }

    @Override
    public EventType getEventType() {
        return EventType.TABLE_OPERATION_TRUNCATE;
    }
}
