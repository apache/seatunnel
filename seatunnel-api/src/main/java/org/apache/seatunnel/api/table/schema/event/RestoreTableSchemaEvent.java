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

package org.apache.seatunnel.api.table.schema.event;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.api.table.catalog.CatalogTable;

import lombok.ToString;

/**
 * Restores the runtime table schema from checkpoint state after a failover recovery.
 *
 * <p>Contract for every schema-change consumer (dispatchers, transforms, sinks): this event only
 * refreshes runtime schema state such as row types, converters, serializers and writers. It must
 * never be translated into physical DDL against the external system, because the original DDL was
 * already applied before the checkpoint this event is restored from. A consumer that does not
 * explicitly handle {@link EventType#SCHEMA_CHANGE_RESTORE} must treat it as a runtime schema
 * refresh only and must not re-execute any DDL for it.
 */
@ToString(callSuper = true)
public class RestoreTableSchemaEvent extends AlterTableEvent {

    public RestoreTableSchemaEvent(CatalogTable restoredTable) {
        super(
                java.util.Objects.requireNonNull(restoredTable, "restoredTable cannot be null")
                        .getTableId());
        setChangeAfter(restoredTable);
    }

    public CatalogTable getRestoredTable() {
        CatalogTable restoredTable = getChangeAfter();
        if (restoredTable == null) {
            throw new IllegalStateException(
                    "RestoreTableSchemaEvent requires changeAfter to be present.");
        }
        return restoredTable;
    }

    @Override
    public EventType getEventType() {
        return EventType.SCHEMA_CHANGE_RESTORE;
    }
}
