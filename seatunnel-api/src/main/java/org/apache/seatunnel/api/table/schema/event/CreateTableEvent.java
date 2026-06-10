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
import org.apache.seatunnel.api.table.catalog.TableIdentifier;

import lombok.ToString;

/**
 * Schema event that represents a newly created table entering the pipeline.
 *
 * <p>The event carries the full table schema after creation so downstream connectors can create the
 * physical target table before the first data row of the table arrives.
 */
@ToString(callSuper = true)
public class CreateTableEvent extends TableEvent {

    /** Creates a table-creation event and stores the full table schema as {@code changeAfter}. */
    public CreateTableEvent(TableIdentifier tableIdentifier, CatalogTable changeAfter) {
        super(tableIdentifier);
        setChangeAfter(changeAfter);
    }

    @Override
    public EventType getEventType() {
        return EventType.SCHEMA_CHANGE_CREATE_TABLE;
    }
}
