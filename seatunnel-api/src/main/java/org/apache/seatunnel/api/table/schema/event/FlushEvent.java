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

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Used for communication between SchemaEvolutionOperator and sink */
@Getter
@Setter
@ToString
public class FlushEvent implements SchemaChangeEvent {
    private final TableIdentifier tableIdentifier;
    @Getter @Setter private CatalogTable changeAfter;
    @Getter @Setter private String jobId;
    @Getter @Setter private String statement;
    @Getter @Setter protected String sourceDialectName;
    private final Long createdTime;
    private final SchemaChangeEvent schemaChangeEvent;
    private final EventType eventType = EventType.SCHEMA_CHANGE_FLUSH;

    public FlushEvent(SchemaChangeEvent schemaChangeEvent) {
        this.schemaChangeEvent = schemaChangeEvent;
        this.tableIdentifier = schemaChangeEvent.tableIdentifier();
        this.jobId = schemaChangeEvent.getJobId();
        this.createdTime = System.currentTimeMillis();
    }

    @Override
    public TableIdentifier tableIdentifier() {
        return tableIdentifier;
    }

    @Override
    public long getCreatedTime() {
        return createdTime;
    }

    @Override
    public EventType getEventType() {
        return eventType;
    }
}
