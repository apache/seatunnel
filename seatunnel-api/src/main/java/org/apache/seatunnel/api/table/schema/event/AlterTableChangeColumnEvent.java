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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class AlterTableChangeColumnEvent extends AlterTableColumnEvent {
    private final Column column;
    private final boolean first;
    private final String afterColumn;
    private final String oldColumn;

    public AlterTableChangeColumnEvent(
            TableIdentifier tableIdentifier,
            String oldColumn,
            Column column,
            boolean first,
            String afterColumn) {
        super(tableIdentifier);
        this.oldColumn = oldColumn;
        this.column = column;
        this.first = first;
        this.afterColumn = afterColumn;
    }

    public static AlterTableChangeColumnEvent changeFirst(
            TableIdentifier tableIdentifier, String oldColumn, Column column) {
        return new AlterTableChangeColumnEvent(tableIdentifier, oldColumn, column, true, null);
    }

    public static AlterTableChangeColumnEvent change(
            TableIdentifier tableIdentifier, String oldColumn, Column column) {
        return new AlterTableChangeColumnEvent(tableIdentifier, oldColumn, column, false, null);
    }

    public static AlterTableChangeColumnEvent changeAfter(
            TableIdentifier tableIdentifier, String oldColumn, Column column, String afterColumn) {
        return new AlterTableChangeColumnEvent(
                tableIdentifier, oldColumn, column, false, afterColumn);
    }

    @Override
    public EventType getEventType() {
        return EventType.SCHEMA_CHANGE_CHANGE_COLUMN;
    }
}
