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
public class AlterTableModifyColumnEvent extends AlterTableColumnEvent {
    private final Column column;
    private final boolean first;
    private Boolean typeChanged;
    private final String afterColumn;

    public AlterTableModifyColumnEvent(
            TableIdentifier tableIdentifier, Column column, boolean first, String afterColumn) {
        super(tableIdentifier);
        this.column = column;
        this.first = first;
        this.afterColumn = afterColumn;
    }

    public void setTypeChanged(boolean typeChanged) {
        this.typeChanged = typeChanged;
    }

    public static AlterTableModifyColumnEvent modifyFirst(
            TableIdentifier tableIdentifier, Column column) {
        return new AlterTableModifyColumnEvent(tableIdentifier, column, true, null);
    }

    public static AlterTableModifyColumnEvent modify(
            TableIdentifier tableIdentifier, Column column) {
        return new AlterTableModifyColumnEvent(tableIdentifier, column, false, null);
    }

    public static AlterTableModifyColumnEvent modifyAfter(
            TableIdentifier tableIdentifier, Column column, String afterColumn) {
        return new AlterTableModifyColumnEvent(tableIdentifier, column, false, afterColumn);
    }

    @Override
    public EventType getEventType() {
        return EventType.SCHEMA_CHANGE_MODIFY_COLUMN;
    }
}
