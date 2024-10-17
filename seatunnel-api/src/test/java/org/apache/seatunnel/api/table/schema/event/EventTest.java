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
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EventTest {

    @Test
    public void testTableColumnEventInstanceOf() {
        AlterTableModifyColumnEvent modifyColumnEvent =
                AlterTableModifyColumnEvent.modify(
                        TableIdentifier.of("", TablePath.DEFAULT),
                        PhysicalColumn.builder()
                                .name("test")
                                .dataType(BasicType.STRING_TYPE)
                                .build());
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_MODIFY_COLUMN, getEventType(modifyColumnEvent));

        AlterTableChangeColumnEvent changeColumnEvent =
                AlterTableChangeColumnEvent.change(
                        TableIdentifier.of("", TablePath.DEFAULT),
                        "old",
                        PhysicalColumn.builder()
                                .name("test")
                                .dataType(BasicType.STRING_TYPE)
                                .build());
        Assertions.assertEquals(
                EventType.SCHEMA_CHANGE_CHANGE_COLUMN, getEventType(changeColumnEvent));

        AlterTableAddColumnEvent addColumnEvent =
                AlterTableAddColumnEvent.add(
                        TableIdentifier.of("", TablePath.DEFAULT),
                        PhysicalColumn.builder()
                                .name("test")
                                .dataType(BasicType.STRING_TYPE)
                                .build());
        Assertions.assertEquals(EventType.SCHEMA_CHANGE_ADD_COLUMN, getEventType(addColumnEvent));

        AlterTableDropColumnEvent dropColumnEvent =
                new AlterTableDropColumnEvent(TableIdentifier.of("", TablePath.DEFAULT), "test");
        Assertions.assertEquals(EventType.SCHEMA_CHANGE_DROP_COLUMN, getEventType(dropColumnEvent));
    }

    private EventType getEventType(AlterTableColumnEvent event) {
        if (event instanceof AlterTableAddColumnEvent) {
            return EventType.SCHEMA_CHANGE_ADD_COLUMN;
        } else if (event instanceof AlterTableDropColumnEvent) {
            return EventType.SCHEMA_CHANGE_DROP_COLUMN;
        } else if (event instanceof AlterTableModifyColumnEvent) {
            return EventType.SCHEMA_CHANGE_MODIFY_COLUMN;
        } else if (event instanceof AlterTableChangeColumnEvent) {
            return EventType.SCHEMA_CHANGE_CHANGE_COLUMN;
        }
        throw new UnsupportedOperationException(
                "Unsupported event type: " + event.getClass().getName());
    }
}
