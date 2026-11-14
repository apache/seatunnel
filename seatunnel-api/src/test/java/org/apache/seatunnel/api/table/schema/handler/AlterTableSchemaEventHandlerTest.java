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

package org.apache.seatunnel.api.table.schema.handler;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AlterTableSchemaEventHandlerTest {

    private static final TableIdentifier TABLE = TableIdentifier.of("catalog", "db", "t");

    @Test
    void addColumnAfterExistingColumnKeepsItsPosition() {
        TableSchema result =
                new AlterTableSchemaEventHandler()
                        .reset(schema("a", "b"))
                        .apply(AlterTableAddColumnEvent.addAfter(TABLE, column("c"), "a"));

        Assertions.assertArrayEquals(new String[] {"a", "c", "b"}, result.getFieldNames());
    }

    /**
     * The event names the column it must follow. When the schema has no such column the event does
     * not describe this table, and inserting the column anywhere would silently misalign every
     * positional row-to-schema mapping downstream, so it is rejected.
     */
    @Test
    void addColumnAfterUnknownColumnIsRejected() {
        TableSchemaChangeEventHandler handler =
                new AlterTableSchemaEventHandler().reset(schema("a", "b"));

        SchemaEvolutionException failure =
                Assertions.assertThrows(
                        SchemaEvolutionException.class,
                        () ->
                                handler.apply(
                                        AlterTableAddColumnEvent.addAfter(
                                                TABLE, column("c"), "missing")));

        Assertions.assertTrue(failure.getMessage().contains("missing"), failure.getMessage());
    }

    private static PhysicalColumn column(String name) {
        return PhysicalColumn.of(name, BasicType.INT_TYPE, 0L, true, null, null);
    }

    private static TableSchema schema(String... names) {
        TableSchema.Builder builder = TableSchema.builder();
        for (String name : names) {
            builder.column(column(name));
        }
        return builder.build();
    }
}
