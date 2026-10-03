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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class AbstractSchemaTest {

    private static Column column(String name) {
        return PhysicalColumn.of(name, BasicType.STRING_TYPE, 1L, true, null, "");
    }

    @Test
    void testLookups() {
        TableSchema schema =
                TableSchema.builder()
                        .column(column("id"))
                        .column(column("name"))
                        .column(column("age"))
                        .build();

        Assertions.assertEquals(0, schema.indexOf("id"));
        Assertions.assertEquals(2, schema.indexOf("age"));
        Assertions.assertEquals(-1, schema.indexOf("missing"));

        Assertions.assertSame(schema.getColumns().get(1), schema.getColumn("name"));
        Assertions.assertTrue(schema.contains("id"));
        Assertions.assertFalse(schema.contains("missing"));
    }

    @Test
    void testDuplicateColumnNamesKeepFirstMatch() {
        // A schema should not contain duplicate names, but if it does the lookup must keep the
        // first occurrence, matching the previous linear scan semantics.
        List<Column> columns = new ArrayList<>();
        Column first = column("dup");
        columns.add(first);
        columns.add(column("other"));
        columns.add(column("dup"));
        TableSchema schema = new TableSchema(columns, null, null);

        Assertions.assertEquals(0, schema.indexOf("dup"));
        Assertions.assertSame(first, schema.getColumn("dup"));
    }

    @Test
    void testDefensivelyCopiesConstructorColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(column("id"));
        TableSchema schema = new TableSchema(columns, null, null);

        Assertions.assertEquals(0, schema.indexOf("id"));

        // Mutating the list passed to the constructor must not change the schema or its caches.
        columns.add(column("extra"));
        Assertions.assertEquals(-1, schema.indexOf("extra"));
        Assertions.assertEquals(1, schema.getColumns().size());
        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> schema.getColumns().add(column("another")));
    }
}
