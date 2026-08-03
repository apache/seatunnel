/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Test for defaultValue support in JsonDeserializationSchema. */
public class JsonDefaultValueTest {

    @Test
    public void testDefaultValueWhenFieldMissing() throws IOException {
        // Create schema with defaultValue
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, null),
                    PhysicalColumn.of(
                            "age",
                            BasicType.INT_TYPE,
                            (Long) null,
                            false,
                            18,
                            "age with default 18"),
                    PhysicalColumn.of(
                            "score",
                            BasicType.DOUBLE_TYPE,
                            (Long) null,
                            false,
                            0.0,
                            "score with default 0.0")
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "age", "score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE, BasicType.DOUBLE_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Test 1: Field missing - should use defaultValue
        String jsonMissing = "{\"name\": \"Alice\"}";
        SeaTunnelRow rowMissing = deserializationSchema.deserialize(jsonMissing.getBytes());
        assertEquals("Alice", rowMissing.getField(0));
        assertEquals(18, rowMissing.getField(1)); // defaultValue
        assertEquals(0.0, rowMissing.getField(2)); // defaultValue

        // Test 2: Field is null - should use defaultValue
        String jsonNull = "{\"name\": \"Bob\", \"age\": null, \"score\": null}";
        SeaTunnelRow rowNull = deserializationSchema.deserialize(jsonNull.getBytes());
        assertEquals("Bob", rowNull.getField(0));
        assertEquals(18, rowNull.getField(1)); // defaultValue
        assertEquals(0.0, rowNull.getField(2)); // defaultValue

        // Test 3: Field has value - should use actual value
        String jsonWithValue = "{\"name\": \"Charlie\", \"age\": 25, \"score\": 95.5}";
        SeaTunnelRow rowWithValue = deserializationSchema.deserialize(jsonWithValue.getBytes());
        assertEquals("Charlie", rowWithValue.getField(0));
        assertEquals(25, rowWithValue.getField(1)); // actual value
        assertEquals(95.5, rowWithValue.getField(2)); // actual value
    }

    @Test
    public void testNoDefaultValueWhenFieldMissing() throws IOException {
        // Create schema without defaultValue
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of("name", BasicType.STRING_TYPE, (Long) null, true, null, null),
                    PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, true, null, null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"name", "age"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Field missing and no defaultValue - should be null
        String json = "{\"name\": \"David\"}";
        SeaTunnelRow row = deserializationSchema.deserialize(json.getBytes());
        assertEquals("David", row.getField(0));
        assertNull(row.getField(1)); // no defaultValue, should be null
    }

    @Test
    public void testDefaultValueWithStringType() throws IOException {
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of("id", BasicType.INT_TYPE, (Long) null, false, 0, null),
                    PhysicalColumn.of(
                            "status",
                            BasicType.STRING_TYPE,
                            (Long) null,
                            false,
                            "PENDING",
                            "status with default PENDING")
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "status"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.STRING_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        String json = "{\"id\": 123}";
        SeaTunnelRow row = deserializationSchema.deserialize(json.getBytes());
        assertEquals(123, row.getField(0));
        assertEquals("PENDING", row.getField(1)); // defaultValue
    }
}
