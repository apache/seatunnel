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
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    @Test
    public void testDefaultValueTypeConvertedToFieldType() throws IOException {
        // HOCON config parses "0.0" as Integer 0 (Typesafe Config renders it as 0), so the raw
        // defaultValue on the Column may be Integer even for a double field. The deserializer
        // must normalize it to the field type, otherwise downstream (de)serialization fails
        // with ClassCastException.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "score",
                            BasicType.DOUBLE_TYPE,
                            (Long) null,
                            false,
                            0,
                            "double field with Integer 0 default")
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.DOUBLE_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        SeaTunnelRow row = deserializationSchema.deserialize("{}".getBytes());
        assertEquals(0.0, row.getField(0)); // Integer 0 normalized to Double 0.0
    }

    @Test
    public void testDefaultValueWithMoreNumericAndPrimitiveTypes() throws IOException {
        // Cover boolean / long / float / decimal defaultValue, applied when the field is
        // missing or explicitly null, and kept as-is when a real value is present.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "flag", BasicType.BOOLEAN_TYPE, (Long) null, false, true, null),
                    PhysicalColumn.of("count", BasicType.LONG_TYPE, (Long) null, false, 100L, null),
                    PhysicalColumn.of(
                            "ratio", BasicType.FLOAT_TYPE, (Long) null, false, 1.5f, null),
                    PhysicalColumn.of(
                            "amount",
                            new DecimalType(10, 2),
                            (Long) null,
                            false,
                            new BigDecimal("10.50"),
                            null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"flag", "count", "ratio", "amount"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.BOOLEAN_TYPE,
                            BasicType.LONG_TYPE,
                            BasicType.FLOAT_TYPE,
                            new DecimalType(10, 2)
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Field missing -> defaultValue applied
        SeaTunnelRow rowMissing = deserializationSchema.deserialize("{}".getBytes());
        assertEquals(true, rowMissing.getField(0));
        assertEquals(100L, rowMissing.getField(1));
        assertEquals(1.5f, rowMissing.getField(2));
        // Compare numerically: the JSON round-trip may normalize the scale (10.50 -> 10.5)
        assertEquals(0, new BigDecimal("10.50").compareTo((BigDecimal) rowMissing.getField(3)));

        // Explicit null -> defaultValue applied
        SeaTunnelRow rowNull =
                deserializationSchema.deserialize(
                        "{\"flag\":null,\"count\":null,\"ratio\":null,\"amount\":null}".getBytes());
        assertEquals(true, rowNull.getField(0));
        assertEquals(100L, rowNull.getField(1));
        assertEquals(1.5f, rowNull.getField(2));
        assertEquals(0, new BigDecimal("10.50").compareTo((BigDecimal) rowNull.getField(3)));

        // Real values -> kept as-is
        SeaTunnelRow rowWithValue =
                deserializationSchema.deserialize(
                        "{\"flag\":false,\"count\":200,\"ratio\":2.5,\"amount\":99.99}".getBytes());
        assertEquals(false, rowWithValue.getField(0));
        assertEquals(200L, rowWithValue.getField(1));
        assertEquals(2.5f, rowWithValue.getField(2));
        assertEquals(0, new BigDecimal("99.99").compareTo((BigDecimal) rowWithValue.getField(3)));
    }

    @Test
    public void testDefaultValueWithDateAndTimestampTypes() throws IOException {
        // Cover date / timestamp defaultValue (configured as strings, matching HOCON config).
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "birthday",
                            LocalTimeType.LOCAL_DATE_TYPE,
                            (Long) null,
                            false,
                            "2024-01-01",
                            null),
                    PhysicalColumn.of(
                            "created_at",
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            (Long) null,
                            false,
                            "2024-01-01 12:30:45",
                            null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"birthday", "created_at"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Field missing -> defaultValue applied
        SeaTunnelRow rowMissing = deserializationSchema.deserialize("{}".getBytes());
        assertEquals(LocalDate.of(2024, 1, 1), rowMissing.getField(0));
        assertEquals(LocalDateTime.of(2024, 1, 1, 12, 30, 45), rowMissing.getField(1));

        // Explicit null -> defaultValue applied
        SeaTunnelRow rowNull =
                deserializationSchema.deserialize(
                        "{\"birthday\":null,\"created_at\":null}".getBytes());
        assertEquals(LocalDate.of(2024, 1, 1), rowNull.getField(0));
        assertEquals(LocalDateTime.of(2024, 1, 1, 12, 30, 45), rowNull.getField(1));

        // Real values -> kept as-is
        SeaTunnelRow rowWithValue =
                deserializationSchema.deserialize(
                        "{\"birthday\":\"2024-06-15\",\"created_at\":\"2024-06-15 08:00:00\"}"
                                .getBytes());
        assertEquals(LocalDate.of(2024, 6, 15), rowWithValue.getField(0));
        assertEquals(LocalDateTime.of(2024, 6, 15, 8, 0, 0), rowWithValue.getField(1));
    }

    @Test
    public void testDefaultValueWithDoubleNumberFormats() throws IOException {
        // Cover various numeric representations of a double defaultValue:
        // scientific notation, negative values, non-zero decimals, integer-valued doubles.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "scientific", BasicType.DOUBLE_TYPE, (Long) null, false, 1.5e3, null),
                    PhysicalColumn.of(
                            "negative", BasicType.DOUBLE_TYPE, (Long) null, false, -3.14, null),
                    PhysicalColumn.of(
                            "fraction", BasicType.DOUBLE_TYPE, (Long) null, false, 0.5, null),
                    PhysicalColumn.of("whole", BasicType.DOUBLE_TYPE, (Long) null, false, 5, null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"scientific", "negative", "fraction", "whole"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.DOUBLE_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.DOUBLE_TYPE,
                            BasicType.DOUBLE_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Field missing -> defaultValue applied, normalized to the field type
        SeaTunnelRow rowMissing = deserializationSchema.deserialize("{}".getBytes());
        assertEquals(1500.0, rowMissing.getField(0)); // 1.5e3
        assertEquals(-3.14, rowMissing.getField(1));
        assertEquals(0.5, rowMissing.getField(2));
        assertEquals(5.0, rowMissing.getField(3)); // Integer 5 normalized to Double

        // Explicit null -> defaultValue applied
        SeaTunnelRow rowNull =
                deserializationSchema.deserialize(
                        "{\"scientific\":null,\"negative\":null,\"fraction\":null,\"whole\":null}"
                                .getBytes());
        assertEquals(1500.0, rowNull.getField(0));
        assertEquals(-3.14, rowNull.getField(1));
        assertEquals(0.5, rowNull.getField(2));
        assertEquals(5.0, rowNull.getField(3));

        // Real values in various JSON number formats (incl. scientific notation) -> kept as-is
        SeaTunnelRow rowWithValue =
                deserializationSchema.deserialize(
                        "{\"scientific\":2e3,\"negative\":-1.25,\"fraction\":0.75,\"whole\":42}"
                                .getBytes());
        assertEquals(2000.0, rowWithValue.getField(0)); // JSON 2e3
        assertEquals(-1.25, rowWithValue.getField(1));
        assertEquals(0.75, rowWithValue.getField(2));
        assertEquals(42.0, rowWithValue.getField(3)); // JSON integer 42 -> Double
    }

    @Test
    public void testDefaultValueWithStringNumericValue() throws IOException {
        // HOCON config may carry a numeric defaultValue as a string (e.g. quoted "1.5e3").
        // The deserializer must still convert it to the double field type.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "score", BasicType.DOUBLE_TYPE, (Long) null, false, "1.5e3", null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.DOUBLE_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        SeaTunnelRow row = deserializationSchema.deserialize("{}".getBytes());
        assertEquals(1500.0, row.getField(0)); // "1.5e3" parsed to double
    }

    @Test
    public void testDefaultValueNotAppliedToNestedRowFields() throws IOException {
        // Top-level defaults must never leak into nested ROW fields: the nested row
        // converter resolves its own field indexes, so without scoping it would consult
        // the top-level columns array by position and write a wrong default (e.g. "18"
        // into address.city because both are index 0).
        SeaTunnelRowType addressType =
                new SeaTunnelRowType(
                        new String[] {"city", "zip"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.INT_TYPE
                        });

        Column[] columns =
                new Column[] {
                    PhysicalColumn.of("id", BasicType.INT_TYPE, (Long) null, false, 18, null),
                    PhysicalColumn.of("address", addressType, (Long) null, true, null, null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "address"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, addressType
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Nested missing field must stay null, not pick up the top-level default at index 0
        SeaTunnelRow row =
                deserializationSchema.deserialize(
                        "{\"id\":1,\"address\":{\"zip\":100}}".getBytes());
        assertEquals(1, row.getField(0));
        SeaTunnelRow address = (SeaTunnelRow) row.getField(1);
        assertNull(address.getField(0)); // address.city stays null (top-level default NOT applied)
        assertEquals(100, address.getField(1));
    }

    @Test
    public void testExplicitNullWithFailOnMissingField() throws IOException {
        // With failOnMissingField = true, an explicit JSON null must NOT be treated as
        // missing: without a default it keeps returning null (previous behavior), with a
        // default it applies the default; only a genuinely absent field throws.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of("age", BasicType.INT_TYPE, (Long) null, false, null, null),
                    PhysicalColumn.of("score", BasicType.DOUBLE_TYPE, (Long) null, false, 0.0, null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"age", "score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.INT_TYPE, BasicType.DOUBLE_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, true, false);

        // Explicit null without default -> null (not an error)
        SeaTunnelRow nullRow =
                deserializationSchema.deserialize("{\"age\":null,\"score\":null}".getBytes());
        assertNull(nullRow.getField(0));
        assertEquals(0.0, nullRow.getField(1)); // explicit null with default -> default

        // Genuinely missing field with failOnMissingField = true -> still throws
        Assertions.assertThrows(
                RuntimeException.class,
                () -> deserializationSchema.deserialize("{\"score\":1.0}".getBytes()));
    }

    @Test
    public void testMutableDefaultValueNotSharedAcrossRows() throws IOException {
        // ARRAY/MAP/BYTES defaults are converted per record so no single mutable
        // instance is shared between rows (in-place mutation by downstream stages
        // would otherwise corrupt every row that took the default).
        ArrayType<Integer[], Integer> tagsType = ArrayType.INT_ARRAY_TYPE;
        MapType<String, String> attrsType =
                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "tags", tagsType, (Long) null, false, Arrays.asList(1, 2), null),
                    PhysicalColumn.of(
                            "attrs",
                            attrsType,
                            (Long) null,
                            false,
                            Collections.singletonMap("k", "v"),
                            null),
                    PhysicalColumn.of(
                            "blob",
                            PrimitiveByteArrayType.INSTANCE,
                            (Long) null,
                            false,
                            new byte[] {1, 2},
                            null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"tags", "attrs", "blob"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            tagsType, attrsType, PrimitiveByteArrayType.INSTANCE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        SeaTunnelRow row1 = deserializationSchema.deserialize("{}".getBytes());
        SeaTunnelRow row2 = deserializationSchema.deserialize("{}".getBytes());
        Integer[] tags1 = (Integer[]) row1.getField(0);
        Integer[] tags2 = (Integer[]) row2.getField(0);
        Assertions.assertNotSame(tags1, tags2); // distinct instances per row
        Assertions.assertArrayEquals(new Integer[] {1, 2}, tags1);
        Assertions.assertArrayEquals(new Integer[] {1, 2}, tags2);

        Map<String, String> attrs1 = (Map<String, String>) row1.getField(1);
        Map<String, String> attrs2 = (Map<String, String>) row2.getField(1);
        Assertions.assertNotSame(attrs1, attrs2); // distinct instances per row
        Assertions.assertEquals("v", attrs1.get("k"));
        Assertions.assertEquals("v", attrs2.get("k"));

        byte[] blob1 = (byte[]) row1.getField(2);
        byte[] blob2 = (byte[]) row2.getField(2);
        Assertions.assertNotSame(blob1, blob2); // distinct instances per row
        Assertions.assertArrayEquals(new byte[] {1, 2}, blob1);
        Assertions.assertArrayEquals(new byte[] {1, 2}, blob2);
    }

    @Test
    public void testUnconvertibleDefaultFailsAtConstruction() {
        // A defaultValue that cannot be converted to the column type must fail at
        // converter construction (job start), regardless of ignoreParseErrors —
        // silently dropping it would reproduce the very nulls this fix removes.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "score", BasicType.DOUBLE_TYPE, (Long) null, false, "abc", null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"score"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.DOUBLE_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        Assertions.assertThrows(
                RuntimeException.class,
                () -> new JsonDeserializationSchema(catalogTable, false, false));
        Assertions.assertThrows(
                RuntimeException.class,
                () -> new JsonDeserializationSchema(catalogTable, false, true));
    }

    @Test
    public void testSerializableAfterDateDefaultValuePreComputation() throws Exception {
        // Construction-time default pre-computation for DATE/TIMESTAMP columns populates
        // fieldFormatterMap with DateTimeFormatter (not Serializable). The converter must
        // stay serializable (fieldFormatterMap is transient) so the job graph can be
        // shipped to workers, and must lazily rebuild the formatter cache after
        // deserialization.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "birthday",
                            LocalTimeType.LOCAL_DATE_TYPE,
                            (Long) null,
                            false,
                            "2024-01-01",
                            null),
                    PhysicalColumn.of(
                            "created_at",
                            LocalTimeType.LOCAL_DATE_TIME_TYPE,
                            (Long) null,
                            false,
                            "2024-01-01 12:30:45",
                            null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"birthday", "created_at"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            LocalTimeType.LOCAL_DATE_TYPE, LocalTimeType.LOCAL_DATE_TIME_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Round-trip through ObjectOutputStream: must not throw NotSerializableException
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(deserializationSchema);
        }
        JsonDeserializationSchema deserialized;
        try (ObjectInputStream ois =
                new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            deserialized = (JsonDeserializationSchema) ois.readObject();
        }

        // After deserialization the transient formatter cache is null; the deserializer
        // must lazily rebuild it and still apply the defaults correctly
        SeaTunnelRow row = deserialized.deserialize("{}".getBytes());
        assertEquals(LocalDate.of(2024, 1, 1), row.getField(0));
        assertEquals(LocalDateTime.of(2024, 1, 1, 12, 30, 45), row.getField(1));
    }

    @Test
    public void testPresentEmptyStringNotOverwrittenByDefault() throws IOException {
        // A field that is present with an empty string ("") is neither missing nor JSON
        // null, so it must keep the empty string instead of being replaced by the
        // configured default value.
        Column[] columns =
                new Column[] {
                    PhysicalColumn.of(
                            "status", BasicType.STRING_TYPE, (Long) null, false, "PENDING", null)
                };

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"status"},
                        new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                            BasicType.STRING_TYPE
                        });

        TableSchema tableSchema = TableSchema.builder().columns(Arrays.asList(columns)).build();
        TableIdentifier tableId = TableIdentifier.of("test", TablePath.of("test.test_table"));
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId, tableSchema, new HashMap<>(), new ArrayList<>(), "test table");

        JsonDeserializationSchema deserializationSchema =
                new JsonDeserializationSchema(catalogTable, false, false);

        // Present with empty string -> kept as-is (not the default)
        SeaTunnelRow row = deserializationSchema.deserialize("{\"status\":\"\"}".getBytes());
        assertEquals("", row.getField(0));

        // Missing field -> default still applies
        SeaTunnelRow missingRow = deserializationSchema.deserialize("{}".getBytes());
        assertEquals("PENDING", missingRow.getField(0));
    }
}
