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

package org.apache.seatunnel.connectors.seatunnel.azureiothub;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the real writer's early rejection before native conversion can expand shared values.
 * Exact small payloads, numeric map keys and deep schemas guard serialization compatibility.
 */
class AzureIoTHubSinkWriterTest {
    @Test
    void stopsMapTraversalBeforeConvertingEveryEntry() throws Exception {
        TrackingMap values = new TrackingMap(16, repeat('x', 128));
        SeaTunnelRowType type =
                rowType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
        AzureIoTHubSinkWriter writer = writer(type, 512);
        try {
            assertThrows(
                    IOException.class, () -> writer.write(new SeaTunnelRow(new Object[] {values})));
            assertTrue(values.visited > 0);
            assertTrue(values.visited < 16, "The old whole-tree path consumes all entries first");
        } finally {
            writer.close();
        }
    }

    @Test
    void rejectsSharedWideRowsBeforeExpandingTheInputGraph() throws Exception {
        String[] names = new String[1000];
        SeaTunnelDataType<?>[] fields = new SeaTunnelDataType<?>[1000];
        for (int index = 0; index < names.length; index++) {
            names[index] = "field" + index;
            fields[index] = BasicType.STRING_TYPE;
        }
        SeaTunnelRowType nestedType = new SeaTunnelRowType(names, fields);
        Object[] values = new Object[1000];
        Arrays.fill(values, "v");
        TrackingRow shared = new TrackingRow(values);
        TrackingMap map = new TrackingMap(16, shared);
        AzureIoTHubSinkWriter writer =
                writer(rowType(new MapType<>(BasicType.STRING_TYPE, nestedType)), 512);
        try {
            assertThrows(
                    IOException.class, () -> writer.write(new SeaTunnelRow(new Object[] {map})));
            assertTrue(shared.visited > 0);
            assertTrue(shared.visited < 1000, "Reject within the first logical nested row");
            assertTrue(map.visited < 16);
        } finally {
            writer.close();
        }
    }

    @Test
    void preservesExactSmallNestedMapArrayBinaryAndUnicode() throws Exception {
        SeaTunnelRowType nestedType =
                new SeaTunnelRowType(
                        new String[] {"label", "items", "bytes"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.STRING_TYPE,
                            ArrayType.INT_ARRAY_TYPE,
                            PrimitiveByteArrayType.INSTANCE
                        });
        SeaTunnelRowType type = rowType(new MapType<>(new DecimalType(20, 2), nestedType));
        Map<BigDecimal, SeaTunnelRow> map = new LinkedHashMap<>();
        map.put(
                new BigDecimal("1E+4"),
                new SeaTunnelRow(
                        new Object[] {
                            "雪", new Integer[] {1, null}, new byte[] {0, (byte) 255, (byte) 128}
                        }));
        assertEquals(
                "{\"value\":{\"1E+4\":{\"label\":\"雪\",\"items\":[1,null],\"bytes\":\"AP+A\"}}}",
                json(type, map, 1024));
    }

    @Test
    void preservesCompactDecimalMapKeysAndLastNormalizedKeyWins() throws Exception {
        Map<BigDecimal, String> compact = new LinkedHashMap<>();
        compact.put(new BigDecimal("1E+1000000"), "v");
        assertEquals(
                "{\"value\":{\"1E+1000000\":\"v\"}}",
                json(
                        rowType(new MapType<>(new DecimalType(20, 2), BasicType.STRING_TYPE)),
                        compact,
                        128));
        Map<Object, String> collisions = new LinkedHashMap<>();
        collisions.put(1, "first");
        collisions.put(new BigDecimal("1"), "second");
        assertEquals(
                "{\"value\":{\"1\":\"second\"}}",
                json(
                        rowType(new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE)),
                        collisions,
                        128));
        assertThrows(
                IOException.class,
                () ->
                        new AzureIoTHubPayload(rowType(new DecimalType(20, 2)))
                                .json(
                                        new SeaTunnelRow(
                                                new Object[] {new BigDecimal("1E+1000000")}),
                                        128));
    }

    @Test
    void preservesDeepNarrowValuesAndNullableSharedSchemasWithoutRecursion() throws Exception {
        SeaTunnelDataType<?> type = BasicType.INT_TYPE;
        Object value = 7;
        for (int depth = 0; depth < 5000; depth++) {
            type = new ArrayType<>(Object[].class, type);
            value = new Object[] {value};
        }
        String encoded = json(rowType(type), value, 20000);
        assertEquals(10011, encoded.length());
        assertTrue(encoded.startsWith("{\"value\":[[["));
        assertTrue(encoded.endsWith("]]]}"));
        SeaTunnelDataType<?> shared = BasicType.STRING_TYPE;
        for (int depth = 0; depth < 8; depth++) {
            shared =
                    new SeaTunnelRowType(
                            new String[] {"left", "right"},
                            new SeaTunnelDataType<?>[] {shared, shared});
        }
        assertEquals("{\"value\":null}", json(rowType(shared), null, 64));
    }

    @Test
    void keepsNativeNumericRuntimeValuesAndBoundsExactEscapedOutput() throws Exception {
        assertEquals("{\"value\":7.00}", json(rowType(BasicType.INT_TYPE), "7.00", 128));
        assertEquals(
                "{\"value\":\"not-a-number\"}",
                json(rowType(BasicType.INT_TYPE), "not-a-number", 128));
        assertThrows(
                IOException.class,
                () ->
                        new AzureIoTHubPayload(rowType(BasicType.STRING_TYPE))
                                .json(new SeaTunnelRow(new Object[] {repeat('"', 40)}), 64));
    }

    @Test
    void rejectsUnsupportedPropertyValuesWithoutChangingAllowedPunctuation() {
        SeaTunnelRowType type = rowType(BasicType.STRING_TYPE);
        for (String invalid : Arrays.asList("room 1", "a/b", "east:1", " east ")) {
            Map<String, Object> options = options(512);
            Map<String, String> properties = new HashMap<>();
            properties.put("route", invalid);
            options.put("application_properties", properties);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new AzureIoTHubConfig(ReadonlyConfig.fromMap(options), type));
        }
        Map<String, Object> options = options(512);
        Map<String, String> properties = new HashMap<>();
        String valid = "value!#$%&'*+-.^_`|~";
        properties.put("Route", valid);
        options.put("application_properties", properties);
        assertEquals(
                valid,
                new AzureIoTHubConfig(ReadonlyConfig.fromMap(options), type)
                        .properties.get("route"));
    }

    private static AzureIoTHubSinkWriter writer(SeaTunnelRowType type, int limit) {
        return new AzureIoTHubSinkWriter(
                new AzureIoTHubConfig(ReadonlyConfig.fromMap(options(limit)), type), type);
    }

    private static Map<String, Object> options(int limit) {
        Map<String, Object> options = new HashMap<>();
        options.put("hub_url", "https://example.invalid");
        options.put("device_id", "Sensor");
        options.put("secret_key", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");
        options.put("max_payload_bytes", limit);
        return options;
    }

    private static SeaTunnelRowType rowType(SeaTunnelDataType<?> type) {
        return new SeaTunnelRowType(new String[] {"value"}, new SeaTunnelDataType<?>[] {type});
    }

    private static String json(SeaTunnelRowType type, Object value, int limit) throws IOException {
        return new String(
                new AzureIoTHubPayload(type).json(new SeaTunnelRow(new Object[] {value}), limit),
                StandardCharsets.UTF_8);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    /**
     * Counts actual conversion visits while lazily presenting distinct keys with one shared value.
     * This oracle distinguishes early bounded admission from a post-conversion size check.
     */
    private static final class TrackingMap extends AbstractMap<String, Object> {
        private final int count;
        private final Object value;
        private int visited;

        private TrackingMap(int count, Object value) {
            this.count = count;
            this.value = value;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<Entry<String, Object>>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<Entry<String, Object>>() {
                        private int index;

                        @Override
                        public boolean hasNext() {
                            return index < count;
                        }

                        @Override
                        public Entry<String, Object> next() {
                            visited++;
                            return new SimpleImmutableEntry<>("key" + index++, value);
                        }
                    };
                }

                @Override
                public int size() {
                    return count;
                }
            };
        }

        @Override
        public String toString() {
            throw new AssertionError("Rejected input must not be rendered in diagnostics");
        }
    }

    /**
     * Proves shared wide rows are rejected before all fields, rather than after full tree creation.
     * Diagnostic rendering fails deliberately because it would expand the rejected input again.
     */
    private static final class TrackingRow extends SeaTunnelRow {
        private int visited;

        private TrackingRow(Object[] values) {
            super(values);
        }

        @Override
        public Object getField(int index) {
            visited++;
            return super.getField(index);
        }

        @Override
        public String toString() {
            throw new AssertionError("Rejected row must not be rendered in diagnostics");
        }
    }
}
