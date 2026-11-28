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

package org.apache.seatunnel.connectors.seatunnel.websocket.server;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.VectorType;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.format.text.TextDeserializationSchema;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks queue admission and real typed decoding. Small tracking schemas expose early expansion
 * rejection without recreating production-scale memory pressure or requiring a running server.
 */
class WebSocketServerQueueTest {
    @Test
    void checksCountAndByteLimitsAtomicallyIncludingEmptyMessages() {
        WebSocketServer.MessageQueue queue = new WebSocketServer.MessageQueue(2, 3);
        WebSocketServer.Message first = new WebSocketServer.Message(false, new byte[2]);
        WebSocketServer.Message empty = new WebSocketServer.Message(true, new byte[0]);
        WebSocketServer.Message full = new WebSocketServer.Message(false, new byte[3]);

        assertTrue(queue.offer(first));
        assertFalse(queue.offer(full));
        assertTrue(queue.offer(empty));
        assertFalse(queue.offer(empty));
        assertSame(first, queue.poll());
        assertTrue(queue.offer(full));
        assertSame(empty, queue.poll());
        assertSame(full, queue.poll());
        assertNull(queue.poll());

        assertTrue(queue.offer(first));
        queue.close();
        assertFalse(queue.offer(empty));
        assertNull(queue.poll());
    }

    @Test
    void stopsSparseJsonRowsBeforeTheFullMapExpansion() throws Exception {
        TrackingRow wide = new TrackingRow(128);
        CatalogTable table = table(new String[] {"m"}, new MapType<>(BasicType.STRING_TYPE, wide));
        WebSocketServerDecoder decoder = new WebSocketServerDecoder(table, "json", "|", 768);
        wide.visited = 0;
        AtomicInteger lookedUp = new AtomicInteger();
        ObjectNode input = new ObjectMapper().createObjectNode();
        ObjectNode map = input.putObject("m");
        for (int i = 0; i < 16; i++) {
            map.set("k" + i, new TrackingObjectNode(JsonNodeFactory.instance, lookedUp));
        }
        assertThrows(IllegalArgumentException.class, () -> decoder.decodeJson(input));
        assertTrue(
                wide.visited > 0 && wide.visited < 128,
                "admission must stop before even the first full wide row is traversed");
        assertTrue(
                lookedUp.get() > 0 && lookedUp.get() < 128,
                "both native and bounded ROW converters access this same runtime lookup surface");
    }

    @Test
    void stopsSparseTextRowsBeforeNativePaddingAndPreservesSmallNullableRows() throws Exception {
        TrackingRow wide = new TrackingRow(128);
        CatalogTable table = table(new String[] {"m"}, new MapType<>(BasicType.STRING_TYPE, wide));
        WebSocketServerDecoder decoder = new WebSocketServerDecoder(table, "text", "|", 768);
        wide.visited = 0;
        StringBuilder input = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            if (i != 0) {
                input.append('\u0002');
            }
            input.append('k').append(i).append('\u0003').append('v');
        }
        assertThrows(
                IOException.class,
                () -> decoder.decode(input.toString().getBytes(StandardCharsets.UTF_8)));
        assertTrue(
                wide.visited > 0 && wide.visited < 128,
                "preflight must stop before native conversion visits all padded fields");
        SeaTunnelRow decoded =
                new WebSocketServerDecoder(table, "text", "|", 4096)
                        .decode("k\u0003v".getBytes(StandardCharsets.UTF_8));
        SeaTunnelRow nested = (SeaTunnelRow) ((Map<?, ?>) decoded.getField(0)).get("k");
        assertEquals(128, nested.getArity());
        assertEquals("v", nested.getField(0));
        assertNull(nested.getField(127));
    }

    @Test
    void matchesNativeSmallJsonAndTextNullArrayMapAndNumericSemantics() throws Exception {
        SeaTunnelRowType nested =
                new SeaTunnelRowType(
                        new String[] {"name", "amount"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
        CatalogTable table =
                table(
                        new String[] {"r", "m", "a"},
                        nested,
                        new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE),
                        ArrayType.STRING_ARRAY_TYPE);
        byte[] json =
                "{\"r\":[\"雪\",7],\"m\":{\"1\":\"first\",\"01\":\"last\"},\"a\":[\"x\",null]}"
                        .getBytes(StandardCharsets.UTF_8);
        SeaTunnelRow actual = new WebSocketServerDecoder(table, "json", "|", 8192).decode(json);
        SeaTunnelRow expected =
                new JsonDeserializationSchema(table, false, false).deserialize(json);
        assertArrayEquals(expected.getFields(), actual.getFields());
        assertEquals("last", ((Map<?, ?>) actual.getField(1)).get(1));
        byte[] text =
                "雪\u00027|1\u0003first\u000201\u0003last|x\u0002y\u0002"
                        .getBytes(StandardCharsets.UTF_8);
        SeaTunnelRow textActual = new WebSocketServerDecoder(table, "text", "|", 8192).decode(text);
        SeaTunnelRow textExpected =
                TextDeserializationSchema.builder()
                        .seaTunnelRowType(table.getSeaTunnelRowType())
                        .delimiter("|")
                        .setCatalogTable(table)
                        .build()
                        .deserialize(text);
        assertArrayEquals(textExpected.getFields(), textActual.getFields());
        assertEquals("last", ((Map<?, ?>) textActual.getField(1)).get(1));
        assertArrayEquals(new String[] {"x", "y"}, (String[]) textActual.getField(2));

        CatalogTable scalarTable =
                table(
                        new String[] {"s", "v", "d", "b", "date", "time", "stamp", "zone"},
                        BasicType.STRING_TYPE,
                        VectorType.VECTOR_FLOAT_TYPE,
                        new DecimalType(20, 4),
                        PrimitiveByteArrayType.INSTANCE,
                        LocalTimeType.LOCAL_DATE_TYPE,
                        LocalTimeType.LOCAL_TIME_TYPE,
                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                        LocalTimeType.OFFSET_DATE_TIME_TYPE);
        byte[] scalarJson =
                ("{\"s\":{\"n\":1e-1000000000},\"v\":[1,2.5],\"d\":1e-1000000000,"
                                + "\"b\":\"AAH/\",\"date\":\"2026-11-28\",\"time\":\"01:02:03.123\","
                                + "\"stamp\":\"2026-11-28 01:02:03\",\"zone\":\"2026-11-28T01:02:03Z\"}")
                        .getBytes(StandardCharsets.UTF_8);
        SeaTunnelRow scalarExpected =
                new JsonDeserializationSchema(scalarTable, false, false).deserialize(scalarJson);
        SeaTunnelRow scalarActual =
                new WebSocketServerDecoder(scalarTable, "json", "|", 8192).decode(scalarJson);
        assertArrayEquals(scalarExpected.getFields(), scalarActual.getFields());
        assertEquals("{\"n\":1E-1000000000}", scalarActual.getField(0));
        assertArrayEquals(new byte[] {0, 1, (byte) 255}, (byte[]) scalarActual.getField(3));
    }

    @Test
    void inspectsDeepAndSharedNullableSchemasWithoutRecursiveCompositeConstruction()
            throws Exception {
        SeaTunnelDataType<?> deep = BasicType.STRING_TYPE;
        for (int i = 0; i < 5000; i++) {
            deep = new SeaTunnelRowType(new String[] {"v"}, new SeaTunnelDataType<?>[] {deep});
        }
        CatalogTable deepTable = table(new String[] {"deep"}, deep);
        assertNull(
                new WebSocketServerDecoder(deepTable, "json", "|", 65536)
                        .decode("{\"deep\":null}".getBytes(StandardCharsets.UTF_8))
                        .getField(0));
        CatalogTable nullableText =
                table(new String[] {"head", "deep"}, BasicType.STRING_TYPE, deep);
        SeaTunnelRow text =
                new WebSocketServerDecoder(nullableText, "text", "|", 65536)
                        .decode("ok".getBytes(StandardCharsets.UTF_8));
        assertEquals("ok", text.getField(0));
        assertNull(text.getField(1));
        SeaTunnelDataType<?> level = BasicType.STRING_TYPE;
        for (int i = 0; i < 6; i++) {
            level = new SeaTunnelRowType(new String[] {"v"}, new SeaTunnelDataType<?>[] {level});
        }
        assertTrue(
                new WebSocketServerDecoder(table(new String[] {"nested"}, level), "text", "|", 8192)
                                .decode("v".getBytes(StandardCharsets.UTF_8))
                                .getField(0)
                        instanceof SeaTunnelRow);
        SeaTunnelDataType<?> tooDeep =
                new SeaTunnelRowType(new String[] {"v"}, new SeaTunnelDataType<?>[] {level});
        WebSocketServerDecoder nativeBoundary =
                new WebSocketServerDecoder(
                        table(new String[] {"nested"}, tooDeep), "text", "|", 8192);
        assertThrows(
                IOException.class,
                () -> nativeBoundary.decode("v".getBytes(StandardCharsets.UTF_8)));
        SeaTunnelDataType<?> shared = BasicType.STRING_TYPE;
        for (int i = 0; i < 8; i++) {
            shared =
                    new SeaTunnelRowType(
                            new String[] {"left", "right"},
                            new SeaTunnelDataType<?>[] {shared, shared});
        }
        assertNull(
                new WebSocketServerDecoder(
                                table(new String[] {"shared"}, shared), "json", "|", 4096)
                        .decode("{\"shared\":null}".getBytes(StandardCharsets.UTF_8))
                        .getField(0));
    }

    @Test
    void preservesByteParserEncodingDetectionAndRejectsMalformedUtf8Independently()
            throws Exception {
        CatalogTable table = table(new String[] {"s"}, BasicType.STRING_TYPE);
        WebSocketServerDecoder decoder = new WebSocketServerDecoder(table, "json", "|", 4096);
        byte[] ascii = "{\"s\":\"ok\"}".getBytes(StandardCharsets.US_ASCII);
        byte[] utf8Bom = new byte[ascii.length + 3];
        utf8Bom[0] = (byte) 0xef;
        utf8Bom[1] = (byte) 0xbb;
        utf8Bom[2] = (byte) 0xbf;
        System.arraycopy(ascii, 0, utf8Bom, 3, ascii.length);
        assertEquals("ok", decoder.decode(utf8Bom).getField(0));
        // The old enqueue UTF8 check admits ASCII-only zero-interleaved UTF16/32 without a BOM;
        // retain native byte-parser auto-detection instead of replacing it with a String parser.
        for (int width : new int[] {2, 4}) {
            for (boolean little : new boolean[] {false, true}) {
                byte[] encoded = new byte[ascii.length * width];
                for (int i = 0; i < ascii.length; i++) {
                    encoded[i * width + (little ? 0 : width - 1)] = ascii[i];
                }
                assertEquals("ok", decoder.decode(encoded).getField(0));
            }
        }
        assertThrows(
                IOException.class,
                () ->
                        decoder.decode(
                                new byte[] {
                                    '{', '"', 's', '"', ':', '"', (byte) 0xc3, 0x28, '"', '}'
                                }));
        assertThrows(
                IOException.class,
                () -> decoder.decode("{\"s\":\"ok\"}{}".getBytes(StandardCharsets.UTF_8)));
    }

    private static CatalogTable table(String[] names, SeaTunnelDataType<?>... types) {
        TableSchema.Builder schema = TableSchema.builder();
        for (int i = 0; i < names.length; i++) {
            schema.column(PhysicalColumn.of(names[i], types[i], 0, false, null, null));
        }
        return CatalogTable.of(
                TableIdentifier.of("test", TablePath.DEFAULT),
                schema.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                null);
    }

    /**
     * Counts JSON field lookups during actual typed ROW conversion. Values are distinct object
     * nodes; only this observation counter is shared, so the native and bounded converters expose
     * the same runtime lookup surface without constructing a production-scale expansion fixture.
     */
    private static final class TrackingObjectNode extends ObjectNode {
        private static final long serialVersionUID = 1L;
        private final AtomicInteger lookedUp;

        private TrackingObjectNode(JsonNodeFactory factory, AtomicInteger lookedUp) {
            super(factory);
            this.lookedUp = lookedUp;
        }

        @Override
        public JsonNode get(String name) {
            lookedUp.incrementAndGet();
            return super.get(name);
        }
    }

    /**
     * Counts schema-field access, including the native TEXT padding path that a late guard misses.
     */
    private static final class TrackingRow extends SeaTunnelRowType {
        private static final long serialVersionUID = 1L;
        private int visited;

        private TrackingRow(int width) {
            super(names(width), types(width));
        }

        @Override
        public SeaTunnelDataType<?> getFieldType(int index) {
            visited++;
            return super.getFieldType(index);
        }

        private static String[] names(int width) {
            String[] names = new String[width];
            for (int i = 0; i < width; i++) {
                names[i] = "f" + i;
            }
            return names;
        }

        private static SeaTunnelDataType<?>[] types(int width) {
            SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[width];
            Arrays.fill(types, BasicType.STRING_TYPE);
            return types;
        }
    }
}
