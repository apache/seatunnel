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

package org.apache.seatunnel.connectors.seatunnel.syslog.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyslogMessageEncoderTest {
    static final String[] NAMES = {
        "facility",
        "severity",
        "timestamp",
        "hostname",
        "app_name",
        "proc_id",
        "msg_id",
        "structured_data",
        "message"
    };
    static final SeaTunnelRowType TYPE =
            new SeaTunnelRowType(
                    NAMES,
                    new SeaTunnelDataType<?>[] {
                        BasicType.INT_TYPE,
                        BasicType.INT_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.STRING_TYPE,
                        new MapType<>(
                                BasicType.STRING_TYPE,
                                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)),
                        BasicType.STRING_TYPE
                    });

    @Test
    void encodesIndependentRfcExampleAndUtf8OctetCount() {
        byte[] frame = new SyslogMessageEncoder(TYPE, 8192).encode(row());
        String expected =
                "<165>1 2003-10-11T22:14:15.003Z mymachine su 123 ID47 - \uFEFFhello \u4e16\u754c\nsecond line";
        assertEquals(
                expected.getBytes(StandardCharsets.UTF_8).length + " " + expected,
                new String(frame, StandardCharsets.UTF_8));
        assertTrue(expected.getBytes(StandardCharsets.UTF_8).length > expected.length());
    }

    @Test
    void encodesDefaultsNullAndEmptyMessageDistinctly() {
        SeaTunnelRowType type =
                new SeaTunnelRowType(
                        new String[] {"message"},
                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        SyslogMessageEncoder encoder = new SyslogMessageEncoder(type, 8192);
        String absent = "<14>1 - - - - - -";
        assertEquals(
                absent.length() + " " + absent,
                new String(
                        encoder.encode(new SeaTunnelRow(new Object[] {null})),
                        StandardCharsets.UTF_8));
        String empty = absent + " \uFEFF";
        assertEquals(
                empty.getBytes(StandardCharsets.UTF_8).length + " " + empty,
                new String(
                        encoder.encode(new SeaTunnelRow(new Object[] {""})),
                        StandardCharsets.UTF_8));
    }

    @Test
    void escapesStructuredDataWithoutEscapingMessageOrSplittingLines() {
        SeaTunnelRow row = row();
        Map<String, String> params = new LinkedHashMap<>();
        params.put("text", "\"\\]\n\u4e16");
        row.setField(7, Collections.singletonMap("example@32473", params));
        String frame =
                new String(
                        new SyslogMessageEncoder(TYPE, 8192).encode(row), StandardCharsets.UTF_8);
        assertTrue(frame.contains("[example@32473 text=\"\\\"\\\\\\]\n\u4e16\"]"));
        assertTrue(frame.endsWith("\uFEFFhello \u4e16\u754c\nsecond line"));
    }

    @Test
    void rejectsEveryHeaderInjectionAndOverlongFieldWithoutLeakingPayload() {
        for (int index : new int[] {3, 4, 5, 6}) {
            for (String value :
                    new String[] {
                        "",
                        "has space",
                        "secret\n<0>1",
                        "secret\r",
                        "\t",
                        "\u0000",
                        "\u007f",
                        "\u00e9"
                    }) {
                SeaTunnelRow row = row();
                row.setField(index, value);
                IllegalArgumentException e =
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
                assertTrue(e.getMessage().contains(NAMES[index]));
                assertFalse(e.getMessage().contains("secret"));
            }
        }
        int[] limits = {255, 48, 128, 32};
        for (int i = 0; i < limits.length; i++) {
            SeaTunnelRow row = row();
            row.setField(i + 3, repeat("a", limits[i]));
            new SyslogMessageEncoder(TYPE, 8192).encode(row);
            row.setField(i + 3, repeat("a", limits[i] + 1));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
        }
    }

    @Test
    void validatesPriorityAndRowKinds() {
        for (int[] pair : new int[][] {{0, 0}, {23, 7}}) {
            SeaTunnelRow row = row();
            row.setField(0, pair[0]);
            row.setField(1, pair[1]);
            new SyslogMessageEncoder(TYPE, 8192).encode(row);
        }
        for (int index : new int[] {0, 1}) {
            for (Object value : new Object[] {-1, index == 0 ? 24 : 8, 1L, "1"}) {
                SeaTunnelRow row = row();
                row.setField(index, value);
                assertThrows(
                        IllegalArgumentException.class,
                        () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
            }
        }
        for (RowKind kind :
                new RowKind[] {RowKind.DELETE, RowKind.UPDATE_BEFORE, RowKind.UPDATE_AFTER}) {
            SeaTunnelRow row = row();
            row.setRowKind(kind);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
        }
    }

    @Test
    void validatesTimestampCalendarPrecisionAndOffset() {
        for (String timestamp :
                new String[] {
                    "-", "2024-02-29T23:59:59.123456+23:59", "2024-01-01T00:00:00-00:00"
                }) {
            SeaTunnelRow row = row();
            row.setField(2, timestamp);
            new SyslogMessageEncoder(TYPE, 8192).encode(row);
        }
        for (String timestamp :
                new String[] {
                    "",
                    "Oct 11 22:14:15",
                    "2023-02-29T00:00:00Z",
                    "2024-01-01T24:00:00Z",
                    "2024-01-01T00:00:60Z",
                    "2024-01-01t00:00:00Z",
                    "2024-01-01T00:00:00.1234567Z",
                    "2024-01-01T00:00:00",
                    "2024-01-01T00:00:00+24:00"
                }) {
            SeaTunnelRow row = row();
            row.setField(2, timestamp);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
        }
    }

    @Test
    void validatesStructuredDataNamesTypesAndUnicode() {
        for (String name : new String[] {"", "x y", "x]", "x=", "x\"", "\u00e9", repeat("a", 33)}) {
            SeaTunnelRow row = row();
            row.setField(7, Collections.singletonMap(name, Collections.emptyMap()));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
            row.setField(
                    7,
                    Collections.singletonMap("example@32473", Collections.singletonMap(name, "v")));
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
        }
        for (Object value :
                new Object[] {
                    "[raw]",
                    Collections.singletonMap("id", null),
                    Collections.singletonMap("id", Collections.singletonMap("key", null)),
                    Collections.singletonMap("id", Collections.singletonMap("key", "\uD800"))
                }) {
            SeaTunnelRow row = row();
            row.setField(7, value);
            assertThrows(
                    IllegalArgumentException.class,
                    () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
        }
        SeaTunnelRow row = row();
        row.setField(8, "\uD800");
        assertThrows(
                IllegalArgumentException.class,
                () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
    }

    @Test
    void enforcesEncodedByteLimitIncludingHeaderBomAndEscapes() {
        SeaTunnelRow row = row();
        byte[] frame = new SyslogMessageEncoder(TYPE, 8192).encode(row);
        int length = Integer.parseInt(new String(frame, StandardCharsets.UTF_8).split(" ", 2)[0]);
        new SyslogMessageEncoder(TYPE, length).encode(row);
        assertThrows(
                IllegalArgumentException.class,
                () -> new SyslogMessageEncoder(TYPE, length - 1).encode(row));
        row.setField(8, repeat("\u4e16", 8192));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
        row.setField(8, "");
        row.setField(
                7,
                Collections.singletonMap("id", Collections.singletonMap("key", repeat("]", 8192))));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SyslogMessageEncoder(TYPE, 8192).encode(row));
    }

    @Test
    void rejectsWrongSchemaAndArity() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SyslogMessageEncoder(
                                new SeaTunnelRowType(
                                        new String[] {"message"},
                                        new SeaTunnelDataType<?>[] {BasicType.INT_TYPE}),
                                8192));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SyslogMessageEncoder(
                                new SeaTunnelRowType(
                                        new String[] {"other"},
                                        new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE}),
                                8192));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new SyslogMessageEncoder(TYPE, 8192)
                                .encode(new SeaTunnelRow(new Object[] {"x"})));
    }

    static SeaTunnelRow row() {
        return new SeaTunnelRow(
                new Object[] {
                    20,
                    5,
                    "2003-10-11T22:14:15.003Z",
                    "mymachine",
                    "su",
                    "123",
                    "ID47",
                    null,
                    "hello \u4e16\u754c\nsecond line"
                });
    }

    private static String repeat(String value, int count) {
        return String.join("", Collections.nCopies(count, value));
    }
}
