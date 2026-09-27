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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** RFC 5424 messages, followed by RFC 5425 octet-counted framing. */
final class SyslogMessageEncoder {
    private static final Pattern TIMESTAMP =
            Pattern.compile(
                    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])");
    private static final MapType<String, Map<String, String>> STRUCTURED_DATA_TYPE =
            new MapType<>(
                    BasicType.STRING_TYPE,
                    new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
    private final Map<String, Integer> indexes = new HashMap<>();
    private final int arity;
    private final int maxBytes;

    SyslogMessageEncoder(SeaTunnelRowType rowType, int maxBytes) {
        this.arity = rowType.getTotalFields();
        this.maxBytes = maxBytes;
        String[] names = rowType.getFieldNames();
        for (int i = 0; i < names.length; i++) {
            if (indexes.put(names[i], i) != null) {
                throw invalid("schema", "duplicate column names");
            }
        }
        requireType(rowType, "message", BasicType.STRING_TYPE, true);
        for (String field :
                new String[] {"timestamp", "hostname", "app_name", "proc_id", "msg_id"}) {
            requireType(rowType, field, BasicType.STRING_TYPE, false);
        }
        requireType(rowType, "facility", BasicType.INT_TYPE, false);
        requireType(rowType, "severity", BasicType.INT_TYPE, false);
        requireType(rowType, "structured_data", STRUCTURED_DATA_TYPE, false);
    }

    private void requireType(
            SeaTunnelRowType type, String name, SeaTunnelDataType<?> expected, boolean required) {
        Integer index = indexes.get(name);
        if (index == null) {
            if (required) {
                throw invalid(name, "required column is missing");
            }
        } else if (!expected.equals(type.getFieldType(index))) {
            throw invalid(name, "expected " + expected);
        }
    }

    byte[] encode(SeaTunnelRow row) {
        if (row == null || row.getArity() != arity) {
            throw invalid("row", "arity does not match the catalog schema");
        }
        if (row.getRowKind() != RowKind.INSERT) {
            throw invalid("row", "only INSERT rows are supported");
        }
        StringBuilder message = new StringBuilder(Math.min(maxBytes, 1024));
        append(
                message,
                "<" + (number(row, "facility", 1, 23) * 8 + number(row, "severity", 6, 7)) + ">1 ");
        append(message, timestamp(string(row, "timestamp")));
        append(message, " " + header(string(row, "hostname"), "hostname", 255));
        append(message, " " + header(string(row, "app_name"), "app_name", 48));
        append(message, " " + header(string(row, "proc_id"), "proc_id", 128));
        append(message, " " + header(string(row, "msg_id"), "msg_id", 32) + " ");
        structuredData(message, value(row, "structured_data"));
        String text = string(row, "message");
        if (text != null) {
            append(message, " \uFEFF");
            append(message, text);
        }
        try {
            // The character cap above bounds intermediate allocations; UTF-8 may take more bytes.
            ByteBuffer encoded =
                    StandardCharsets.UTF_8
                            .newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .encode(CharBuffer.wrap(message));
            if (encoded.remaining() > maxBytes) {
                throw invalid("max_message_bytes", "encoded message exceeds limit");
            }
            byte[] prefix = (encoded.remaining() + " ").getBytes(StandardCharsets.US_ASCII);
            byte[] frame = new byte[prefix.length + encoded.remaining()];
            System.arraycopy(prefix, 0, frame, 0, prefix.length);
            encoded.get(frame, prefix.length, encoded.remaining());
            return frame;
        } catch (CharacterCodingException e) {
            throw invalid("message/structured_data", "malformed Unicode");
        }
    }

    private Object value(SeaTunnelRow row, String name) {
        Integer index = indexes.get(name);
        return index == null ? null : row.getField(index);
    }

    private String string(SeaTunnelRow row, String name) {
        Object value = value(row, name);
        if (value != null && !(value instanceof String)) {
            throw invalid(name, "expected STRING value");
        }
        return (String) value;
    }

    private int number(SeaTunnelRow row, String name, int fallback, int max) {
        Object value = value(row, name);
        if (value == null) {
            return fallback;
        }
        if (!(value instanceof Integer) || (int) value < 0 || (int) value > max) {
            throw invalid(name, "expected INT between 0 and " + max);
        }
        return (int) value;
    }

    private static String header(String value, String name, int max) {
        if (value == null) {
            return "-";
        }
        if (value.isEmpty() || value.length() > max) {
            throw invalid(name, "length must be between 1 and " + max);
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) < 33 || value.charAt(i) > 126) {
                throw invalid(name, "only printable US-ASCII without spaces is allowed");
            }
        }
        return value;
    }

    private static String timestamp(String value) {
        if (value == null || "-".equals(value)) {
            return "-";
        }
        if (value.length() > 32 || !TIMESTAMP.matcher(value).matches()) {
            throw invalid(
                    "timestamp",
                    "expected RFC 5424 timestamp with an offset and at most six fractional digits");
        }
        try {
            // Validate calendar/time strictly, retaining RFC offsets up to 23:59 (not ZoneOffset's
            // 18h cap).
            int end = value.endsWith("Z") ? value.length() - 1 : value.length() - 6;
            LocalDateTime.parse(value.substring(0, end));
        } catch (DateTimeParseException e) {
            throw invalid("timestamp", "invalid calendar date or time");
        }
        return value;
    }

    private void structuredData(StringBuilder target, Object value) {
        if (value == null || (value instanceof Map && ((Map<?, ?>) value).isEmpty())) {
            append(target, "-");
            return;
        }
        if (!(value instanceof Map)) {
            throw invalid("structured_data", "expected MAP<STRING, MAP<STRING, STRING>>");
        }
        for (Map.Entry<?, ?> element : ((Map<?, ?>) value).entrySet()) {
            append(target, "[" + sdName(element.getKey()));
            if (!(element.getValue() instanceof Map)) {
                throw invalid("structured_data", "each SD-ID must map to a non-null parameter map");
            }
            for (Map.Entry<?, ?> parameter : ((Map<?, ?>) element.getValue()).entrySet()) {
                append(target, " " + sdName(parameter.getKey()) + "=\"");
                if (!(parameter.getValue() instanceof String)) {
                    throw invalid("structured_data", "parameter values must be non-null STRINGs");
                }
                String text = (String) parameter.getValue();
                if (text.length() > maxBytes) {
                    throw invalid("max_message_bytes", "structured data exceeds limit");
                }
                for (int i = 0; i < text.length(); i++) {
                    char c = text.charAt(i);
                    if (c == '\\' || c == '"' || c == ']') {
                        append(target, "\\");
                    }
                    append(target, String.valueOf(c));
                }
                append(target, "\"");
            }
            append(target, "]");
        }
    }

    private static String sdName(Object value) {
        if (!(value instanceof String)) {
            throw invalid("structured_data", "SD-ID and parameter names must be non-null STRINGs");
        }
        String name = header((String) value, "structured_data", 32);
        if (name.indexOf('=') >= 0 || name.indexOf(']') >= 0 || name.indexOf('"') >= 0) {
            throw invalid("structured_data", "invalid SD-ID or parameter name");
        }
        return name;
    }

    private void append(StringBuilder target, String value) {
        if (value.length() > maxBytes - target.length()) {
            throw invalid("max_message_bytes", "message exceeds limit");
        }
        target.append(value);
    }

    private static IllegalArgumentException invalid(String field, String reason) {
        // Never include row contents: syslog payloads can contain credentials or personal data.
        return new IllegalArgumentException("Syslog " + field + ": " + reason);
    }
}
