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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.format.json.JsonToRowConverters;
import org.apache.seatunnel.format.text.TextDeserializationSchema;
import org.apache.seatunnel.format.text.constant.TextFormatConstant;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Bounds prospective typed allocations independently of wire/queue bytes. Units count schema nodes,
 * slots, entries, visited characters and scalar work, not JVM heap bytes. Only this reader owns the
 * parser/converters; no peer-controlled field names enter a persistent parser symbol table.
 */
final class WebSocketServerDecoder {
    private final SeaTunnelRowType rowType;
    private final int limit;
    private final ObjectMapper mapper;
    private final TextDeserializationSchema text;
    private final JsonToRowConverters nativeScalars = new JsonToRowConverters(false, false);
    private final Map<SeaTunnelDataType<?>, JsonToRowConverters.JsonToObjectConverter> scalars =
            new IdentityHashMap<>();
    private final String[] separators = TextFormatConstant.SEPARATOR.clone();

    WebSocketServerDecoder(CatalogTable table, String format, String delimiter, int limit) {
        this.rowType = table.getSeaTunnelRowType();
        this.limit = limit;
        separators[0] = delimiter;
        boolean decimal = inspectSchema("json".equals(format));
        // Native temporal caches use schema paths (never map keys). Bound their retained footprint
        // too, including shared schema DAGs with many reachable paths across successive messages.
        nativeScalars.fieldFormatterMap = new FormatterCache(limit);
        if ("json".equals(format)) {
            JsonFactory factory = new JsonFactory();
            factory.enable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);
            factory.disable(JsonFactory.Feature.INTERN_FIELD_NAMES);
            mapper = new ObjectMapper(factory);
            mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
            mapper.disable(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature());
            if (decimal) {
                mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
            }
            text = null;
        } else {
            mapper = null;
            text =
                    TextDeserializationSchema.builder()
                            .seaTunnelRowType(rowType)
                            .delimiter(delimiter)
                            .setCatalogTable(table)
                            .build();
            text.fieldFormatterMap = new FormatterCache(limit);
        }
    }

    /**
     * Parse privately owned bounded wire data; conversion never invokes a composite native factory.
     */
    SeaTunnelRow decode(byte[] bytes) throws IOException {
        try {
            if (mapper != null) {
                // Enqueue validates both binary and text JSON, but keep this boundary independently
                // strict too. Preserve the native byte parser's BOM/encoding detection.
                WebSocketServerProtocol.text(bytes);
                // JsonFactory.copy creates fresh root symbol tables. The template never parses
                // input, so peer map keys cannot accumulate between successive messages.
                JsonNode node = mapper.copy().readTree(bytes);
                if (node == null || !node.isObject()) {
                    throw invalid();
                }
                return decodeJson(node);
            }
            String value = WebSocketServerProtocol.text(bytes);
            admitText(value);
            return text.deserialize(bytes);
        } catch (IOException | RuntimeException ex) {
            // Do not stringify the rejected tree, row, schema, scalar exception or peer text.
            throw invalid();
        }
    }

    /**
     * Package entry permits real typed conversion with a tracking tree in component regressions.
     */
    SeaTunnelRow decodeJson(JsonNode node) throws IOException {
        Budget budget = new Budget(limit);
        Deque<JsonValue> stack = new ArrayDeque<>();
        stack.push(new JsonValue(rowType, node, ""));
        Object result = null;
        while (!stack.isEmpty()) {
            JsonValue value = stack.peek();
            if (!value.started) {
                value.start(budget);
                if (value.scalar) {
                    admitScalar(value.node, budget);
                    value.result =
                            value.node == null || value.node.isNull() || value.node.isMissingNode()
                                    ? null
                                    : scalars.get(value.type).convert(value.node, value.path);
                }
            }
            JsonValue child = value.next(budget);
            if (child != null) {
                stack.push(child);
                continue;
            }
            stack.pop();
            result = value.result;
            if (!stack.isEmpty()) {
                stack.peek().accept(result);
            }
        }
        return (SeaTunnelRow) result;
    }

    /**
     * Complete the lazy TEXT admission before native split arrays/padded rows are allocated.
     * Literal ROW splitting retains empty fields; native ARRAY/MAP splitting drops trailing empty
     * fields.
     */
    void admitText(String value) throws IOException {
        Budget budget = new Budget(limit);
        Deque<TextValue> stack = new ArrayDeque<>();
        stack.push(new TextValue(rowType, value, 0, value.length(), 0, true, 0));
        while (!stack.isEmpty()) {
            TextValue current = stack.peek();
            if (!current.started) {
                current.start(budget);
            }
            TextValue next = current.next(budget);
            if (next == null) {
                stack.pop();
            } else {
                stack.push(next);
            }
        }
    }

    private boolean inspectSchema(boolean json) {
        Budget budget = new Budget(limit);
        Map<SeaTunnelDataType<?>, Boolean> active = new IdentityHashMap<>();
        Map<SeaTunnelDataType<?>, Boolean> finished = new IdentityHashMap<>();
        Deque<SchemaValue> stack = new ArrayDeque<>();
        stack.push(new SchemaValue(rowType));
        boolean decimal = false;
        while (!stack.isEmpty()) {
            SchemaValue value = stack.peek();
            if (!value.started) {
                if (finished.containsKey(value.type)) {
                    stack.pop();
                    continue;
                }
                if (active.put(value.type, Boolean.TRUE) != null) {
                    throw new IllegalArgumentException("WebSocketServer schema contains a cycle");
                }
                value.started = true;
                budget.take(1);
                SqlType sqlType = value.type.getSqlType();
                decimal |= sqlType == SqlType.DECIMAL;
                if (sqlType == SqlType.ROW) {
                    budget.take(((SeaTunnelRowType) value.type).getTotalFields());
                } else if (sqlType != SqlType.ARRAY && sqlType != SqlType.MAP && json) {
                    scalars.put(value.type, nativeScalars.createConverter(value.type));
                }
            }
            SeaTunnelDataType<?> child = value.next(budget);
            if (child != null) {
                stack.push(new SchemaValue(child));
            } else {
                active.remove(value.type);
                finished.put(value.type, Boolean.TRUE);
                stack.pop();
            }
        }
        return decimal;
    }

    private static void admitScalar(JsonNode node, Budget budget) {
        if (node == null) {
            return;
        }
        Deque<Iterator<JsonNode>> parents = new ArrayDeque<>();
        while (true) {
            budget.take(1);
            if (node.isContainerNode()) {
                // STRING may accept an entire container; FLOAT_VECTOR allocates per element.
                // Account for those native scalar branches before invoking them.
                if (node.isObject()) {
                    Iterator<String> names = node.fieldNames();
                    while (names.hasNext()) {
                        budget.take(1L + names.next().length());
                    }
                }
                Iterator<JsonNode> children = node.elements();
                if (children.hasNext()) {
                    parents.push(children);
                    node = children.next();
                    continue;
                }
            } else {
                budget.take(node.asText().length());
            }
            while (!parents.isEmpty() && !parents.peek().hasNext()) {
                parents.pop();
            }
            if (parents.isEmpty()) {
                return;
            }
            node = parents.peek().next();
        }
    }

    private static String path(String parent, String child, Budget budget, boolean row) {
        long size = (long) parent.length() + child.length() + 1;
        budget.take(size);
        return row && StringUtils.isBlank(parent) ? child : parent + "." + child;
    }

    private static IOException invalid() {
        return new IOException(
                "WebSocketServer payload exceeds decoded work budget or does not match schema");
    }

    /**
     * A lower-bound work/slot counter; reservations precede every proportional typed allocation.
     */
    private static final class Budget {
        private long remaining;

        private Budget(int limit) {
            remaining = limit;
        }

        private void take(long count) {
            if (count < 0 || count > remaining) {
                throw new IllegalArgumentException("WebSocketServer decoded work budget exceeded");
            }
            remaining -= count;
        }
    }

    /**
     * Inspects schema children lazily, with identity-based cycle/completion tracking. Shared
     * nullable schema DAGs never expand into recursive composite converter trees.
     */
    private static final class SchemaValue {
        private final SeaTunnelDataType<?> type;
        private int index;
        private boolean started;

        private SchemaValue(SeaTunnelDataType<?> type) {
            this.type = type;
        }

        private SeaTunnelDataType<?> next(Budget budget) {
            switch (type.getSqlType()) {
                case ROW:
                    SeaTunnelRowType row = (SeaTunnelRowType) type;
                    if (index < row.getTotalFields()) {
                        budget.take(row.getFieldNames()[index].length());
                        return row.getFieldType(index++);
                    }
                    return null;
                case ARRAY:
                    return index++ == 0 ? ((ArrayType<?, ?>) type).getElementType() : null;
                case MAP:
                    MapType<?, ?> map = (MapType<?, ?>) type;
                    if (index++ == 0) {
                        return map.getKeyType();
                    }
                    return index == 2 ? map.getValueType() : null;
                default:
                    return null;
            }
        }
    }

    /**
     * Lazy composite conversion preserves native nullable ROW, typed arrays and last-key-wins MAP.
     */
    private static final class JsonValue {
        private final SeaTunnelDataType<?> type;
        private final JsonNode node;
        private final String path;
        private Object result;
        private boolean started;
        private boolean scalar;
        private int index;
        private Iterator<Map.Entry<String, JsonNode>> entries;
        private Map.Entry<String, JsonNode> entry;
        private Object key;
        private boolean keyNext = true;

        private JsonValue(SeaTunnelDataType<?> type, JsonNode node, String path) {
            this.type = type;
            this.node = node;
            this.path = path;
        }

        private void start(Budget budget) throws IOException {
            started = true;
            budget.take(1);
            if (node == null || node.isNull() || node.isMissingNode()) {
                return;
            }
            switch (type.getSqlType()) {
                case ROW:
                    int fields = ((SeaTunnelRowType) type).getTotalFields();
                    budget.take(fields);
                    result = new SeaTunnelRow(fields);
                    break;
                case ARRAY:
                    if (!node.isArray()) {
                        throw invalid();
                    }
                    budget.take(node.size());
                    result =
                            Array.newInstance(
                                    ((ArrayType<?, ?>) type).getElementType().getTypeClass(),
                                    node.size());
                    break;
                case MAP:
                    if (!node.isObject()) {
                        throw invalid();
                    }
                    result = new HashMap<>();
                    entries = node.fields();
                    break;
                default:
                    scalar = true;
            }
        }

        private JsonValue next(Budget budget) {
            if (result == null || scalar) {
                return null;
            }
            switch (type.getSqlType()) {
                case ROW:
                    SeaTunnelRowType row = (SeaTunnelRowType) type;
                    if (index < row.getTotalFields()) {
                        String name = row.getFieldNames()[index];
                        return new JsonValue(
                                row.getFieldType(index),
                                node.isArray() ? node.get(index) : node.get(name),
                                path(path, name, budget, true));
                    }
                    return null;
                case ARRAY:
                    return index < node.size()
                            ? new JsonValue(
                                    ((ArrayType<?, ?>) type).getElementType(),
                                    node.get(index),
                                    path)
                            : null;
                case MAP:
                    MapType<?, ?> map = (MapType<?, ?>) type;
                    if (keyNext) {
                        if (!entries.hasNext()) {
                            return null;
                        }
                        budget.take(1);
                        entry = entries.next();
                        budget.take(entry.getKey().length());
                        return new JsonValue(
                                map.getKeyType(),
                                TextNode.valueOf(entry.getKey()),
                                path(path, "key", budget, false));
                    }
                    return new JsonValue(
                            map.getValueType(),
                            entry.getValue(),
                            path(path, "value", budget, false));
                default:
                    return null;
            }
        }

        @SuppressWarnings("unchecked")
        private void accept(Object child) {
            switch (type.getSqlType()) {
                case ROW:
                    ((SeaTunnelRow) result).setField(index++, child);
                    break;
                case ARRAY:
                    Array.set(result, index++, child);
                    break;
                case MAP:
                    if (keyNext) {
                        key = child;
                    } else {
                        ((Map<Object, Object>) result).put(key, child);
                    }
                    keyNext = !keyNext;
                    break;
                default:
                    throw new IllegalStateException("Unexpected scalar parent");
            }
        }
    }

    /**
     * String ranges avoid allocating the very split arrays and null-padding maps being admitted.
     */
    private final class TextValue {
        private final SeaTunnelDataType<?> type;
        private final String input;
        private final int start;
        private final int end;
        private final int level;
        private final boolean root;
        private final long pathUnits;
        private boolean started;
        private Parts parts;
        private int index;
        private int entryEnd;
        private int valueStart;
        private boolean valueNext;

        private TextValue(
                SeaTunnelDataType<?> type,
                String input,
                int start,
                int end,
                int level,
                boolean root,
                long pathUnits) {
            this.type = type;
            this.input = input;
            this.start = start;
            this.end = end;
            this.level = level;
            this.root = root;
            this.pathUnits = pathUnits;
        }

        private void start(Budget budget) throws IOException {
            started = true;
            budget.take(1L + end - start);
            if (!root && start == end) {
                return;
            }
            switch (type.getSqlType()) {
                case ROW:
                    budget.take(((SeaTunnelRowType) type).getTotalFields());
                    parts = new Parts(input, start, end, separator(root ? level : level + 1), true);
                    break;
                case ARRAY:
                case MAP:
                    parts = new Parts(input, start, end, separator(level + 1), false);
                    break;
                default:
                    break;
            }
        }

        private TextValue next(Budget budget) throws IOException {
            if (parts == null) {
                return null;
            }
            switch (type.getSqlType()) {
                case ROW:
                    SeaTunnelRowType row = (SeaTunnelRowType) type;
                    if (index >= row.getTotalFields()) {
                        if (parts.hasNext()) {
                            // Native nested ROW rejects excess fields. Top-level extra columns are
                            // split but ignored; still admit their split-array/string allocation.
                            if (!root) {
                                throw invalid();
                            }
                            while (parts.hasNext()) {
                                int from = parts.nextStart();
                                budget.take(1L + parts.lastEnd - from);
                            }
                        }
                        return null;
                    }
                    String name = row.getFieldNames()[index];
                    long childPath = root ? name.length() : pathUnits + 1 + name.length();
                    // Native ROW constructs the schema path even for null-padded fields. Admit
                    // that work too, instead of skipping missing columns after reserving slots.
                    budget.take(1L + childPath);
                    int from = end;
                    int until = end;
                    if (parts.hasNext()) {
                        from = parts.nextStart();
                        until = parts.lastEnd;
                    }
                    return new TextValue(
                            row.getFieldType(index++),
                            input,
                            from,
                            until,
                            root ? level : level + 1,
                            false,
                            childPath);
                case ARRAY:
                    if (!parts.hasNext()) {
                        return null;
                    }
                    int elementStart = parts.nextStart();
                    budget.take(1);
                    return new TextValue(
                            ((ArrayType<?, ?>) type).getElementType(),
                            input,
                            elementStart,
                            parts.lastEnd,
                            level + 1,
                            false,
                            pathUnits);
                case MAP:
                    MapType<?, ?> map = (MapType<?, ?>) type;
                    if (valueNext) {
                        valueNext = false;
                        return valueStart >= entryEnd
                                ? nullValue()
                                : new TextValue(
                                        map.getValueType(),
                                        input,
                                        valueStart,
                                        entryEnd,
                                        level + 1,
                                        false,
                                        pathUnits);
                    }
                    if (!parts.hasNext()) {
                        return null;
                    }
                    int keyStart = parts.nextStart();
                    int stop = parts.lastEnd;
                    budget.take(1L + stop - keyStart);
                    Parts pair = new Parts(input, keyStart, stop, separator(level + 2), false);
                    if (!pair.hasNext()) {
                        throw invalid();
                    }
                    int keyFrom = pair.nextStart();
                    int keyEnd = pair.lastEnd;
                    if (pair.hasNext()) {
                        valueStart = pair.nextStart();
                        entryEnd = pair.lastEnd;
                        valueNext = true;
                        // Native split materializes all additional pieces even though it ignores
                        // them after the second. Charge those pieces before native conversion.
                        while (pair.hasNext()) {
                            int ignored = pair.nextStart();
                            budget.take(1L + pair.lastEnd - ignored);
                        }
                    }
                    return new TextValue(
                            map.getKeyType(), input, keyFrom, keyEnd, level + 1, false, pathUnits);
                default:
                    return null;
            }
        }

        private TextValue nullValue() {
            // Continue the MAP iterator without traversing a nonexistent value.
            return new TextValue(BasicType.STRING_TYPE, input, end, end, level, false, pathUnits);
        }
    }

    private String separator(int level) throws IOException {
        if (level >= separators.length) {
            // Match the existing seven native separators; do not add an unrelated depth cap.
            throw invalid();
        }
        return separators[level];
    }

    /**
     * Literal range iterator; only fixed control separators use native regex split in composites.
     */
    private static final class Parts {
        private final String input;
        private final String delimiter;
        private final int end;
        private int cursor;
        private int lastEnd;

        private Parts(String input, int start, int end, String delimiter, boolean keepTrailing) {
            this.input = input;
            this.delimiter = delimiter;
            this.cursor = start;
            if (!keepTrailing && end > start) {
                while (end - delimiter.length() >= start
                        && input.startsWith(delimiter, end - delimiter.length())) {
                    end -= delimiter.length();
                }
                this.end = end;
                if (end == start) {
                    cursor = end + 1;
                }
            } else {
                this.end = end;
            }
        }

        private boolean hasNext() {
            return cursor <= end;
        }

        private int nextStart() {
            int from = cursor;
            int match = input.indexOf(delimiter, from);
            lastEnd = match < 0 || match >= end ? end : match;
            cursor = lastEnd == end ? end + 1 : lastEnd + delimiter.length();
            return from;
        }
    }

    /**
     * Bounds native temporal-parser cache retention across successive messages. Keys are admitted
     * schema paths, never dynamic incoming map keys or peer field names.
     */
    private static final class FormatterCache extends HashMap<String, DateTimeFormatter> {
        private static final long serialVersionUID = 1L;
        private final Budget budget;

        private FormatterCache(int limit) {
            budget = new Budget(limit);
        }

        @Override
        public DateTimeFormatter put(String key, DateTimeFormatter value) {
            if (!containsKey(key)) {
                budget.take(1L + key.length());
            }
            return super.put(key, value);
        }
    }
}
