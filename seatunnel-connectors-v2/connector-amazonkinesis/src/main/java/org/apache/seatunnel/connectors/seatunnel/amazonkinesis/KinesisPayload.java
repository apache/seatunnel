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

package org.apache.seatunnel.connectors.seatunnel.amazonkinesis;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.format.json.RowToJsonConverters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Bounds Kinesis conversion work before native JSON tree creation and bounds exact encoded bytes.
 * Shared input objects are charged at every occurrence because native JSON expands each occurrence.
 */
final class KinesisPayload {
    private final SeaTunnelRowType type;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<SeaTunnelDataType<?>, RowToJsonConverters.RowToJsonConverter> scalars =
            new IdentityHashMap<>();

    KinesisPayload(SeaTunnelRowType type) {
        this.type = type;
        // Match the native serializer's numeric representation without its row-stringifying
        // wrapper.
        mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
        verifySchema(type);
    }

    /**
     * Preflight bounds typed input before allocation; iterative composite conversion retains native
     * scalar semantics. The final stream independently enforces escaping and punctuation bytes.
     */
    byte[] json(SeaTunnelRow row, int limit) throws IOException {
        preflight(row, limit);
        LimitedOutput output = new LimitedOutput(limit);
        write(convert(row, limit), output);
        return output.bytes();
    }

    /**
     * Count UTF-8 before creating the encoded array; malformed surrogate replacement matches
     * String.getBytes(UTF_8), which remains the actual text encoder.
     */
    static byte[] text(String value, int limit) {
        utf8(value, limit);
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /*
     * Native scalar converters retain numeric/temporal/null semantics. Composite frames reproduce
     * native reuse and last-key-wins behavior without recursively expanding the declared schema.
     */
    private JsonNode convert(SeaTunnelRow row, int limit) {
        Budget budget = new Budget(limit);
        Set<Object> active = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Value> pending = new ArrayDeque<>();
        pending.push(new Value(type, row, false));
        while (true) {
            Value current = pending.peek();
            if (!current.entered) {
                current.entered = true;
                // Recheck during allocation too; a changing container cannot escape the first walk.
                budget.take(1);
                SqlType sql = current.type.getSqlType();
                if (current.value != null) {
                    if (sql == SqlType.ROW || sql == SqlType.ARRAY || sql == SqlType.MAP) {
                        if (!active.add(current.value)) {
                            throw new IllegalArgumentException("Cyclic Kinesis payload value");
                        }
                    } else {
                        scalar(current, budget);
                    }
                }
                current.node = node(current);
                if (current.type.getSqlType() == SqlType.MAP && current.value != null) {
                    current.entries = ((Map<?, ?>) current.value).entrySet().iterator();
                }
            }
            SqlType sql = current.type.getSqlType();
            Value child =
                    current.value != null
                                    && (sql == SqlType.ROW
                                            || sql == SqlType.ARRAY
                                            || sql == SqlType.MAP)
                            ? current.next(budget)
                            : null;
            if (child != null) {
                pending.push(child);
                continue;
            }
            pending.pop();
            active.remove(current.value);
            if (pending.isEmpty()) {
                return current.node;
            }
            Value parent = pending.peek();
            if (parent.type.getSqlType() == SqlType.ARRAY) {
                ((ArrayNode) parent.node).add(current.node);
            } else if (current.key) {
                parent.fieldName =
                        current.node.isTextual() ? current.node.asText() : current.node.toString();
            } else {
                ((ObjectNode) parent.node).set(current.fieldName, current.node);
            }
        }
    }

    private JsonNode node(Value value) {
        if (value.value == null) {
            return mapper.getNodeFactory().nullNode();
        }
        JsonNode reuse = value.reuse;
        switch (value.type.getSqlType()) {
            case ROW:
                return reuse == null || reuse.isNull()
                        ? mapper.createObjectNode()
                        : (ObjectNode) reuse;
            case MAP:
                return reuse == null || reuse.isNull()
                        ? mapper.createObjectNode()
                        : ((ObjectNode) reuse).removeAll();
            case ARRAY:
                return reuse == null || reuse.isNull()
                        ? mapper.createArrayNode()
                        : ((ArrayNode) reuse).removeAll();
            default:
                return scalars.get(value.type).convert(mapper, reuse, value.value);
        }
    }

    /*
     * Jackson 2.15 does not provide a write-depth constraint. Emit containers iteratively too:
     * otherwise an admitted deep, narrow tree would recurse in ObjectNode/ArrayNode.serialize.
     */
    private void write(JsonNode root, LimitedOutput output) throws IOException {
        Deque<Output> pending = new ArrayDeque<>();
        pending.push(new Output(root));
        try (JsonGenerator generator = mapper.getFactory().createGenerator(output)) {
            while (!pending.isEmpty()) {
                Output current = pending.peek();
                if (!current.entered) {
                    current.entered = true;
                    if (current.node.isObject()) {
                        generator.writeStartObject();
                        current.fields = current.node.fields();
                    } else if (current.node.isArray()) {
                        generator.writeStartArray();
                        current.elements = current.node.elements();
                    } else {
                        mapper.writeTree(generator, current.node);
                        pending.pop();
                        continue;
                    }
                }
                if (current.fields != null) {
                    if (current.fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = current.fields.next();
                        generator.writeFieldName(field.getKey());
                        pending.push(new Output(field.getValue()));
                    } else {
                        generator.writeEndObject();
                        pending.pop();
                    }
                } else if (current.elements.hasNext()) {
                    pending.push(new Output(current.elements.next()));
                } else {
                    generator.writeEndArray();
                    pending.pop();
                }
            }
        }
    }

    /*
     * Visit one child at a time so a large container cannot first allocate a full traversal stack.
     * Only active ancestors are cycle-checked; shared completed values must consume budget again.
     */
    private void preflight(SeaTunnelRow row, int limit) {
        Budget budget = new Budget(limit);
        Deque<Value> pending = new ArrayDeque<>();
        Set<Object> active = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.push(new Value(type, row, false));
        while (!pending.isEmpty()) {
            Value current = pending.peek();
            if (!current.entered) {
                current.entered = true;
                budget.take(1);
                if (current.value == null) {
                    pending.pop();
                    continue;
                }
                SqlType sql = current.type.getSqlType();
                if (sql != SqlType.ROW && sql != SqlType.ARRAY && sql != SqlType.MAP) {
                    scalar(current, budget);
                    pending.pop();
                    continue;
                }
                if (!active.add(current.value)) {
                    throw new IllegalArgumentException("Cyclic Kinesis payload value");
                }
                if (sql == SqlType.MAP) {
                    current.entries = ((Map<?, ?>) current.value).entrySet().iterator();
                }
            }
            Value child = current.next(budget);
            if (child == null) {
                active.remove(current.value);
                pending.pop();
            } else {
                pending.push(child);
            }
        }
    }

    private static void scalar(Value current, Budget budget) {
        Object value = current.value;
        switch (current.type.getSqlType()) {
            case STRING:
                budget.take(utf8((String) value, budget.remaining));
                break;
            case BYTES:
                budget.take(4L * ((((byte[]) value).length + 2L) / 3L));
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                budget.take(
                        numeric(value, current.type.getSqlType(), current.key, budget.remaining));
                break;
            default:
                // Native boolean/null and java.time values have no input-sized expansion.
                break;
        }
    }

    /*
     * Mirror native numeric runtime flexibility without materializing an unbounded decimal String.
     * Input text itself costs conversion work even when parsing normalizes it to a shorter number.
     */
    private static long numeric(Object value, SqlType declared, boolean key, long remaining) {
        long input = 0;
        if (value instanceof CharSequence) {
            input = utf8((CharSequence) value, remaining);
            try {
                value = new BigDecimal(value.toString());
            } catch (NumberFormatException ex) {
                return input;
            }
        } else if (declared == SqlType.DECIMAL
                && (value instanceof Float || value instanceof Double)) {
            value = BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof BigDecimal) {
            BigDecimal decimal = (BigDecimal) value;
            checkInteger(decimal.unscaledValue(), remaining);
            long length;
            if (key) {
                // Map keys use JsonNode.toString(), whose decimal form remains compact.
                length = decimal.toString().length();
            } else {
                long precision = decimal.precision();
                long scale = decimal.scale();
                length =
                        scale <= 0
                                ? (decimal.signum() == 0 ? 1 : precision - scale)
                                : Math.max(precision + 1, scale + 2);
                length += decimal.signum() < 0 ? 1 : 0;
            }
            return Math.max(input, length);
        }
        if (value instanceof BigInteger) {
            checkInteger((BigInteger) value, remaining);
            return Math.max(input, value.toString().length());
        }
        if (value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double) {
            return Math.max(input, value.toString().length());
        }
        throw invalid();
    }

    private static void checkInteger(BigInteger value, long remaining) {
        // This lower bound precedes any decimal String/precision allocation and cannot overflow.
        if (Math.max(1L, (value.bitLength() - 1L) * 301L / 1000L) > remaining) {
            throw invalid();
        }
    }

    private static long utf8(CharSequence value, long limit) {
        if (value.length() > limit) {
            throw invalid();
        }
        long bytes = 0;
        for (int index = 0; index < value.length(); index++) {
            char c = value.charAt(index);
            if (c < 0x80) {
                bytes++;
            } else if (c < 0x800) {
                bytes += 2;
            } else if (Character.isHighSurrogate(c)
                    && index + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(index + 1))) {
                bytes += 4;
                index++;
            } else {
                bytes += Character.isSurrogate(c) ? 1 : 3;
            }
            if (bytes > limit) {
                throw invalid();
            }
        }
        return bytes;
    }

    private void verifySchema(SeaTunnelDataType<?> root) {
        Set<SeaTunnelDataType<?>> active = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<SeaTunnelDataType<?>> finished = Collections.newSetFromMap(new IdentityHashMap<>());
        Deque<Schema> pending = new ArrayDeque<>();
        pending.push(new Schema(root));
        while (!pending.isEmpty()) {
            Schema current = pending.peek();
            if (!current.entered) {
                if (finished.contains(current.type)) {
                    pending.pop();
                    continue;
                }
                if (!active.add(current.type)) {
                    throw new IllegalArgumentException("Cyclic Kinesis payload schema");
                }
                current.entered = true;
                SqlType sql = current.type.getSqlType();
                if (sql != SqlType.ROW && sql != SqlType.ARRAY && sql != SqlType.MAP) {
                    // Scalar-only native construction cannot expand a shared composite schema DAG.
                    scalars.put(
                            current.type, new RowToJsonConverters().createConverter(current.type));
                }
            }
            SeaTunnelDataType<?> child = current.next();
            if (child == null) {
                active.remove(current.type);
                finished.add(current.type);
                pending.pop();
            } else {
                pending.push(new Schema(child));
            }
        }
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("Kinesis payload conversion exceeds the record budget");
    }

    /**
     * One active typed value in an iterative walk. Iterators keep memory proportional to nesting,
     * while completed shared objects are deliberately revisited at their next serialized
     * occurrence.
     */
    private static final class Value {
        private final SeaTunnelDataType<?> type;
        private final Object value;
        // Native map keys use compact node text, unlike plain-decimal payload values.
        private final boolean key;
        private boolean entered;
        private int index;
        private Iterator<? extends Map.Entry<?, ?>> entries;
        // Retained only between admitting an entry's key and admitting its value.
        private Map.Entry<?, ?> entry;
        // Conversion-only state; preflight leaves these unset and allocates no JSON tree.
        private JsonNode node;
        private JsonNode reuse;
        private String fieldName;

        private Value(SeaTunnelDataType<?> type, Object value, boolean key) {
            this.type = type;
            this.value = value;
            this.key = key;
        }

        private Value next(Budget budget) {
            switch (type.getSqlType()) {
                case ROW:
                    SeaTunnelRowType rowType = (SeaTunnelRowType) type;
                    if (index == rowType.getTotalFields()) {
                        return null;
                    }
                    if (budget != null) {
                        budget.take(utf8(rowType.getFieldName(index), budget.remaining));
                    }
                    int field = index++;
                    Value rowChild =
                            new Value(
                                    rowType.getFieldType(field),
                                    ((SeaTunnelRow) value).getField(field),
                                    false);
                    rowChild.fieldName = rowType.getFieldName(field);
                    if (node != null) {
                        rowChild.reuse = ((ObjectNode) node).get(rowChild.fieldName);
                    }
                    return rowChild;
                case ARRAY:
                    Object[] array = (Object[]) value;
                    return index == array.length
                            ? null
                            : new Value(
                                    ((ArrayType<?, ?>) type).getElementType(),
                                    array[index++],
                                    false);
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) type;
                    if (entry != null) {
                        Object next = entry.getValue();
                        entry = null;
                        Value mapChild = new Value(mapType.getValueType(), next, false);
                        mapChild.fieldName = fieldName;
                        if (node != null) {
                            mapChild.reuse = ((ObjectNode) node).get(fieldName);
                        }
                        return mapChild;
                    }
                    if (!entries.hasNext()) {
                        return null;
                    }
                    entry = entries.next();
                    return new Value(mapType.getKeyType(), entry.getKey(), true);
                default:
                    throw invalid();
            }
        }
    }

    /**
     * Detect schema cycles iteratively and construct only native scalar converters. Shared schema
     * branches can be skipped because composite conversion never recursively constructs them.
     */
    private static final class Schema {
        private final SeaTunnelDataType<?> type;
        private boolean entered;
        private int index;

        private Schema(SeaTunnelDataType<?> type) {
            this.type = type;
        }

        private SeaTunnelDataType<?> next() {
            switch (type.getSqlType()) {
                case ROW:
                    SeaTunnelRowType row = (SeaTunnelRowType) type;
                    return index == row.getTotalFields() ? null : row.getFieldType(index++);
                case ARRAY:
                    return index++ == 0 ? ((ArrayType<?, ?>) type).getElementType() : null;
                case MAP:
                    MapType<?, ?> map = (MapType<?, ?>) type;
                    return index++ == 0 ? map.getKeyType() : index == 2 ? map.getValueType() : null;
                default:
                    return null;
            }
        }
    }

    /**
     * Retains one JSON output container and its next child without recursive node serialization.
     * Stack memory follows admitted nesting, never the full width of an input array or map.
     */
    private static final class Output {
        private final JsonNode node;
        private boolean entered;
        private Iterator<Map.Entry<String, JsonNode>> fields;
        private Iterator<JsonNode> elements;

        private Output(JsonNode node) {
            this.node = node;
        }
    }

    /**
     * Conversion work includes overwritten map occurrences, not just the final object size, because
     * native conversion allocates those values before replacing the corresponding field.
     */
    private static final class Budget {
        private long remaining;

        private Budget(int limit) {
            remaining = limit;
        }

        private void take(long size) {
            if (size < 0 || size > remaining) {
                throw invalid();
            }
            remaining -= size;
        }
    }

    /**
     * Exact output admission precedes every byte-array growth. Native node ordering is retained,
     * while the native mapper determines scalar escaping, base64, numbers and temporal values.
     */
    private static final class LimitedOutput extends OutputStream {
        private final ByteArrayOutputStream output;
        private int remaining;

        private LimitedOutput(int limit) {
            if (limit < 0) {
                throw invalid();
            }
            remaining = limit;
            output = new ByteArrayOutputStream(Math.min(limit, 8192));
        }

        @Override
        public void write(int value) throws IOException {
            admit(1);
            output.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int length) throws IOException {
            admit(length);
            output.write(value, offset, length);
        }

        private byte[] bytes() {
            return output.toByteArray();
        }

        private void admit(int length) throws IOException {
            if (length < 0 || length > remaining) {
                throw new IOException("Kinesis encoded payload exceeds the record budget");
            }
            remaining -= length;
        }
    }
}
