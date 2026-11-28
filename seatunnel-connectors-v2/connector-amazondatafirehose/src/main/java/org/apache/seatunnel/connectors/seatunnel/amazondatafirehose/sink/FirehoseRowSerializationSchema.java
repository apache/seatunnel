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

package org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink;

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
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.amazondatafirehose.sink.AmazonDataFirehoseSinkOptions.Format;
import org.apache.seatunnel.format.csv.CsvSerializationSchema;
import org.apache.seatunnel.format.csv.constant.CsvFormatConstant;
import org.apache.seatunnel.format.json.RowToJsonConverters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Bounds expanded native conversion work before the common serializers allocate an output
 * tree/array. Every occurrence is charged, including shared values and map entries later
 * overwritten by key coercion.
 */
final class FirehoseRowSerializationSchema {
    private final SeaTunnelRowType rowType;
    private final Format format;
    private final int limit;
    private final String[] separators = CsvFormatConstant.SEPARATOR.clone();
    // Only JSON owns a converter/mapper; DELIMITED retains the existing common CSV serializer.
    private final CsvSerializationSchema csv;
    private final ObjectMapper mapper;
    private final RowToJsonConverters converters = new RowToJsonConverters();
    private final Map<SeaTunnelDataType<?>, RowToJsonConverters.RowToJsonConverter> scalars =
            new IdentityHashMap<>();

    FirehoseRowSerializationSchema(
            SeaTunnelRowType type, Format format, String delimiter, int maxBytes) {
        this.rowType = type;
        this.format = format;
        this.limit = maxBytes - 1; // The existing writer appends LF after native serialization.
        separators[0] = delimiter;
        validateSchema(type);
        csv =
                format == Format.DELIMITED
                        ? CsvSerializationSchema.builder()
                                .seaTunnelRowType(type)
                                .delimiter(delimiter)
                                .build()
                        : null;
        mapper =
                format == Format.JSON
                        ? new ObjectMapper()
                                .configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true)
                        : null;
    }

    /**
     * Reject expanded work before conversion; final JSON bytes also go through an exact output cap.
     * CSV conversion is retained after its complete typed preflight, preserving its existing
     * output.
     */
    byte[] serialize(SeaTunnelRow row) throws IOException {
        try {
            preflight(row);
            if (format == Format.DELIMITED) {
                return csv.serialize(row);
            }
            LimitedOutput output = new LimitedOutput(limit);
            writeJson(output, buildJson(row));
            return output.bytes.toByteArray();
        } catch (NativeFailure e) {
            throw new IOException(e.getMessage());
        } catch (IOException | RuntimeException e) {
            // Common format errors can quote the row; never retain their diagnostics or causes.
            throw new IOException(
                    "Amazon Data Firehose Sink could not serialize the native record");
        }
    }

    /*
     * Lazy iterators retain only the typed traversal path, never a copied map/array worklist.
     * Acyclic schema traversal is finite even when input values share references; do not deduplicate them.
     */
    private void preflight(SeaTunnelRow row) {
        Budget budget = new Budget(limit);
        Deque<Iterator<Value>> pending = new ArrayDeque<>();
        pending.push(Collections.singletonList(new Value(rowType, row, -1)).iterator());
        while (!pending.isEmpty()) {
            Iterator<Value> next = pending.peek();
            if (!next.hasNext()) {
                pending.pop();
                continue;
            }
            Value value = next.next();
            budget.work();
            if (value.data == null) {
                budget.take(format == Format.JSON ? 4 : 0);
                continue;
            }
            switch (value.type.getSqlType()) {
                case ROW:
                    separatorBytes(value.level + 1);
                    SeaTunnelRowType type = (SeaTunnelRowType) value.type;
                    SeaTunnelRow fields = (SeaTunnelRow) value.data;
                    if ((format == Format.JSON && fields.getArity() < type.getTotalFields())
                            || (format == Format.DELIMITED
                                    && (fields.getArity() > type.getTotalFields()
                                            || (value.level == -1
                                                    && fields.getArity()
                                                            != type.getTotalFields())))) {
                        throw invalid();
                    }
                    budget.take(format == Format.JSON ? 2 : 0);
                    pending.push(
                            new Iterator<Value>() {
                                private int index;

                                public boolean hasNext() {
                                    return index
                                            < (format == Format.DELIMITED
                                                    ? fields.getArity()
                                                    : type.getTotalFields());
                                }

                                public Value next() {
                                    int i = index++;
                                    budget.take(i == 0 ? 0 : separatorBytes(value.level + 1));
                                    if (format == Format.JSON) {
                                        String name = type.getFieldName(i);
                                        if (name.length() > budget.remaining) {
                                            throw invalid();
                                        }
                                        budget.take(3L + utf8(name));
                                    }
                                    return new Value(
                                            type.getFieldType(i),
                                            fields.getField(i),
                                            value.level + 1);
                                }
                            });
                    break;
                case ARRAY:
                    separatorBytes(value.level + 1);
                    Object[] array = (Object[]) value.data;
                    SeaTunnelDataType<?> element = ((ArrayType<?, ?>) value.type).getElementType();
                    budget.take(format == Format.JSON ? 2 : 0);
                    pending.push(
                            new Iterator<Value>() {
                                private int index;

                                public boolean hasNext() {
                                    return index < array.length;
                                }

                                public Value next() {
                                    int i = index++;
                                    budget.take(i == 0 ? 0 : separatorBytes(value.level + 1));
                                    return new Value(element, array[i], value.level + 1);
                                }
                            });
                    break;
                case MAP:
                    separatorBytes(value.level + 1);
                    separatorBytes(value.level + 2);
                    MapType<?, ?> mapType = (MapType<?, ?>) value.type;
                    Iterator<? extends Map.Entry<?, ?>> entries =
                            ((Map<?, ?>) value.data).entrySet().iterator();
                    budget.take(format == Format.JSON ? 2 : 0);
                    pending.push(
                            new Iterator<Value>() {
                                private Map.Entry<?, ?> entry;
                                private boolean key;
                                private boolean first = true;

                                public boolean hasNext() {
                                    return key || entries.hasNext();
                                }

                                public Value next() {
                                    if (key) {
                                        key = false;
                                        return new Value(
                                                mapType.getValueType(),
                                                entry.getValue(),
                                                value.level + 1);
                                    }
                                    budget.take(first ? 0 : separatorBytes(value.level + 1));
                                    first = false;
                                    entry = entries.next();
                                    if (format == Format.JSON) {
                                        JsonNode name =
                                                scalar(
                                                        mapType.getKeyType(),
                                                        entry.getKey(),
                                                        budget,
                                                        true);
                                        if (name == null) {
                                            throw invalid();
                                        }
                                        String text =
                                                name.isTextual() ? name.asText() : name.toString();
                                        budget.take(3L + utf8(text));
                                        return new Value(
                                                mapType.getValueType(),
                                                entry.getValue(),
                                                value.level + 1);
                                    }
                                    budget.take(separatorBytes(value.level + 2));
                                    key = true;
                                    return new Value(
                                            mapType.getKeyType(), entry.getKey(), value.level + 1);
                                }
                            });
                    break;
                default:
                    if (format == Format.JSON) {
                        budget.take(jsonSize(scalar(value.type, value.data, budget, false)));
                    } else {
                        budget.take(csvSize(value.type, value.data, budget));
                    }
            }
        }
    }

    private int separatorBytes(int level) {
        if (format == Format.JSON) {
            return 1;
        }
        if (level >= separators.length) {
            throw invalid();
        }
        return separators[level].getBytes(StandardCharsets.UTF_8).length;
    }

    /* Inspect numeric expansion before a node's textual rendering can allocate a huge plain number. */
    private JsonNode scalar(SeaTunnelDataType<?> type, Object data, Budget budget, boolean key) {
        if (data == null) {
            return mapper.nullNode();
        }
        if (type.getSqlType() == SqlType.NULL) {
            return null;
        }
        if (data instanceof CharSequence && ((CharSequence) data).length() > budget.remaining) {
            throw invalid();
        }
        if (data instanceof byte[] && ((byte[]) data).length > budget.remaining) {
            throw invalid();
        }
        JsonNode node =
                scalars.computeIfAbsent(type, converters::createConverter)
                        .convert(mapper, null, data);
        if (node != null && node.isBigDecimal()) {
            BigDecimal number = node.decimalValue();
            checkInteger(number.unscaledValue(), budget.remaining);
            if (key) {
                if (number.precision() > budget.remaining) {
                    throw invalid();
                }
            } else if (plainSize(number) > budget.remaining) {
                throw invalid();
            }
        }
        if (node != null && node.isBigInteger()) {
            checkInteger(node.bigIntegerValue(), budget.remaining);
        }
        if (data instanceof CharSequence) {
            long raw = utf8((CharSequence) data);
            long rendered =
                    key ? utf8(node.isTextual() ? node.asText() : node.toString()) : jsonSize(node);
            // Numeric text can normalize to fewer output characters but still costs parsing work.
            budget.take(Math.max(0, raw - rendered));
        }
        return node;
    }

    private static long jsonSize(JsonNode node) {
        if (node == null || node.isNull()) {
            return 4;
        }
        if (node.isTextual()) {
            return 2L + utf8(node.asText());
        }
        if (node.isBinary()) {
            try {
                return 2L + 4L * ((node.binaryValue().length + 2L) / 3L);
            } catch (IOException e) {
                throw invalid();
            }
        }
        if (node.isBigDecimal()) {
            return plainSize(node.decimalValue());
        }
        return utf8(node.toString());
    }

    /*
     * Match the common CSV scalar formatting without allocating its aggregate joined result.
     * String quoting only expands the charged content; the existing final exact-byte check is retained.
     */
    private static long csvSize(SeaTunnelDataType<?> type, Object value, Budget budget) {
        switch (type.getSqlType()) {
            case NULL:
                return 0;
            case STRING:
                String text = (String) value;
                if (text.length() > budget.remaining) {
                    throw invalid();
                }
                return utf8(text);
            case BYTES:
                return ((byte[]) value).length;
            case DECIMAL:
                BigDecimal number = ((BigDecimal) value).stripTrailingZeros();
                checkInteger(number.unscaledValue(), budget.remaining);
                return plainSize(number);
            case DATE:
                return utf8(DateUtils.toString((LocalDate) value, DateUtils.Formatter.YYYY_MM_DD));
            case TIME:
                return utf8(TimeUtils.toString((LocalTime) value, TimeUtils.Formatter.HH_MM_SS));
            case TIMESTAMP:
                return utf8(
                        DateTimeUtils.toString(
                                (LocalDateTime) value,
                                DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS));
            case TIMESTAMP_TZ:
                return utf8(
                        ((OffsetDateTime) value).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            default:
                if (value instanceof CharSequence
                        && ((CharSequence) value).length() > budget.remaining) {
                    throw invalid();
                }
                if (value instanceof BigInteger) {
                    checkInteger((BigInteger) value, budget.remaining);
                }
                if (value instanceof BigDecimal) {
                    checkInteger(((BigDecimal) value).unscaledValue(), budget.remaining);
                    if (((BigDecimal) value).precision() > budget.remaining) {
                        throw invalid();
                    }
                }
                if (!(value instanceof Number
                        || value instanceof Boolean
                        || value instanceof CharSequence)) {
                    throw invalid();
                }
                return utf8(value.toString());
        }
    }

    private static void checkInteger(BigInteger value, long remaining) {
        // A decimal-digit lower bound precedes precision/String allocation and cannot overflow.
        if (Math.max(1L, (value.bitLength() - 1L) * 301L / 1000L) > remaining) {
            throw invalid();
        }
    }

    private static long plainSize(BigDecimal value) {
        long scale = value.scale();
        long precision = value.precision();
        long size =
                scale <= 0
                        ? (value.signum() == 0 ? 1 : precision - scale)
                        : Math.max(precision + 1, scale + 2);
        return size + (value.signum() < 0 ? 1 : 0);
    }

    private static long utf8(CharSequence value) {
        long size = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                size++;
            } else if (c < 0x800) {
                size += 2;
            } else if (Character.isHighSurrogate(c)
                    && i + 1 < value.length()
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                size += 4;
                i++;
            } else if (Character.isSurrogate(c)) {
                size++;
            } else {
                size += 3;
            }
        }
        return size;
    }

    /* Only scalar converters are constructed; shared composite schemas never expand a converter DAG. */
    private void validateSchema(SeaTunnelRowType type) {
        Deque<Schema> pending = new ArrayDeque<>();
        Map<SeaTunnelDataType<?>, Boolean> path = new IdentityHashMap<>();
        Map<SeaTunnelDataType<?>, Boolean> finished = new IdentityHashMap<>();
        pending.push(new Schema(type));
        while (!pending.isEmpty()) {
            Schema current = pending.peek();
            if (!current.entered) {
                if (path.put(current.type, Boolean.TRUE) != null) {
                    throw new IllegalArgumentException("Firehose native schema is cyclic");
                }
                if (format == Format.JSON
                        && current.type.getSqlType() != SqlType.ROW
                        && current.type.getSqlType() != SqlType.ARRAY
                        && current.type.getSqlType() != SqlType.MAP) {
                    // Scalar-only construction cannot recursively expand the schema graph.
                    scalars.put(current.type, converters.createConverter(current.type));
                }
                current.entered = true;
            }
            SeaTunnelDataType<?> child = current.next();
            if (child == null) {
                path.remove(current.type);
                finished.put(current.type, Boolean.TRUE);
                pending.pop();
            } else if (!finished.containsKey(child)) {
                pending.push(new Schema(child));
            }
        }
    }

    /* Build only the preflight-bounded tree, retaining native last-key-wins and insertion order. */
    private JsonNode buildJson(SeaTunnelRow row) {
        JsonNode[] result = new JsonNode[1];
        Deque<Iterator<Build>> pending = new ArrayDeque<>();
        pending.push(
                Collections.singletonList(new Build(rowType, row, node -> result[0] = node))
                        .iterator());
        Budget budget = new Budget(limit);
        while (!pending.isEmpty()) {
            Iterator<Build> next = pending.peek();
            if (!next.hasNext()) {
                pending.pop();
                continue;
            }
            Build value = next.next();
            budget.work();
            if (value.data == null || value.type.getSqlType() == SqlType.NULL) {
                budget.take(4);
                value.assign.accept(mapper.nullNode());
                continue;
            }
            switch (value.type.getSqlType()) {
                case ROW:
                    SeaTunnelRowType type = (SeaTunnelRowType) value.type;
                    SeaTunnelRow fields = (SeaTunnelRow) value.data;
                    ObjectNode object = mapper.createObjectNode();
                    budget.take(2);
                    value.assign.accept(object);
                    pending.push(
                            new Iterator<Build>() {
                                private int index;

                                public boolean hasNext() {
                                    return index < type.getTotalFields();
                                }

                                public Build next() {
                                    int i = index++;
                                    String name = type.getFieldName(i);
                                    budget.take(3L + utf8(name) + (i == 0 ? 0 : 1));
                                    return new Build(
                                            type.getFieldType(i),
                                            fields.getField(i),
                                            node -> object.set(name, node));
                                }
                            });
                    break;
                case ARRAY:
                    Object[] array = (Object[]) value.data;
                    SeaTunnelDataType<?> element = ((ArrayType<?, ?>) value.type).getElementType();
                    ArrayNode nodes = mapper.createArrayNode();
                    budget.take(2);
                    value.assign.accept(nodes);
                    pending.push(
                            new Iterator<Build>() {
                                private int index;

                                public boolean hasNext() {
                                    return index < array.length;
                                }

                                public Build next() {
                                    int i = index++;
                                    budget.take(i == 0 ? 0 : 1);
                                    return new Build(element, array[i], nodes::add);
                                }
                            });
                    break;
                case MAP:
                    MapType<?, ?> mapType = (MapType<?, ?>) value.type;
                    Iterator<? extends Map.Entry<?, ?>> entries =
                            ((Map<?, ?>) value.data).entrySet().iterator();
                    ObjectNode map = mapper.createObjectNode();
                    budget.take(2);
                    value.assign.accept(map);
                    pending.push(
                            new Iterator<Build>() {
                                private boolean first = true;

                                public boolean hasNext() {
                                    return entries.hasNext();
                                }

                                public Build next() {
                                    Map.Entry<?, ?> entry = entries.next();
                                    JsonNode key =
                                            scalar(
                                                    mapType.getKeyType(),
                                                    entry.getKey(),
                                                    budget,
                                                    true);
                                    if (key == null) {
                                        throw invalid();
                                    }
                                    String name = key.isTextual() ? key.asText() : key.toString();
                                    budget.take(3L + utf8(name) + (first ? 0 : 1));
                                    first = false;
                                    return new Build(
                                            mapType.getValueType(),
                                            entry.getValue(),
                                            node -> map.set(name, node));
                                }
                            });
                    break;
                default:
                    JsonNode node = scalar(value.type, value.data, budget, false);
                    budget.take(jsonSize(node));
                    value.assign.accept(node);
            }
        }
        return result[0];
    }

    /* Container emission is iterative too: mapper.writeValue(tree) would recurse on deep valid rows. */
    private void writeJson(OutputStream output, JsonNode root) throws IOException {
        try (JsonGenerator generator = mapper.getFactory().createGenerator(output)) {
            Deque<NodeFrame> pending = new ArrayDeque<>();
            JsonNode node = root;
            while (true) {
                if (node != null) {
                    if (node.isObject()) {
                        generator.writeStartObject();
                        pending.push(new NodeFrame(node));
                    } else if (node.isArray()) {
                        generator.writeStartArray();
                        pending.push(new NodeFrame(node));
                    } else {
                        mapper.writeTree(generator, node);
                    }
                }
                node = null;
                while (!pending.isEmpty()) {
                    NodeFrame frame = pending.peek();
                    if (frame.children.hasNext()) {
                        if (frame.object) {
                            Map.Entry<String, JsonNode> field = frame.fields.next();
                            generator.writeFieldName(field.getKey());
                            node = field.getValue();
                        } else {
                            node = frame.elements.next();
                        }
                        break;
                    }
                    if (frame.object) {
                        generator.writeEndObject();
                    } else {
                        generator.writeEndArray();
                    }
                    pending.pop();
                }
                if (node == null && pending.isEmpty()) {
                    break;
                }
            }
        }
    }

    private static NativeFailure invalid() {
        return new NativeFailure(
                "Amazon Data Firehose Sink native conversion exceeds max_record_bytes or has an invalid value");
    }

    /**
     * Carries a typed value and its existing CSV nesting level without copying container contents.
     */
    private static final class Value {
        private final SeaTunnelDataType<?> type;
        private final Object data;
        private final int level;

        private Value(SeaTunnelDataType<?> type, Object data, int level) {
            this.type = type;
            this.data = data;
            this.level = level;
        }
    }

    /**
     * Tracks the schema path lazily for cycle detection without expanding shared composite schemas.
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
                    return index < row.getTotalFields() ? checked(row.getFieldType(index++)) : null;
                case ARRAY:
                    return index++ == 0 ? checked(((ArrayType<?, ?>) type).getElementType()) : null;
                case MAP:
                    MapType<?, ?> map = (MapType<?, ?>) type;
                    return index++ == 0 ? map.getKeyType() : index == 2 ? map.getValueType() : null;
                default:
                    return null;
            }
        }

        private SeaTunnelDataType<?> checked(SeaTunnelDataType<?> child) {
            if (child == null) {
                throw new IllegalArgumentException("Firehose native schema has a null type");
            }
            return child;
        }
    }

    /**
     * One pending native JSON assignment, retaining shared input occurrences without recursive
     * calls.
     */
    private static final class Build {
        private final SeaTunnelDataType<?> type;
        private final Object data;
        private final Consumer<JsonNode> assign;

        private Build(SeaTunnelDataType<?> type, Object data, Consumer<JsonNode> assign) {
            this.type = type;
            this.data = data;
            this.assign = assign;
        }
    }

    /**
     * Iterates one already bounded native JSON container without invoking recursive node
     * serialization.
     */
    private static final class NodeFrame {
        private final boolean object;
        private final Iterator<Map.Entry<String, JsonNode>> fields;
        private final Iterator<JsonNode> elements;
        private final Iterator<?> children;

        private NodeFrame(JsonNode node) {
            object = node.isObject();
            fields = object ? node.fields() : null;
            elements = object ? null : node.elements();
            children = object ? fields : elements;
        }
    }

    /**
     * Consumes both content and structural conversion work for every occurrence; shared input
     * identities never bypass admission or permit overwritten map entries to escape the budget.
     */
    private static final class Budget {
        private long remaining;
        private long work;

        private Budget(long remaining) {
            this.remaining = remaining;
            this.work = remaining;
        }

        private void work() {
            if (--work < 0) {
                throw invalid();
            }
        }

        private void take(long count) {
            if (count < 0 || count > remaining) {
                throw invalid();
            }
            remaining -= count;
        }
    }

    /**
     * Keeps the exact final native JSON output within the record budget before growing its buffer.
     */
    private static final class LimitedOutput extends OutputStream {
        private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        private final int limit;

        private LimitedOutput(int limit) {
            this.limit = limit;
        }

        @Override
        public void write(int value) throws IOException {
            require(1);
            bytes.write(value);
        }

        @Override
        public void write(byte[] value, int offset, int count) throws IOException {
            require(count);
            bytes.write(value, offset, count);
        }

        private void require(int count) {
            if (count > limit - bytes.size()) {
                throw invalid();
            }
        }
    }

    /**
     * Carries sanitized internal control flow across bounded traversal/output without retaining row
     * values, rendered map keys, or native formatter exception causes in the public failure chain.
     */
    private static final class NativeFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private NativeFailure(String message) {
            super(message);
        }
    }
}
