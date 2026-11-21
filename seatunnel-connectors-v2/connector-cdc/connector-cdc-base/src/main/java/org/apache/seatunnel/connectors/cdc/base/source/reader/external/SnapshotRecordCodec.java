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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Lossless transient record encoding with explicit types and record-local schema references. */
final class SnapshotRecordCodec {
    private final SnapshotKeyEncoder keys;
    private final Map<Schema, Integer> writtenSchemas = new IdentityHashMap<>();
    private final List<Schema> readSchemas = new ArrayList<>();
    private final Map<byte[], Integer> writtenArrays = new IdentityHashMap<>();
    private final List<byte[]> readArrays = new ArrayList<>();

    public SnapshotRecordCodec() {
        this(new SnapshotKeyEncoder());
    }

    public SnapshotRecordCodec(SnapshotKeyEncoder keys) {
        this.keys = keys;
    }

    public byte[] encode(SourceRecord record) throws IOException {
        writtenSchemas.clear();
        writtenArrays.clear();
        try {
            return encodeRecord(record);
        } finally {
            writtenSchemas.clear();
            writtenArrays.clear();
        }
    }

    private byte[] encodeRecord(SourceRecord record) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        block(keys.encode((Struct) record.key()), out);
        value(record.sourcePartition(), out);
        value(record.sourceOffset(), out);
        value(record.topic(), out);
        value(record.kafkaPartition(), out);
        schema(record.keySchema(), out);
        value(record.key(), out);
        schema(record.valueSchema(), out);
        value(record.value(), out);
        value(record.timestamp(), out);
        out.writeInt(record.headers().size());
        for (Header header : record.headers()) {
            value(header.key(), out);
            schema(header.schema(), out);
            value(header.value(), out);
        }
        return buffer.toByteArray();
    }

    @SuppressWarnings("unchecked")
    public SourceRecord decode(byte[] bytes) throws IOException {
        readSchemas.clear();
        readArrays.clear();
        try {
            return decodeRecord(bytes);
        } finally {
            readSchemas.clear();
            readArrays.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private SourceRecord decodeRecord(byte[] bytes) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
        byte[] originalKey = block(in);
        Map<String, ?> partition = (Map<String, ?>) value(in);
        Map<String, ?> offset = (Map<String, ?>) value(in);
        String topic = (String) value(in);
        Integer kafkaPartition = (Integer) value(in);
        Schema keySchema = schema(in);
        Object key = value(in);
        keys.restoredKey((Struct) key, originalKey);
        Schema valueSchema = schema(in);
        Object value = value(in);
        Long timestamp = (Long) value(in);
        ConnectHeaders headers = new ConnectHeaders();
        int count = size(in);
        for (int i = 0; i < count; i++) {
            String name = (String) value(in);
            Schema headerSchema = schema(in);
            headers.add(name, value(in), headerSchema);
        }
        if (in.available() != 0) throw new IOException("Trailing record bytes");
        return new SourceRecord(
                partition,
                offset,
                topic,
                kafkaPartition,
                keySchema,
                key,
                valueSchema,
                value,
                timestamp,
                headers);
    }

    private void block(byte[] value, DataOutputStream out) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private int size(DataInputStream in) throws IOException {
        int size = in.readInt();
        if (size < 0 || size > in.available())
            throw new IOException("Invalid block/count: " + size);
        return size;
    }

    private byte[] block(DataInputStream in) throws IOException {
        byte[] bytes = new byte[size(in)];
        in.readFully(bytes);
        return bytes;
    }

    private void schema(Schema schema, DataOutputStream out) throws IOException {
        if (schema == null) {
            out.writeInt(0);
            return;
        }
        Integer existing = writtenSchemas.get(schema);
        if (existing != null) {
            out.writeInt(existing + 1);
            return;
        }
        int id = writtenSchemas.size();
        writtenSchemas.put(schema, id);
        out.writeInt(-id - 1);
        out.writeByte(schema.type().ordinal());
        out.writeBoolean(schema.isOptional());
        value(schema.name(), out);
        value(schema.version(), out);
        value(schema.doc(), out);
        value(schema.parameters(), out);
        value(schema.defaultValue(), out);
        if (schema.type() == Schema.Type.STRUCT) {
            out.writeInt(schema.fields().size());
            for (Field field : schema.fields()) {
                value(field.name(), out);
                schema(field.schema(), out);
            }
        } else if (schema.type() == Schema.Type.MAP) {
            schema(schema.keySchema(), out);
            schema(schema.valueSchema(), out);
        } else if (schema.type() == Schema.Type.ARRAY) {
            schema(schema.valueSchema(), out);
        }
    }

    @SuppressWarnings("unchecked")
    private Schema schema(DataInputStream in) throws IOException {
        int marker = in.readInt();
        if (marker == 0) return null;
        if (marker > 0) {
            if (marker > readSchemas.size() || readSchemas.get(marker - 1) == null)
                throw new IOException("Invalid/cyclic schema reference");
            return readSchemas.get(marker - 1);
        }
        int id = -marker - 1;
        if (id != readSchemas.size()) throw new IOException("Invalid schema identifier");
        readSchemas.add(null);
        int ordinal = in.readUnsignedByte();
        if (ordinal >= Schema.Type.values().length) throw new IOException("Invalid schema type");
        Schema.Type type = Schema.Type.values()[ordinal];
        boolean optional = in.readBoolean();
        String name = (String) value(in);
        Integer version = (Integer) value(in);
        String doc = (String) value(in);
        Map<String, String> parameters = (Map<String, String>) value(in);
        Object defaultValue = value(in);
        List<Field> fields = null;
        Schema keySchema = null;
        Schema valueSchema = null;
        if (type == Schema.Type.STRUCT) {
            fields = new ArrayList<>();
            int count = size(in);
            for (int i = 0; i < count; i++) {
                fields.add(new Field((String) value(in), i, schema(in)));
            }
        } else if (type == Schema.Type.MAP) {
            keySchema = schema(in);
            valueSchema = schema(in);
        } else if (type == Schema.Type.ARRAY) {
            valueSchema = schema(in);
        }
        Schema result =
                new ConnectSchema(
                        type,
                        optional,
                        defaultValue,
                        name,
                        version,
                        doc,
                        parameters,
                        fields,
                        keySchema,
                        valueSchema);
        readSchemas.set(id, result);
        return result;
    }

    private void value(Object value, DataOutputStream out) throws IOException {
        if (value == null) out.writeByte(0);
        else if (value instanceof Struct) {
            out.writeByte(1);
            Struct struct = (Struct) value;
            schema(struct.schema(), out);
            for (Field field : struct.schema().fields())
                value(struct.getWithoutDefault(field.name()), out);
        } else if (value instanceof byte[]) {
            out.writeByte(2);
            Integer existing = writtenArrays.get(value);
            if (existing != null) out.writeInt(existing + 1);
            else {
                int id = writtenArrays.size();
                writtenArrays.put((byte[]) value, id);
                out.writeInt(-id - 1);
                out.writeLong(keys.identity((byte[]) value));
                block((byte[]) value, out);
            }
        } else if (value instanceof ByteBuffer) {
            out.writeByte(3);
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            // Preserve all observable buffer positions/content, including sliced buffers.
            int position = buffer.position();
            int limit = buffer.limit();
            buffer.clear();
            byte[] bytes = new byte[buffer.capacity()];
            buffer.get(bytes);
            block(bytes, out);
            out.writeInt(position);
            out.writeInt(limit);
            out.writeBoolean(((ByteBuffer) value).isReadOnly());
            out.writeBoolean(((ByteBuffer) value).order() == java.nio.ByteOrder.LITTLE_ENDIAN);
        } else if (value instanceof List) {
            out.writeByte(4);
            out.writeInt(((List<?>) value).size());
            for (Object element : (List<?>) value) value(element, out);
        } else if (value instanceof Map) {
            out.writeByte(5);
            out.writeInt(((Map<?, ?>) value).size());
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                value(entry.getKey(), out);
                value(entry.getValue(), out);
            }
        } else if (value instanceof String) {
            out.writeByte(6);
            String text = (String) value;
            out.writeInt(text.length());
            boolean latin1 = true;
            for (int i = 0; i < text.length(); i++) {
                if (text.charAt(i) > 255) {
                    latin1 = false;
                    break;
                }
            }
            out.writeBoolean(latin1);
            if (!latin1 && text.length() > Integer.MAX_VALUE / 2)
                throw new IOException("String too large");
            byte[] encoded = new byte[text.length() * (latin1 ? 1 : 2)];
            for (int i = 0; i < text.length(); i++) {
                char unit = text.charAt(i);
                if (latin1) encoded[i] = (byte) unit;
                else {
                    encoded[2 * i] = (byte) (unit >>> 8);
                    encoded[2 * i + 1] = (byte) unit;
                }
            }
            out.write(encoded);
        } else if (value instanceof Boolean) {
            out.writeByte(7);
            out.writeBoolean((Boolean) value);
        } else if (value instanceof Byte) {
            out.writeByte(8);
            out.writeByte((Byte) value);
        } else if (value instanceof Short) {
            out.writeByte(9);
            out.writeShort((Short) value);
        } else if (value instanceof Integer) {
            out.writeByte(10);
            out.writeInt((Integer) value);
        } else if (value instanceof Long) {
            out.writeByte(11);
            out.writeLong((Long) value);
        } else if (value instanceof Float) {
            out.writeByte(12);
            out.writeInt(Float.floatToRawIntBits((Float) value));
        } else if (value instanceof Double) {
            out.writeByte(13);
            out.writeLong(Double.doubleToRawLongBits((Double) value));
        } else if (value instanceof BigDecimal) {
            out.writeByte(14);
            BigDecimal decimal = (BigDecimal) value;
            out.writeInt(decimal.scale());
            block(decimal.unscaledValue().toByteArray(), out);
        } else if (value.getClass() == Date.class) {
            out.writeByte(15);
            out.writeLong(((Date) value).getTime());
        } else if (value instanceof BigInteger) {
            out.writeByte(16);
            block(((BigInteger) value).toByteArray(), out);
        } else
            throw new IOException(
                    "Unsupported snapshot record type: " + value.getClass().getName());
    }

    private Object value(DataInputStream in) throws IOException {
        switch (in.readUnsignedByte()) {
            case 0:
                return null;
            case 1:
                Schema schema = schema(in);
                Struct struct = new Struct(schema);
                for (Field field : schema.fields()) struct.put(field, value(in));
                return struct;
            case 2:
                int marker = in.readInt();
                if (marker > 0) {
                    if (marker > readArrays.size())
                        throw new IOException("Invalid array reference");
                    return readArrays.get(marker - 1);
                }
                if (-marker - 1 != readArrays.size())
                    throw new IOException("Invalid array identifier");
                long token = in.readLong();
                byte[] bytes = keys.restore(token, block(in));
                readArrays.add(bytes);
                return bytes;
            case 3:
                ByteBuffer buffer = ByteBuffer.wrap(block(in));
                int position = in.readInt();
                int limit = in.readInt();
                buffer.limit(limit).position(position);
                if (in.readBoolean()) buffer = buffer.asReadOnlyBuffer();
                buffer.order(
                        in.readBoolean()
                                ? java.nio.ByteOrder.LITTLE_ENDIAN
                                : java.nio.ByteOrder.BIG_ENDIAN);
                return buffer;
            case 4:
                List<Object> list = new ArrayList<>();
                int count = size(in);
                for (int i = 0; i < count; i++) list.add(value(in));
                return list;
            case 5:
                Map<Object, Object> map = new LinkedHashMap<>();
                int entries = size(in);
                for (int i = 0; i < entries; i++) map.put(value(in), value(in));
                return map;
            case 6:
                int length = size(in);
                boolean latin1 = in.readBoolean();
                long encodedLength = (long) length * (latin1 ? 1 : 2);
                if (encodedLength > in.available()) throw new IOException("Truncated string");
                byte[] encoded = new byte[(int) encodedLength];
                in.readFully(encoded);
                if (latin1)
                    return new String(encoded, java.nio.charset.StandardCharsets.ISO_8859_1);
                char[] units = new char[length];
                for (int i = 0; i < length; i++)
                    units[i] = (char) ((encoded[2 * i] & 255) << 8 | encoded[2 * i + 1] & 255);
                return new String(units);
            case 7:
                return in.readBoolean();
            case 8:
                return in.readByte();
            case 9:
                return in.readShort();
            case 10:
                return in.readInt();
            case 11:
                return in.readLong();
            case 12:
                return Float.intBitsToFloat(in.readInt());
            case 13:
                return Double.longBitsToDouble(in.readLong());
            case 14:
                int scale = in.readInt();
                return new BigDecimal(new BigInteger(block(in)), scale);
            case 15:
                return new Date(in.readLong());
            case 16:
                return new BigInteger(block(in));
            default:
                throw new IOException("Invalid value tag");
        }
    }
}
