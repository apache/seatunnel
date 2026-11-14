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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/** Encodes equality keys separately from snapshot record serialization. */
final class SnapshotKeyEncoder {
    private final Map<Schema, byte[]> schemas = new WeakHashMap<>();
    private final Map<IdentityRef, Long> identities = new HashMap<>();
    private final Map<Long, IdentityRef> representatives = new HashMap<>();
    private final ReferenceQueue<byte[]> collected = new ReferenceQueue<>();
    private final Map<Integer, List<OriginRef>> origins = new HashMap<>();
    private final ReferenceQueue<Struct> collectedOrigins = new ReferenceQueue<>();
    private long nextIdentity;

    public byte[] encode(Struct key) {
        drain();
        byte[] restored = origin(key);
        if (restored != null) return restored;
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            // Kafka 3.2 schema default arrays violate equals/hashCode consistency. A Map also
            // requires the original hash to match, so preserve its actual bucket behavior.
            out.writeInt(key == null ? 0 : key.hashCode());
            out.write(bytes(key, false));
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int liveIdentityCount() {
        drain();
        return identities.size();
    }

    public int schemaCount() {
        return schemas.size();
    }

    private void drain() {
        IdentityRef ref;
        while ((ref = (IdentityRef) collected.poll()) != null) {
            Long token = identities.remove(ref);
            if (token != null && representatives.get(token) == ref) representatives.remove(token);
        }
        OriginRef origin;
        while ((origin = (OriginRef) collectedOrigins.poll()) != null) {
            List<OriginRef> bucket = origins.get(origin.hash);
            if (bucket != null) {
                bucket.remove(origin);
                if (bucket.isEmpty()) origins.remove(origin.hash);
            }
        }
    }

    long identity(byte[] value) {
        drain();
        IdentityRef lookup = new IdentityRef(value, null);
        Long token = identities.get(lookup);
        if (token == null) {
            token = ++nextIdentity;
            IdentityRef ref = new IdentityRef(value, collected);
            identities.put(ref, token);
            representatives.put(token, ref);
        }
        return token;
    }

    byte[] restore(long token, byte[] content) {
        drain();
        IdentityRef ref = representatives.get(token);
        byte[] value = ref == null ? null : ref.get();
        if (value != null) return value;
        ref = new IdentityRef(content, collected);
        identities.put(ref, token);
        representatives.put(token, ref);
        nextIdentity = Math.max(nextIdentity, token);
        return content;
    }

    void restoredKey(Struct key, byte[] encoding) {
        if (key == null) return;
        int hash = key.hashCode();
        origins.computeIfAbsent(hash, ignored -> new ArrayList<>())
                .add(new OriginRef(key, hash, encoding));
    }

    private byte[] origin(Struct key) {
        if (key == null) return null;
        List<OriginRef> bucket = origins.get(key.hashCode());
        if (bucket == null) return null;
        byte[] match = null;
        boolean exact = false;
        for (OriginRef ref : bucket) {
            Struct live = ref.get();
            if (live != null && key.equals(live)) {
                match = ref.encoding;
                if (live == key) {
                    exact = true;
                    break;
                }
            }
        }
        if (match != null && !exact) restoredKey(key, match);
        return match;
    }

    private final class OriginRef extends WeakReference<Struct> {
        private final int hash;
        private final byte[] encoding;

        private OriginRef(Struct value, int hash, byte[] encoding) {
            super(value, collectedOrigins);
            this.hash = hash;
            this.encoding = encoding;
        }
    }

    private byte[] bytes(Object value, boolean identityArrays) {
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buffer);
            write(value, identityArrays, out);
            return buffer.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void block(byte[] value, DataOutputStream out) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private void write(Object value, boolean identityArrays, DataOutputStream out)
            throws IOException {
        if (value == null) {
            out.writeByte(0);
        } else if (value instanceof Struct) {
            Struct struct = (Struct) value;
            out.writeByte(1);
            block(schemaBytes(struct.schema()), out);
            for (Field field : struct.schema().fields()) {
                // Struct.equals uses Arrays.deepEquals on its raw fields, not get(default).
                write(struct.getWithoutDefault(field.name()), false, out);
            }
        } else if (value instanceof byte[]) {
            out.writeByte(identityArrays ? 2 : 3);
            if (identityArrays) {
                // List/Map.equals delegates to byte[].equals, which is object identity.
                out.writeLong(identity((byte[]) value));
            } else {
                block((byte[]) value, out);
            }
        } else if (value instanceof ByteBuffer) {
            out.writeByte(4);
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            byte[] remaining = new byte[buffer.remaining()];
            buffer.get(remaining);
            block(remaining, out);
        } else if (value instanceof List) {
            out.writeByte(5);
            List<?> list = (List<?>) value;
            out.writeInt(list.size());
            for (Object element : list) {
                write(element, true, out);
            }
        } else if (value instanceof Map) {
            out.writeByte(6);
            List<byte[]> entries = new ArrayList<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                DataOutputStream pair = new DataOutputStream(buffer);
                write(entry.getKey(), true, pair);
                write(entry.getValue(), true, pair);
                entries.add(buffer.toByteArray());
            }
            entries.sort(Arrays::compareUnsigned);
            out.writeInt(entries.size());
            for (byte[] entry : entries) {
                block(entry, out);
            }
        } else if (value instanceof String) {
            out.writeByte(7);
            String text = (String) value;
            out.writeInt(text.length());
            byte[] encoded = new byte[Math.multiplyExact(text.length(), 2)];
            for (int i = 0; i < text.length(); i++) {
                char unit = text.charAt(i);
                encoded[2 * i] = (byte) (unit >>> 8);
                encoded[2 * i + 1] = (byte) unit;
            }
            out.write(encoded);
        } else if (value instanceof Boolean) {
            out.writeByte(8);
            out.writeBoolean((Boolean) value);
        } else if (value instanceof Byte) {
            out.writeByte(9);
            out.writeByte((Byte) value);
        } else if (value instanceof Short) {
            out.writeByte(10);
            out.writeShort((Short) value);
        } else if (value instanceof Integer) {
            out.writeByte(11);
            out.writeInt((Integer) value);
        } else if (value instanceof Long) {
            out.writeByte(12);
            out.writeLong((Long) value);
        } else if (value instanceof Float) {
            out.writeByte(13);
            out.writeInt(Float.floatToIntBits((Float) value));
        } else if (value instanceof Double) {
            out.writeByte(14);
            out.writeLong(Double.doubleToLongBits((Double) value));
        } else if (value instanceof BigDecimal) {
            out.writeByte(15);
            BigDecimal decimal = (BigDecimal) value;
            out.writeInt(decimal.scale());
            block(decimal.unscaledValue().toByteArray(), out);
        } else if (value.getClass() == Date.class) {
            out.writeByte(16);
            out.writeLong(((Date) value).getTime());
        } else {
            throw new IllegalArgumentException("Unproven key type: " + value.getClass().getName());
        }
    }

    private byte[] schemaBytes(Schema schema) throws IOException {
        byte[] cached = schemas.get(schema);
        if (cached != null) return cached;
        if (schema.getClass() != ConnectSchema.class) {
            throw new IllegalArgumentException("Unproven key schema class: " + schema.getClass());
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(buffer);
        out.writeInt(schema.type().ordinal());
        out.writeBoolean(schema.isOptional());
        write(schema.name(), false, out);
        write(schema.doc(), false, out);
        write(schema.version(), false, out);
        write(schema.parameters(), false, out);
        write(schema.defaultValue(), false, out);
        if (schema.type() == Schema.Type.STRUCT) {
            out.writeInt(schema.fields().size());
            for (Field field : schema.fields()) {
                write(field.name(), false, out);
                out.writeInt(field.index());
                block(schemaBytes(field.schema()), out);
            }
        } else if (schema.type() == Schema.Type.MAP) {
            block(schemaBytes(schema.keySchema()), out);
            block(schemaBytes(schema.valueSchema()), out);
        } else if (schema.type() == Schema.Type.ARRAY) {
            block(schemaBytes(schema.valueSchema()), out);
        }
        byte[] encoded = buffer.toByteArray();
        if (encoded.length <= 16384) {
            if (schemas.size() >= 64) schemas.remove(schemas.keySet().iterator().next());
            schemas.put(schema, encoded);
        }
        return encoded;
    }

    private static final class IdentityRef extends WeakReference<byte[]> {
        private final int hash;

        private IdentityRef(byte[] referent, ReferenceQueue<byte[]> queue) {
            super(referent, queue);
            hash = System.identityHashCode(referent);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            byte[] referent = get();
            return this == other
                    || other instanceof IdentityRef
                            && referent != null
                            && referent == ((IdentityRef) other).get();
        }
    }
}
