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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotBufferTest {
    private static final Schema KEY =
            SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    private static final Schema VALUE =
            SchemaBuilder.struct().field("payload", Schema.STRING_SCHEMA).build();

    @Test
    void rejectsCorruptedFrameLengthBeforeAllocation() throws Exception {
        try (DiskSnapshotMap store = new DiskSnapshotMap(1)) {
            int largest = 0;
            for (int i = 0; i < 64; i++) {
                SourceRecord record = record(i, "frame");
                largest = Math.max(largest, new SnapshotRecordCodec().encode(record).length);
                store.put((Struct) record.key(), record);
            }
            store.flush();
            int corruptedLength = largest * 4;
            assertTrue(corruptedLength < store.data.length() - 5);
            store.data.seek(1);
            store.data.writeInt(corruptedLength);
            java.lang.reflect.Field pages = DiskSnapshotMap.class.getDeclaredField("dataPages");
            pages.setAccessible(true);
            ((BoundedSnapshotFile) pages.get(store)).invalidateCleanPages();
            UncheckedIOException failure =
                    assertThrows(UncheckedIOException.class, () -> store.get(key(0)));
            assertEquals("Invalid snapshot record length", failure.getCause().getMessage());
        }
    }

    @Test
    void spillsAtRecordLimitAndPreservesReconciliationOrder() throws Exception {
        SnapshotBuffer buffer = new SnapshotBuffer(2, Long.MAX_VALUE);
        Path directory;
        try {
            SourceRecord first = record(1, "first");
            SourceRecord second = record(2, "second");
            assertNull(buffer.put((Struct) first.key(), first));
            buffer.put((Struct) second.key(), second);
            assertNull(buffer.spillDirectory());
            assertSame(first, buffer.get(key(1)));

            SourceRecord updated = record(2, "updated");
            assertSame(second, buffer.put(key(2), updated));
            assertNull(buffer.spillDirectory());
            buffer.put(key(3), record(3, "third"));
            directory = buffer.spillDirectory();
            assertNotNull(directory);
            assertTrue(Files.exists(directory));
            assertEquals(Arrays.asList(1, 2, 3), ids(buffer));
            assertEquals("updated", payload(buffer.remove(key(2))));
            assertNull(buffer.get(key(2)));
            buffer.put(key(2), record(2, "reinserted"));
            assertEquals(Arrays.asList(1, 3, 2), ids(buffer));
            assertEquals("reinserted", payload(buffer.get(key(2))));
            assertEquals(3, buffer.size());
            buffer.seal();
        } finally {
            directory = buffer.spillDirectory();
            buffer.close();
        }
        assertFalse(Files.exists(directory));
        buffer.close();
        assertThrows(IllegalStateException.class, buffer::size);
    }

    @Test
    void cumulativeEncodedByteLimitAlsoBoundsRepeatedReplacements() throws Exception {
        SourceRecord record = record(1, "payload");
        int encodedSize = new SnapshotRecordCodec().encode(record).length;
        Path directory;
        try (SnapshotBuffer buffer = new SnapshotBuffer(10, encodedSize)) {
            buffer.put(key(1), record);
            assertNull(buffer.spillDirectory());
            buffer.put(key(1), record);
            directory = buffer.spillDirectory();
            assertNotNull(directory);
            assertEquals(Collections.singletonList(1), ids(buffer));
            assertEquals("payload", payload(buffer.get(key(1))));
        }
        assertFalse(Files.exists(directory));
    }

    @Test
    void failedMigrationRetainsOriginalMemoryAndCanBeRetried() throws Exception {
        Map<String, Object> offset = new HashMap<>();
        offset.put("pos", 1L);
        SourceRecord original =
                new SourceRecord(
                        Collections.emptyMap(),
                        offset,
                        "topic",
                        KEY,
                        key(1),
                        VALUE,
                        new Struct(VALUE).put("payload", "first"));
        try (SnapshotBuffer buffer = new SnapshotBuffer(1, Long.MAX_VALUE)) {
            buffer.put(key(1), original);
            // Inject a copy/codec failure after initial admission, not a supported producer shape.
            offset.put("unsupported", new Object());
            assertThrows(UncheckedIOException.class, () -> buffer.put(key(2), record(2, "second")));
            assertNull(buffer.spillDirectory());
            assertEquals(1, buffer.size());
            assertSame(original, buffer.get(key(1)));
            assertEquals(Collections.singletonList(1), ids(buffer));

            offset.remove("unsupported");
            buffer.put(key(2), record(2, "second"));
            assertNotNull(buffer.spillDirectory());
            assertEquals(Arrays.asList(1, 2), ids(buffer));
        }
    }

    @Test
    void spillPreservesRawDefaultsIdentityKeysBuffersAndRecordMetadata() throws Exception {
        Schema nullable = SchemaBuilder.string().optional().defaultValue("DEFAULT").build();
        Schema child = SchemaBuilder.struct().field("raw", nullable).build();
        Schema array =
                SchemaBuilder.array(child)
                        .optional()
                        .defaultValue(Collections.singletonList(new Struct(child).put("raw", null)))
                        .build();
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name("value")
                        .version(2)
                        .doc("metadata")
                        .parameter("source", "test")
                        .field("raw", nullable)
                        .field("children", array)
                        .field("bytes", Schema.BYTES_SCHEMA)
                        .field("latin1", Schema.STRING_SCHEMA)
                        .field("unicode", Schema.STRING_SCHEMA)
                        .build();
        Schema keySchema =
                SchemaBuilder.struct()
                        .field("parts", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
                        .build();
        byte[] shared = {1, 2};
        Struct originalKey = new Struct(keySchema).put("parts", Collections.singletonList(shared));
        Struct equalKey = new Struct(keySchema).put("parts", Collections.singletonList(shared));
        Struct differentKey =
                new Struct(keySchema).put("parts", Collections.singletonList(new byte[] {1, 2}));
        ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0, 1, 2, 3}).asReadOnlyBuffer();
        bytes.position(1).limit(3);
        bytes.order(ByteOrder.LITTLE_ENDIAN);
        Struct value =
                new Struct(valueSchema)
                        .put("raw", null)
                        .put(
                                "children",
                                Collections.singletonList(new Struct(child).put("raw", null)))
                        .put("bytes", bytes)
                        .put("latin1", "\u0000\u00ff")
                        .put("unicode", "\uD800?");
        ConnectHeaders headers = new ConnectHeaders();
        headers.addString("duplicate", "first").addString("duplicate", "second");
        SourceRecord original =
                new SourceRecord(
                        Collections.singletonMap("server", "test"),
                        Collections.singletonMap("pos", 42L),
                        "topic",
                        3,
                        keySchema,
                        originalKey,
                        valueSchema,
                        value,
                        123L,
                        headers);
        try (SnapshotBuffer buffer = new SnapshotBuffer(0, 0)) {
            buffer.put(originalKey, original);
            assertNotNull(buffer.get(equalKey));
            assertNull(buffer.get(differentKey));
            SourceRecord decoded = buffer.values().iterator().next();
            Struct decodedKey = (Struct) decoded.key();
            Struct freshKey = new Struct(decodedKey.schema()).put("parts", decodedKey.get("parts"));
            assertNotNull(buffer.get(freshKey));
            assertEquals(original.sourcePartition(), decoded.sourcePartition());
            assertEquals(original.sourceOffset(), decoded.sourceOffset());
            assertEquals(original.topic(), decoded.topic());
            assertEquals(original.kafkaPartition(), decoded.kafkaPartition());
            assertEquals(original.timestamp(), decoded.timestamp());
            assertEquals(headers, decoded.headers());
            assertEquals(valueSchema, decoded.valueSchema());
            Struct decodedValue = (Struct) decoded.value();
            assertNull(decodedValue.getWithoutDefault("raw"));
            Struct defaultChild =
                    (Struct)
                            ((List<?>)
                                            decoded.valueSchema()
                                                    .field("children")
                                                    .schema()
                                                    .defaultValue())
                                    .get(0);
            assertNull(defaultChild.getWithoutDefault("raw"));
            assertEquals("\u0000\u00ff", decodedValue.getString("latin1"));
            assertEquals("\uD800?", decodedValue.getString("unicode"));
            ByteBuffer decodedBytes = (ByteBuffer) decodedValue.get("bytes");
            assertEquals(bytes, decodedBytes);
            assertEquals(1, decodedBytes.position());
            assertEquals(3, decodedBytes.limit());
            assertTrue(decodedBytes.isReadOnly());
            assertEquals(ByteOrder.LITTLE_ENDIAN, decodedBytes.order());
            assertEquals(1, bytes.position());
            assertNotNull(buffer.remove(freshKey));
            assertTrue(buffer.isEmpty());
        }
    }

    private static Struct key(int id) {
        return new Struct(KEY).put("id", id);
    }

    private static SourceRecord record(int id, String payload) {
        return new SourceRecord(
                Collections.emptyMap(),
                Collections.singletonMap("pos", (long) id),
                "topic",
                KEY,
                key(id),
                VALUE,
                new Struct(VALUE).put("payload", payload));
    }

    private static String payload(SourceRecord record) {
        return ((Struct) record.value()).getString("payload");
    }

    private static List<Integer> ids(SnapshotBuffer buffer) {
        return buffer.values().stream()
                .map(record -> ((Struct) record.key()).getInt32("id"))
                .collect(Collectors.toList());
    }
}
