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

import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.SIGNAL_EVENT_VALUE_SCHEMA_NAME;
import static org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent.WATERMARK_KIND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SnapshotStateBufferTest {

    private static final Schema KEY_SCHEMA =
            SchemaBuilder.struct().name("test.Key").field("id", Schema.INT32_SCHEMA).build();
    private static final Schema VALUE_SCHEMA =
            SchemaBuilder.struct()
                    .name("test.Value")
                    .field("id", Schema.INT32_SCHEMA)
                    .field("value", Schema.STRING_SCHEMA)
                    .build();

    @TempDir private Path temporaryDirectory;

    @Test
    void overwritesByConnectKeyAndPreservesLinkedHashMapOrder() throws Exception {
        SnapshotStateBuffer buffer = SnapshotStateBuffer.create(temporaryDirectory);
        try {
            buffer.put(record(1, "before", 10L));
            buffer.put(record(2, "second"));
            buffer.put(record(1, "after", 101L));

            List<SourceRecord> records = drain(buffer.iterator());

            assertEquals(Arrays.asList("after", "second"), values(records));
            assertEquals(Long.valueOf(101L), records.get(0).sourceOffset().get("position"));
        } finally {
            buffer.close();
        }
        try (Stream<Path> paths = Files.list(temporaryDirectory)) {
            assertEquals(0L, paths.count());
        }
    }

    @Test
    void deletesKeysAndReinsertionMovesThemToTheEnd() throws Exception {
        SnapshotStateBuffer buffer = SnapshotStateBuffer.create(temporaryDirectory);
        try {
            buffer.put(record(1, "first"));
            buffer.put(record(2, "second"));
            buffer.remove(key(1));
            buffer.put(record(1, "reinserted"));

            assertEquals(Arrays.asList("second", "reinserted"), values(drain(buffer.iterator())));
        } finally {
            buffer.close();
        }
    }

    @Test
    void supportsNullKeysDuringReconciliation() throws Exception {
        SnapshotStateBuffer buffer = SnapshotStateBuffer.create(temporaryDirectory);
        try {
            buffer.put(nullKeyRecord("first"));
            buffer.put(record(2, "second"));
            buffer.put(nullKeyRecord("updated"));

            assertEquals(Arrays.asList("updated", "second"), values(drain(buffer.iterator())));
        } finally {
            buffer.close();
        }
    }

    @Test
    void removesNullKeysDuringReconciliation() throws Exception {
        SnapshotStateBuffer buffer = SnapshotStateBuffer.create(temporaryDirectory);
        try {
            buffer.put(nullKeyRecord("first"));
            buffer.put(record(2, "second"));
            buffer.remove(null);

            assertEquals(Arrays.asList("second"), values(drain(buffer.iterator())));
        } finally {
            buffer.close();
        }
    }

    @Test
    void storesMoreRowsThanTheConfiguredSnapshotQueueWithoutMaterializingThem() throws Exception {
        SnapshotStateBuffer buffer = SnapshotStateBuffer.create(temporaryDirectory);
        try {
            for (int id = 0; id < 10_000; id++) {
                buffer.put(record(id, "value-" + id));
            }

            Iterator<SourceRecord> records = buffer.iterator();
            int count = 0;
            while (records.hasNext()) {
                records.next();
                count++;
            }

            assertEquals(10_000, count);
            assertFalse(records.hasNext());
        } finally {
            buffer.close();
        }
        try (Stream<Path> paths = Files.list(temporaryDirectory)) {
            assertTrue(paths.count() == 0L);
        }
    }

    @Test
    void exactlyOnceFetcherEmitsOneLazyGroupBetweenWatermarks() throws Exception {
        FetchTask.Context context = mock(FetchTask.Context.class);
        when(context.formatMessageTimestamp(any()))
                .thenAnswer(invocation -> invocation.getArgument(0));

        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        when(queue.poll())
                .thenReturn(
                        Collections.singletonList(new DataChangeEvent(watermark("LOW"))),
                        Collections.singletonList(new DataChangeEvent(record(1, "snapshot"))),
                        Collections.singletonList(new DataChangeEvent(watermark("HIGH"))),
                        Collections.singletonList(new DataChangeEvent(watermark("END"))));

        IncrementalSourceScanFetcher fetcher = new IncrementalSourceScanFetcher(context, 0);
        java.lang.reflect.Field queueField =
                IncrementalSourceScanFetcher.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(fetcher, queue);

        List<SourceRecord> output = new ArrayList<>();
        Iterator<SourceRecords> groups = fetcher.pollSplitRecordsIfExactlyOnce();
        assertTrue(groups.hasNext());
        SourceRecords sourceRecords = groups.next();
        assertFalse(groups.hasNext());
        try {
            sourceRecords.iterator().forEachRemaining(output::add);
        } finally {
            sourceRecords.close();
            fetcher.close();
        }

        assertEquals(3, output.size());
        assertTrue(isWatermark(output.get(0), "LOW"));
        assertEquals("snapshot", ((Struct) output.get(1).value()).getString("value"));
        assertTrue(isWatermark(output.get(2), "HIGH"));
    }

    private static SourceRecord record(int id, String value) {
        return record(id, value, (long) id);
    }

    private static SourceRecord record(int id, String value, long position) {
        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("position", position),
                "test.topic",
                0,
                KEY_SCHEMA,
                key(id),
                VALUE_SCHEMA,
                new Struct(VALUE_SCHEMA).put("id", id).put("value", value));
    }

    private static SourceRecord nullKeyRecord(String value) {
        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("position", 1L),
                "test.topic",
                0,
                null,
                null,
                VALUE_SCHEMA,
                new Struct(VALUE_SCHEMA).put("id", 1).put("value", value));
    }

    private static Struct key(int id) {
        return new Struct(KEY_SCHEMA).put("id", id);
    }

    private static SourceRecord watermark(String kind) {
        Schema valueSchema =
                SchemaBuilder.struct()
                        .name(SIGNAL_EVENT_VALUE_SCHEMA_NAME)
                        .field(WATERMARK_KIND, Schema.STRING_SCHEMA)
                        .build();
        return new SourceRecord(
                Collections.singletonMap("server", "test"),
                Collections.singletonMap("position", (long) kind.hashCode()),
                "test.topic",
                Schema.STRING_SCHEMA,
                "split",
                valueSchema,
                new Struct(valueSchema).put(WATERMARK_KIND, kind));
    }

    private static boolean isWatermark(SourceRecord record, String kind) {
        return kind.equals(((Struct) record.value()).getString(WATERMARK_KIND));
    }

    private static List<SourceRecord> drain(Iterator<SourceRecord> iterator) {
        List<SourceRecord> records = new ArrayList<>();
        iterator.forEachRemaining(records::add);
        return records;
    }

    private static List<String> values(List<SourceRecord> records) {
        return records.stream()
                .map(record -> ((Struct) record.value()).getString("value"))
                .collect(Collectors.toList());
    }
}
