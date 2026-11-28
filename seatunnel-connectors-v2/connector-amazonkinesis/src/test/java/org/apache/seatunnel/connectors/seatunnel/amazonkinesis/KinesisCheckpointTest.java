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

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises real logical restore and writer admission without starting a provider or an engine.
 * Serialization budgets are checked through the writer before native conversion can exhaust input.
 */
class KinesisCheckpointTest {
    @Test
    void restoresBeforeAssignmentAndAfterTerminalCompletion() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("stream_name", "test");
        options.put("start_position", "LATEST");
        AmazonKinesisSource source = new AmazonKinesisSource(ReadonlyConfig.fromMap(options));
        EnumeratorContext earlyContext = new EnumeratorContext();
        SourceSplitEnumerator<KinesisSplit, KinesisSplit> early =
                source.createEnumerator(earlyContext);
        ReaderContext readerContext = new ReaderContext();
        try (SourceReader<SeaTunnelRow, KinesisSplit> reader = source.createReader(readerContext)) {
            KinesisSplit checkpoint = early.snapshotState(1);
            List<KinesisSplit> notYetAssigned = reader.snapshotState(1);
            assertEquals(Collections.emptyList(), notYetAssigned);

            SourceSplitEnumerator<KinesisSplit, KinesisSplit> restored =
                    source.restoreEnumerator(earlyContext, checkpoint);
            restored.addSplitsBack(notYetAssigned, 0);
            restored.run();
            assertEquals(1, earlyContext.assigned.size());
            assertEquals(checkpoint.anchor, earlyContext.assigned.get(0).anchor);

            KinesisSplit terminal = earlyContext.assigned.get(0).copy();
            KinesisSplit.ShardState shard =
                    new KinesisSplit.ShardState("shard-1", Collections.emptyList(), true, true);
            shard.done = true;
            shard.committed = "9007199254740993";
            terminal.shards.put(shard.id, shard);
            terminal.complete = true;

            EnumeratorContext lastContext = new EnumeratorContext();
            SourceSplitEnumerator<KinesisSplit, KinesisSplit> last =
                    source.restoreEnumerator(lastContext, checkpoint);
            last.addSplitsBack(Collections.singletonList(terminal), 0);
            last.run();
            reader.addSplits(lastContext.assigned);
            reader.pollNext(
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow row) {
                            throw new AssertionError("Terminal checkpoint must not emit");
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return this;
                        }
                    });
            assertEquals(1, readerContext.terminalSignals);
            KinesisSplit after = reader.snapshotState(2).get(0);
            assertTrue(after.complete);
            assertEquals(checkpoint.anchor, after.anchor);
            assertEquals("9007199254740993", after.shards.get(shard.id).committed);
        }
    }

    @Test
    void rejectsMapExpansionBeforeNativeConversionVisitsEveryEntry() throws Exception {
        SeaTunnelRowType type =
                rowType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE));
        TrackingMap entries = new TrackingMap();
        char[] characters = new char[128];
        Arrays.fill(characters, 'a');
        String shared = new String(characters);
        for (int index = 0; index < 16; index++) {
            entries.values.put("k" + index, shared);
        }
        try (KinesisWriter writer = writer(type, "json", 512)) {
            assertThrows(
                    IOException.class,
                    () -> writer.write(new SeaTunnelRow(new Object[] {entries})));
            // The old post-encoding limit visits all 16 entries before it can reject the row.
            assertTrue(entries.visited > 0 && entries.visited < 16);
        }
        String[] repeated = new String[16];
        Arrays.fill(repeated, shared);
        try (KinesisWriter writer = writer(rowType(ArrayType.STRING_ARRAY_TYPE), "json", 512)) {
            assertThrows(
                    IOException.class,
                    () -> writer.write(new SeaTunnelRow(new Object[] {repeated})));
        }
    }

    @Test
    void preservesSmallNativeMapsArraysDecimalKeysAndExactEscaping() throws Exception {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("snow", "雪");
        map.put("quote", "\"\\\n");
        KinesisPayload maps =
                new KinesisPayload(
                        rowType(new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE)));
        assertEquals(
                "{\"value\":{\"snow\":\"雪\",\"quote\":\"\\\"\\\\\\n\"}}",
                new String(
                        maps.json(new SeaTunnelRow(new Object[] {map}), 512),
                        StandardCharsets.UTF_8));
        Map<BigDecimal, String> keys = new LinkedHashMap<>();
        keys.put(new BigDecimal("1E+100"), "compact");
        KinesisPayload decimals =
                new KinesisPayload(
                        rowType(new MapType<>(new DecimalType(10, 2), BasicType.STRING_TYPE)));
        assertEquals(
                "{\"value\":{\"1E+100\":\"compact\"}}",
                new String(
                        decimals.json(new SeaTunnelRow(new Object[] {keys}), 64),
                        StandardCharsets.UTF_8));
        Map<Object, String> collisions = new LinkedHashMap<>();
        collisions.put(1, "first");
        collisions.put(2, "second");
        collisions.put("1", "last");
        KinesisPayload collisionKeys =
                new KinesisPayload(
                        rowType(new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE)));
        assertEquals(
                "{\"value\":{\"1\":\"last\",\"2\":\"second\"}}",
                new String(
                        collisionKeys.json(new SeaTunnelRow(new Object[] {collisions}), 128),
                        StandardCharsets.UTF_8));
        KinesisPayload times = new KinesisPayload(rowType(LocalTimeType.LOCAL_TIME_TYPE));
        assertEquals(
                "{\"value\":\"01:02:03.123\"}",
                new String(
                        times.json(
                                new SeaTunnelRow(new Object[] {LocalTime.of(1, 2, 3, 123000000)}),
                                64),
                        StandardCharsets.UTF_8));
        KinesisPayload binary = new KinesisPayload(rowType(PrimitiveByteArrayType.INSTANCE));
        assertEquals(
                "{\"value\":\"AP8=\"}",
                new String(
                        binary.json(
                                new SeaTunnelRow(new Object[] {new byte[] {0, (byte) 255}}), 64),
                        StandardCharsets.UTF_8));
        KinesisPayload arrays = new KinesisPayload(rowType(ArrayType.STRING_ARRAY_TYPE));
        assertEquals(
                "{\"value\":[\"same\",null,\"same\"]}",
                new String(
                        arrays.json(
                                new SeaTunnelRow(
                                        new Object[] {new String[] {"same", null, "same"}}),
                                64),
                        StandardCharsets.UTF_8));
        assertThrows(
                IOException.class,
                () ->
                        maps.json(
                                new SeaTunnelRow(
                                        new Object[] {
                                            Collections.singletonMap(
                                                    "quote",
                                                    "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"")
                                        }),
                                50));
    }

    @Test
    void convertsAndWritesDeepNarrowValuesWithoutRecursiveCompositeConstruction() throws Exception {
        SeaTunnelDataType<?> nested = BasicType.STRING_TYPE;
        Object value = "leaf";
        StringBuilder expected = new StringBuilder();
        for (int depth = 0; depth < 2000; depth++) {
            nested = new SeaTunnelRowType(new String[] {"v"}, new SeaTunnelDataType<?>[] {nested});
            value = new SeaTunnelRow(new Object[] {value});
            expected.append("{\"v\":");
        }
        expected.append("\"leaf\"");
        for (int depth = 0; depth < 2000; depth++) {
            expected.append('}');
        }
        assertEquals(
                expected.toString(),
                new String(
                        new KinesisPayload((SeaTunnelRowType) nested)
                                .json((SeaTunnelRow) value, 32768),
                        StandardCharsets.UTF_8));

        SeaTunnelRowType shared = rowType(BasicType.STRING_TYPE);
        for (int level = 0; level < 8; level++) {
            shared =
                    new SeaTunnelRowType(
                            new String[] {"left", "right"},
                            new SeaTunnelDataType<?>[] {shared, shared});
        }
        assertEquals(
                "{\"left\":null,\"right\":null}",
                new String(
                        new KinesisPayload(shared)
                                .json(new SeaTunnelRow(new Object[] {null, null}), 64),
                        StandardCharsets.UTF_8));
        SeaTunnelRowType cyclic = rowType(BasicType.STRING_TYPE);
        cyclic.getFieldTypes()[0] = cyclic;
        assertThrows(IllegalArgumentException.class, () -> new KinesisPayload(cyclic));
    }

    @Test
    void preflightsUtf8TextAndPlainDecimalExpansion() throws Exception {
        assertArrayEquals("雪😀".getBytes(StandardCharsets.UTF_8), KinesisPayload.text("雪😀", 7));
        assertThrows(IllegalArgumentException.class, () -> KinesisPayload.text("雪😀", 6));
        try (KinesisWriter writer = writer(rowType(BasicType.STRING_TYPE), "text", 6)) {
            assertThrows(
                    IOException.class, () -> writer.write(new SeaTunnelRow(new Object[] {"雪😀"})));
        }
        KinesisPayload numbers = new KinesisPayload(rowType(new DecimalType(10, 2)));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        numbers.json(
                                new SeaTunnelRow(new Object[] {new BigDecimal("1E+1000")}), 512));
        assertEquals(
                "{\"value\":1.20}",
                new String(
                        numbers.json(new SeaTunnelRow(new Object[] {new BigDecimal("1.20")}), 64),
                        StandardCharsets.UTF_8));
    }

    private static SeaTunnelRowType rowType(SeaTunnelDataType<?> field) {
        return new SeaTunnelRowType(new String[] {"value"}, new SeaTunnelDataType<?>[] {field});
    }

    private static KinesisWriter writer(SeaTunnelRowType type, String format, int limit) {
        Map<String, Object> options = new HashMap<>();
        options.put("region", "us-east-1");
        options.put("stream_name", "out");
        options.put("access_key", "fixture-access");
        options.put("secret_key", "fixture-secret");
        options.put("format", format);
        if (!"json".equals(format)) {
            options.put("data_field", "value");
        }
        options.put("partition_key", "k");
        options.put("record_bytes", limit);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        return new KinesisWriter(
                new KinesisConnection.Settings(config),
                new AmazonKinesisSink.SinkSettings(config, type),
                true);
    }

    /**
     * A legal map whose real iterator exposes whether rejected conversion consumed every entry.
     * Values and ordering remain ordinary map data; the counter is an early-admission oracle.
     */
    private static final class TrackingMap extends AbstractMap<String, String> {
        private final Map<String, String> values = new LinkedHashMap<>();
        private int visited;

        @Override
        public Set<Entry<String, String>> entrySet() {
            return new AbstractSet<Entry<String, String>>() {
                @Override
                public Iterator<Entry<String, String>> iterator() {
                    Iterator<Entry<String, String>> delegate = values.entrySet().iterator();
                    return new Iterator<Entry<String, String>>() {
                        @Override
                        public boolean hasNext() {
                            return delegate.hasNext();
                        }

                        @Override
                        public Entry<String, String> next() {
                            visited++;
                            return delegate.next();
                        }
                    };
                }

                @Override
                public int size() {
                    return values.size();
                }
            };
        }
    }

    /**
     * Records terminal completion from a real reader restored from an already complete split. An
     * unexpected collection fails the test rather than simulating any provider behavior.
     */
    private static final class ReaderContext implements SourceReader.Context {
        private int terminalSignals;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {
            terminalSignals++;
        }

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    /**
     * Captures real enumerator assignments for an early checkpoint and a returned terminal split.
     * Subtask zero models the source single-reader contract without adding a network dependency.
     */
    private static final class EnumeratorContext
            implements SourceSplitEnumerator.Context<KinesisSplit> {
        private final List<KinesisSplit> assigned = new ArrayList<>();

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtask, List<KinesisSplit> splits) {
            assigned.addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtask, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
