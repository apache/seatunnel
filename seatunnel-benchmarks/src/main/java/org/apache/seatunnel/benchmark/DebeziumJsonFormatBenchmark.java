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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.seatunnel.format.json.debezium.DebeziumJsonSerializationSchema;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Benchmarks Debezium JSON serialization and deserialization hot paths used by CDC pipelines.
 *
 * <p>Throughput scores count CDC events, not emitted {@link SeaTunnelRow} records. {@code
 * deserializeUpdateEvent} consumes one UPDATE envelope that emits two rows, and {@code
 * serializeMergedUpdateEvent} includes both {@code UPDATE_BEFORE} and {@code UPDATE_AFTER} calls.
 * Do not compare INSERT and UPDATE scores as if they measured the same unit of work.
 */
public class DebeziumJsonFormatBenchmark extends BenchmarkBase {

    private static final String CATALOG_NAME = "default";
    private static final String DATABASE_NAME = "inventory";
    private static final String SCHEMA_NAME = "public";
    private static final String TABLE_NAME = "orders";

    private static final long INSERT_EVENT_TIME = 1_589_355_606_100L;
    private static final long UPDATE_EVENT_TIME = 1_589_361_987_936L;

    private static final LocalDateTime INSERT_TIMESTAMP =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(INSERT_EVENT_TIME), ZoneOffset.UTC);
    private static final LocalDateTime UPDATE_TIMESTAMP =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(UPDATE_EVENT_TIME), ZoneOffset.UTC);

    private static final BigDecimal INSERT_AMOUNT = new BigDecimal("199.9900");
    private static final BigDecimal UPDATE_AMOUNT = new BigDecimal("249.5000");

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id", "name", "enabled", "score", "amount", "updated_at"},
                    new SeaTunnelDataType<?>[] {
                        BasicType.LONG_TYPE,
                        BasicType.STRING_TYPE,
                        BasicType.BOOLEAN_TYPE,
                        BasicType.DOUBLE_TYPE,
                        new DecimalType(20, 4),
                        LocalTimeType.LOCAL_DATE_TIME_TYPE
                    });

    private static final String INSERT_EVENT_JSON =
            "{\"before\":null,"
                    + "\"after\":{\"id\":1001,\"name\":\"seatunnel-order\",\"enabled\":true,"
                    + "\"score\":12.5,\"amount\":199.99,\"updated_at\":1589355606100},"
                    + "\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\","
                    + "\"name\":\"dbserver1\",\"ts_ms\":1589355606100,\"snapshot\":\"false\","
                    + "\"db\":\"inventory\",\"table\":\"orders\",\"server_id\":223344,"
                    + "\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":154,\"row\":0,"
                    + "\"thread\":7,\"query\":null},"
                    + "\"op\":\"c\",\"ts_ms\":1589355606100,\"transaction\":null}";

    private static final String UPDATE_EVENT_JSON =
            "{\"before\":{\"id\":1001,\"name\":\"seatunnel-order\",\"enabled\":true,"
                    + "\"score\":12.5,\"amount\":199.99,\"updated_at\":1589355606100},"
                    + "\"after\":{\"id\":1001,\"name\":\"seatunnel-order\",\"enabled\":true,"
                    + "\"score\":13.75,\"amount\":249.5,\"updated_at\":1589361987936},"
                    + "\"source\":{\"version\":\"1.9.7.Final\",\"connector\":\"mysql\","
                    + "\"name\":\"dbserver1\",\"ts_ms\":1589361987936,\"snapshot\":\"false\","
                    + "\"db\":\"inventory\",\"table\":\"orders\",\"server_id\":223344,"
                    + "\"gtid\":null,\"file\":\"mysql-bin.000003\",\"pos\":4096,\"row\":0,"
                    + "\"thread\":7,\"query\":null},"
                    + "\"op\":\"u\",\"ts_ms\":1589361987936,\"transaction\":null}";

    private DebeziumJsonDeserializationSchema deserializer;
    private DebeziumJsonSerializationSchema insertSerializer;
    private DebeziumJsonSerializationSchema mergeUpdateSerializer;
    private ReusableRowCollector collector;
    private byte[] insertEventBytes;
    private byte[] updateEventBytes;
    private SeaTunnelRow insertRow;
    private SeaTunnelRow updateBeforeRow;
    private SeaTunnelRow updateAfterRow;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + DebeziumJsonFormatBenchmark.class.getCanonicalName() + ".*")
                        .build();
        new Runner(options).run();
    }

    @Setup
    public void setUp() {
        CatalogTable catalogTable =
                CatalogTableUtil.getCatalogTable(
                        CATALOG_NAME, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, ROW_TYPE);
        String tableId = catalogTable.getTablePath().toString();

        deserializer = new DebeziumJsonDeserializationSchema(catalogTable, false, false);
        insertSerializer = new DebeziumJsonSerializationSchema(ROW_TYPE);
        mergeUpdateSerializer =
                new DebeziumJsonSerializationSchema(ROW_TYPE, StandardCharsets.UTF_8, true);

        insertEventBytes = INSERT_EVENT_JSON.getBytes(StandardCharsets.UTF_8);
        updateEventBytes = UPDATE_EVENT_JSON.getBytes(StandardCharsets.UTF_8);

        insertRow = newRow(1001L, "seatunnel-order", true, 12.5D, INSERT_AMOUNT, INSERT_TIMESTAMP);
        insertRow.setTableId(tableId);
        insertRow.setRowKind(RowKind.INSERT);
        MetadataUtil.setEventTime(insertRow, INSERT_EVENT_TIME);

        updateBeforeRow =
                newRow(1001L, "seatunnel-order", true, 12.5D, INSERT_AMOUNT, INSERT_TIMESTAMP);
        updateBeforeRow.setTableId(tableId);
        updateBeforeRow.setRowKind(RowKind.UPDATE_BEFORE);
        MetadataUtil.setEventTime(updateBeforeRow, UPDATE_EVENT_TIME);

        updateAfterRow =
                newRow(1001L, "seatunnel-order", true, 13.75D, UPDATE_AMOUNT, UPDATE_TIMESTAMP);
        updateAfterRow.setTableId(tableId);
        updateAfterRow.setRowKind(RowKind.UPDATE_AFTER);
        MetadataUtil.setEventTime(updateAfterRow, UPDATE_EVENT_TIME);

        collector = new ReusableRowCollector();
        validateFixtures();
        collector.reset();
    }

    /**
     * Deserialize one schema-less Debezium CREATE/INSERT event and consume the single emitted row.
     */
    @Benchmark
    public SeaTunnelRow deserializeInsertEvent() {
        collector.reset();
        deserializer.deserialize(insertEventBytes, collector);
        return collector.row0;
    }

    /**
     * Deserialize one schema-less Debezium UPDATE event. One invocation is one CDC event even
     * though the deserializer emits {@code UPDATE_BEFORE} followed by {@code UPDATE_AFTER}.
     */
    @Benchmark
    public SeaTunnelRow deserializeUpdateEvent() {
        collector.reset();
        deserializer.deserialize(updateEventBytes, collector);
        SeaTunnelRow before = collector.row0;
        SeaTunnelRow after = collector.row1;
        return before.getArity() == after.getArity() ? after : before;
    }

    /** Serialize one {@link RowKind#INSERT} row into a Debezium JSON envelope. */
    @Benchmark
    public byte[] serializeInsertEvent() {
        return insertSerializer.serialize(insertRow);
    }

    /**
     * Serialize one logical UPDATE by calling the merge-enabled serializer with {@code
     * UPDATE_BEFORE} then {@code UPDATE_AFTER}. One invocation is one CDC event, not two.
     */
    @Benchmark
    public byte[] serializeMergedUpdateEvent() {
        byte[] cachedBefore = mergeUpdateSerializer.serialize(updateBeforeRow);
        byte[] updateEvent = mergeUpdateSerializer.serialize(updateAfterRow);
        return cachedBefore == null ? updateEvent : cachedBefore;
    }

    private void validateFixtures() {
        collector.reset();
        deserializer.deserialize(insertEventBytes, collector);
        if (collector.count != 1) {
            throw new IllegalStateException(
                    "INSERT deserialization must emit exactly one row, but emitted "
                            + collector.count);
        }
        if (collector.row0.getRowKind() != RowKind.INSERT) {
            throw new IllegalStateException(
                    "INSERT deserialization must emit RowKind.INSERT, but emitted "
                            + collector.row0.getRowKind());
        }

        collector.reset();
        deserializer.deserialize(updateEventBytes, collector);
        if (collector.count != 2) {
            throw new IllegalStateException(
                    "UPDATE deserialization must emit two rows, but emitted " + collector.count);
        }
        if (collector.row0.getRowKind() != RowKind.UPDATE_BEFORE
                || collector.row1.getRowKind() != RowKind.UPDATE_AFTER) {
            throw new IllegalStateException(
                    "UPDATE deserialization must emit UPDATE_BEFORE then UPDATE_AFTER, but emitted "
                            + collector.row0.getRowKind()
                            + " then "
                            + collector.row1.getRowKind());
        }
        if (Objects.equals(collector.row0.getField(3), collector.row1.getField(3))) {
            throw new IllegalStateException(
                    "UPDATE before and after payloads must differ on at least one field");
        }

        byte[] insertJson = insertSerializer.serialize(insertRow);
        String insertEnvelope = new String(insertJson, StandardCharsets.UTF_8);
        if (!insertEnvelope.contains("\"op\":\"c\"") || !insertEnvelope.contains("\"after\":{")) {
            throw new IllegalStateException(
                    "INSERT serialization must produce a Debezium CREATE envelope: "
                            + insertEnvelope);
        }

        byte[] cachedBefore = mergeUpdateSerializer.serialize(updateBeforeRow);
        if (cachedBefore != null) {
            throw new IllegalStateException(
                    "Merged UPDATE serialization must cache UPDATE_BEFORE and return null");
        }
        byte[] updateJson = mergeUpdateSerializer.serialize(updateAfterRow);
        if (updateJson == null) {
            throw new IllegalStateException(
                    "Merged UPDATE serialization must emit an envelope for UPDATE_AFTER");
        }
        String updateEnvelope = new String(updateJson, StandardCharsets.UTF_8);
        if (!updateEnvelope.contains("\"op\":\"u\"")
                || !updateEnvelope.contains("\"before\":{")
                || !updateEnvelope.contains("\"after\":{")) {
            throw new IllegalStateException(
                    "Merged UPDATE serialization must contain before and after payloads: "
                            + updateEnvelope);
        }
    }

    private static SeaTunnelRow newRow(
            long id,
            String name,
            boolean enabled,
            double score,
            BigDecimal amount,
            LocalDateTime updatedAt) {
        return new SeaTunnelRow(new Object[] {id, name, enabled, score, amount, updatedAt});
    }

    private static final class ReusableRowCollector implements Collector<SeaTunnelRow> {

        private SeaTunnelRow row0;
        private SeaTunnelRow row1;
        private int count;

        @Override
        public void collect(SeaTunnelRow record) {
            if (count == 0) {
                row0 = record;
            } else if (count == 1) {
                row1 = record;
            }
            count++;
        }

        @Override
        public Object getCheckpointLock() {
            return null;
        }

        private void reset() {
            count = 0;
            row0 = null;
            row1 = null;
        }
    }
}
