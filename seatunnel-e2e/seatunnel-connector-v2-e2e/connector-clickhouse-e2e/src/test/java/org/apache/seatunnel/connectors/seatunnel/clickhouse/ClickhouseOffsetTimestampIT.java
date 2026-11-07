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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseSink;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.inject.DateTimeInjectFunction;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ClickHouseContainer;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ClickhouseOffsetTimestampIT {

    @Test
    void writesOffsetTimestampsToExistingTable() throws Exception {
        try (ClickHouseContainer database =
                new ClickHouseContainer("clickhouse/clickhouse-server:23.3.13.6")) {
            database.start();
            try (Connection connection =
                            DriverManager.getConnection(
                                    database.getJdbcUrl(),
                                    database.getUsername(),
                                    database.getPassword());
                    Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE offset_timestamps (id Int32, seconds DateTime('Asia/Shanghai'), micros DateTime64(6, 'America/New_York'), nanos Nullable(DateTime64(9, 'UTC')), label String, amount Decimal(20, 6), tags Array(String), local_time DateTime64(9, 'UTC'), sql_timestamp DateTime64(9, 'UTC')) ENGINE = MergeTree ORDER BY tuple()");
                statement.execute(
                        "CREATE TABLE legacy_timestamp (value DateTime64(9, 'UTC')) ENGINE=Memory");
                Properties properties = new Properties();
                properties.setProperty("user", database.getUsername());
                properties.setProperty("password", database.getPassword());
                // The unwrapped insert retains the driver's existing Timestamp conversion.
                try (Connection legacyConnection =
                                new com.clickhouse.jdbc.ClickHouseDriver()
                                        .connect(database.getJdbcUrl(), properties);
                        PreparedStatement legacy =
                                legacyConnection.prepareStatement(
                                        "INSERT INTO legacy_timestamp VALUES (?)")) {
                    new DateTimeInjectFunction()
                            .injectFields(
                                    legacy,
                                    1,
                                    Timestamp.valueOf(
                                            LocalDateTime.parse("2026-09-12T10:00:00.123456789")));
                    legacy.addBatch();
                    legacy.executeBatch();
                }
                String legacyTimestamp;
                try (ResultSet rows =
                        statement.executeQuery("SELECT toString(value) FROM legacy_timestamp")) {
                    assertTrue(rows.next());
                    legacyTimestamp = rows.getString(1);
                }
                Map<String, Object> options = new HashMap<>();
                options.put("host", database.getHost() + ":" + database.getMappedPort(8123));
                options.put("database", "default");
                options.put("table", "offset_timestamps");
                options.put("username", database.getUsername());
                options.put("password", database.getPassword());
                TableSchema.Builder schema =
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0L, false, null, null));
                for (String name : new String[] {"seconds", "micros", "nanos"}) {
                    schema.column(
                            PhysicalColumn.of(
                                    name,
                                    LocalTimeType.OFFSET_DATE_TIME_TYPE,
                                    0L,
                                    true,
                                    null,
                                    null));
                }
                schema.column(
                        PhysicalColumn.of("label", BasicType.STRING_TYPE, 0L, false, null, null));
                schema.column(
                        PhysicalColumn.of("amount", new DecimalType(20, 6), 0L, false, null, null));
                schema.column(
                        PhysicalColumn.of(
                                "tags", ArrayType.STRING_ARRAY_TYPE, 0L, false, null, null));
                schema.column(
                        PhysicalColumn.of(
                                "local_time",
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                0L,
                                false,
                                null,
                                null));
                schema.column(
                        PhysicalColumn.of(
                                "sql_timestamp",
                                LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                0L,
                                false,
                                null,
                                null));
                CatalogTable table =
                        CatalogTable.of(
                                TableIdentifier.of("clickhouse", "default", "offset_timestamps"),
                                schema.build(),
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                null);
                ClickhouseSink sink =
                        (ClickhouseSink)
                                new ClickhouseSinkFactory()
                                        .createSink(
                                                new TableSinkFactoryContext(
                                                        table,
                                                        ReadonlyConfig.fromMap(options),
                                                        getClass().getClassLoader()))
                                        .createSink();
                try (SaveModeHandler saveMode = sink.getSaveModeHandler().get()) {
                    saveMode.open();
                    saveMode.handleSaveMode();
                }
                String[] literals = {
                    "2026-09-12T10:00:00.123456789Z",
                    "2026-09-12T10:00:00.123456789+05:30",
                    "2026-09-12T10:00:00.123456789-07:00",
                    "2026-11-01T01:30:00.123456789-04:00",
                    "2026-11-01T01:30:00.123456789-05:00"
                };
                ClickhouseSinkWriter writer = sink.createWriter(mock(SinkWriter.Context.class));
                try {
                    for (int i = 0; i < literals.length; i++) {
                        OffsetDateTime value = OffsetDateTime.parse(literals[i]);
                        writer.write(row(i, value, value));
                    }
                    OffsetDateTime value = OffsetDateTime.parse(literals[0]);
                    writer.write(row(literals.length, value, null));
                    writer.prepareCommit();
                } finally {
                    writer.close();
                }
                try (ResultSet rows =
                        statement.executeQuery(
                                "SELECT id, toUnixTimestamp(seconds), toUnixTimestamp64Micro(micros), toUnixTimestamp64Nano(nanos), isNull(nanos), label, amount, tags, toString(local_time), toString(sql_timestamp) FROM offset_timestamps ORDER BY id")) {
                    for (int i = 0; i < literals.length; i++) {
                        assertTrue(rows.next());
                        Instant expected = OffsetDateTime.parse(literals[i]).toInstant();
                        assertEquals(i, rows.getInt(1));
                        assertEquals(expected.getEpochSecond(), rows.getLong(2));
                        assertEquals(
                                expected.getEpochSecond() * 1_000_000L + expected.getNano() / 1_000,
                                rows.getLong(3));
                        assertEquals(
                                expected.getEpochSecond() * 1_000_000_000L + expected.getNano(),
                                rows.getLong(4));
                        assertEquals("a'b\\c", rows.getString(6));
                        assertEquals(new BigDecimal("1234567890.123456"), rows.getBigDecimal(7));
                        assertArrayEquals(
                                new String[] {"a'b", "c\\d"},
                                (Object[]) rows.getArray(8).getArray());
                        assertEquals("2026-09-12 10:00:00.123456789", rows.getString(9));
                        assertEquals(legacyTimestamp, rows.getString(10));
                    }
                    assertTrue(rows.next());
                    assertEquals(literals.length, rows.getInt(1));
                    assertTrue(rows.getBoolean(5));
                    assertFalse(rows.next());
                }
                options.put("primary_key", "micros");
                options.put("support_upsert", true);
                ClickhouseSink upsert =
                        (ClickhouseSink)
                                new ClickhouseSinkFactory()
                                        .createSink(
                                                new TableSinkFactoryContext(
                                                        table,
                                                        ReadonlyConfig.fromMap(options),
                                                        getClass().getClassLoader()))
                                        .createSink();
                writer = upsert.createWriter(mock(SinkWriter.Context.class));
                try {
                    // Both overlap occurrences have the same local time in the target column.
                    SeaTunnelRow updated =
                            row(10, OffsetDateTime.parse("2026-11-01T05:30:00.123456789Z"), null);
                    updated.setRowKind(RowKind.UPDATE_AFTER);
                    writer.write(updated);
                    writer.prepareCommit();
                    SeaTunnelRow deleted =
                            row(4, OffsetDateTime.parse("2026-11-01T06:30:00.123456789Z"), null);
                    deleted.setRowKind(RowKind.DELETE);
                    writer.write(deleted);
                    writer.prepareCommit();
                } finally {
                    writer.close();
                }
                try (ResultSet rows =
                        statement.executeQuery("SELECT id FROM offset_timestamps ORDER BY id")) {
                    for (int id : new int[] {0, 1, 2, 5, 10}) {
                        assertTrue(rows.next());
                        assertEquals(id, rows.getInt(1));
                    }
                    assertFalse(rows.next());
                }
            }
        }
    }

    private SeaTunnelRow row(int id, OffsetDateTime timestamp, OffsetDateTime nullableTimestamp) {
        LocalDateTime local = LocalDateTime.parse("2026-09-12T10:00:00.123456789");
        return new SeaTunnelRow(
                new Object[] {
                    id,
                    timestamp,
                    timestamp,
                    nullableTimestamp,
                    "a'b\\c",
                    new BigDecimal("1234567890.123456"),
                    new String[] {"a'b", "c\\d"},
                    local,
                    Timestamp.valueOf(local)
                });
    }
}
