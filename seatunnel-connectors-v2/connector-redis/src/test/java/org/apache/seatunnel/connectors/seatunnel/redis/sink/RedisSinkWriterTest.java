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

package org.apache.seatunnel.connectors.seatunnel.redis.sink;

import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;

public class RedisSinkWriterTest {

    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of("catalog", "shop", null, "schema_events");

    private RedisClient mockRedisClient;

    private RedisParameters mockRedisParameters;

    private SeaTunnelRowType rowType;
    private RedisSinkWriter redisSinkWriter;

    @BeforeEach
    void setUp() {
        rowType =
                new SeaTunnelRowType(
                        new String[] {"id", "name", "age", "email"},
                        new SeaTunnelDataType<?>[] {
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE,
                            BasicType.INT_TYPE,
                            BasicType.STRING_TYPE
                        });

        mockRedisParameters = Mockito.mock(RedisParameters.class);
        mockRedisClient = Mockito.mock(RedisClient.class);

        when(mockRedisParameters.buildRedisClient()).thenReturn(mockRedisClient);
        when(mockRedisParameters.getBatchSize()).thenReturn(3);
        when(mockRedisParameters.getFormat()).thenReturn(RedisBaseOptions.Format.JSON);
        when(mockRedisParameters.getFieldDelimiter()).thenReturn(",");
    }

    @Test
    void testGetCustomKey() {
        // Set custom key mode
        when(mockRedisParameters.getKeyField()).thenReturn("user:${id}:profile");
        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.STRING);
        when(mockRedisParameters.getExpire()).thenReturn(3600L);

        redisSinkWriter = new RedisSinkWriter(rowType, mockRedisParameters);

        // create test data
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"});
        row.setRowKind(RowKind.INSERT);

        String customKey =
                redisSinkWriter.getCustomKey(
                        row,
                        Arrays.asList(rowType.getFieldNames()),
                        mockRedisParameters.getKeyField());

        Assertions.assertEquals("user:1:profile", customKey);
    }

    @Test
    void testGetCustomKeyWithMultipleCurlyBraces() {
        // Set custom key mode
        when(mockRedisParameters.getKeyField()).thenReturn("user:{${id}}:${age}:profile");
        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.STRING);
        when(mockRedisParameters.getExpire()).thenReturn(3600L);

        redisSinkWriter = new RedisSinkWriter(rowType, mockRedisParameters);

        // create test data
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"});
        row.setRowKind(RowKind.INSERT);

        String customKey =
                redisSinkWriter.getCustomKey(
                        row,
                        Arrays.asList(rowType.getFieldNames()),
                        mockRedisParameters.getKeyField());

        Assertions.assertEquals("user:{1}:25:profile", customKey);
    }

    @Test
    public void testLegacyCustomKey() {
        when(mockRedisParameters.getKeyField()).thenReturn("user:{id}:profile");

        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.STRING);
        when(mockRedisParameters.getExpire()).thenReturn(3600L);

        redisSinkWriter = new RedisSinkWriter(rowType, mockRedisParameters);

        // create test data
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"});
        row.setRowKind(RowKind.INSERT);

        String customKey =
                redisSinkWriter.getCustomKey(
                        row,
                        Arrays.asList(rowType.getFieldNames()),
                        mockRedisParameters.getKeyField());

        Assertions.assertEquals("user:1:profile", customKey);
    }

    @Test
    public void testLegacyCustomKeyWithMultipleCurlyBraces() {
        when(mockRedisParameters.getKeyField()).thenReturn("user:{{id}}:profile");

        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.STRING);
        when(mockRedisParameters.getExpire()).thenReturn(3600L);

        redisSinkWriter = new RedisSinkWriter(rowType, mockRedisParameters);

        // create test data
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1, "Alice", 25, "alice@test.com"});
        row.setRowKind(RowKind.INSERT);

        String customKey =
                redisSinkWriter.getCustomKey(
                        row,
                        Arrays.asList(rowType.getFieldNames()),
                        mockRedisParameters.getKeyField());

        Assertions.assertEquals("user:{1}:profile", customKey);
    }

    @Test
    void testAddColumnUsesLatestSchemaForJsonSerialization() throws IOException {
        List<List<String>> writtenValues = captureStringWrites();
        configureJsonKeySink(1);
        redisSinkWriter = new RedisSinkWriter(initialSchema(), mockRedisParameters);

        schemaEvolutionWriter().applySchemaChange(addEmailEvent());
        redisSinkWriter.write(insertRow(1L, "Alice", "alice@example.test", "legacy-value"));

        Assertions.assertEquals(1, writtenValues.size());
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@example.test\","
                        + "\"legacy_note\":\"legacy-value\"}",
                writtenValues.get(0).get(0));
    }

    @Test
    void testDropColumnRemovesFieldFromJsonSerialization() throws IOException {
        List<List<String>> writtenValues = captureStringWrites();
        configureJsonKeySink(1);
        redisSinkWriter = new RedisSinkWriter(initialSchema(), mockRedisParameters);

        SupportSchemaEvolutionSinkWriter schemaEvolutionWriter = schemaEvolutionWriter();
        schemaEvolutionWriter.applySchemaChange(addEmailEvent());
        schemaEvolutionWriter.applySchemaChange(
                new AlterTableDropColumnEvent(TABLE_IDENTIFIER, "legacy_note"));
        redisSinkWriter.write(insertRow(1L, "Alice", "alice@example.test"));

        Assertions.assertEquals(1, writtenValues.size());
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@example.test\"}",
                writtenValues.get(0).get(0));
    }

    @Test
    void testSchemaChangeFlushesOldRowsBeforeSerializerRefresh() throws IOException {
        List<List<String>> writtenValues = captureStringWrites();
        configureJsonKeySink(2);
        redisSinkWriter = new RedisSinkWriter(initialSchema(), mockRedisParameters);

        redisSinkWriter.write(insertRow(1L, "Before", "old-schema-value"));
        schemaEvolutionWriter().applySchemaChange(addEmailEvent());

        Assertions.assertEquals(1, writtenValues.size());
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Before\",\"legacy_note\":\"old-schema-value\"}",
                writtenValues.get(0).get(0));

        redisSinkWriter.write(insertRow(2L, "After", "after@example.test", "new-schema-value"));
        redisSinkWriter.prepareCommit();

        Assertions.assertEquals(2, writtenValues.size());
        Assertions.assertEquals(
                "{\"id\":2,\"name\":\"After\",\"email\":\"after@example.test\","
                        + "\"legacy_note\":\"new-schema-value\"}",
                writtenValues.get(1).get(0));
    }

    @Test
    void testSnapshotStateRestoresLatestSchema() throws IOException {
        List<List<String>> writtenValues = captureStringWrites();
        configureJsonKeySink(1);
        redisSinkWriter = new RedisSinkWriter(initialSchema(), mockRedisParameters);

        schemaEvolutionWriter().applySchemaChange(addEmailEvent());
        List<?> states = redisSinkWriter.snapshotState(1L);

        Assertions.assertEquals(1, states.size());
        redisSinkWriter = new RedisSinkWriter((TableSchema) states.get(0), mockRedisParameters);
        redisSinkWriter.write(insertRow(1L, "Recovered", "restored@example.test", "legacy"));

        Assertions.assertEquals(1, writtenValues.size());
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Recovered\",\"email\":\"restored@example.test\","
                        + "\"legacy_note\":\"legacy\"}",
                writtenValues.get(0).get(0));
    }

    @Test
    void testSnapshotStateNormalizesMissingConstraintKeys() {
        configureJsonKeySink(1);
        TableSchema schema = new TableSchema(initialSchema().getColumns(), null, null);
        redisSinkWriter = new RedisSinkWriter(schema, mockRedisParameters);

        List<TableSchema> states = redisSinkWriter.snapshotState(1L);

        Assertions.assertEquals(1, states.size());
        Assertions.assertNotSame(schema, states.get(0));
        Assertions.assertNotNull(states.get(0).getConstraintKeys());
        Assertions.assertTrue(states.get(0).getConstraintKeys().isEmpty());
    }

    private SupportSchemaEvolutionSinkWriter schemaEvolutionWriter() {
        Assertions.assertInstanceOf(SupportSchemaEvolutionSinkWriter.class, redisSinkWriter);
        return (SupportSchemaEvolutionSinkWriter) redisSinkWriter;
    }

    private List<List<String>> captureStringWrites() {
        List<List<String>> writtenValues = new ArrayList<>();
        Mockito.doAnswer(
                        invocation -> {
                            List<String> values = invocation.getArgument(2);
                            writtenValues.add(new ArrayList<>(values));
                            return null;
                        })
                .when(mockRedisClient)
                .batchWriteString(
                        Mockito.anyList(), Mockito.anyList(), Mockito.anyList(), Mockito.anyLong());
        return writtenValues;
    }

    private void configureJsonKeySink(int batchSize) {
        when(mockRedisParameters.getBatchSize()).thenReturn(batchSize);
        when(mockRedisParameters.getKeyField()).thenReturn("schema-change:${id}");
        when(mockRedisParameters.getSupportCustomKey()).thenReturn(true);
        when(mockRedisParameters.getRedisDataType()).thenReturn(RedisDataType.KEY);
        when(mockRedisParameters.getExpire()).thenReturn(0L);
    }

    private static SeaTunnelRow insertRow(Object... fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private static AlterTableAddColumnEvent addEmailEvent() {
        return AlterTableAddColumnEvent.addAfter(
                TABLE_IDENTIFIER,
                PhysicalColumn.of("email", BasicType.STRING_TYPE, 128L, true, null, null),
                "name");
    }

    private static TableSchema initialSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0L, false, null, null))
                .column(PhysicalColumn.of("name", BasicType.STRING_TYPE, 64L, true, null, null))
                .column(
                        PhysicalColumn.of(
                                "legacy_note", BasicType.STRING_TYPE, 128L, true, null, null))
                .build();
    }
}
