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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

class RedisSinkTest {

    private static final TableIdentifier TABLE_IDENTIFIER =
            TableIdentifier.of("catalog", "shop", null, "schema_events");

    private RedisClient redisClient;
    private RedisParameters redisParameters;
    private RedisSink redisSink;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        redisClient = Mockito.mock(RedisClient.class);
        redisParameters = Mockito.mock(RedisParameters.class);
        when(redisParameters.buildRedisClient()).thenReturn(redisClient);
        when(redisParameters.getBatchSize()).thenReturn(1);
        when(redisParameters.getFormat()).thenReturn(RedisBaseOptions.Format.JSON);
        when(redisParameters.getFieldDelimiter()).thenReturn(",");
        when(redisParameters.getKeyField()).thenReturn("schema-change:${id}");
        when(redisParameters.getSupportCustomKey()).thenReturn(true);
        when(redisParameters.getRedisDataType()).thenReturn(RedisDataType.KEY);
        when(redisParameters.getExpire()).thenReturn(0L);

        redisSink =
                new RedisSink(ReadonlyConfig.fromMap(sinkConfig()), catalogTable(initialSchema()));
        Field redisParametersField = RedisSink.class.getDeclaredField("redisParameters");
        redisParametersField.setAccessible(true);
        redisParametersField.set(redisSink, redisParameters);
    }

    @Test
    void testAdvertisesSupportedSchemaChanges() {
        Assertions.assertInstanceOf(SupportSchemaEvolutionSink.class, redisSink);
        SupportSchemaEvolutionSink schemaEvolutionSink = (SupportSchemaEvolutionSink) redisSink;

        Assertions.assertEquals(
                Arrays.asList(
                        SchemaChangeType.ADD_COLUMN,
                        SchemaChangeType.DROP_COLUMN,
                        SchemaChangeType.RENAME_COLUMN,
                        SchemaChangeType.UPDATE_COLUMN),
                schemaEvolutionSink.supports());
    }

    @Test
    void testRestoreWriterUsesConfiguredSchemaWhenStatesAreNull() throws IOException {
        RedisSinkWriter writer = redisSink.restoreWriter(context(), null);

        Assertions.assertEquals(initialSchema(), writer.snapshotState(1L).get(0));
    }

    @Test
    void testRestoreWriterUsesConfiguredSchemaWhenStatesAreEmpty() throws IOException {
        RedisSinkWriter writer = redisSink.restoreWriter(context(), Collections.emptyList());

        Assertions.assertEquals(initialSchema(), writer.snapshotState(1L).get(0));
    }

    @Test
    void testRestoreWriterUsesSingleState() throws IOException {
        TableSchema restoredSchema = evolvedSchema();

        RedisSinkWriter writer =
                redisSink.restoreWriter(context(), Collections.singletonList(restoredSchema));

        Assertions.assertEquals(restoredSchema, writer.snapshotState(1L).get(0));
    }

    @Test
    void testRestoreWriterAcceptsIdenticalStates() throws IOException {
        TableSchema restoredSchema = evolvedSchema();

        RedisSinkWriter writer =
                redisSink.restoreWriter(
                        context(), Arrays.asList(restoredSchema, restoredSchema.copy()));

        Assertions.assertEquals(restoredSchema, writer.snapshotState(1L).get(0));
    }

    @Test
    void testRestoreWriterRejectsConflictingStatesWithTableAndFieldContext() {
        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                redisSink.restoreWriter(
                                        context(),
                                        Arrays.asList(initialSchema(), evolvedSchema())));

        Assertions.assertTrue(
                exception.getMessage().contains(TABLE_IDENTIFIER.toTablePath().getFullName()),
                exception.getMessage());
        Assertions.assertTrue(exception.getMessage().contains("state 1"), exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("[id, name]"), exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("[id, name, email]"), exception.getMessage());
    }

    @Test
    void testSerializedStateRestoresWriterWithLatestSchema() throws IOException {
        List<List<String>> writtenValues = new ArrayList<>();
        Mockito.doAnswer(
                        invocation -> {
                            List<String> values = invocation.getArgument(2);
                            writtenValues.add(new ArrayList<>(values));
                            return null;
                        })
                .when(redisClient)
                .batchWriteString(
                        Mockito.anyList(), Mockito.anyList(), Mockito.anyList(), Mockito.anyLong());
        Serializer<TableSchema> serializer =
                redisSink.getWriterStateSerializer().orElseThrow(AssertionError::new);
        TableSchema restoredSchema = serializer.deserialize(serializer.serialize(evolvedSchema()));
        RedisSinkWriter writer =
                redisSink.restoreWriter(context(), Collections.singletonList(restoredSchema));
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "Alice", "alice@example.test"});
        row.setRowKind(RowKind.INSERT);

        writer.write(row);

        Assertions.assertEquals(1, writtenValues.size());
        Assertions.assertEquals(
                "{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@example.test\"}",
                writtenValues.get(0).get(0));
    }

    private static SinkWriter.Context context() {
        return Mockito.mock(SinkWriter.Context.class);
    }

    private static CatalogTable catalogTable(TableSchema schema) {
        return CatalogTable.of(
                TABLE_IDENTIFIER, schema, new HashMap<>(), new ArrayList<>(), null, "catalog");
    }

    private static TableSchema initialSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0L, false, null, null))
                .column(PhysicalColumn.of("name", BasicType.STRING_TYPE, 64L, true, null, null))
                .build();
    }

    private static TableSchema evolvedSchema() {
        return TableSchema.builder()
                .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 0L, false, null, null))
                .column(PhysicalColumn.of("name", BasicType.STRING_TYPE, 64L, true, null, null))
                .column(PhysicalColumn.of("email", BasicType.STRING_TYPE, 128L, true, null, null))
                .build();
    }

    private static Map<String, Object> sinkConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("mode", "SINGLE");
        config.put("host", "localhost");
        config.put("port", 6379);
        config.put("key", "schema-change:${id}");
        config.put("data_type", "KEY");
        return config;
    }
}
