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

package org.apache.seatunnel.connectors.bigquery.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.convert.BigQuerySerializer;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;
import org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryWriter;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import com.google.api.core.ApiFutures;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.Exceptions.SchemaMismatchedException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.bigquery.sink.writer.BigQueryStreamWriter.SEQUENCE_NUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQuerySinkSchemaEvolutionTest {
    private static final TableIdentifier SOURCE_TABLE =
            TableIdentifier.of("mysql", "shop", "orders");

    @Test
    void testAddColumnRefreshesWriterAndSequenceColumnIndex() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(BigQuerySinkOptions.PROJECT_ID.key(), "test-project");
        options.put(BigQuerySinkOptions.DATASET_ID.key(), "test_dataset");
        options.put(BigQuerySinkOptions.TABLE_ID.key(), "test_table");
        options.put(BigQuerySinkOptions.WRITE_MODE.key(), BigQuerySinkStreamWriter.STREAMING);
        options.put(BigQuerySinkOptions.SCHEMA_EVOLUTION_ENABLED.key(), true);
        options.put(BigQuerySinkOptions.SEQUENCE_NUMBER_COLUMN.key(), "version");
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        TableSchema originalSchema =
                schema(column("id", BasicType.LONG_TYPE), column("version", BasicType.LONG_TYPE));

        BigQueryWriter oldWriter = mock(BigQueryWriter.class);
        BigQueryWriter refreshedWriter = mock(BigQueryWriter.class);
        BigQueryWriteClient writeClient = mock(BigQueryWriteClient.class);
        when(oldWriter.refreshSchema(writeClient, config)).thenReturn(refreshedWriter);
        when(refreshedWriter.append(any(JSONArray.class)))
                .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()));

        BigQuery bigQuery = mockBigQueryWithSchema();
        BigQuerySinkStreamWriter sinkWriter =
                new BigQuerySinkStreamWriter(
                        config,
                        oldWriter,
                        new BigQuerySerializer(originalSchema.toPhysicalRowDataType(), config),
                        originalSchema,
                        writeClient);

        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.addAfter(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE), "id");

        try (MockedStatic<BigQueryClientFactory> clientFactory =
                mockStatic(BigQueryClientFactory.class)) {
            clientFactory
                    .when(() -> BigQueryClientFactory.getBigQuery(any(ReadonlyConfig.class)))
                    .thenReturn(bigQuery);

            sinkWriter.applySchemaChange(event);

            SeaTunnelRow row = new SeaTunnelRow(new Object[] {1L, "alice@example.com", 15L});
            row.setRowKind(RowKind.INSERT);
            sinkWriter.write(row);
            sinkWriter.prepareCommit();
        }

        verify(bigQuery).query(any(QueryJobConfiguration.class));
        verify(oldWriter).refreshSchema(writeClient, config);
        ArgumentCaptor<JSONArray> rows = ArgumentCaptor.forClass(JSONArray.class);
        verify(refreshedWriter).append(rows.capture());
        JSONObject writtenRow = rows.getValue().getJSONObject(0);
        assertEquals("alice@example.com", writtenRow.getString("email"));
        assertEquals("F", writtenRow.getString(SEQUENCE_NUM));
    }

    @Test
    void testRetriesWithOpenWriterUntilStorageWriteApiDetectsSchemaChange() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(BigQuerySinkOptions.PROJECT_ID.key(), "test-project");
        options.put(BigQuerySinkOptions.DATASET_ID.key(), "test_dataset");
        options.put(BigQuerySinkOptions.TABLE_ID.key(), "test_table");
        options.put(BigQuerySinkOptions.WRITE_MODE.key(), BigQuerySinkStreamWriter.STREAMING);
        options.put(BigQuerySinkOptions.SCHEMA_EVOLUTION_ENABLED.key(), true);
        options.put(BigQuerySinkOptions.BATCH_SIZE.key(), 1);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        TableSchema originalSchema = schema(column("id", BasicType.LONG_TYPE));
        BigQueryWriter oldWriter = mock(BigQueryWriter.class);
        BigQueryWriter staleSchemaWriter = mock(BigQueryWriter.class);
        BigQueryWriteClient writeClient = mock(BigQueryWriteClient.class);
        SchemaMismatchedException schemaMismatch = mock(SchemaMismatchedException.class);

        when(oldWriter.refreshSchema(writeClient, config)).thenReturn(staleSchemaWriter);
        when(staleSchemaWriter.append(any(JSONArray.class)))
                .thenReturn(
                        ApiFutures.immediateFailedFuture(schemaMismatch),
                        ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()));

        BigQuery bigQuery = mockBigQueryWithSchema();
        BigQuerySinkStreamWriter sinkWriter =
                new BigQuerySinkStreamWriter(
                        config,
                        oldWriter,
                        new BigQuerySerializer(originalSchema.toPhysicalRowDataType(), config),
                        originalSchema,
                        writeClient) {
                    @Override
                    void waitForSchemaPropagation(long delayMillis) {
                        // Avoid a real backoff in the unit test.
                    }
                };

        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(SOURCE_TABLE, column("email", BasicType.STRING_TYPE));

        try (MockedStatic<BigQueryClientFactory> clientFactory =
                mockStatic(BigQueryClientFactory.class)) {
            clientFactory
                    .when(() -> BigQueryClientFactory.getBigQuery(any(ReadonlyConfig.class)))
                    .thenReturn(bigQuery);

            sinkWriter.applySchemaChange(event);
            sinkWriter.write(new SeaTunnelRow(new Object[] {1L, "alice@example.com"}));
        }

        verify(staleSchemaWriter, times(2)).append(any(JSONArray.class));
        verify(staleSchemaWriter, never()).refreshSchema(writeClient, config);
        verify(staleSchemaWriter).onAppendSuccess(1);
    }

    @Test
    void testRecreatesClosedWriterWhileWaitingForStorageSchema() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(BigQuerySinkOptions.PROJECT_ID.key(), "test-project");
        options.put(BigQuerySinkOptions.DATASET_ID.key(), "test_dataset");
        options.put(BigQuerySinkOptions.TABLE_ID.key(), "test_table");
        options.put(BigQuerySinkOptions.WRITE_MODE.key(), BigQuerySinkStreamWriter.STREAMING);
        options.put(BigQuerySinkOptions.SCHEMA_EVOLUTION_ENABLED.key(), true);
        options.put(BigQuerySinkOptions.BATCH_SIZE.key(), 1);
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);

        TableSchema originalSchema = schema(column("id", BasicType.LONG_TYPE));
        BigQueryWriter oldWriter = mock(BigQueryWriter.class);
        BigQueryWriter closedWriter = mock(BigQueryWriter.class);
        BigQueryWriter recreatedWriter = mock(BigQueryWriter.class);
        BigQueryWriteClient writeClient = mock(BigQueryWriteClient.class);
        SchemaMismatchedException schemaMismatch = mock(SchemaMismatchedException.class);

        when(oldWriter.refreshSchema(writeClient, config)).thenReturn(closedWriter);
        when(closedWriter.append(any(JSONArray.class)))
                .thenReturn(ApiFutures.immediateFailedFuture(schemaMismatch));
        when(closedWriter.isClosed()).thenReturn(true);
        when(closedWriter.refreshSchema(writeClient, config)).thenReturn(recreatedWriter);
        when(recreatedWriter.append(any(JSONArray.class)))
                .thenReturn(ApiFutures.immediateFuture(AppendRowsResponse.getDefaultInstance()));

        BigQuery bigQuery = mockBigQueryWithSchema();
        BigQuerySinkStreamWriter sinkWriter =
                new BigQuerySinkStreamWriter(
                        config,
                        oldWriter,
                        new BigQuerySerializer(originalSchema.toPhysicalRowDataType(), config),
                        originalSchema,
                        writeClient) {
                    @Override
                    void waitForSchemaPropagation(long delayMillis) {
                        // Avoid a real backoff in the unit test.
                    }
                };

        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(SOURCE_TABLE, column("email", BasicType.STRING_TYPE));

        try (MockedStatic<BigQueryClientFactory> clientFactory =
                mockStatic(BigQueryClientFactory.class)) {
            clientFactory
                    .when(() -> BigQueryClientFactory.getBigQuery(any(ReadonlyConfig.class)))
                    .thenReturn(bigQuery);

            sinkWriter.applySchemaChange(event);
            sinkWriter.write(new SeaTunnelRow(new Object[] {1L, "alice@example.com"}));
        }

        verify(closedWriter).append(any(JSONArray.class));
        verify(closedWriter).refreshSchema(writeClient, config);
        verify(recreatedWriter).append(any(JSONArray.class));
        verify(recreatedWriter).onAppendSuccess(1);
    }

    private static BigQuery mockBigQueryWithSchema() throws InterruptedException {
        BigQuery bigQuery = mock(BigQuery.class);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mock(TableResult.class));

        Table beforeSchemaChange =
                mockTable(
                        Schema.of(
                                Field.of("id", StandardSQLTypeName.INT64),
                                Field.of("version", StandardSQLTypeName.INT64)));
        Table afterSchemaChange =
                mockTable(
                        Schema.of(
                                Field.of("id", StandardSQLTypeName.INT64),
                                Field.of("email", StandardSQLTypeName.STRING),
                                Field.of("version", StandardSQLTypeName.INT64)));
        when(bigQuery.getTable(TableId.of("test-project", "test_dataset", "test_table")))
                .thenReturn(beforeSchemaChange, afterSchemaChange);
        return bigQuery;
    }

    private static Table mockTable(Schema schema) {
        Table table = mock(Table.class);
        TableDefinition definition = mock(TableDefinition.class);
        when(table.getDefinition()).thenReturn(definition);
        when(definition.getSchema()).thenReturn(schema);
        return table;
    }

    private static TableSchema schema(Column... columns) {
        return TableSchema.builder().columns(Arrays.asList(columns)).build();
    }

    private static Column column(
            String name, org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> type) {
        return PhysicalColumn.of(name, type, (Long) null, true, null, null);
    }
}
