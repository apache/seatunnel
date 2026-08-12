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

package org.apache.seatunnel.connectors.bigquery.schema;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQuerySchemaChangeManagerTest {
    private static final TableIdentifier SOURCE_TABLE =
            TableIdentifier.of("mysql", "shop", "products");
    private static final TableId TARGET_TABLE =
            TableId.of("test-project", "test_dataset", "test_table");

    private Map<String, Object> options;
    private BigQuery bigQuery;
    private BigQuerySchemaChangeManager manager;

    @BeforeEach
    void setUp() throws InterruptedException {
        options = new HashMap<>();
        options.put(BigQuerySinkOptions.PROJECT_ID.key(), TARGET_TABLE.getProject());
        options.put(BigQuerySinkOptions.DATASET_ID.key(), TARGET_TABLE.getDataset());
        options.put(BigQuerySinkOptions.TABLE_ID.key(), TARGET_TABLE.getTable());

        bigQuery = mock(BigQuery.class);
        when(bigQuery.query(any(QueryJobConfiguration.class))).thenReturn(mock(TableResult.class));
        manager = new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery);
    }

    @Test
    void testApplyNullableAddColumn() throws Exception {
        mockTargetSchema(Field.of("email", StandardSQLTypeName.STRING));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `email` STRING",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testApplyRepeatedAddColumn() throws Exception {
        mockTargetSchema(
                Field.newBuilder("tags", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REPEATED)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("tags", ArrayType.STRING_ARRAY_TYPE, false));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `tags` ARRAY<STRING>",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testRejectNullableArrayColumn() throws Exception {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("tags", ArrayType.STRING_ARRAY_TYPE, true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("cannot preserve nullable array semantics"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testSkipDdlWhenCompatibleColumnAlreadyExists() throws Exception {
        mockExistingTargetSchema(Field.of("email", StandardSQLTypeName.STRING));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        manager.applySchemaChange(event);

        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testApplyMultipleAddColumnsAsOneStatement() throws Exception {
        mockTargetSchema(
                Field.of("score", StandardSQLTypeName.INT64),
                Field.of("amount", StandardSQLTypeName.NUMERIC));
        AlterTableColumnsEvent event =
                new AlterTableColumnsEvent(SOURCE_TABLE)
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        SOURCE_TABLE, column("score", BasicType.INT_TYPE, true)))
                        .addEvent(
                                AlterTableAddColumnEvent.add(
                                        SOURCE_TABLE,
                                        column("amount", new DecimalType(20, 2), true)));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `score` INT64, "
                        + "ADD COLUMN IF NOT EXISTS `amount` NUMERIC(20, 2)",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testRejectRequiredColumn() throws Exception {
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("required_value", BasicType.STRING_TYPE, false));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertEquals(
                BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED, exception.getSeaTunnelErrorCode());
        assertTrue(exception.getMessage().contains("REQUIRED"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRelaxRequiredColumnToNullable() throws Exception {
        options.put(BigQuerySinkOptions.SCHEMA_EVOLUTION_RELAX_NOT_NULL.key(), true);
        manager = new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery);
        mockTargetSchema(Field.of("required_value", StandardSQLTypeName.STRING));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("required_value", BasicType.STRING_TYPE, false));

        manager.applySchemaChange(event);

        ArgumentCaptor<QueryJobConfiguration> queryCaptor =
                ArgumentCaptor.forClass(QueryJobConfiguration.class);
        verify(bigQuery).query(queryCaptor.capture());
        assertEquals(
                "ALTER TABLE `test-project.test_dataset.test_table` "
                        + "ADD COLUMN IF NOT EXISTS `required_value` STRING",
                queryCaptor.getValue().getQuery());
    }

    @Test
    void testRejectDropColumn() throws Exception {
        AlterTableDropColumnEvent event = new AlterTableDropColumnEvent(SOURCE_TABLE, "obsolete");

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("only supports ADD COLUMN"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingColumnWithDifferentType() throws Exception {
        mockExistingTargetSchema(Field.of("email", StandardSQLTypeName.INT64));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("incompatible type or mode"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingRequiredColumn() throws Exception {
        mockExistingTargetSchema(
                Field.newBuilder("email", StandardSQLTypeName.STRING)
                        .setMode(Field.Mode.REQUIRED)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("incompatible type or mode"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testConcurrentHandlersRecoverFromTableUpdateQuota() throws Exception {
        int writerCount = 6;
        Table missingColumnTable = mockTable(Schema.of());
        Table appliedTable = mockTable(Schema.of(Field.of("email", StandardSQLTypeName.STRING)));
        CountDownLatch initialReads = new CountDownLatch(writerCount);
        CountDownLatch ddlApplied = new CountDownLatch(1);
        ThreadLocal<Boolean> firstRead = ThreadLocal.withInitial(() -> true);
        AtomicBoolean schemaVisible = new AtomicBoolean(false);
        AtomicInteger ddlAttempts = new AtomicInteger();

        when(bigQuery.getTable(TARGET_TABLE))
                .thenAnswer(
                        invocation -> {
                            if (firstRead.get()) {
                                firstRead.set(false);
                                initialReads.countDown();
                                assertTrue(initialReads.await(5, TimeUnit.SECONDS));
                                return missingColumnTable;
                            }
                            return schemaVisible.get() ? appliedTable : missingColumnTable;
                        });
        when(bigQuery.query(any(QueryJobConfiguration.class)))
                .thenAnswer(
                        invocation -> {
                            int attempt = ddlAttempts.incrementAndGet();
                            if (attempt > 5) {
                                assertTrue(ddlApplied.await(5, TimeUnit.SECONDS));
                                throw tableUpdateQuotaExceeded();
                            }
                            schemaVisible.set(true);
                            ddlApplied.countDown();
                            return mock(TableResult.class);
                        });

        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));
        ExecutorService executor = Executors.newFixedThreadPool(writerCount);
        try {
            List<Future<Void>> results = new ArrayList<>();
            for (int index = 0; index < writerCount; index++) {
                BigQuerySchemaChangeManager concurrentManager =
                        new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery);
                results.add(
                        executor.submit(
                                () -> {
                                    concurrentManager.applySchemaChange(event);
                                    return null;
                                }));
            }
            for (Future<Void> result : results) {
                result.get(10, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(writerCount, ddlAttempts.get());
        verify(bigQuery, times(writerCount)).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRetryDdlAfterQuotaFailureWhileColumnIsStillMissing() throws Exception {
        Table missingColumnTable = mockTable(Schema.of());
        Table appliedTable = mockTable(Schema.of(Field.of("email", StandardSQLTypeName.STRING)));
        when(bigQuery.getTable(TARGET_TABLE))
                .thenReturn(
                        missingColumnTable, missingColumnTable, missingColumnTable, appliedTable);
        JobException quotaException = tableUpdateQuotaExceeded();
        when(bigQuery.query(any(QueryJobConfiguration.class)))
                .thenThrow(quotaException)
                .thenReturn(mock(TableResult.class));
        AtomicInteger waits = new AtomicInteger();
        manager =
                new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery) {
                    @Override
                    void waitForRetry(long delayMillis) {
                        waits.incrementAndGet();
                    }
                };
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        manager.applySchemaChange(event);

        assertEquals(1, waits.get());
        verify(bigQuery, times(2)).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testPollSchemaWithoutRepeatingSuccessfulDdl() throws Exception {
        Table missingColumnTable = mockTable(Schema.of());
        Table appliedTable = mockTable(Schema.of(Field.of("email", StandardSQLTypeName.STRING)));
        when(bigQuery.getTable(TARGET_TABLE))
                .thenReturn(missingColumnTable, missingColumnTable, appliedTable);
        AtomicInteger waits = new AtomicInteger();
        manager =
                new BigQuerySchemaChangeManager(ReadonlyConfig.fromMap(options), bigQuery) {
                    @Override
                    void waitForRetry(long delayMillis) {
                        waits.incrementAndGet();
                    }
                };
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("email", BasicType.STRING_TYPE, true));

        manager.applySchemaChange(event);

        assertEquals(1, waits.get());
        verify(bigQuery).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingNarrowerDecimal() throws Exception {
        mockExistingTargetSchema(
                Field.newBuilder("amount", StandardSQLTypeName.NUMERIC)
                        .setPrecision(10L)
                        .setScale(2L)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("amount", new DecimalType(20, 2), true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("incompatible decimal precision or scale"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testAcceptExistingDecimalWithSufficientCapacity() throws Exception {
        mockExistingTargetSchema(
                Field.newBuilder("amount", StandardSQLTypeName.NUMERIC)
                        .setPrecision(22L)
                        .setScale(4L)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("amount", new DecimalType(20, 2), true));

        manager.applySchemaChange(event);

        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingStructWithIncompatibleNestedField() throws Exception {
        mockExistingTargetSchema(
                Field.of(
                        "profile",
                        StandardSQLTypeName.STRUCT,
                        Field.of("name", StandardSQLTypeName.STRING),
                        Field.of("score", StandardSQLTypeName.STRING)));
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(SOURCE_TABLE, column("profile", profileType(), true));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("profile.score"));
        assertTrue(exception.getMessage().contains("incompatible type or mode"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    @Test
    void testRejectExistingArrayOfStructWithIncompatibleNestedMode() throws Exception {
        mockExistingTargetSchema(
                Field.newBuilder(
                                "profiles",
                                StandardSQLTypeName.STRUCT,
                                Field.of("name", StandardSQLTypeName.STRING),
                                Field.newBuilder("score", StandardSQLTypeName.INT64)
                                        .setMode(Field.Mode.REQUIRED)
                                        .build())
                        .setMode(Field.Mode.REPEATED)
                        .build());
        AlterTableAddColumnEvent event =
                AlterTableAddColumnEvent.add(
                        SOURCE_TABLE, column("profiles", ArrayType.of(profileType()), false));

        BigQueryConnectorException exception =
                assertThrows(
                        BigQueryConnectorException.class, () -> manager.applySchemaChange(event));

        assertTrue(exception.getMessage().contains("profiles.score"));
        assertTrue(exception.getMessage().contains("incompatible type or mode"));
        verify(bigQuery, never()).query(any(QueryJobConfiguration.class));
    }

    private void mockTargetSchema(Field... fields) {
        Table beforeSchemaChange = mockTable(Schema.of());
        Table afterSchemaChange = mockTable(Schema.of(fields));
        when(bigQuery.getTable(TARGET_TABLE)).thenReturn(beforeSchemaChange, afterSchemaChange);
    }

    private void mockExistingTargetSchema(Field... fields) {
        Table existingTable = mockTable(Schema.of(fields));
        when(bigQuery.getTable(TARGET_TABLE)).thenReturn(existingTable);
    }

    private Table mockTable(Schema schema) {
        Table table = mock(Table.class);
        TableDefinition definition = mock(TableDefinition.class);
        when(table.getDefinition()).thenReturn(definition);
        when(definition.getSchema()).thenReturn(schema);
        return table;
    }

    private static Column column(String name, SeaTunnelDataType<?> type, boolean nullable) {
        return PhysicalColumn.of(name, type, (Long) null, nullable, null, null);
    }

    private static SeaTunnelRowType profileType() {
        return new SeaTunnelRowType(
                new String[] {"name", "score"},
                new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }

    private static JobException tableUpdateQuotaExceeded() {
        String message = "Exceeded rate limits: too many table update operations for this table";
        JobException exception = mock(JobException.class);
        when(exception.getErrors())
                .thenReturn(
                        Collections.singletonList(
                                new BigQueryError("rateLimitExceeded", "table", message)));
        return exception;
    }
}
