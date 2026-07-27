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
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.connectors.bigquery.client.BigQueryClientFactory;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.bigquery.option.BigQuerySinkOptions;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/** Applies supported SeaTunnel schema change events to a configured BigQuery table. */
public class BigQuerySchemaChangeManager {
    private final BigQuery bigQuery;
    private final TableId tableId;
    private final String quotedTable;
    private final boolean relaxNotNull;

    public BigQuerySchemaChangeManager(ReadonlyConfig config) {
        this(config, BigQueryClientFactory.getBigQuery(config));
    }

    BigQuerySchemaChangeManager(ReadonlyConfig config, BigQuery bigQuery) {
        this.bigQuery = bigQuery;
        String projectId = config.get(BigQuerySinkOptions.PROJECT_ID);
        String datasetId = config.get(BigQuerySinkOptions.DATASET_ID);
        String tableName = config.get(BigQuerySinkOptions.TABLE_ID);
        this.relaxNotNull = config.get(BigQuerySinkOptions.SCHEMA_EVOLUTION_RELAX_NOT_NULL);
        this.tableId = TableId.of(projectId, datasetId, tableName);
        this.quotedTable =
                "`"
                        + validateIdentifier(projectId)
                        + "."
                        + validateIdentifier(datasetId)
                        + "."
                        + validateIdentifier(tableName)
                        + "`";
    }

    /**
     * Applies one ADD COLUMN event, or an atomic group containing only ADD COLUMN events.
     *
     * <p>BigQuery can only add NULLABLE or REPEATED columns to an existing table. Position hints
     * such as FIRST and AFTER are intentionally ignored because BigQuery appends new fields.
     */
    public void applySchemaChange(SchemaChangeEvent event) {
        List<AlterTableAddColumnEvent> addColumnEvents = extractAddColumnEvents(event);
        validateColumns(addColumnEvents);

        try {
            if (!hasMissingColumns(addColumnEvents)) {
                return;
            }
            String actions =
                    addColumnEvents.stream()
                            .map(this::toAddColumnAction)
                            .collect(Collectors.joining(", "));
            String sql = "ALTER TABLE " + quotedTable + " " + actions;
            bigQuery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build());
            validateAppliedSchema(addColumnEvents);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw schemaChangeFailed(event, e);
        } catch (BigQueryConnectorException e) {
            throw e;
        } catch (RuntimeException e) {
            throw schemaChangeFailed(event, e);
        }
    }

    private List<AlterTableAddColumnEvent> extractAddColumnEvents(SchemaChangeEvent event) {
        List<AlterTableAddColumnEvent> events = new ArrayList<>();
        if (event instanceof AlterTableAddColumnEvent) {
            events.add((AlterTableAddColumnEvent) event);
            return events;
        }
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                if (!(columnEvent instanceof AlterTableAddColumnEvent)) {
                    throw unsupportedEvent(event);
                }
                events.add((AlterTableAddColumnEvent) columnEvent);
            }
            if (events.isEmpty()) {
                throw unsupportedEvent(event);
            }
            return events;
        }
        throw unsupportedEvent(event);
    }

    private void validateColumns(List<AlterTableAddColumnEvent> events) {
        Set<String> columnNames = new HashSet<>();
        for (AlterTableAddColumnEvent event : events) {
            Column column = event.getColumn();
            if (!column.isPhysical()) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "BigQuery schema evolution only supports physical columns: "
                                + column.getName());
            }
            // BigQuery represents ARRAY columns as REPEATED fields, so source nullability does not
            // map to REQUIRED/NULLABLE for this type.
            boolean repeated = column.getDataType() instanceof ArrayType;
            if (repeated && column.isNullable()) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "BigQuery ARRAY columns cannot preserve nullable array semantics because "
                                + "REPEATED fields cannot be NULL: "
                                + column.getName());
            }
            if (!column.isNullable() && !repeated && !relaxNotNull) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "BigQuery cannot add a REQUIRED column to an existing table: "
                                + column.getName()
                                + ". Set schema_evolution_relax_not_null=true to add it as "
                                + "NULLABLE.");
            }
            if (!columnNames.add(column.getName().toLowerCase(Locale.ROOT))) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "Duplicate ADD COLUMN event for BigQuery column: " + column.getName());
            }
            BigQueryTypeConverter.toDdlType(column.getDataType());
        }
    }

    private String toAddColumnAction(AlterTableAddColumnEvent event) {
        Column column = event.getColumn();
        return "ADD COLUMN IF NOT EXISTS "
                + quoteIdentifier(column.getName())
                + " "
                + BigQueryTypeConverter.toDdlType(column.getDataType());
    }

    private boolean hasMissingColumns(List<AlterTableAddColumnEvent> events) {
        FieldList fields = getTargetFields("before ADD COLUMN");
        boolean hasMissingColumn = false;
        for (AlterTableAddColumnEvent event : events) {
            Column column = event.getColumn();
            Field actualField = findField(fields, column.getName());
            if (actualField == null) {
                hasMissingColumn = true;
            } else {
                validateCompatibleField(column, actualField);
            }
        }
        return hasMissingColumn;
    }

    private void validateAppliedSchema(List<AlterTableAddColumnEvent> events) {
        FieldList fields = getTargetFields("after ADD COLUMN");
        for (AlterTableAddColumnEvent event : events) {
            Column column = event.getColumn();
            Field actualField = findField(fields, column.getName());
            if (actualField == null) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "BigQuery did not expose the added column after schema change: "
                                + column.getName());
            }
            validateCompatibleField(column, actualField);
        }
    }

    private FieldList getTargetFields(String operation) {
        Table table = bigQuery.getTable(tableId);
        Schema schema =
                table == null || table.getDefinition() == null
                        ? null
                        : table.getDefinition().getSchema();
        FieldList fields = schema == null ? null : schema.getFields();
        if (fields == null) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    String.format(
                            "BigQuery target table does not exist or has no schema %s: %s",
                            operation, tableId));
        }
        return fields;
    }

    private Field findField(FieldList fields, String columnName) {
        return fields.stream()
                .filter(field -> columnName.equalsIgnoreCase(field.getName()))
                .findFirst()
                .orElse(null);
    }

    private void validateCompatibleField(Column column, Field actualField) {
        StandardSQLTypeName expectedType =
                BigQueryTypeConverter.toStandardType(column.getDataType());
        StandardSQLTypeName actualType = actualField.getType().getStandardType();
        Field.Mode expectedMode =
                column.getDataType() instanceof ArrayType
                        ? Field.Mode.REPEATED
                        : Field.Mode.NULLABLE;
        Field.Mode actualMode =
                actualField.getMode() == null ? Field.Mode.NULLABLE : actualField.getMode();
        if (expectedType != actualType || expectedMode != actualMode) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    String.format(
                            "BigQuery column %s exists with incompatible type or mode. "
                                    + "Expected %s %s but found %s %s",
                            column.getName(), expectedType, expectedMode, actualType, actualMode));
        }
    }

    static String quoteIdentifier(String identifier) {
        return "`" + validateIdentifier(identifier) + "`";
    }

    private static String validateIdentifier(String identifier) {
        if (identifier == null
                || identifier.isEmpty()
                || identifier.indexOf('`') >= 0
                || identifier.indexOf('\n') >= 0
                || identifier.indexOf('\r') >= 0) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    "Invalid BigQuery identifier in schema change: " + identifier);
        }
        return identifier;
    }

    private BigQueryConnectorException unsupportedEvent(SchemaChangeEvent event) {
        return new BigQueryConnectorException(
                BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                "BigQuery schema evolution only supports ADD COLUMN events, but received: "
                        + event.getEventType());
    }

    private BigQueryConnectorException schemaChangeFailed(
            SchemaChangeEvent event, Throwable cause) {
        return new BigQueryConnectorException(
                BigQueryConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                String.format(
                        "Failed to apply %s to BigQuery table %s", event.getEventType(), tableId),
                cause);
    }
}
