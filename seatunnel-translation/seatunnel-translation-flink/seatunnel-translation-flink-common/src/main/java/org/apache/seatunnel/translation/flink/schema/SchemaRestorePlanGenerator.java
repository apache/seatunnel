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

package org.apache.seatunnel.translation.flink.schema;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.event.TableEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Builds a bounded restore event from the initial sink schema and a complete target snapshot. */
final class SchemaRestorePlanGenerator {

    private SchemaRestorePlanGenerator() {}

    /**
     * Builds one composite event that deterministically changes a newly-created writer from its
     * initial schema to the latest checkpointed target schema.
     *
     * <p>Every target column is represented as an add operation. SeaTunnel's common schema handler
     * interprets an add of an existing column as a modify/reposition operation. Consequently this
     * plan reconstructs column definitions and order without retaining the full DDL history. A
     * connector's physical schema handler must treat already-satisfied operations as successful.
     */
    static AlterTableColumnsEvent generate(
            CatalogTable initialTable, SchemaChangeEvent latestEvent) {
        TableIdentifier tableId = latestEvent == null ? null : latestEvent.tableIdentifier();
        String jobId = latestEvent == null ? null : latestEvent.getJobId();
        if (initialTable == null) {
            throw failure(
                    "Cannot restore sink schema because the initial sink schema is missing",
                    tableId,
                    jobId,
                    null);
        }
        if (latestEvent == null || latestEvent.getChangeAfter() == null) {
            throw failure(
                    "Cannot restore sink schema because the latest schema event has no complete changeAfter snapshot",
                    tableId,
                    jobId,
                    null);
        }

        CatalogTable targetTable = latestEvent.getChangeAfter();
        TablePath eventTablePath = tableId.toTablePath();
        if (!initialTable.getTablePath().equals(eventTablePath)
                || !targetTable.getTablePath().equals(eventTablePath)) {
            throw failure(
                    String.format(
                            "Cannot restore sink schema because table paths differ "
                                    + "(initial=%s, event=%s, target=%s)",
                            initialTable.getTableId(), tableId, targetTable.getTableId()),
                    tableId,
                    jobId,
                    null);
        }

        List<Column> targetColumns = targetTable.getTableSchema().getColumns();
        Set<String> targetColumnNames = new HashSet<>();
        for (Column targetColumn : targetColumns) {
            if (!targetColumnNames.add(targetColumn.getName())) {
                throw failure(
                        "Cannot restore sink schema because target schema contains duplicate column "
                                + targetColumn.getName(),
                        tableId,
                        jobId,
                        null);
            }
        }

        List<AlterTableColumnEvent> operations = new ArrayList<>();
        for (Column initialColumn : initialTable.getTableSchema().getColumns()) {
            if (!targetColumnNames.contains(initialColumn.getName())) {
                AlterTableDropColumnEvent dropColumnEvent =
                        new AlterTableDropColumnEvent(tableId, initialColumn.getName());
                copyMetadata(latestEvent, dropColumnEvent);
                operations.add(dropColumnEvent);
            }
        }
        for (int index = 0; index < targetColumns.size(); index++) {
            Column targetColumn = targetColumns.get(index).copy();
            AlterTableAddColumnEvent addColumnEvent;
            if (index == 0) {
                addColumnEvent = AlterTableAddColumnEvent.addFirst(tableId, targetColumn);
            } else {
                addColumnEvent =
                        AlterTableAddColumnEvent.addAfter(
                                tableId, targetColumn, targetColumns.get(index - 1).getName());
            }
            copyMetadata(latestEvent, addColumnEvent);
            operations.add(addColumnEvent);
        }

        AlterTableColumnsEvent restorePlan = new AlterTableColumnsEvent(tableId, operations);
        restorePlan.setJobId(jobId);
        restorePlan.setChangeAfter(targetTable.copy());
        if (latestEvent instanceof TableEvent) {
            TableEvent tableEvent = (TableEvent) latestEvent;
            restorePlan.setStatement(tableEvent.getStatement());
            restorePlan.setSourceDialectName(tableEvent.getSourceDialectName());
        }

        validate(initialTable.getTableSchema(), targetTable.getTableSchema(), restorePlan, jobId);
        return restorePlan;
    }

    private static void copyMetadata(SchemaChangeEvent source, TableEvent target) {
        target.setJobId(source.getJobId());
        if (source instanceof TableEvent) {
            TableEvent sourceTableEvent = (TableEvent) source;
            target.setStatement(sourceTableEvent.getStatement());
            target.setSourceDialectName(sourceTableEvent.getSourceDialectName());
        }
    }

    private static void validate(
            TableSchema initialSchema,
            TableSchema targetSchema,
            AlterTableColumnsEvent restorePlan,
            String jobId) {
        TableSchema rebuiltSchema;
        try {
            rebuiltSchema =
                    new TableSchemaChangeEventDispatcher().reset(initialSchema).apply(restorePlan);
        } catch (Exception e) {
            throw failure(
                    "Failed to validate the compact sink schema restore plan",
                    restorePlan.tableIdentifier(),
                    jobId,
                    e);
        }
        if (!targetSchema.equals(rebuiltSchema)) {
            throw failure(
                    String.format(
                            "Compact sink schema restore plan cannot reconstruct the target schema "
                                    + "(rebuilt=%s, target=%s)",
                            rebuiltSchema, targetSchema),
                    restorePlan.tableIdentifier(),
                    jobId,
                    null);
        }
    }

    private static SchemaEvolutionException failure(
            String message, TableIdentifier tableId, String jobId, Throwable cause) {
        return new SchemaEvolutionException(
                SchemaEvolutionErrorCode.SCHEMA_EVENT_PROCESSING_FAILED,
                message,
                tableId,
                jobId,
                cause);
    }
}
