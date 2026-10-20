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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode;
import org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionException;
import org.apache.seatunnel.api.table.schema.exception.SchemaValidationException;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

/** Resolves PostgreSQL ADD COLUMN events by diffing the cached schema and a pgoutput RELATION. */
public class PostgresRelationSchemaChangeResolver implements SchemaChangeResolver {

    @Override
    public boolean support(SourceRecord record) {
        return record.keySchema() != null
                && PostgresRelationSchemaRecord.KEY_SCHEMA_NAME.equals(record.keySchema().name());
    }

    @Override
    public SchemaChangeEvent resolve(SourceRecord record, List<CatalogTable> catalogTables) {
        Table after = null;
        CatalogTable before = null;
        try {
            after = extractTable(record);
            before = findCatalogTable(after, catalogTables);
            List<AlterTableColumnEvent> events = resolveAddedColumns(before, after);
            if (events.isEmpty()) {
                return null;
            }

            events.forEach(event -> event.setSourceDialectName(DatabaseIdentifier.POSTGRESQL));
            AlterTableColumnsEvent result = new AlterTableColumnsEvent(before.getTableId(), events);
            result.setSourceDialectName(DatabaseIdentifier.POSTGRESQL);
            return result;
        } catch (SchemaEvolutionException e) {
            throw e;
        } catch (Exception e) {
            String relationId = after == null ? "unknown" : after.id().toString();
            throw new SchemaEvolutionException(
                    SchemaEvolutionErrorCode.INVALID_SCHEMA_STRUCTURE,
                    "Failed to resolve PostgreSQL RELATION schema change for "
                            + relationId
                            + ". Continuing could make the produced row schema diverge from the source relation.",
                    before == null ? null : before.getTableId(),
                    null,
                    e);
        }
    }

    private Table extractTable(SourceRecord record) {
        Struct value = (Struct) record.value();
        List<Struct> changes = value.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        if (changes == null || changes.isEmpty()) {
            throw invalidRelationRecord("PostgreSQL relation record has no table change payload");
        }
        TableChanges tableChanges = new ConnectTableChangeSerializer().deserialize(changes, true);
        return StreamSupport.stream(tableChanges.spliterator(), false)
                .map(TableChanges.TableChange::getTable)
                .findFirst()
                .orElseThrow(
                        () -> invalidRelationRecord("PostgreSQL relation record has no table"));
    }

    private CatalogTable findCatalogTable(Table after, List<CatalogTable> catalogTables) {
        if (catalogTables == null) {
            throw invalidRelationRecord(
                    "Cached schemas are unavailable for PostgreSQL relation " + after.id());
        }
        return catalogTables.stream()
                .filter(
                        table ->
                                Objects.equals(
                                                table.getTablePath().getSchemaName(),
                                                relationSchemaName(after))
                                        && Objects.equals(
                                                table.getTablePath().getTableName(),
                                                after.id().table()))
                .findFirst()
                .orElseThrow(
                        () ->
                                invalidRelationRecord(
                                        "Cannot find cached schema for PostgreSQL table "
                                                + after.id()));
    }

    public static String relationSchemaName(Table relation) {
        // SeaTunnel's ConnectTableChangeSerializer parses a two-part quoted PostgreSQL identifier
        // into TableId.catalog + TableId.table. Prefer the real schema field when present and fall
        // back to catalog for synthetic relation records after deserialization.
        return relation.id().schema() != null ? relation.id().schema() : relation.id().catalog();
    }

    private List<AlterTableColumnEvent> resolveAddedColumns(CatalogTable before, Table after) {
        List<Column> beforeColumns = before.getTableSchema().getColumns();

        if (after.columns().size() < beforeColumns.size()) {
            throw unsupportedChange(before, after);
        }

        for (int i = 0; i < beforeColumns.size(); i++) {
            Column beforeColumn = beforeColumns.get(i);
            Column afterColumn = convertToSeaTunnelColumn(after, i);
            if (!Objects.equals(beforeColumn.getName(), afterColumn.getName())
                    || !Objects.equals(beforeColumn.getDataType(), afterColumn.getDataType())
                    || beforeColumn.isNullable() != afterColumn.isNullable()) {
                throw unsupportedChange(before, after);
            }
        }

        List<AlterTableColumnEvent> events = new ArrayList<>();
        for (int i = beforeColumns.size(); i < after.columns().size(); i++) {
            Column addedColumn = convertToSeaTunnelColumn(after, i);
            AlterTableAddColumnEvent event;
            if (i == 0) {
                event = AlterTableAddColumnEvent.addFirst(before.getTableId(), addedColumn);
            } else {
                event =
                        AlterTableAddColumnEvent.addAfter(
                                before.getTableId(),
                                addedColumn,
                                after.columns().get(i - 1).name());
            }
            events.add(event);
        }
        return events;
    }

    /** Compare SeaTunnel's tracked catalog schema with a pgoutput RELATION schema. */
    public static boolean hasSameCatalogSchema(CatalogTable before, Table after) {
        List<Column> beforeColumns = before.getTableSchema().getColumns();
        if (beforeColumns.size() != after.columns().size()) {
            return false;
        }
        for (int i = 0; i < beforeColumns.size(); i++) {
            Column beforeColumn = beforeColumns.get(i);
            Column afterColumn = convertToSeaTunnelColumn(after, i);
            if (!Objects.equals(beforeColumn.getName(), afterColumn.getName())
                    || !Objects.equals(beforeColumn.getDataType(), afterColumn.getDataType())
                    || beforeColumn.isNullable() != afterColumn.isNullable()) {
                return false;
            }
        }
        return true;
    }

    private static Column convertToSeaTunnelColumn(Table table, int columnIndex) {
        return PostgresTypeConverter.INSTANCE.convert(
                PostgresTypeUtils.convertRelationColumnToTypeDefine(
                        table.columns().get(columnIndex)));
    }

    private SchemaValidationException invalidRelationRecord(String message) {
        return new SchemaValidationException(
                SchemaEvolutionErrorCode.INVALID_SCHEMA_STRUCTURE, message, null, null);
    }

    private SchemaValidationException unsupportedChange(CatalogTable before, Table after) {
        return new SchemaValidationException(
                SchemaEvolutionErrorCode.UNSUPPORTED_SCHEMA_CHANGE_TYPE,
                String.format(
                        "PostgreSQL CDC currently supports only ADD COLUMN relation changes. "
                                + "The job stopped before processing rows with the new schema. "
                                + "Restoring the same checkpoint will encounter this relation again until the change is supported or the job is restarted through a controlled schema migration. "
                                + "Cached columns: %s, relation columns: %s",
                        before.getTableSchema().getColumns(), after.columns()),
                before.getTableId(),
                null);
    }
}
