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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils.SqlServerTypeUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.Table;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerSchemaChangeResolver implements SchemaChangeResolver {

    private static final String SOURCE_DIALECT = DatabaseIdentifier.SQLSERVER;

    private final ConnectTableChangeSerializer tableChangeSerializer =
            new ConnectTableChangeSerializer();

    @Override
    public boolean support(SourceRecord record) {
        if (!SourceRecordUtils.isSchemaChangeEvent(record)) {
            return false;
        }
        Struct value = (Struct) record.value();
        List<Struct> tableChanges = value.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        return tableChanges != null && !tableChanges.isEmpty();
    }

    @Override
    public SchemaChangeEvent resolve(SourceRecord record, List<CatalogTable> catalogTables) {
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        CatalogTable currentCatalogTable = findCatalogTable(catalogTables, tablePath);
        if (currentCatalogTable == null) {
            log.warn("Ignoring SQL Server schema change for unknown table {}", tablePath);
            return null;
        }

        Table currentTable = getCurrentTable(record, tablePath);
        if (currentTable == null) {
            log.warn(
                    "Ignoring SQL Server schema change with missing table change payload {}",
                    tablePath);
            return null;
        }

        List<AlterTableColumnEvent> events = diffColumns(currentCatalogTable, currentTable);
        if (events.isEmpty()) {
            log.info(
                    "Ignoring SQL Server schema change without column diff for table {}",
                    tablePath);
            return null;
        }

        TableIdentifier tableIdentifier = currentCatalogTable.getTableId();
        AlterTableColumnsEvent event = new AlterTableColumnsEvent(tableIdentifier, events);
        event.setStatement(SourceRecordUtils.getDdl(record));
        event.setSourceDialectName(SOURCE_DIALECT);
        return event;
    }

    private CatalogTable findCatalogTable(List<CatalogTable> catalogTables, TablePath tablePath) {
        if (catalogTables == null) {
            return null;
        }
        return catalogTables.stream()
                .filter(table -> table.getTablePath().equals(tablePath))
                .findFirst()
                .orElse(null);
    }

    private Table getCurrentTable(SourceRecord record, TablePath tablePath) {
        Struct value = (Struct) record.value();
        List<Struct> tableChangesStruct = value.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        TableChanges tableChanges = tableChangeSerializer.deserialize(tableChangesStruct, false);
        for (TableChanges.TableChange tableChange : tableChanges) {
            Table table = tableChange.getTable();
            if (table == null) {
                continue;
            }
            if (StringUtils.equals(table.id().catalog(), tablePath.getDatabaseName())
                    && StringUtils.equals(table.id().schema(), tablePath.getSchemaName())
                    && StringUtils.equals(table.id().table(), tablePath.getTableName())) {
                return table;
            }
        }
        return null;
    }

    private List<AlterTableColumnEvent> diffColumns(
            CatalogTable currentCatalogTable, Table currentTable) {
        List<Column> previousColumns = currentCatalogTable.getTableSchema().getColumns();
        List<io.debezium.relational.Column> newColumns = currentTable.columns();
        TableIdentifier tableIdentifier = currentCatalogTable.getTableId();

        Map<String, Column> previousByName =
                previousColumns.stream()
                        .collect(Collectors.toMap(Column::getName, column -> column));
        Map<String, io.debezium.relational.Column> currentByName =
                newColumns.stream()
                        .collect(
                                Collectors.toMap(
                                        io.debezium.relational.Column::name, column -> column));

        Set<String> matchedNames = new HashSet<>(previousByName.keySet());
        matchedNames.retainAll(currentByName.keySet());

        List<AlterTableColumnEvent> events = new ArrayList<>();
        for (int index = 0; index < newColumns.size(); index++) {
            io.debezium.relational.Column newColumn = newColumns.get(index);
            if (!matchedNames.contains(newColumn.name())) {
                continue;
            }
            Column oldColumn = previousByName.get(newColumn.name());
            Column convertedColumn = convertColumn(newColumn);
            boolean changed = hasColumnChanged(oldColumn, convertedColumn, index, previousColumns);
            if (!changed) {
                continue;
            }
            AlterTableModifyColumnEvent modifyEvent =
                    buildModifyEvent(tableIdentifier, convertedColumn, index, newColumns);
            modifyEvent.setTypeChanged(hasTypeChanged(oldColumn, convertedColumn));
            modifyEvent.setSourceDialectName(SOURCE_DIALECT);
            events.add(modifyEvent);
        }

        List<ColumnWithIndex<Column>> removedColumns = new ArrayList<>();
        for (int index = 0; index < previousColumns.size(); index++) {
            Column column = previousColumns.get(index);
            if (!currentByName.containsKey(column.getName())) {
                removedColumns.add(new ColumnWithIndex<>(column, index));
            }
        }

        List<ColumnWithIndex<io.debezium.relational.Column>> addedColumns = new ArrayList<>();
        for (int index = 0; index < newColumns.size(); index++) {
            io.debezium.relational.Column column = newColumns.get(index);
            if (!previousByName.containsKey(column.name())) {
                addedColumns.add(new ColumnWithIndex<>(column, index));
            }
        }

        pairRenameColumns(events, tableIdentifier, newColumns, removedColumns, addedColumns);

        for (ColumnWithIndex<io.debezium.relational.Column> added : addedColumns) {
            Column convertedColumn = convertColumn(added.value);
            AlterTableAddColumnEvent addEvent =
                    buildAddEvent(tableIdentifier, convertedColumn, added.index, newColumns);
            addEvent.setSourceDialectName(SOURCE_DIALECT);
            events.add(addEvent);
        }

        for (ColumnWithIndex<Column> removed : removedColumns) {
            AlterTableDropColumnEvent dropEvent =
                    new AlterTableDropColumnEvent(tableIdentifier, removed.value.getName());
            dropEvent.setSourceDialectName(SOURCE_DIALECT);
            events.add(dropEvent);
        }

        return events;
    }

    private void pairRenameColumns(
            List<AlterTableColumnEvent> events,
            TableIdentifier tableIdentifier,
            List<io.debezium.relational.Column> newColumns,
            List<ColumnWithIndex<Column>> removedColumns,
            List<ColumnWithIndex<io.debezium.relational.Column>> addedColumns) {
        List<ColumnWithIndex<Column>> matchedRemoved = new ArrayList<>();
        List<ColumnWithIndex<io.debezium.relational.Column>> matchedAdded = new ArrayList<>();

        for (ColumnWithIndex<io.debezium.relational.Column> added : addedColumns) {
            Column convertedAdded = convertColumn(added.value);
            ColumnWithIndex<Column> renameCandidate = null;
            for (ColumnWithIndex<Column> removed : removedColumns) {
                if (matchedRemoved.contains(removed)) {
                    continue;
                }
                if (removed.index != added.index) {
                    continue;
                }
                if (!sameDefinitionExceptName(removed.value, convertedAdded)) {
                    continue;
                }
                if (renameCandidate != null) {
                    renameCandidate = null;
                    break;
                }
                renameCandidate = removed;
            }
            if (renameCandidate == null) {
                continue;
            }
            AlterTableChangeColumnEvent changeEvent =
                    buildChangeEvent(
                            tableIdentifier,
                            renameCandidate.value.getName(),
                            convertedAdded,
                            added.index,
                            newColumns);
            changeEvent.setSourceDialectName(SOURCE_DIALECT);
            events.add(changeEvent);
            matchedRemoved.add(renameCandidate);
            matchedAdded.add(added);
        }

        removedColumns.removeAll(matchedRemoved);
        addedColumns.removeAll(matchedAdded);
    }

    private AlterTableAddColumnEvent buildAddEvent(
            TableIdentifier tableIdentifier,
            Column column,
            int newIndex,
            List<io.debezium.relational.Column> newColumns) {
        if (newIndex == 0) {
            return AlterTableAddColumnEvent.addFirst(tableIdentifier, column);
        }
        return AlterTableAddColumnEvent.addAfter(
                tableIdentifier, column, newColumns.get(newIndex - 1).name());
    }

    private AlterTableModifyColumnEvent buildModifyEvent(
            TableIdentifier tableIdentifier,
            Column column,
            int newIndex,
            List<io.debezium.relational.Column> newColumns) {
        if (newIndex == 0) {
            return AlterTableModifyColumnEvent.modifyFirst(tableIdentifier, column);
        }
        return AlterTableModifyColumnEvent.modifyAfter(
                tableIdentifier, column, newColumns.get(newIndex - 1).name());
    }

    private AlterTableChangeColumnEvent buildChangeEvent(
            TableIdentifier tableIdentifier,
            String oldColumnName,
            Column newColumn,
            int newIndex,
            List<io.debezium.relational.Column> newColumns) {
        if (newIndex == 0) {
            return AlterTableChangeColumnEvent.changeFirst(
                    tableIdentifier, oldColumnName, newColumn);
        }
        return AlterTableChangeColumnEvent.changeAfter(
                tableIdentifier, oldColumnName, newColumn, newColumns.get(newIndex - 1).name());
    }

    private boolean hasColumnChanged(
            Column oldColumn, Column newColumn, int newIndex, List<Column> previousColumns) {
        if (oldColumn == null || newColumn == null) {
            return true;
        }
        if (oldColumn.equals(newColumn)) {
            int oldIndex = indexOfColumn(previousColumns, oldColumn.getName());
            return oldIndex != newIndex;
        }
        return true;
    }

    private int indexOfColumn(List<Column> columns, String columnName) {
        for (int index = 0; index < columns.size(); index++) {
            if (StringUtils.equals(columns.get(index).getName(), columnName)) {
                return index;
            }
        }
        return -1;
    }

    private boolean hasTypeChanged(Column oldColumn, Column newColumn) {
        return !Objects.equals(oldColumn.getDataType(), newColumn.getDataType())
                || !Objects.equals(oldColumn.getColumnLength(), newColumn.getColumnLength())
                || !Objects.equals(oldColumn.getScale(), newColumn.getScale())
                || !Objects.equals(oldColumn.getSourceType(), newColumn.getSourceType());
    }

    private boolean sameDefinitionExceptName(Column oldColumn, Column newColumn) {
        return Objects.equals(oldColumn.getDataType(), newColumn.getDataType())
                && Objects.equals(oldColumn.getColumnLength(), newColumn.getColumnLength())
                && Objects.equals(oldColumn.getScale(), newColumn.getScale())
                && Objects.equals(oldColumn.isNullable(), newColumn.isNullable())
                && Objects.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue())
                && Objects.equals(oldColumn.getComment(), newColumn.getComment())
                && Objects.equals(oldColumn.getSourceType(), newColumn.getSourceType());
    }

    private Column convertColumn(io.debezium.relational.Column column) {
        String sourceType = column.typeExpression();
        if (StringUtils.isBlank(sourceType)) {
            sourceType = column.typeName();
        }
        return PhysicalColumn.builder()
                .name(column.name())
                .dataType(SqlServerTypeUtils.convertFromColumn(column))
                .columnLength(
                        column.length() == io.debezium.relational.Column.UNSET_INT_VALUE
                                ? null
                                : (long) column.length())
                .scale(column.scale().orElse(null))
                .nullable(column.isOptional())
                .defaultValue(column.defaultValueExpression().orElse(null))
                .comment(column.comment())
                .sourceType(sourceType)
                .build();
    }

    private static class ColumnWithIndex<T> implements Serializable {
        private static final long serialVersionUID = 1L;

        private final T value;
        private final int index;

        private ColumnWithIndex(T value, int index) {
            this.value = value;
            this.index = index;
        }
    }
}
