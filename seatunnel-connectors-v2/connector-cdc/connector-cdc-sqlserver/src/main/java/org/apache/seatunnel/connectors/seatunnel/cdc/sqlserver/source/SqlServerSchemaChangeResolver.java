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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class SqlServerSchemaChangeResolver implements SchemaChangeResolver {

    private static final String SOURCE_DIALECT = DatabaseIdentifier.SQLSERVER;
    private static final String SQLSERVER_SCHEMA_CHANGE_KEY =
            "io.debezium.connector.sqlserver.SchemaChangeKey";

    // This value must stay in sync with the entry in
    // SourceRecordUtils.SUPPORT_SCHEMA_CHANGE_EVENT_KEY_NAME.
    // TODO: expose it as a named constant in SourceRecordUtils and reference it here.
    private static final Pattern ALTER_TABLE_PATTERN =
            Pattern.compile(
                    "(?i)ALTER\\s+TABLE\\s+(.+?)\\s+(ADD|DROP|ALTER|RENAME|WITH|SWITCH)\\b");
    private static final Pattern SP_RENAME_COLUMN_PATTERN =
            Pattern.compile(
                    "(?i)(?:EXEC(?:UTE)?\\s+)?(?:(?:\\[[^\\]]+\\]|[^.\\s]+)\\.)?(?:sys\\.)?sp_rename\\s+N?'([^']+)'\\s*,\\s*N?'([^']+)'\\s*,\\s*N?'COLUMN'");
    private static final Pattern TABLE_IDENTIFIER_PART_PATTERN =
            Pattern.compile("\\[([^\\]]+)]|\"([^\"]+)\"|`([^`]+)`|([^\\.\\s]+)");

    private final ConnectTableChangeSerializer tableChangeSerializer =
            new ConnectTableChangeSerializer();

    @Override
    public boolean support(SourceRecord record) {
        if (!isSqlServerSchemaChangeEvent(record)) {
            return false;
        }
        Struct value = (Struct) record.value();
        List<Struct> tableChanges = value.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        return tableChanges != null && !tableChanges.isEmpty();
    }

    @Override
    public SchemaChangeEvent resolve(SourceRecord record, List<CatalogTable> catalogTables) {
        TablePath tablePath = resolveTablePath(record);
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

        String ddl = SourceRecordUtils.getDdl(record);
        List<AlterTableColumnEvent> events =
                diffColumns(currentCatalogTable, currentTable, parseSpRenameColumns(ddl));
        if (events.isEmpty()) {
            log.info(
                    "Ignoring SQL Server schema change without column diff for table {}",
                    tablePath);
            return null;
        }

        TableIdentifier tableIdentifier = currentCatalogTable.getTableId();
        AlterTableColumnsEvent event = new AlterTableColumnsEvent(tableIdentifier, events);
        event.setStatement(ddl);
        event.setSourceDialectName(SOURCE_DIALECT);
        return event;
    }

    private CatalogTable findCatalogTable(List<CatalogTable> catalogTables, TablePath tablePath) {
        if (catalogTables == null) {
            return null;
        }
        return catalogTables.stream()
                .filter(table -> isSameTablePath(table.getTablePath(), tablePath))
                .findFirst()
                .orElse(null);
    }

    private TablePath resolveTablePath(SourceRecord record) {
        TablePath pathFromSource = SourceRecordUtils.getTablePath(record);
        TablePath pathFromDdl = parseTablePathFromDdl(SourceRecordUtils.getDdl(record));
        if (pathFromDdl == null) {
            return pathFromSource;
        }
        return TablePath.of(
                StringUtils.defaultIfBlank(
                        pathFromSource.getDatabaseName(), pathFromDdl.getDatabaseName()),
                StringUtils.defaultIfBlank(
                        pathFromSource.getSchemaName(), pathFromDdl.getSchemaName()),
                StringUtils.defaultIfBlank(
                        pathFromSource.getTableName(), pathFromDdl.getTableName()));
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
            if (isSameIdentifier(table.id().catalog(), tablePath.getDatabaseName())
                    && isSameIdentifier(table.id().schema(), tablePath.getSchemaName())
                    && isSameIdentifier(table.id().table(), tablePath.getTableName())) {
                return table;
            }
        }
        return null;
    }

    private List<AlterTableColumnEvent> diffColumns(
            CatalogTable currentCatalogTable,
            Table currentTable,
            Map<String, String> explicitRenames) {
        List<Column> previousColumns = currentCatalogTable.getTableSchema().getColumns();
        List<io.debezium.relational.Column> newColumns = currentTable.columns();
        TableIdentifier tableIdentifier = currentCatalogTable.getTableId();

        Map<String, Column> previousByName =
                previousColumns.stream()
                        .collect(
                                Collectors.toMap(
                                        column -> normalizeIdentifier(column.getName()),
                                        column -> column,
                                        (left, right) -> left,
                                        LinkedHashMap::new));
        Map<String, io.debezium.relational.Column> currentByName =
                newColumns.stream()
                        .collect(
                                Collectors.toMap(
                                        column -> normalizeIdentifier(column.name()),
                                        column -> column,
                                        (left, right) -> left,
                                        LinkedHashMap::new));

        Set<String> matchedNames = new HashSet<>(previousByName.keySet());
        matchedNames.retainAll(currentByName.keySet());

        List<AlterTableColumnEvent> events = new ArrayList<>();
        for (int index = 0; index < newColumns.size(); index++) {
            io.debezium.relational.Column newColumn = newColumns.get(index);
            String normalizedName = normalizeIdentifier(newColumn.name());
            if (!matchedNames.contains(normalizedName)) {
                continue;
            }
            Column oldColumn = previousByName.get(normalizedName);
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
            if (!currentByName.containsKey(normalizeIdentifier(column.getName()))) {
                removedColumns.add(new ColumnWithIndex<>(column, index));
            }
        }

        List<ColumnWithIndex<io.debezium.relational.Column>> addedColumns = new ArrayList<>();
        for (int index = 0; index < newColumns.size(); index++) {
            io.debezium.relational.Column column = newColumns.get(index);
            if (!previousByName.containsKey(normalizeIdentifier(column.name()))) {
                addedColumns.add(new ColumnWithIndex<>(column, index));
            }
        }

        pairRenameColumns(
                events, tableIdentifier, newColumns, removedColumns, addedColumns, explicitRenames);

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

    /** Extracts explicit SQL Server column renames declared through {@code sp_rename}. */
    private Map<String, String> parseSpRenameColumns(String ddl) {
        if (StringUtils.isBlank(ddl)) {
            return Collections.emptyMap();
        }
        Map<String, String> renames = new LinkedHashMap<>();
        Matcher matcher = SP_RENAME_COLUMN_PATTERN.matcher(ddl);
        while (matcher.find()) {
            String oldColumnName = extractLastIdentifierPart(matcher.group(1));
            String newColumnName = extractLastIdentifierPart(matcher.group(2));
            if (StringUtils.isBlank(oldColumnName) || StringUtils.isBlank(newColumnName)) {
                continue;
            }
            renames.put(normalizeIdentifier(oldColumnName), normalizeIdentifier(newColumnName));
        }
        return renames;
    }

    private void pairRenameColumns(
            List<AlterTableColumnEvent> events,
            TableIdentifier tableIdentifier,
            List<io.debezium.relational.Column> newColumns,
            List<ColumnWithIndex<Column>> removedColumns,
            List<ColumnWithIndex<io.debezium.relational.Column>> addedColumns,
            Map<String, String> explicitRenames) {
        Set<ColumnWithIndex<Column>> matchedRemoved = new HashSet<>();
        Set<ColumnWithIndex<io.debezium.relational.Column>> matchedAdded = new HashSet<>();

        for (ColumnWithIndex<io.debezium.relational.Column> added : addedColumns) {
            String addedName = normalizeIdentifier(added.value.name());
            Column convertedAdded = convertColumn(added.value);
            ColumnWithIndex<Column> renameCandidate = null;
            for (ColumnWithIndex<Column> removed : removedColumns) {
                if (matchedRemoved.contains(removed)) {
                    continue;
                }
                String removedName = normalizeIdentifier(removed.value.getName());
                if (!StringUtils.equalsIgnoreCase(addedName, explicitRenames.get(removedName))) {
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
        if (isSameColumnDefinition(oldColumn, newColumn)) {
            int oldIndex = indexOfColumn(previousColumns, oldColumn.getName());
            return oldIndex != newIndex;
        }
        return true;
    }

    private int indexOfColumn(List<Column> columns, String columnName) {
        for (int index = 0; index < columns.size(); index++) {
            if (isSameIdentifier(columns.get(index).getName(), columnName)) {
                return index;
            }
        }
        return -1;
    }

    private boolean hasTypeChanged(Column oldColumn, Column newColumn) {
        return !Objects.equals(oldColumn.getDataType(), newColumn.getDataType())
                || !isLengthCompatible(oldColumn.getColumnLength(), newColumn.getColumnLength())
                || !isScaleCompatible(oldColumn.getScale(), newColumn.getScale())
                || !isSourceTypeCompatible(oldColumn.getSourceType(), newColumn.getSourceType());
    }

    /**
     * Returns {@code true} when the two columns share the same structural definition (type, length,
     * scale, nullability, default, comment), ignoring the column name. Used to heuristically detect
     * renames. Note: comment equality is included — a rename that also changes the comment will not
     * be detected as a rename but as DROP + ADD.
     */
    private boolean sameDefinitionExceptName(Column oldColumn, Column newColumn) {
        return isSameColumnDefinition(oldColumn, newColumn);
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

    private boolean isSameColumnDefinition(Column oldColumn, Column newColumn) {
        return Objects.equals(oldColumn.getDataType(), newColumn.getDataType())
                && isLengthCompatible(oldColumn.getColumnLength(), newColumn.getColumnLength())
                && isScaleCompatible(oldColumn.getScale(), newColumn.getScale())
                && Objects.equals(oldColumn.isNullable(), newColumn.isNullable())
                && Objects.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue())
                && Objects.equals(oldColumn.getComment(), newColumn.getComment())
                && isSourceTypeCompatible(oldColumn.getSourceType(), newColumn.getSourceType());
    }

    private boolean isLengthCompatible(Long oldLength, Long newLength) {
        return oldLength == null || newLength == null || Objects.equals(oldLength, newLength);
    }

    private boolean isScaleCompatible(Integer oldScale, Integer newScale) {
        return oldScale == null || newScale == null || Objects.equals(oldScale, newScale);
    }

    private boolean isSourceTypeCompatible(String oldSourceType, String newSourceType) {
        if (StringUtils.isBlank(oldSourceType) || StringUtils.isBlank(newSourceType)) {
            return true;
        }
        String normalizedOld = oldSourceType.replace(" ", "").toLowerCase(Locale.ENGLISH);
        String normalizedNew = newSourceType.replace(" ", "").toLowerCase(Locale.ENGLISH);
        if (StringUtils.equals(normalizedOld, normalizedNew)) {
            return true;
        }
        // When the CDC migration path reads column metadata via JDBC DatabaseMetaData, the
        // typeExpression field may not be populated, causing convertColumn() to fall back to
        // typeName() which lacks length/precision (e.g. "varchar" instead of "varchar(255)").
        // Treat the types as compatible when their base names match and at least one side is
        // missing the length qualifier — the actual column type has not changed in that case.
        String baseOld =
                normalizedOld.contains("(")
                        ? normalizedOld.substring(0, normalizedOld.indexOf('('))
                        : normalizedOld;
        String baseNew =
                normalizedNew.contains("(")
                        ? normalizedNew.substring(0, normalizedNew.indexOf('('))
                        : normalizedNew;
        return StringUtils.equals(baseOld, baseNew)
                && (!normalizedOld.contains("(") || !normalizedNew.contains("("));
    }

    private String extractLastIdentifierPart(String identifier) {
        List<String> parts = parseIdentifierParts(identifier);
        if (!parts.isEmpty()) {
            return parts.get(parts.size() - 1);
        }
        return identifier;
    }

    private TablePath parseTablePathFromDdl(String ddl) {
        if (StringUtils.isBlank(ddl)) {
            return null;
        }
        Matcher matcher = ALTER_TABLE_PATTERN.matcher(ddl);
        if (!matcher.find()) {
            return null;
        }
        String tableIdentifier = matcher.group(1);
        List<String> parts = parseIdentifierParts(tableIdentifier);
        if (parts.isEmpty()) {
            return null;
        }
        if (parts.size() == 1) {
            return TablePath.of(null, null, parts.get(0));
        }
        if (parts.size() == 2) {
            return TablePath.of(null, parts.get(0), parts.get(1));
        }
        return TablePath.of(
                parts.get(parts.size() - 3),
                parts.get(parts.size() - 2),
                parts.get(parts.size() - 1));
    }

    private List<String> parseIdentifierParts(String rawIdentifier) {
        if (StringUtils.isBlank(rawIdentifier)) {
            return Collections.emptyList();
        }
        List<String> parts = new ArrayList<>();
        Matcher matcher = TABLE_IDENTIFIER_PART_PATTERN.matcher(rawIdentifier);
        while (matcher.find()) {
            String part =
                    firstNonBlank(
                            matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4));
            if (StringUtils.isNotBlank(part)) {
                parts.add(part.trim());
            }
        }
        return parts;
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (StringUtils.isNotBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private boolean isSqlServerSchemaChangeEvent(SourceRecord record) {
        if (record == null || record.keySchema() == null) {
            return false;
        }
        String keySchemaName = record.keySchema().name();
        return StringUtils.equalsIgnoreCase(keySchemaName, SQLSERVER_SCHEMA_CHANGE_KEY)
                || SourceRecordUtils.isSchemaChangeEvent(record);
    }

    private boolean isSameTablePath(TablePath left, TablePath right) {
        return left != null
                && right != null
                && isSameIdentifier(left.getDatabaseName(), right.getDatabaseName())
                && isSameIdentifier(left.getSchemaName(), right.getSchemaName())
                && isSameIdentifier(left.getTableName(), right.getTableName());
    }

    private boolean isSameIdentifier(String left, String right) {
        String normalizedLeft = normalizeIdentifier(left);
        String normalizedRight = normalizeIdentifier(right);
        if (StringUtils.isBlank(normalizedLeft) || StringUtils.isBlank(normalizedRight)) {
            return StringUtils.equals(normalizedLeft, normalizedRight);
        }
        return StringUtils.equalsIgnoreCase(normalizedLeft, normalizedRight);
    }

    private String normalizeIdentifier(String identifier) {
        if (identifier == null) {
            return null;
        }
        String normalized = identifier.trim();
        if (StringUtils.startsWith(normalized, "[") && StringUtils.endsWith(normalized, "]")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        if (StringUtils.startsWith(normalized, "\"") && StringUtils.endsWith(normalized, "\"")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        if (StringUtils.startsWith(normalized, "`") && StringUtils.endsWith(normalized, "`")) {
            normalized = normalized.substring(1, normalized.length() - 1);
        }
        return normalized;
    }

    private static class ColumnWithIndex<T> {

        private final T value;
        private final int index;

        private ColumnWithIndex(T value, int index) {
            this.value = value;
            this.index = index;
        }
    }
}
