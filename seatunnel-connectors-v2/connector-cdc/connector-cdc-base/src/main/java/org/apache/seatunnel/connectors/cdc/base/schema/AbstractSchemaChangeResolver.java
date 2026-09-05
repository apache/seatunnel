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

package org.apache.seatunnel.connectors.cdc.base.schema;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.schema.event.AlterColumnCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractSchemaChangeResolver implements SchemaChangeResolver {

    protected static final List<String> SUPPORT_DDL = Lists.newArrayList("ALTER TABLE");

    protected final JdbcSourceConfig jdbcSourceConfig;
    @Setter protected transient DdlParser ddlParser;
    @Setter protected transient Tables tables;
    @Setter protected String sourceDialectName;

    public AbstractSchemaChangeResolver(JdbcSourceConfig jdbcSourceConfig) {
        this.jdbcSourceConfig = jdbcSourceConfig;
    }

    @Override
    public boolean support(SourceRecord record) {
        String ddl = SourceRecordUtils.getDdl(record);
        Struct value = (Struct) record.value();
        List<Struct> tableChanges = value.getArray(HistoryRecord.Fields.TABLE_CHANGES);
        if (tableChanges == null || tableChanges.isEmpty()) {
            log.warn("Ignoring statement for non-captured table {}", ddl);
            return false;
        }
        return StringUtils.isNotBlank(ddl)
                && SUPPORT_DDL.stream()
                        .map(String::toUpperCase)
                        .anyMatch(prefix -> ddl.toUpperCase().contains(prefix));
    }

    @Override
    public SchemaChangeEvent resolve(SourceRecord record, List<CatalogTable> catalogTables) {
        TablePath tablePath = SourceRecordUtils.getTablePath(record);
        String ddl = SourceRecordUtils.getDdl(record);
        if (Objects.isNull(ddlParser)) {
            this.ddlParser = createDdlParser(tablePath);
        }
        if (Objects.isNull(tables)) {
            this.tables = new Tables();
        }
        ddlParser.setCurrentDatabase(tablePath.getDatabaseName());
        ddlParser.setCurrentSchema(tablePath.getSchemaName());
        // Parse DDL statement using Debezium's Antlr parser
        ddlParser.parse(ddl, tables);
        List<AlterTableEvent> parsedEvents = new ArrayList<>(getAndClearAlterTableEvents());
        parsedEvents = normalizeTableIdentifiers(parsedEvents, tablePath);
        parsedEvents = completeSchemaChangeEvents(parsedEvents, catalogTables, tablePath);
        parsedEvents.forEach(e -> e.setSourceDialectName(getSourceDialectName()));

        if (parsedEvents.isEmpty()) {
            return null;
        }

        // If there's a single table-level comment event, return it directly
        if (parsedEvents.size() == 1 && parsedEvents.get(0) instanceof AlterTableCommentEvent) {
            AlterTableCommentEvent commentEvent = (AlterTableCommentEvent) parsedEvents.get(0);
            commentEvent.setStatement(ddl);
            return commentEvent;
        }

        // Filter column events for AlterTableColumnsEvent
        List<AlterTableColumnEvent> columnEvents =
                parsedEvents.stream()
                        .filter(e -> e instanceof AlterTableColumnEvent)
                        .map(e -> (AlterTableColumnEvent) e)
                        .collect(Collectors.toList());

        if (columnEvents.isEmpty()) {
            return null;
        }

        // Warn if non-column events (e.g. table comment changes) are present alongside column
        // events, since only column events can be batched in AlterTableColumnsEvent and the
        // others will not be propagated for this DDL.
        long droppedCount = parsedEvents.size() - columnEvents.size();
        if (droppedCount > 0) {
            log.warn(
                    "DDL '{}' produced {} non-column event(s) alongside column events; "
                            + "only column changes will be propagated. "
                            + "Non-column events (e.g. table comment changes) in mixed DDL are not yet supported.",
                    ddl,
                    droppedCount);
        }

        AlterTableColumnsEvent alterTableColumnsEvent =
                new AlterTableColumnsEvent(
                        TableIdentifier.of(
                                StringUtils.EMPTY,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName()),
                        columnEvents);
        alterTableColumnsEvent.setStatement(ddl);
        alterTableColumnsEvent.setSourceDialectName(getSourceDialectName());
        return alterTableColumnsEvent;
    }

    List<AlterTableColumnEvent> completionEvent(
            List<AlterTableColumnEvent> events, List<CatalogTable> catalogTables) {
        return completeSchemaChangeEvents(
                        Lists.newArrayList(events),
                        catalogTables,
                        events.isEmpty() ? null : events.get(0).getTablePath())
                .stream()
                .filter(event -> event instanceof AlterTableColumnEvent)
                .map(event -> (AlterTableColumnEvent) event)
                .collect(Collectors.toList());
    }

    List<AlterTableEvent> completeSchemaChangeEvents(
            List<AlterTableEvent> events, List<CatalogTable> catalogTables, TablePath tablePath) {
        return events.stream()
                .map(
                        event -> {
                            event.setSourceDialectName(getSourceDialectName());
                            if (catalogTables == null || catalogTables.isEmpty()) {
                                return event;
                            }

                            CatalogTable table = findCatalogTable(catalogTables, tablePath);

                            // Handle table comment event - fill in old comment
                            if (event instanceof AlterTableCommentEvent) {
                                AlterTableCommentEvent commentEvent =
                                        (AlterTableCommentEvent) event;
                                if (table != null && commentEvent.getOldComment() == null) {
                                    String oldComment = table.getComment();
                                    AlterTableCommentEvent newEvent =
                                            AlterTableCommentEvent.of(
                                                    commentEvent.getTableIdentifier(),
                                                    oldComment,
                                                    commentEvent.getNewComment());
                                    newEvent.setSourceDialectName(getSourceDialectName());
                                    return newEvent;
                                }
                                return event;
                            }

                            // Handle column change event - complete type info
                            if (event instanceof AlterTableChangeColumnEvent) {
                                AlterTableChangeColumnEvent changeColumnEvent =
                                        (AlterTableChangeColumnEvent) event;
                                if (changeColumnEvent.getColumn().getDataType() != null) {
                                    return event;
                                }
                                if (table != null) {
                                    Column oldColumn =
                                            table.getTableSchema()
                                                    .getColumn(changeColumnEvent.getOldColumn());
                                    Column newColumn =
                                            oldColumn.rename(
                                                    changeColumnEvent.getColumn().getName());
                                    AlterTableChangeColumnEvent newEvent =
                                            new AlterTableChangeColumnEvent(
                                                    changeColumnEvent.getTableIdentifier(),
                                                    changeColumnEvent.getOldColumn(),
                                                    newColumn,
                                                    changeColumnEvent.isFirst(),
                                                    changeColumnEvent.getAfterColumn());
                                    newEvent.setSourceDialectName(getSourceDialectName());
                                    return newEvent;
                                } else {
                                    log.warn(
                                            "Ignoring rename column {} type completion for table {}",
                                            changeColumnEvent.getOldColumn(),
                                            changeColumnEvent.getTablePath());
                                }
                            }
                            if (event instanceof AlterTableModifyColumnEvent && table != null) {
                                AlterTableModifyColumnEvent modifyColumnEvent =
                                        (AlterTableModifyColumnEvent) event;
                                if (table.getTableSchema()
                                        .contains(modifyColumnEvent.getColumn().getName())) {
                                    Column oldColumn =
                                            table.getTableSchema()
                                                    .getColumn(
                                                            modifyColumnEvent
                                                                    .getColumn()
                                                                    .getName());
                                    AlterColumnCommentEvent columnCommentEvent =
                                            convertToColumnCommentEventIfOnlyCommentChanged(
                                                    modifyColumnEvent, oldColumn);
                                    if (columnCommentEvent != null) {
                                        columnCommentEvent.setSourceDialectName(
                                                getSourceDialectName());
                                        return columnCommentEvent;
                                    }
                                }
                            }
                            return event;
                        })
                .collect(Collectors.toList());
    }

    List<AlterTableEvent> normalizeTableIdentifiers(
            List<AlterTableEvent> events, TablePath tablePath) {
        // The parser may lose the database while walking a SourceRecord. The SourceRecord table
        // path is authoritative because it comes from the CDC event's captured source table.
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        StringUtils.EMPTY,
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName());
        return events.stream()
                .map(
                        event -> {
                            if (event instanceof AlterTableCommentEvent) {
                                AlterTableCommentEvent commentEvent =
                                        (AlterTableCommentEvent) event;
                                return AlterTableCommentEvent.of(
                                        tableIdentifier,
                                        commentEvent.getOldComment(),
                                        commentEvent.getNewComment());
                            }
                            return event;
                        })
                .collect(Collectors.toList());
    }

    private CatalogTable findCatalogTable(List<CatalogTable> catalogTables, TablePath tablePath) {
        if (tablePath == null) {
            return null;
        }
        return catalogTables.stream()
                .filter(catalogTable -> catalogTable.getTablePath().equals(tablePath))
                .findFirst()
                .orElse(null);
    }

    private AlterColumnCommentEvent convertToColumnCommentEventIfOnlyCommentChanged(
            AlterTableModifyColumnEvent modifyColumnEvent, Column oldColumn) {
        if (oldColumn == null
                || modifyColumnEvent.isFirst()
                || StringUtils.isNotBlank(modifyColumnEvent.getAfterColumn())) {
            return null;
        }
        Column newColumn = modifyColumnEvent.getColumn();
        if (isSameColumnExceptComment(oldColumn, newColumn)
                && !StringUtils.equals(oldColumn.getComment(), newColumn.getComment())) {
            return AlterColumnCommentEvent.of(
                    modifyColumnEvent.getTableIdentifier(),
                    newColumn.getName(),
                    oldColumn.getComment(),
                    newColumn.getComment());
        }
        return null;
    }

    private boolean isSameColumnExceptComment(Column oldColumn, Column newColumn) {
        return StringUtils.equals(oldColumn.getName(), newColumn.getName())
                && isSameDataType(oldColumn.getDataType(), newColumn.getDataType())
                && isSameColumnLength(oldColumn, newColumn)
                && Objects.equals(oldColumn.getScale(), newColumn.getScale())
                && oldColumn.isNullable() == newColumn.isNullable()
                && Objects.equals(oldColumn.getDefaultValue(), newColumn.getDefaultValue())
                && StringUtils.equals(oldColumn.getSourceType(), newColumn.getSourceType())
                && Objects.equals(oldColumn.getOptions(), newColumn.getOptions());
    }

    private boolean isSameColumnLength(Column oldColumn, Column newColumn) {
        if (Objects.equals(oldColumn.getColumnLength(), newColumn.getColumnLength())) {
            return true;
        }
        // Some dialects canonicalize character lengths (for example MySQL converts character
        // counts to a four-byte storage length). An identical source type is the stable semantic
        // representation in that case; genuine length changes still have a different source type.
        return oldColumn.getDataType() != null
                && oldColumn.getDataType().getSqlType() == SqlType.STRING
                && newColumn.getDataType() != null
                && newColumn.getDataType().getSqlType() == SqlType.STRING
                && StringUtils.isNotBlank(oldColumn.getSourceType())
                && StringUtils.equals(oldColumn.getSourceType(), newColumn.getSourceType());
    }

    private boolean isSameDataType(
            SeaTunnelDataType<?> oldDataType, SeaTunnelDataType<?> newDataType) {
        return Objects.equals(oldDataType, newDataType)
                || (oldDataType != null
                        && newDataType != null
                        && oldDataType.getSqlType() == newDataType.getSqlType());
    }

    protected abstract DdlParser createDdlParser(TablePath tablePath);

    /**
     * Returns and clears parsed column events.
     *
     * @deprecated Override {@link #getAndClearAlterTableEvents()} to emit table-level events. This
     *     method remains as a compatibility extension point for downstream CDC connectors.
     */
    @Deprecated
    protected List<AlterTableColumnEvent> getAndClearParsedEvents() {
        return new ArrayList<>();
    }

    /**
     * Returns and clears all parsed alter-table events.
     *
     * <p>The default delegates to the legacy column-only extension point so existing downstream
     * resolvers continue to work without source changes.
     */
    protected List<? extends AlterTableEvent> getAndClearAlterTableEvents() {
        return getAndClearParsedEvents();
    }

    protected abstract String getSourceDialectName();
}
