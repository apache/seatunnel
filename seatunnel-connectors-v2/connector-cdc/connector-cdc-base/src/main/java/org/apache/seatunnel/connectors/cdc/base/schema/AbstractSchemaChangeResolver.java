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
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.HistoryRecord;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

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
        List<AlterTableColumnEvent> parsedEvents = getAndClearParsedEvents();
        parsedEvents = completionEvent(parsedEvents, catalogTables);
        parsedEvents.forEach(e -> e.setSourceDialectName(getSourceDialectName()));
        AlterTableColumnsEvent alterTableColumnsEvent =
                new AlterTableColumnsEvent(
                        TableIdentifier.of(
                                StringUtils.EMPTY,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName()),
                        parsedEvents);
        alterTableColumnsEvent.setStatement(ddl);
        alterTableColumnsEvent.setSourceDialectName(getSourceDialectName());
        return parsedEvents.isEmpty() ? null : alterTableColumnsEvent;
    }

    List<AlterTableColumnEvent> completionEvent(
            List<AlterTableColumnEvent> events, List<CatalogTable> catalogTables) {
        return events.stream()
                .map(
                        columnEvent -> {
                            columnEvent.setSourceDialectName(getSourceDialectName());
                            if (catalogTables == null || catalogTables.isEmpty()) {
                                return columnEvent;
                            }
                            if (!(columnEvent instanceof AlterTableChangeColumnEvent)) {
                                return columnEvent;
                            }

                            AlterTableChangeColumnEvent changeColumnEvent =
                                    (AlterTableChangeColumnEvent) columnEvent;
                            if (changeColumnEvent.getColumn().getDataType() != null) {
                                return columnEvent;
                            }
                            CatalogTable table =
                                    catalogTables.stream()
                                            .filter(
                                                    catalogTable ->
                                                            catalogTable
                                                                    .getTablePath()
                                                                    .equals(
                                                                            columnEvent
                                                                                    .getTablePath()))
                                            .findFirst()
                                            .orElse(null);
                            if (table != null) {
                                Column oldColumn =
                                        table.getTableSchema()
                                                .getColumn(changeColumnEvent.getOldColumn());
                                Column newColumn =
                                        oldColumn.rename(changeColumnEvent.getColumn().getName());
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
                            return columnEvent;
                        })
                .collect(Collectors.toList());
    }

    protected abstract DdlParser createDdlParser(TablePath tablePath);

    protected abstract List<AlterTableColumnEvent> getAndClearParsedEvents();

    protected abstract String getSourceDialectName();
}
