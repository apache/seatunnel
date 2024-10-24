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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.parser;

import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlTypeUtils;

import org.apache.commons.lang3.StringUtils;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

import java.util.LinkedList;
import java.util.List;

public class CustomAlterTableParserListener extends MySqlParserBaseListener {
    private static final int STARTING_INDEX = 1;
    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private final LinkedList<AlterTableColumnEvent> changes;
    private List<ColumnEditor> columnEditors;
    private TableIdentifier tableIdentifier;

    private CustomColumnDefinitionParserListener columnDefinitionListener;

    private int parsingColumnIndex = STARTING_INDEX;

    private RelationalDatabaseConnectorConfig dbzConnectorConfig;

    public CustomAlterTableParserListener(
            RelationalDatabaseConnectorConfig dbzConnectorConfig,
            MySqlAntlrDdlParser parser,
            List<ParseTreeListener> listeners,
            LinkedList<AlterTableColumnEvent> changes) {
        this.dbzConnectorConfig = dbzConnectorConfig;
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        TableId tableId = parser.parseQualifiedTableId(ctx.tableName().fullId());
        this.tableIdentifier = toTableIdentifier(tableId);
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        listeners.remove(columnDefinitionListener);
        super.exitAlterTable(ctx);
        this.tableIdentifier = null;
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    org.apache.seatunnel.api.table.catalog.Column seatunnelColumn =
                            toSeatunnelColumn(column);
                    String sourceColumnType = getSourceColumnType(column);
                    seatunnelColumn = seatunnelColumn.reSourceType(sourceColumnType);
                    if (ctx.FIRST() != null) {
                        AlterTableAddColumnEvent alterTableAddColumnEvent =
                                AlterTableAddColumnEvent.addFirst(tableIdentifier, seatunnelColumn);
                        changes.add(alterTableAddColumnEvent);
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        AlterTableAddColumnEvent alterTableAddColumnEvent =
                                AlterTableAddColumnEvent.addAfter(
                                        tableIdentifier, seatunnelColumn, afterColumn);
                        changes.add(alterTableAddColumnEvent);
                    } else {
                        AlterTableAddColumnEvent alterTableAddColumnEvent =
                                AlterTableAddColumnEvent.add(tableIdentifier, seatunnelColumn);
                        changes.add(alterTableAddColumnEvent);
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors != null) {
                        // column editor list is not null when a multiple columns are parsed in one
                        // statement
                        if (columnEditors.size() > parsingColumnIndex) {
                            // assign next column editor to parse another column definition
                            columnDefinitionListener.setColumnEditor(
                                    columnEditors.get(parsingColumnIndex++));
                        }
                    }
                },
                columnEditors);
        super.exitColumnDefinition(ctx);
    }

    @Override
    public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByModifyColumn(ctx);
    }

    @Override
    public void exitAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    org.apache.seatunnel.api.table.catalog.Column seatunnelColumn =
                            toSeatunnelColumn(column);
                    String sourceColumnType = getSourceColumnType(column);
                    seatunnelColumn = seatunnelColumn.reSourceType(sourceColumnType);
                    if (ctx.FIRST() != null) {
                        AlterTableModifyColumnEvent alterTableModifyColumnEvent =
                                AlterTableModifyColumnEvent.modifyFirst(
                                        tableIdentifier, seatunnelColumn);
                        changes.add(alterTableModifyColumnEvent);
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        AlterTableModifyColumnEvent alterTableModifyColumnEvent =
                                AlterTableModifyColumnEvent.modifyAfter(
                                        tableIdentifier, seatunnelColumn, afterColumn);
                        changes.add(alterTableModifyColumnEvent);
                    } else {
                        AlterTableModifyColumnEvent alterTableModifyColumnEvent =
                                AlterTableModifyColumnEvent.modify(
                                        tableIdentifier, seatunnelColumn);
                        changes.add(alterTableModifyColumnEvent);
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByModifyColumn(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.oldColumn);
        ColumnEditor columnEditor = Column.editor().name(oldColumnName);
        columnEditor.unsetDefaultValueExpression();

        columnDefinitionListener =
                new CustomColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    org.apache.seatunnel.api.table.catalog.Column seatunnelColumn =
                            toSeatunnelColumn(column);
                    String sourceColumnType = getSourceColumnType(column);
                    seatunnelColumn = seatunnelColumn.reSourceType(sourceColumnType);
                    String oldColumnName = column.name();
                    String newColumnName = parser.parseName(ctx.newColumn);
                    seatunnelColumn = seatunnelColumn.rename(newColumnName);
                    AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                            AlterTableChangeColumnEvent.change(
                                    tableIdentifier, oldColumnName, seatunnelColumn);
                    if (StringUtils.isNotBlank(newColumnName)
                            && !StringUtils.equals(oldColumnName, newColumnName)) {
                        changes.add(alterTableChangeColumnEvent);
                    }
                    listeners.remove(columnDefinitionListener);
                },
                columnDefinitionListener);
        super.exitAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        String removedColName = parser.parseName(ctx.uid());
        changes.add(new AlterTableDropColumnEvent(tableIdentifier, removedColName));
        super.enterAlterByDropColumn(ctx);
    }

    private org.apache.seatunnel.api.table.catalog.Column toSeatunnelColumn(Column column) {
        return MySqlTypeUtils.convertToSeaTunnelColumn(column, dbzConnectorConfig);
    }

    private TableIdentifier toTableIdentifier(TableId tableId) {
        return new TableIdentifier("", tableId.catalog(), tableId.schema(), tableId.table());
    }

    private String getSourceColumnType(Column column) {
        StringBuilder sb = new StringBuilder(column.typeName());
        if (column.length() >= 0) {
            sb.append('(').append(column.length());
            if (column.scale().isPresent()) {
                sb.append(", ").append(column.scale().get());
            }

            sb.append(')');
        }
        return sb.toString();
    }
}
