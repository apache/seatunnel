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
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
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
                    if (ctx.FIRST() != null) {
                        changes.add(
                                AlterTableAddColumnEvent.addFirst(
                                        tableIdentifier, toSeatunnelColumn(column)));
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        changes.add(
                                AlterTableAddColumnEvent.addAfter(
                                        tableIdentifier, toSeatunnelColumn(column), afterColumn));
                    } else {
                        changes.add(
                                AlterTableAddColumnEvent.add(
                                        tableIdentifier, toSeatunnelColumn(column)));
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
                    if (ctx.FIRST() != null) {
                        changes.add(
                                AlterTableModifyColumnEvent.addFirst(
                                        tableIdentifier, toSeatunnelColumn(column)));
                    } else if (ctx.AFTER() != null) {
                        String afterColumn = parser.parseName(ctx.uid(1));
                        changes.add(
                                AlterTableModifyColumnEvent.addAfter(
                                        tableIdentifier, toSeatunnelColumn(column), afterColumn));
                    } else {
                        changes.add(
                                AlterTableModifyColumnEvent.add(
                                        tableIdentifier, toSeatunnelColumn(column)));
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
                    String oldColumnName = column.name();
                    String newColumnName = parser.parseName(ctx.newColumn);
                    Column newColumn = column.edit().name(newColumnName).create();
                    if (StringUtils.isNotBlank(newColumnName)
                            && !StringUtils.equals(oldColumnName, newColumnName)) {
                        changes.add(
                                AlterTableChangeColumnEvent.change(
                                        tableIdentifier,
                                        oldColumnName,
                                        toSeatunnelColumn(newColumn)));
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
}
