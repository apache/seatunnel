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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.listener;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.MySqlAntlrDdlParser;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.schema.ddl.MysqlColumnConverter;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AlterTableParserListener extends MySqlParserBaseListener {
    private static final int STARTING_INDEX = 1;

    private final MySqlAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private ColumnDefinitionParserListener columnDefinitionListener;
    private ColumnEditor defaultValueColumnEditor;
    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    private TablePath tablePath;
    private AlterTableColumnsEvent alterTableColumnsEvent;

    public AlterTableParserListener(MySqlAntlrDdlParser parser, List<ParseTreeListener> listeners) {
        this.parser = parser;
        this.listeners = listeners;
    }

    @Override
    public void enterAlterTable(MySqlParser.AlterTableContext ctx) {
        tablePath = parser.parseQualifiedTableId(ctx.tableName().fullId());
        alterTableColumnsEvent = new AlterTableColumnsEvent(tablePath);
        super.enterAlterTable(ctx);
    }

    @Override
    public void exitAlterTable(MySqlParser.AlterTableContext ctx) {
        parser.getSchemaChanges().add(alterTableColumnsEvent);

        tablePath = null;
        alterTableColumnsEvent = null;

        super.exitAlterTable(ctx);
    }

    @Override
    public void enterAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        ColumnEditor columnEditor = Column.editor().name(columnName);
        columnDefinitionListener =
                new ColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void exitAlterByAddColumn(MySqlParser.AlterByAddColumnContext ctx) {
        Column column = columnDefinitionListener.getColumn();
        AlterTableAddColumnEvent addColumn;
        if (ctx.FIRST() != null) {
            addColumn =
                    AlterTableAddColumnEvent.addFirst(
                            tablePath, MysqlColumnConverter.convert(column));
        } else if (ctx.AFTER() != null) {
            String afterColumn = parser.parseName(ctx.uid(1));
            addColumn =
                    AlterTableAddColumnEvent.addAfter(
                            tablePath, MysqlColumnConverter.convert(column), afterColumn);
        } else {
            addColumn =
                    AlterTableAddColumnEvent.add(tablePath, MysqlColumnConverter.convert(column));
        }
        alterTableColumnsEvent.addEvent(addColumn);

        listeners.remove(columnDefinitionListener);
        super.exitAlterByAddColumn(ctx);
    }

    @Override
    public void enterAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        columnEditors = new ArrayList<>(ctx.uid().size());
        for (MySqlParser.UidContext uidContext : ctx.uid()) {
            String columnName = parser.parseName(uidContext);
            columnEditors.add(Column.editor().name(columnName));
        }
        columnDefinitionListener =
                new ColumnDefinitionParserListener(columnEditors.get(0), parser, listeners);
        listeners.add(columnDefinitionListener);
        super.enterAlterByAddColumns(ctx);
    }

    @Override
    public void exitAlterByAddColumns(MySqlParser.AlterByAddColumnsContext ctx) {
        for (ColumnEditor columnEditor : columnEditors) {
            Column column = columnEditor.create();
            AlterTableAddColumnEvent addColumnEvent =
                    AlterTableAddColumnEvent.add(tablePath, MysqlColumnConverter.convert(column));
            alterTableColumnsEvent.addEvent(addColumnEvent);
        }

        listeners.remove(columnDefinitionListener);
        columnEditors = null;
        super.exitAlterByAddColumns(ctx);
    }

    @Override
    public void enterAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        String columnName = parser.parseName(ctx.uid(0));
        Column existingColumn = parser.getTableEditor().columnWithName(columnName);
        if (existingColumn == null) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }

        ColumnEditor columnEditor = existingColumn.edit();
        columnEditor.unsetDefaultValue();

        columnDefinitionListener =
                new ColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);

        super.enterAlterByModifyColumn(ctx);
    }

    @Override
    public void exitAlterByModifyColumn(MySqlParser.AlterByModifyColumnContext ctx) {
        Column column = columnDefinitionListener.getColumn();
        AlterTableModifyColumnEvent modifyColumnEvent;
        if (ctx.FIRST() != null) {
            modifyColumnEvent =
                    AlterTableModifyColumnEvent.modifyFirst(
                            tablePath, MysqlColumnConverter.convert(column));
        } else if (ctx.AFTER() != null) {
            String afterColumn = parser.parseName(ctx.uid(1));
            modifyColumnEvent =
                    AlterTableModifyColumnEvent.modifyAfter(
                            tablePath, MysqlColumnConverter.convert(column), afterColumn);
        } else {
            modifyColumnEvent =
                    AlterTableModifyColumnEvent.modify(
                            tablePath, MysqlColumnConverter.convert(column));
        }
        alterTableColumnsEvent.addEvent(modifyColumnEvent);

        listeners.remove(columnDefinitionListener);
        super.exitAlterByModifyColumn(ctx);
    }

    @Override
    public void enterAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        String oldColumnName = parser.parseName(ctx.oldColumn);
        String newColumnName = parser.parseName(ctx.newColumn);
        Column oldColumn = parser.getTableEditor().columnWithName(oldColumnName);
        Column newColumn = parser.getTableEditor().columnWithName(newColumnName);
        if (oldColumn == null && newColumn == null) {
            throw new IllegalArgumentException("Column " + oldColumnName + " does not exist");
        }

        ColumnEditor columnEditor = newColumn.edit();
        columnEditor.unsetDefaultValue();
        columnDefinitionListener =
                new ColumnDefinitionParserListener(columnEditor, parser, listeners);
        listeners.add(columnDefinitionListener);

        super.enterAlterByChangeColumn(ctx);
    }

    @Override
    public void exitAlterByChangeColumn(MySqlParser.AlterByChangeColumnContext ctx) {
        Column column = columnDefinitionListener.getColumn();
        String oldColumnName = parser.parseName(ctx.oldColumn);

        AlterTableChangeColumnEvent changeColumnEvent;
        if (ctx.FIRST() != null) {
            changeColumnEvent =
                    AlterTableChangeColumnEvent.changeFirst(
                            tablePath, oldColumnName, MysqlColumnConverter.convert(column));
        } else if (ctx.AFTER() != null) {
            String afterColumn = parser.parseName(ctx.uid(1));
            changeColumnEvent =
                    AlterTableChangeColumnEvent.changeAfter(
                            tablePath,
                            oldColumnName,
                            MysqlColumnConverter.convert(column),
                            afterColumn);
        } else {
            changeColumnEvent =
                    AlterTableChangeColumnEvent.change(
                            tablePath, oldColumnName, MysqlColumnConverter.convert(column));
        }
        alterTableColumnsEvent.addEvent(changeColumnEvent);

        listeners.remove(columnDefinitionListener);
        super.exitAlterByChangeColumn(ctx);
    }

    @Override
    public void enterAlterByDropColumn(MySqlParser.AlterByDropColumnContext ctx) {
        String column = parser.parseName(ctx.uid());
        AlterTableDropColumnEvent dropColumnEvent =
                new AlterTableDropColumnEvent(tablePath, column);
        alterTableColumnsEvent.addEvent(dropColumnEvent);

        super.enterAlterByDropColumn(ctx);
    }

    @Override
    public void exitColumnDefinition(MySqlParser.ColumnDefinitionContext ctx) {
        if (columnEditors != null) {
            // column editor list is not null when a multiple columns are parsed in one statement
            if (columnEditors.size() > parsingColumnIndex) {
                // assign next column editor to parse another column definition
                columnDefinitionListener.setColumnEditor(columnEditors.get(parsingColumnIndex++));
            } else {
                // all columns parsed
                // reset global variables for next parsed statement
                parsingColumnIndex = STARTING_INDEX;
            }
        }
        super.exitColumnDefinition(ctx);
    }
}
