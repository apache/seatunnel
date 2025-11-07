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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.parser;

import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.connectors.cdc.base.source.parser.SeatunnelDDLParser;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleTypeUtils;

import org.apache.commons.lang3.StringUtils;

import org.antlr.v4.runtime.tree.ParseTreeListener;

import io.debezium.ddl.parser.oracle.generated.PlSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class CustomAlterTableParserListener extends BaseParserListener
        implements SeatunnelDDLParser {

    private static final int STARTING_INDEX = 0;
    private CustomOracleAntlrDdlParser parser;
    private final List<ParseTreeListener> listeners;
    private CustomColumnDefinitionParserListener columnDefinitionListener;
    private List<ColumnEditor> columnEditors;
    private int parsingColumnIndex = STARTING_INDEX;

    private final LinkedList<AlterTableColumnEvent> changes;
    private TableIdentifier tableIdentifier;

    public CustomAlterTableParserListener(
            CustomOracleAntlrDdlParser parser,
            List<ParseTreeListener> listeners,
            LinkedList<AlterTableColumnEvent> changes) {
        this.parser = parser;
        this.listeners = listeners;
        this.changes = changes;
    }

    @Override
    public void enterAlter_table(PlSqlParser.Alter_tableContext ctx) {
        TableId tableId = this.parser.parseQualifiedTableId();
        this.tableIdentifier = toTableIdentifier(tableId);
        super.enterAlter_table(ctx);
    }

    @Override
    public void exitAlter_table(PlSqlParser.Alter_tableContext ctx) {
        listeners.remove(columnDefinitionListener);
        super.exitAlter_table(ctx);
    }

    @Override
    public void enterAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        List<PlSqlParser.Column_definitionContext> columns = ctx.column_definition();
        columnEditors = new ArrayList<>(columns.size());
        for (PlSqlParser.Column_definitionContext column : columns) {
            String columnName = getColumnName(column.column_name());
            ColumnEditor editor = Column.editor().name(columnName);
            columnEditors.add(editor);
        }
        columnDefinitionListener = new CustomColumnDefinitionParserListener();
        listeners.add(columnDefinitionListener);
        super.enterAdd_column_clause(ctx);
    }

    @Override
    public void exitAdd_column_clause(PlSqlParser.Add_column_clauseContext ctx) {
        columnEditors.forEach(
                columnEditor -> {
                    Column column = columnEditor.create();
                    org.apache.seatunnel.api.table.catalog.Column seaTunnelColumn =
                            toSeatunnelColumnWithFullTypeInfo(column);
                    AlterTableAddColumnEvent addEvent =
                            AlterTableAddColumnEvent.add(tableIdentifier, seaTunnelColumn);
                    changes.add(addEvent);
                });
        listeners.remove(columnDefinitionListener);
        columnDefinitionListener = null;
        super.exitAdd_column_clause(ctx);
    }

    @Override
    public void enterModify_column_clauses(PlSqlParser.Modify_column_clausesContext ctx) {
        List<PlSqlParser.Modify_col_propertiesContext> columns = ctx.modify_col_properties();
        columnEditors = new ArrayList<>(columns.size());
        for (PlSqlParser.Modify_col_propertiesContext column : columns) {
            String columnName = getColumnName(column.column_name());
            ColumnEditor editor = Column.editor().name(columnName);
            columnEditors.add(editor);
        }
        columnDefinitionListener = new CustomColumnDefinitionParserListener();
        listeners.add(columnDefinitionListener);
        super.enterModify_column_clauses(ctx);
    }

    @Override
    public void exitModify_column_clauses(PlSqlParser.Modify_column_clausesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    Column column = columnDefinitionListener.getColumn();
                    org.apache.seatunnel.api.table.catalog.Column seaTunnelColumn =
                            toSeatunnelColumnWithFullTypeInfo(column);
                    AlterTableModifyColumnEvent alterTableModifyColumnEvent =
                            AlterTableModifyColumnEvent.modify(tableIdentifier, seaTunnelColumn);
                    changes.add(alterTableModifyColumnEvent);
                    listeners.remove(columnDefinitionListener);
                    columnDefinitionListener = null;
                    super.exitModify_column_clauses(ctx);
                },
                columnDefinitionListener);
    }

    @Override
    public void enterModify_col_properties(PlSqlParser.Modify_col_propertiesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    // column editor list is not null when a multiple columns are parsed in one
                    // statement
                    if (columnEditors.size() > parsingColumnIndex) {
                        // assign next column editor to parse another column definition
                        columnDefinitionListener.setColumnEditor(
                                columnEditors.get(parsingColumnIndex++));
                    }
                },
                columnEditors);
        super.enterModify_col_properties(ctx);
    }

    @Override
    public void exitModify_col_properties(PlSqlParser.Modify_col_propertiesContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors.size() == parsingColumnIndex) {
                        // all columns parsed
                        // reset global variables for next parsed statement
                        parsingColumnIndex = STARTING_INDEX;
                    }
                },
                columnEditors);
        super.exitModify_col_properties(ctx);
    }

    @Override
    public void enterColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    // column editor list is not null when a multiple columns are parsed in one
                    // statement
                    if (columnEditors.size() > parsingColumnIndex) {
                        // assign next column editor to parse another column definition
                        columnDefinitionListener.setColumnEditor(
                                columnEditors.get(parsingColumnIndex++));
                    }
                },
                columnEditors);
    }

    @Override
    public void exitColumn_definition(PlSqlParser.Column_definitionContext ctx) {
        parser.runIfNotNull(
                () -> {
                    if (columnEditors.size() == parsingColumnIndex) {
                        // all columns parsed
                        // reset global variables for next parsed statement
                        parsingColumnIndex = STARTING_INDEX;
                    }
                },
                columnEditors);
        super.exitColumn_definition(ctx);
    }

    @Override
    public void enterDrop_column_clause(PlSqlParser.Drop_column_clauseContext ctx) {
        List<PlSqlParser.Column_nameContext> columnNameContexts = ctx.column_name();
        columnEditors = new ArrayList<>(columnNameContexts.size());
        for (PlSqlParser.Column_nameContext columnNameContext : columnNameContexts) {
            String columnName = getColumnName(columnNameContext);
            AlterTableDropColumnEvent alterTableDropColumnEvent =
                    new AlterTableDropColumnEvent(tableIdentifier, columnName);
            changes.add(alterTableDropColumnEvent);
        }
        super.enterDrop_column_clause(ctx);
    }

    @Override
    public void enterRename_column_clause(PlSqlParser.Rename_column_clauseContext ctx) {
        String oldColumnName = getColumnName(ctx.old_column_name());
        String newColumnName = getColumnName(ctx.new_column_name());
        PhysicalColumn newColumn = PhysicalColumn.builder().name(newColumnName).build();
        AlterTableChangeColumnEvent alterTableChangeColumnEvent =
                AlterTableChangeColumnEvent.change(tableIdentifier, oldColumnName, newColumn);
        if (StringUtils.isNotBlank(newColumnName)
                && !StringUtils.equals(oldColumnName, newColumnName)) {
            changes.add(alterTableChangeColumnEvent);
        }
        super.enterRename_column_clause(ctx);
    }

    @Override
    public org.apache.seatunnel.api.table.catalog.Column toSeatunnelColumn(Column column) {
        return OracleTypeUtils.convertToSeaTunnelColumn(column);
    }
}
