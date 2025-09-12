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

package org.apache.seatunnel.connectors.seatunnel.paimon.sink.schema.handler;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.data.PaimonTypeMapper;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.Preconditions;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.paimon.sink.schema.UpdatedDataFields.canConvert;

@Slf4j
public class AlterPaimonTableSchemaEventHandler {

    private final TableSchemaChangeEventDispatcher TABLESCHEMACHANGER =
            new TableSchemaChangeEventDispatcher();

    private final TableSchema sourceTableSchema;

    private final PaimonCatalog paimonCatalog;

    private final org.apache.paimon.schema.TableSchema sinkPaimonTableSchema;

    private final TablePath paimonTablePath;

    public AlterPaimonTableSchemaEventHandler(
            TableSchema sourceTableSchema,
            PaimonCatalog paimonCatalog,
            org.apache.paimon.schema.TableSchema sinkPaimonTableSchema,
            TablePath paimonTablePath) {
        this.sourceTableSchema = sourceTableSchema;
        this.paimonCatalog = paimonCatalog;
        this.sinkPaimonTableSchema = sinkPaimonTableSchema;
        this.paimonTablePath = paimonTablePath;
    }

    public TableSchema apply(SchemaChangeEvent event) {
        TableSchema newSchema = TABLESCHEMACHANGER.reset(sourceTableSchema).apply(event);
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySingleSchemaChangeEvent(columnEvent);
            }
        } else if (event instanceof AlterTableColumnEvent) {
            applySingleSchemaChangeEvent(event);
        } else {
            throw new UnsupportedOperationException("Unsupported alter table event: " + event);
        }
        return newSchema;
    }

    private void applySingleSchemaChangeEvent(SchemaChangeEvent event) {
        Identifier identifier =
                Identifier.create(
                        paimonTablePath.getDatabaseName(), paimonTablePath.getTableName());
        if (event instanceof AlterTableAddColumnEvent) {
            AlterTableAddColumnEvent alterTableAddColumnEvent = (AlterTableAddColumnEvent) event;
            Column column = alterTableAddColumnEvent.getColumn();
            String afterColumnName = alterTableAddColumnEvent.getAfterColumn();
            SchemaChange.Move move =
                    StringUtils.isBlank(afterColumnName)
                            ? null
                            : SchemaChange.Move.after(column.getName(), afterColumnName);
            BasicTypeDefine<DataType> reconvertColumn = PaimonTypeMapper.INSTANCE.reconvert(column);
            DataType nativeType = reconvertColumn.getNativeType();
            List<SchemaChange> schemaChanges = new ArrayList<>();
            schemaChanges.add(
                    SchemaChange.addColumn(
                            column.getName(), nativeType.copy(true), column.getComment(), move));
            if (!nativeType.isNullable()) {
                schemaChanges.add(
                        SchemaChange.updateColumnType(column.getName(), nativeType.copy(false)));
            }
            paimonCatalog.alterTable(identifier, schemaChanges, false);
        } else if (event instanceof AlterTableDropColumnEvent) {
            String columnName = ((AlterTableDropColumnEvent) event).getColumn();
            paimonCatalog.alterTable(identifier, SchemaChange.dropColumn(columnName), true);
        } else if (event instanceof AlterTableModifyColumnEvent) {
            Column column = ((AlterTableModifyColumnEvent) event).getColumn();
            String afterColumn = ((AlterTableModifyColumnEvent) event).getAfterColumn();
            updateColumn(column, column.getName(), identifier, afterColumn);
        } else if (event instanceof AlterTableChangeColumnEvent) {
            Column column = ((AlterTableChangeColumnEvent) event).getColumn();
            String afterColumn = ((AlterTableChangeColumnEvent) event).getAfterColumn();
            String oldColumn = ((AlterTableChangeColumnEvent) event).getOldColumn();
            updateColumn(column, oldColumn, identifier, afterColumn);
            if (!column.getName().equals(oldColumn)) {
                paimonCatalog.alterTable(
                        identifier, SchemaChange.renameColumn(oldColumn, column.getName()), false);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported alter table event: " + event);
        }
    }

    private void updateColumn(
            Column newColumn, String oldColumnName, Identifier identifier, String afterTheColumn) {
        BasicTypeDefine<DataType> reconvertColumn = PaimonTypeMapper.INSTANCE.reconvert(newColumn);
        int idx = sinkPaimonTableSchema.fieldNames().indexOf(oldColumnName);
        Preconditions.checkState(
                idx >= 0,
                "Field name " + oldColumnName + " does not exist in table. This is unexpected.");
        DataType newDataType = reconvertColumn.getNativeType();
        DataField dataField = sinkPaimonTableSchema.fields().get(idx);
        DataType oldDataType = dataField.type();
        switch (canConvert(oldDataType, newDataType)) {
            case CONVERT:
                paimonCatalog.alterTable(
                        identifier,
                        SchemaChange.updateColumnType(oldColumnName, newDataType),
                        false);
                break;
            case IGNORE:
                log.warn(
                        "old: {{}-{}} and new: {{}-{}} belongs to the same type family, but old type has higher precision than new type. Ignore this convert request.",
                        dataField.name(),
                        oldDataType,
                        reconvertColumn.getName(),
                        newDataType);
                break;
            case EXCEPTION:
                throw new UnsupportedOperationException(
                        String.format(
                                "Cannot convert field %s from type %s to %s of Paimon table %s.",
                                oldColumnName, oldDataType, newDataType, identifier.getFullName()));
        }
        if (StringUtils.isNotBlank(afterTheColumn)) {
            paimonCatalog.alterTable(
                    identifier,
                    SchemaChange.updateColumnPosition(
                            SchemaChange.Move.after(oldColumnName, afterTheColumn)),
                    false);
        }
        String comment = newColumn.getComment();
        if (StringUtils.isNotBlank(comment)) {
            paimonCatalog.alterTable(
                    identifier, SchemaChange.updateColumnComment(oldColumnName, comment), false);
        }
        paimonCatalog.alterTable(
                identifier,
                SchemaChange.updateColumnNullability(oldColumnName, newColumn.isNullable()),
                false);
    }
}
