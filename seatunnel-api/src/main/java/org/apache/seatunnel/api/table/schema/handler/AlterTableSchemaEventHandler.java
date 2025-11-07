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

package org.apache.seatunnel.api.table.schema.handler;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class AlterTableSchemaEventHandler implements TableSchemaChangeEventHandler {
    private TableSchema schema;

    @Override
    public TableSchema get() {
        return schema;
    }

    @Override
    public TableSchemaChangeEventHandler reset(TableSchema schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public TableSchema apply(SchemaChangeEvent event) {
        AlterTableEvent alterTableEvent = (AlterTableEvent) event;
        return apply(schema, alterTableEvent);
    }

    private TableSchema apply(TableSchema schema, AlterTableEvent alterTableEvent) {
        if (alterTableEvent instanceof AlterTableNameEvent) {
            return schema;
        }
        if (alterTableEvent instanceof AlterTableDropColumnEvent) {
            return applyDropColumn(schema, (AlterTableDropColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableModifyColumnEvent) {
            return applyModifyColumn(schema, (AlterTableModifyColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableChangeColumnEvent) {
            return applyChangeColumn(schema, (AlterTableChangeColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableAddColumnEvent) {
            return applyAddColumn(schema, (AlterTableAddColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableColumnsEvent) {
            TableSchema newSchema = schema;
            for (AlterTableColumnEvent columnEvent :
                    ((AlterTableColumnsEvent) alterTableEvent).getEvents()) {
                newSchema = apply(newSchema, columnEvent);
            }
            return newSchema;
        }

        throw new UnsupportedOperationException(
                "Unsupported alter table event: " + alterTableEvent);
    }

    private TableSchema applyAddColumn(
            TableSchema schema, AlterTableAddColumnEvent addColumnEvent) {
        LinkedList<String> originFields = new LinkedList<>(Arrays.asList(schema.getFieldNames()));
        Column column = addColumnEvent.getColumn();
        if (originFields.contains(column.getName())) {
            return applyModifyColumn(
                    schema,
                    new AlterTableModifyColumnEvent(
                            addColumnEvent.tableIdentifier(),
                            addColumnEvent.getColumn(),
                            addColumnEvent.isFirst(),
                            addColumnEvent.getAfterColumn()));
        }

        LinkedList<Column> newColumns = new LinkedList<>(schema.getColumns());
        if (addColumnEvent.isFirst()) {
            newColumns.addFirst(column);
        } else if (addColumnEvent.getAfterColumn() != null) {
            int index = originFields.indexOf(addColumnEvent.getAfterColumn());
            newColumns.add(index + 1, column);
        } else {
            newColumns.addLast(column);
        }

        return TableSchema.builder()
                .columns(newColumns)
                .primaryKey(schema.getPrimaryKey())
                .constraintKey(schema.getConstraintKeys())
                .build();
    }

    private TableSchema applyDropColumn(
            TableSchema schema, AlterTableDropColumnEvent dropColumnEvent) {
        List<Column> newColumns =
                schema.getColumns().stream()
                        .filter(c -> !c.getName().equals(dropColumnEvent.getColumn()))
                        .collect(Collectors.toList());

        return TableSchema.builder()
                .columns(newColumns)
                .primaryKey(schema.getPrimaryKey())
                .constraintKey(schema.getConstraintKeys())
                .build();
    }

    private TableSchema applyModifyColumn(
            TableSchema schema, AlterTableModifyColumnEvent modifyColumnEvent) {
        List<String> fieldNames = Arrays.asList(schema.getFieldNames());
        Column modifyColumn = modifyColumnEvent.getColumn();
        if (!fieldNames.contains(modifyColumn.getName())) {
            return schema;
        }
        String modifyColumnName = modifyColumn.getName();
        int modifyColumnIndex = fieldNames.indexOf(modifyColumnName);
        Column oldColumn = schema.getColumns().get(modifyColumnIndex);
        String oldColumnSourceType = oldColumn.getSourceType();
        String modifyColumnSourceType = modifyColumn.getSourceType();
        if (StringUtils.isNoneEmpty(oldColumnSourceType)
                && StringUtils.isNoneEmpty(modifyColumnSourceType)
                && !oldColumnSourceType.split("\\(")[0].equals(
                        modifyColumnSourceType.split("\\(")[0])) {
            modifyColumnEvent.setTypeChanged(true);
        }
        return applyModifyColumn(
                schema,
                modifyColumnIndex,
                modifyColumn,
                modifyColumnEvent.isFirst(),
                modifyColumnEvent.getAfterColumn());
    }

    private TableSchema applyChangeColumn(
            TableSchema schema, AlterTableChangeColumnEvent changeColumnEvent) {
        String oldColumn = changeColumnEvent.getOldColumn();
        int oldColumnIndex = schema.indexOf(oldColumn);

        // The operation of rename column which only has the name of old column and the name of new
        // column,
        // so we need to fill the data type which is the same as the old column.
        Column column = changeColumnEvent.getColumn();
        if (column.getDataType() == null) {
            SeaTunnelDataType<?> fieldType = schema.getColumn(oldColumn).getDataType();
            column = column.copy(fieldType);
        }

        return applyModifyColumn(
                schema,
                oldColumnIndex,
                column,
                changeColumnEvent.isFirst(),
                changeColumnEvent.getAfterColumn());
    }

    private TableSchema applyModifyColumn(
            TableSchema schema, int columnIndex, Column column, boolean first, String afterColumn) {
        LinkedList<Column> originColumns = new LinkedList<>(schema.getColumns());

        if (first) {
            originColumns.remove(columnIndex);
            originColumns.addFirst(column);
        } else if (afterColumn != null) {
            originColumns.remove(columnIndex);

            int index =
                    originColumns.stream()
                            .filter(c -> c.getName().equals(afterColumn))
                            .findFirst()
                            .map(originColumns::indexOf)
                            .get();
            originColumns.add(index + 1, column);
        } else {
            originColumns.set(columnIndex, column);
        }
        return TableSchema.builder()
                .columns(originColumns)
                .primaryKey(schema.getPrimaryKey())
                .constraintKey(schema.getConstraintKeys())
                .build();
    }
}
