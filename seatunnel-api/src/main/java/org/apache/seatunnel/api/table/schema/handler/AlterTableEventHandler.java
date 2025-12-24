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
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/** @deprecated instead by {@link AlterTableSchemaEventHandler} */
@Deprecated
public class AlterTableEventHandler implements DataTypeChangeEventHandler {
    private SeaTunnelRowType dataType;

    @Override
    public SeaTunnelRowType get() {
        return dataType;
    }

    @Override
    public DataTypeChangeEventHandler reset(SeaTunnelRowType dataType) {
        this.dataType = dataType;
        return this;
    }

    @Override
    public SeaTunnelRowType apply(SchemaChangeEvent event) {
        AlterTableEvent alterTableEvent = (AlterTableEvent) event;
        return apply(dataType, alterTableEvent);
    }

    private SeaTunnelRowType apply(SeaTunnelRowType dataType, AlterTableEvent alterTableEvent) {
        if (alterTableEvent instanceof AlterTableNameEvent) {
            return dataType;
        }
        if (alterTableEvent instanceof AlterTableDropColumnEvent) {
            return applyDropColumn(dataType, (AlterTableDropColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableModifyColumnEvent) {
            return applyModifyColumn(dataType, (AlterTableModifyColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableChangeColumnEvent) {
            return applyChangeColumn(dataType, (AlterTableChangeColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableAddColumnEvent) {
            return applyAddColumn(dataType, (AlterTableAddColumnEvent) alterTableEvent);
        }
        if (alterTableEvent instanceof AlterTableColumnsEvent) {
            SeaTunnelRowType newType = dataType;
            for (AlterTableColumnEvent columnEvent :
                    ((AlterTableColumnsEvent) alterTableEvent).getEvents()) {
                newType = apply(newType, columnEvent);
            }
            return newType;
        }

        throw new UnsupportedOperationException(
                "Unsupported alter table event: " + alterTableEvent);
    }

    private SeaTunnelRowType applyAddColumn(
            SeaTunnelRowType dataType, AlterTableAddColumnEvent addColumnEvent) {
        LinkedList<String> originFields = new LinkedList<>(Arrays.asList(dataType.getFieldNames()));
        LinkedList<SeaTunnelDataType<?>> originFieldTypes =
                new LinkedList<>(Arrays.asList(dataType.getFieldTypes()));
        Column column = addColumnEvent.getColumn();
        if (originFields.contains(column.getName())) {
            return applyModifyColumn(
                    dataType,
                    new AlterTableModifyColumnEvent(
                            addColumnEvent.tableIdentifier(),
                            addColumnEvent.getColumn(),
                            addColumnEvent.isFirst(),
                            addColumnEvent.getAfterColumn()));
        }

        if (addColumnEvent.isFirst()) {
            originFields.addFirst(column.getName());
            originFieldTypes.addFirst(column.getDataType());
        } else if (addColumnEvent.getAfterColumn() != null) {
            int index = originFields.indexOf(addColumnEvent.getAfterColumn());
            originFields.add(index + 1, column.getName());
            originFieldTypes.add(index + 1, column.getDataType());
        } else {
            originFields.addLast(column.getName());
            originFieldTypes.addLast(column.getDataType());
        }

        return new SeaTunnelRowType(
                originFields.toArray(new String[0]),
                originFieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private SeaTunnelRowType applyDropColumn(
            SeaTunnelRowType dataType, AlterTableDropColumnEvent dropColumnEvent) {
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType> fieldTypes = new ArrayList<>();
        for (int i = 0; i < dataType.getTotalFields(); i++) {
            if (dataType.getFieldName(i).equals(dropColumnEvent.getColumn())) {
                continue;
            }
            fieldNames.add(dataType.getFieldName(i));
            fieldTypes.add(dataType.getFieldType(i));
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]), fieldTypes.toArray(new SeaTunnelDataType[0]));
    }

    private SeaTunnelRowType applyModifyColumn(
            SeaTunnelRowType dataType, AlterTableModifyColumnEvent modifyColumnEvent) {
        List<String> fieldNames = Arrays.asList(dataType.getFieldNames());
        if (!fieldNames.contains(modifyColumnEvent.getColumn().getName())) {
            return dataType;
        }

        String modifyColumnName = modifyColumnEvent.getColumn().getName();
        int modifyColumnIndex = dataType.indexOf(modifyColumnName);
        return applyModifyColumn(
                dataType,
                modifyColumnIndex,
                modifyColumnEvent.getColumn(),
                modifyColumnEvent.isFirst(),
                modifyColumnEvent.getAfterColumn());
    }

    private SeaTunnelRowType applyChangeColumn(
            SeaTunnelRowType dataType, AlterTableChangeColumnEvent changeColumnEvent) {
        String oldColumn = changeColumnEvent.getOldColumn();
        int oldColumnIndex = dataType.indexOf(oldColumn);

        // The operation of rename column which only has the name of old column and the name of new
        // column,
        // so we need to fill the data type which is the same as the old column.
        SeaTunnelDataType<?> fieldType = dataType.getFieldType(oldColumnIndex);
        Column column = changeColumnEvent.getColumn();
        if (column.getDataType() == null) {
            column = column.copy(fieldType);
        }

        return applyModifyColumn(
                dataType,
                oldColumnIndex,
                column,
                changeColumnEvent.isFirst(),
                changeColumnEvent.getAfterColumn());
    }

    private SeaTunnelRowType applyModifyColumn(
            SeaTunnelRowType dataType,
            int columnIndex,
            Column column,
            boolean first,
            String afterColumn) {
        LinkedList<String> originFields = new LinkedList<>(Arrays.asList(dataType.getFieldNames()));
        LinkedList<SeaTunnelDataType<?>> originFieldTypes =
                new LinkedList<>(Arrays.asList(dataType.getFieldTypes()));

        if (first) {
            originFields.remove(columnIndex);
            originFieldTypes.remove(columnIndex);

            originFields.addFirst(column.getName());
            originFieldTypes.addFirst(column.getDataType());
        } else if (afterColumn != null) {
            originFields.remove(columnIndex);
            originFieldTypes.remove(columnIndex);

            int index = originFields.indexOf(afterColumn);
            originFields.add(index + 1, column.getName());
            originFieldTypes.add(index + 1, column.getDataType());
        } else {
            originFields.set(columnIndex, column.getName());
            originFieldTypes.set(columnIndex, column.getDataType());
        }
        return new SeaTunnelRowType(
                originFields.toArray(new String[0]),
                originFieldTypes.toArray(new SeaTunnelDataType[0]));
    }
}
