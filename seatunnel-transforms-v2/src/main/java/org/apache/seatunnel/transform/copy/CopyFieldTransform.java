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

package org.apache.seatunnel.transform.copy;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CopyFieldTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "Copy";

    private final CopyTransformConfig config;
    private List<String> fieldNames;
    private List<Integer> fieldOriginalIndexes;
    private List<SeaTunnelDataType<?>> fieldTypes;

    public CopyFieldTransform(CopyTransformConfig copyTransformConfig, CatalogTable catalogTable) {
        super(catalogTable);
        this.config = copyTransformConfig;
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        initOutputFields(seaTunnelRowType, config.getFields());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private void initOutputFields(
            SeaTunnelRowType inputRowType, LinkedHashMap<String, String> fields) {
        List<String> fieldNames = new ArrayList<>();
        List<Integer> fieldOriginalIndexes = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldsType = new ArrayList<>();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String srcField = field.getValue();
            int srcFieldIndex;
            try {
                srcFieldIndex = inputRowType.indexOf(srcField);
            } catch (IllegalArgumentException e) {
                throw TransformCommonError.cannotFindInputFieldError(getPluginName(), srcField);
            }
            fieldNames.add(field.getKey());
            fieldOriginalIndexes.add(srcFieldIndex);
            fieldsType.add(inputRowType.getFieldType(srcFieldIndex));
        }
        this.fieldNames = fieldNames;
        this.fieldOriginalIndexes = fieldOriginalIndexes;
        this.fieldTypes = fieldsType;
    }

    @Override
    protected Column[] getOutputColumns() {
        if (inputCatalogTable == null) {
            Column[] columns = new Column[fieldNames.size()];
            for (int i = 0; i < fieldNames.size(); i++) {
                columns[i] =
                        PhysicalColumn.of(fieldNames.get(i), fieldTypes.get(i), 200, true, "", "");
            }
            return columns;
        }

        Map<String, Column> catalogTableColumns =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(column -> column.getName(), column -> column));

        List<Column> columns = new ArrayList<>();
        for (Map.Entry<String, String> copyField : config.getFields().entrySet()) {
            Column srcColumn = catalogTableColumns.get(copyField.getValue());
            PhysicalColumn destColumn =
                    PhysicalColumn.of(
                            copyField.getKey(),
                            srcColumn.getDataType(),
                            srcColumn.getColumnLength(),
                            srcColumn.isNullable(),
                            srcColumn.getDefaultValue(),
                            srcColumn.getComment());
            columns.add(destColumn);
        }
        return columns.toArray(new Column[0]);
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object[] fieldValues = new Object[fieldNames.size()];
        for (int i = 0; i < fieldOriginalIndexes.size(); i++) {
            fieldValues[i] =
                    clone(
                            fieldNames.get(i),
                            fieldTypes.get(i),
                            inputRow.getField(fieldOriginalIndexes.get(i)));
        }
        return fieldValues;
    }

    private Object clone(String field, SeaTunnelDataType<?> dataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case BOOLEAN:
            case STRING:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return value;
            case BYTES:
                byte[] bytes = (byte[]) value;
                byte[] newBytes = new byte[bytes.length];
                System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                return newBytes;
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Object[] array = (Object[]) value;
                Object newArray =
                        Array.newInstance(arrayType.getElementType().getTypeClass(), array.length);
                for (int i = 0; i < array.length; i++) {
                    Array.set(newArray, i, clone(field, arrayType.getElementType(), array[i]));
                }
                return newArray;
            case MAP:
                MapType mapType = (MapType) dataType;
                Map map = (Map) value;
                Map<Object, Object> newMap = new HashMap<>();
                for (Object key : map.keySet()) {
                    newMap.put(
                            clone(field, mapType.getKeyType(), key),
                            clone(field, mapType.getValueType(), map.get(key)));
                }
                return newMap;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                SeaTunnelRow row = (SeaTunnelRow) value;

                Object[] newFields = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    newFields[i] =
                            clone(
                                    rowType.getFieldName(i),
                                    rowType.getFieldType(i),
                                    row.getField(i));
                }
                SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                newRow.setRowKind(row.getRowKind());
                newRow.setTableId(row.getTableId());
                return newRow;
            case NULL:
                return null;
            default:
                throw CommonError.unsupportedDataType(
                        getPluginName(), dataType.getSqlType().toString(), field);
        }
    }
}
