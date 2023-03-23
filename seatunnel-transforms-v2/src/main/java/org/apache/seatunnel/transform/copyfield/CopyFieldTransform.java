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

package org.apache.seatunnel.transform.copyfield;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import org.apache.commons.collections4.CollectionUtils;

import com.google.auto.service.AutoService;
import lombok.NonNull;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
public class CopyFieldTransform extends SingleFieldOutputTransform {

    private int srcFieldIndex;
    private SeaTunnelDataType srcFieldDataType;
    private CopyFieldTransformConfig config;

    public CopyFieldTransform() {
        super();
    }

    public CopyFieldTransform(
            @NonNull CopyFieldTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        this.srcFieldIndex = config.getSrcFieldIndex();
        this.srcFieldDataType = config.getSrcFieldDataType();
    }

    @Override
    public String getPluginName() {
        return "Copy";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new CopyFieldTransformFactory().optionRule());
        this.config = CopyFieldTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
        this.srcFieldIndex = config.getSrcFieldIndex();
        this.srcFieldDataType = config.getSrcFieldDataType();
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
        srcFieldIndex = inputRowType.indexOf(config.getSrcField());
        if (srcFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find [" + config.getSrcField() + "] field in input row type");
        }
        srcFieldDataType = inputRowType.getFieldType(srcFieldIndex);
    }

    @Override
    protected String getOutputFieldName() {
        return config.getDestField();
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return srcFieldDataType;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        return clone(srcFieldDataType, inputRow.getField(srcFieldIndex));
    }

    @Override
    protected Column getOutputColumn() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        List<Column> collect =
                columns.stream()
                        .filter(column -> column.getName().equals(config.getSrcField()))
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(collect)) {
            throw new IllegalArgumentException(
                    "Cannot find [" + config.getSrcField() + "] field in input catalog table");
        }
        Column copyColumn = collect.get(0).copy();
        PhysicalColumn outputColumn =
                PhysicalColumn.of(
                        config.getDestField(),
                        copyColumn.getDataType(),
                        copyColumn.getColumnLength(),
                        copyColumn.isNullable(),
                        copyColumn.getDefaultValue(),
                        copyColumn.getComment());
        return outputColumn;
    }

    private Object clone(SeaTunnelDataType dataType, Object value) {
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
                if (bytes == null) {
                    return null;
                }
                byte[] newBytes = new byte[bytes.length];
                System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                return newBytes;
            case ARRAY:
                ArrayType arrayType = (ArrayType) dataType;
                Object[] array = (Object[]) value;
                if (array == null) {
                    return null;
                }
                Object newArray =
                        Array.newInstance(arrayType.getElementType().getTypeClass(), array.length);
                for (int i = 0; i < array.length; i++) {
                    Array.set(newArray, i, clone(arrayType.getElementType(), array[i]));
                }
                return newArray;
            case MAP:
                MapType mapType = (MapType) dataType;
                Map map = (Map) value;
                Map newMap = new HashMap();
                for (Object key : map.keySet()) {
                    newMap.put(
                            clone(mapType.getKeyType(), key),
                            clone(mapType.getValueType(), map.get(key)));
                }
                return newMap;
            case ROW:
                SeaTunnelRowType rowType = (SeaTunnelRowType) dataType;
                SeaTunnelRow row = (SeaTunnelRow) value;
                if (row == null) {
                    return null;
                }

                Object[] newFields = new Object[rowType.getTotalFields()];
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    newFields[i] = clone(rowType.getFieldType(i), row.getField(i));
                }
                SeaTunnelRow newRow = new SeaTunnelRow(newFields);
                newRow.setRowKind(row.getRowKind());
                newRow.setTableId(row.getTableId());
                return newRow;
            case NULL:
                return null;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported type: " + dataType.getSqlType());
        }
    }
}
