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

package org.apache.seatunnel.transform.sensorsdata;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.sensorsdata.record.RowAccessor;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecordBuilder;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecordType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SensorsDataJsonTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "SensorsDataJson";

    private final RowAccessor rowAccessor;

    private final SensorsDataJsonTransformConfig transformConfig;

    private final SensorsDataRecordType recordType;

    private final SeaTunnelRowType seaTunnelRowType;

    public SensorsDataJsonTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.transformConfig = new SensorsDataJsonTransformConfig(config);
        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        this.seaTunnelRowType = inputCatalogTable.getSeaTunnelRowType();
        this.rowAccessor = new RowAccessor(this.transformConfig, seaTunnelRowType);
        this.recordType =
                SensorsDataRecordBuilder.newBuilder(this.transformConfig, this.rowAccessor)
                        .getRecordType();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        boolean skipErrorRecord = transformConfig.isSkipErrorRecord();
        String json;
        try {
            json =
                    SensorsDataRecordBuilder.newBuilder(this.recordType, this.rowAccessor)
                            .build(inputRow)
                            .toJsonString();
        } catch (Exception e) {
            log.error(
                    "Write error, SeaTunnelRow#tableId={} SeaTunnelRow#kind={} : [{}]",
                    inputRow.getTableId(),
                    inputRow.getRowKind(),
                    fieldsToString(inputRow),
                    e);
            if (!skipErrorRecord) {
                throw e;
            }
            return null;
        }
        Object[] outputDataArray = new Object[1];
        outputDataArray[0] = json;
        SeaTunnelRow outputRow = new SeaTunnelRow(outputDataArray);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }

    /** 将整行数据转为 string */
    private String fieldsToString(SeaTunnelRow row) {
        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            arr[i] = fieldToString(fieldTypes[i], fields[i]);
        }
        return StringUtils.join(arr, ", ");
    }

    /** copy from ConsoleSinkWriter */
    private String fieldToString(SeaTunnelDataType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                List<String> arrayData = new ArrayList<>();
                for (int i = 0; i < Array.getLength(value); i++) {
                    arrayData.add(String.valueOf(Array.get(value, i)));
                }
                return arrayData.toString();
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                List<String> rowData = new ArrayList<>();
                SeaTunnelRowType rowType = (SeaTunnelRowType) type;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    rowData.add(
                            fieldToString(
                                    rowType.getFieldTypes()[i],
                                    ((SeaTunnelRow) value).getField(i)));
                }
                return rowData.toString();
            default:
                return String.valueOf(value);
        }
    }

    @Override
    protected TableSchema transformTableSchema() {
        String columnName = transformConfig.getJsonColumnName();
        Column column =
                PhysicalColumn.of(
                        columnName,
                        BasicType.STRING_TYPE,
                        0L,
                        true,
                        null,
                        "SensorsData json record");
        return TableSchema.builder().column(column).build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }
}
