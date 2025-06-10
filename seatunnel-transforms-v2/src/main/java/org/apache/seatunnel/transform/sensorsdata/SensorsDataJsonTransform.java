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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

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
import org.apache.seatunnel.format.sensorsdata.config.TargetColumnConfig;
import org.apache.seatunnel.format.sensorsdata.record.RowAccessor;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecord;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecordBuilder;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecordType;
import org.apache.seatunnel.format.sensorsdata.record.UserRecord;
import org.apache.seatunnel.format.sensorsdata.utils.UserSchemaUtil;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;

import org.apache.commons.lang3.StringUtils;

import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_UNSET_ACTION_TYPE;

@Slf4j
public class SensorsDataJsonTransform extends AbstractCatalogSupportFlatMapTransform {
    public static String PLUGIN_NAME = "SensorsDataJson";

    private final RowAccessor rowAccessor;

    private final SensorsDataJsonTransformConfig transformConfig;

    private final SensorsDataRecordType recordType;

    private final SeaTunnelRowType seaTunnelRowType;

    private final Set<String> allProperties;

    private final boolean skipErrorRecord;

    private final boolean nullAsProfileUnset;

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
        this.skipErrorRecord = transformConfig.isSkipErrorRecord();
        this.nullAsProfileUnset = transformConfig.isNullAsProfileUnset();
        this.allProperties =
                transformConfig.getPropertyFields().stream()
                        .map(TargetColumnConfig::getTarget)
                        .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        try {
            SensorsDataRecord record =
                    SensorsDataRecordBuilder.newBuilder(this.recordType, this.rowAccessor)
                            .build(inputRow);
            String json = record.toJsonString();

            // 没有开启 nullAsProfileUnset 且 record 不是 UserRecord 时, 不处理 properties 为 null 的情况
            if (!(nullAsProfileUnset && record instanceof UserRecord)) {
                return Lists.newArrayList(buildSeatunnelRow(inputRow, json));
            }
            UserSchema unsetSchema =
                    UserSchemaUtil.buildUnsetUserSchema(
                            ((UserRecord) record).getUserSchema(), allProperties);
            if (unsetSchema == null) {
                // unsetSchema 为 null 时, 不发送 profile unset
                return Lists.newArrayList(buildSeatunnelRow(inputRow, json));
            }
            UserRecord unsetRecord = new UserRecord(unsetSchema, PROFILE_UNSET_ACTION_TYPE);
            String unsetJson = unsetRecord.toJsonString();

            return Lists.newArrayList(
                    buildSeatunnelRow(inputRow, json), buildSeatunnelRow(inputRow, unsetJson));

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
    }

    private SeaTunnelRow buildSeatunnelRow(SeaTunnelRow inputRow, String json) {
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
