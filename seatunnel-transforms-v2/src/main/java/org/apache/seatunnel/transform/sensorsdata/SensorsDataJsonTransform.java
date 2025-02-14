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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.sensorsdata.record.RowAccessor;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecordBuilder;
import org.apache.seatunnel.format.sensorsdata.record.SensorsDataRecordType;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SensorsDataJsonTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "SensorsDataJson";

    private final RowAccessor rowAccessor;

    private final SensorsDataJsonTransformConfig transformConfig;

    private final SensorsDataRecordType recordType;

    public SensorsDataJsonTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.transformConfig = new SensorsDataJsonTransformConfig(config);
        SeaTunnelRowType seaTunnelRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();
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
        String json =
                SensorsDataRecordBuilder.newBuilder(this.recordType, this.rowAccessor)
                        .build(inputRow)
                        .toJsonString();
        Object[] outputDataArray = new Object[1];
        outputDataArray[0] = json;
        SeaTunnelRow outputRow = new SeaTunnelRow(outputDataArray);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
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
