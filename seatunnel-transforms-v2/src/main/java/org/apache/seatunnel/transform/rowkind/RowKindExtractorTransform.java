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

package org.apache.seatunnel.transform.rowkind;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import lombok.NonNull;

import java.util.Arrays;

public class RowKindExtractorTransform extends SingleFieldOutputTransform {

    private final ReadonlyConfig config;

    private final RowKindExtractorTransformType transformType;

    public RowKindExtractorTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        this.transformType = config.get(RowKindExtractorTransformConfig.TRANSFORM_TYPE);
    }

    @Override
    public String getPluginName() {
        return RowKindExtractorTransformConfig.PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object fieldValue = getOutputFieldValue(new SeaTunnelRowAccessor(inputRow));
        inputRow.setRowKind(RowKind.INSERT);
        SeaTunnelRow outputRow = getRowContainerGenerator().apply(inputRow);
        outputRow.setField(getFieldIndex(), fieldValue);
        return outputRow;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        switch (transformType) {
            case SHORT:
                return inputRow.getRowKind().shortString();
            case FULL:
                return inputRow.getRowKind().name();
            default:
                throw new IllegalArgumentException(
                        String.format("Unsupported transform type %s", transformType));
        }
    }

    @Override
    protected Column getOutputColumn() {
        String customFieldName = config.get(RowKindExtractorTransformConfig.CUSTOM_FIELD_NAME);
        String[] fieldNames = inputCatalogTable.getTableSchema().getFieldNames();
        boolean isExist = Arrays.asList(fieldNames).contains(customFieldName);
        if (isExist) {
            throw new IllegalArgumentException(
                    String.format("field name %s already exists", customFieldName));
        }
        return PhysicalColumn.of(
                customFieldName,
                BasicType.STRING_TYPE,
                13L,
                false,
                RowKind.INSERT.shortString(),
                "Output column of RowKind");
    }

    @VisibleForTesting
    public void initRowContainerGenerator() {
        transformTableSchema();
    }
}
