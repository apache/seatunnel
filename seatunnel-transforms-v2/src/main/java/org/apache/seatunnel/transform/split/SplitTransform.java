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

package org.apache.seatunnel.transform.split;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.Arrays;

public class SplitTransform extends MultipleFieldOutputTransform {
    public static String PLUGIN_NAME = "Split";
    private final SplitTransformConfig splitTransformConfig;
    private final int splitFieldIndex;

    public SplitTransform(
            @NonNull SplitTransformConfig splitTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.splitTransformConfig = splitTransformConfig;
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        try {
            splitFieldIndex = seaTunnelRowType.indexOf(splitTransformConfig.getSplitField());
        } catch (IllegalArgumentException e) {
            throw TransformCommonError.cannotFindInputFieldError(
                    getPluginName(), splitTransformConfig.getSplitField());
        }
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return "Split";
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object splitFieldValue = inputRow.getField(splitFieldIndex);
        if (splitFieldValue == null) {
            return splitTransformConfig.getEmptySplits();
        }

        String[] splitFieldValues =
                splitFieldValue
                        .toString()
                        .split(
                                splitTransformConfig.getSeparator(),
                                splitTransformConfig.getOutputFields().length);
        if (splitFieldValues.length < splitTransformConfig.getOutputFields().length) {
            String[] tmp = splitFieldValues;
            splitFieldValues = new String[splitTransformConfig.getOutputFields().length];
            System.arraycopy(tmp, 0, splitFieldValues, 0, tmp.length);
        }
        return splitFieldValues;
    }

    @Override
    protected Column[] getOutputColumns() {
        return Arrays.stream(splitTransformConfig.getOutputFields())
                .map(
                        fieldName ->
                                PhysicalColumn.of(
                                        fieldName, BasicType.STRING_TYPE, 200, true, "", ""))
                .toArray(Column[]::new);
    }
}
