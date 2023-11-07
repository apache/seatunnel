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

package org.apache.seatunnel.transform.replace;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import org.apache.commons.collections4.CollectionUtils;

import lombok.NonNull;

import java.util.List;
import java.util.stream.Collectors;

public class ReplaceTransform extends SingleFieldOutputTransform {
    public static String PLUGIN_NAME = "Replace";
    private ReplaceTransformConfig config;
    private int inputFieldIndex;

    public ReplaceTransform(
            @NonNull ReplaceTransformConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        initOutputFields(
                inputCatalogTable.getTableSchema().toPhysicalRowDataType(),
                this.config.getReplaceField());
    }

    @Override
    public String getPluginName() {
        return "Replace";
    }

    private void initOutputFields(SeaTunnelRowType inputRowType, String replaceField) {
        inputFieldIndex = inputRowType.indexOf(replaceField);
        if (inputFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find [" + replaceField + "] field in input row type");
        }
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        Object inputFieldValue = inputRow.getField(inputFieldIndex);
        if (inputFieldValue == null) {
            return null;
        }

        boolean isRegex = config.getIsRegex() == null ? false : config.getIsRegex();
        if (isRegex) {
            if (config.getReplaceFirst()) {
                return inputFieldValue
                        .toString()
                        .replaceFirst(config.getPattern(), config.getReplacement());
            }
            return inputFieldValue
                    .toString()
                    .replaceAll(config.getPattern(), config.getReplacement());
        }
        return inputFieldValue.toString().replace(config.getPattern(), config.getReplacement());
    }

    @Override
    protected Column getOutputColumn() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        List<Column> collect =
                columns.stream()
                        .filter(column -> column.getName().equals(config.getReplaceField()))
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(collect)) {
            throw new IllegalArgumentException(
                    "Cannot find [" + config.getReplaceField() + "] field in input catalog table");
        }
        return collect.get(0).copy();
    }
}
