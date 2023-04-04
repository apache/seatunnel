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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class SplitTransform extends MultipleFieldOutputTransform {
    private SplitTransformConfig splitTransformConfig;
    private int splitFieldIndex;

    public SplitTransform(
            @NonNull SplitTransformConfig splitTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.splitTransformConfig = splitTransformConfig;
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        splitFieldIndex = seaTunnelRowType.indexOf(splitTransformConfig.getSplitField());
        if (splitFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find ["
                            + splitTransformConfig.getSplitField()
                            + "] field in input row type");
        }
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return "Split";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new SplitTransformFactory().optionRule());
        this.splitTransformConfig =
                SplitTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {
        splitFieldIndex = rowType.indexOf(splitTransformConfig.getSplitField());
        if (splitFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find ["
                            + splitTransformConfig.getSplitField()
                            + "] field in input row type");
        }
    }

    @Override
    protected String[] getOutputFieldNames() {
        return splitTransformConfig.getOutputFields();
    }

    @Override
    protected SeaTunnelDataType[] getOutputFieldDataTypes() {
        return IntStream.range(0, splitTransformConfig.getOutputFields().length)
                .mapToObj((IntFunction<SeaTunnelDataType>) value -> BasicType.STRING_TYPE)
                .toArray(value -> new SeaTunnelDataType[value]);
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
        List<PhysicalColumn> collect =
                Arrays.stream(splitTransformConfig.getOutputFields())
                        .map(
                                fieldName -> {
                                    return PhysicalColumn.of(
                                            fieldName, BasicType.STRING_TYPE, 200, true, "", "");
                                })
                        .collect(Collectors.toList());
        return collect.toArray(new Column[0]);
    }
}
