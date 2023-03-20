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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import com.google.auto.service.AutoService;

import java.util.function.IntFunction;
import java.util.stream.IntStream;

@AutoService(SeaTunnelTransform.class)
public class SplitTransform extends MultipleFieldOutputTransform {

    private CatalogTable catalogTable;

    private SplitTransformConfig splitTransformConfig;

    private int splitFieldIndex;

    public SplitTransform(SplitTransformConfig splitTransformConfig, CatalogTable catalogTable) {
        this.splitTransformConfig = splitTransformConfig;
        this.catalogTable = catalogTable;
    }

    @Override
    public String getPluginName() {
        return "Split";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new SplitTransformFactory().optionRule());
        this.splitTransformConfig = SplitTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {
        splitFieldIndex = rowType.indexOf(splitTransformConfig.getSplitField());
        if (splitFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find [" + splitTransformConfig.getSplitField() + "] field in input row type");
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
                splitFieldValue.toString().split(splitTransformConfig.getSeparator(), splitTransformConfig.getOutputFields().length);
        if (splitFieldValues.length < splitTransformConfig.getOutputFields().length) {
            String[] tmp = splitFieldValues;
            splitFieldValues = new String[splitTransformConfig.getOutputFields().length];
            System.arraycopy(tmp, 0, splitFieldValues, 0, tmp.length);
        }
        return splitFieldValues;
    }
}
