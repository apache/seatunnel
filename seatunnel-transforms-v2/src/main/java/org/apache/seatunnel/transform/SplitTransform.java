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

package org.apache.seatunnel.transform;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

@AutoService(SeaTunnelTransform.class)
public class SplitTransform extends MultipleFieldOutputTransform {

    public static final Option<String> KEY_SEPARATOR = Options.key("separator")
            .stringType()
            .noDefaultValue()
            .withDescription("The separator to split the field");

    public static final Option<String> KEY_SPLIT_FIELD = Options.key("split_field")
            .stringType()
            .noDefaultValue()
            .withDescription("The field to be split");

    public static final Option<List<String>> KEY_OUTPUT_FIELDS = Options.key("output_fields")
            .listType()
            .noDefaultValue()
            .withDescription("The result fields after split");
    private String separator;
    private String splitField;
    private int splitFieldIndex;
    private String[] outputFields;
    private String[] emptySplits;

    @Override
    public String getPluginName() {
        return "Split";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig,
            KEY_SEPARATOR.key(), KEY_SPLIT_FIELD.key(), KEY_OUTPUT_FIELDS.key());
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }

        separator = pluginConfig.getString(KEY_SEPARATOR.key());
        splitField = pluginConfig.getString(KEY_SPLIT_FIELD.key());
        outputFields = pluginConfig.getStringList(KEY_OUTPUT_FIELDS.key()).toArray(new String[0]);
        emptySplits = new String[outputFields.length];
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {
        splitFieldIndex = rowType.indexOf(splitField);
        if (splitFieldIndex == -1) {
            throw new IllegalArgumentException("Cannot find [" + splitField + "] field in input row type");
        }
    }

    @Override
    protected String[] getOutputFieldNames() {
        return outputFields;
    }

    @Override
    protected SeaTunnelDataType[] getOutputFieldDataTypes() {
        return IntStream.range(0, outputFields.length)
            .mapToObj((IntFunction<SeaTunnelDataType>) value -> BasicType.STRING_TYPE)
            .toArray(value -> new SeaTunnelDataType[value]);
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object splitFieldValue = inputRow.getField(splitFieldIndex);
        if (splitFieldValue == null) {
            return emptySplits;
        }

        String[] splitFieldValues = splitFieldValue.toString().split(separator, outputFields.length);
        if (splitFieldValues.length < outputFields.length) {
            String[] tmp = splitFieldValues;
            splitFieldValues = new String[outputFields.length];
            System.arraycopy(tmp, 0, splitFieldValues, 0, tmp.length);
        }
        return splitFieldValues;
    }
}
