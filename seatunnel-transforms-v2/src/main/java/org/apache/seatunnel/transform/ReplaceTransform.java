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
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelTransform.class)
public class ReplaceTransform extends SingleFieldOutputTransform {

    public static final Option<String> KEY_REPLACE_FIELD = Options.key("replace_field")
            .stringType()
            .noDefaultValue()
            .withDescription("The field you want to replace");

    public static final Option<String> KEY_PATTERN = Options.key("pattern")
            .stringType()
            .noDefaultValue()
            .withDescription("The old string that will be replaced");

    public static final Option<String> KEY_REPLACEMENT = Options.key("replacement")
            .stringType()
            .noDefaultValue()
            .withDescription("The new string for replace");

    public static final Option<Boolean> KEY_IS_REGEX = Options.key("is_regex")
            .booleanType()
            .defaultValue(false)
            .withDescription("Use regex for string match");

    public static final Option<Boolean> KEY_REPLACE_FIRST = Options.key("replace_first")
            .booleanType()
            .noDefaultValue()
            .withDescription("Replace the first match string");

    private int inputFieldIndex;
    private String replaceField;
    private String pattern;
    private String replacement;
    private boolean isRegex;
    private boolean replaceFirst;

    @Override
    public String getPluginName() {
        return "Replace";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig,
            KEY_REPLACE_FIELD.key(), KEY_PATTERN.key(), KEY_REPLACEMENT.key());
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }

        replaceField = pluginConfig.getString(KEY_REPLACE_FIELD.key());
        pattern = pluginConfig.getString(KEY_PATTERN.key());
        replacement = pluginConfig.getString(KEY_REPLACEMENT.key());
        if (pluginConfig.hasPath(KEY_IS_REGEX.key())) {
            isRegex = pluginConfig.getBoolean(KEY_IS_REGEX.key());
        }
        if (pluginConfig.hasPath(KEY_REPLACE_FIRST.key())) {
            replaceFirst = pluginConfig.getBoolean(KEY_REPLACE_FIRST.key());
        }
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {
        inputFieldIndex = rowType.indexOf(replaceField);
        if (inputFieldIndex == -1) {
            throw new IllegalArgumentException("Cannot find [" + replaceField + "] field in input row type");
        }
    }

    @Override
    protected String getOutputFieldName() {
        return replaceField;
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return BasicType.STRING_TYPE;
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        Object inputFieldValue = inputRow.getField(inputFieldIndex);
        if (inputFieldValue == null) {
            return null;
        }

        if (isRegex) {
            if (replaceFirst) {
                return inputFieldValue.toString().replaceFirst(pattern, replacement);
            }
            return inputFieldValue.toString().replaceAll(pattern, replacement);
        }
        return inputFieldValue.toString().replace(pattern, replacement);
    }
}
