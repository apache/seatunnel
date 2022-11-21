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

    private static final String KEY_REPLACE_FIELD = "replace_field";
    private static final String KEY_PATTERN = "pattern";
    private static final String KEY_REPLACEMENT = "replacement";
    private static final String KEY_IS_REGEX = "is_regex";
    private static final String KEY_REPLACE_FIRST = "replace_first";

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
            KEY_REPLACE_FIELD, KEY_PATTERN, KEY_REPLACEMENT);
        if (!checkResult.isSuccess()) {
            throw new IllegalArgumentException("Failed to check config! " + checkResult.getMsg());
        }

        replaceField = pluginConfig.getString(KEY_REPLACE_FIELD);
        pattern = pluginConfig.getString(KEY_PATTERN);
        replacement = pluginConfig.getString(KEY_REPLACEMENT);
        if (pluginConfig.hasPath(KEY_IS_REGEX)) {
            isRegex = pluginConfig.getBoolean(KEY_IS_REGEX);
        }
        if (pluginConfig.hasPath(KEY_REPLACE_FIRST)) {
            replaceFirst = pluginConfig.getBoolean(KEY_REPLACE_FIRST);
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
