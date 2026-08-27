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

package org.apache.seatunnel.transform.regexextract;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class RegexExtractTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "RegexExtract";

    private final RegexExtractTransformConfig config;
    private final Pattern pattern;
    private final int sourceFieldIndex;

    public RegexExtractTransform(
            @NonNull RegexExtractTransformConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
        this.pattern = Pattern.compile(config.getRegexPattern());

        try {
            sourceFieldIndex = catalogTable.getTableSchema().indexOf(config.getSourceField());
        } catch (IllegalArgumentException e) {
            throw TransformCommonError.cannotFindInputFieldError(
                    getPluginName(), config.getSourceField());
        }
        int groupCount = pattern.matcher("").groupCount();
        int outputFieldsSize = config.getOutputFields().size();
        if (groupCount != outputFieldsSize) {
            throw new IllegalArgumentException(
                    String.format(
                            "Regex group count (%d) must equal output fields size (%d)",
                            groupCount, outputFieldsSize));
        }

        List<String> defaultValues = config.getDefaultValues();
        if (defaultValues != null
                && !defaultValues.isEmpty()
                && defaultValues.size() != outputFieldsSize) {
            throw new IllegalArgumentException(
                    String.format(
                            "Default values size (%d) must equal output fields size (%d)",
                            defaultValues.size(), outputFieldsSize));
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object sourceValue = inputRow.getField(sourceFieldIndex);

        if (sourceValue == null) {
            Object[] result = new Object[config.getOutputFields().size()];
            fillWithDefaultValues(result);
            return result;
        }

        Matcher sourceFieldMatcher = pattern.matcher(sourceValue.toString());
        Object[] result = new Object[config.getOutputFields().size()];
        if (!sourceFieldMatcher.find()) {
            fillWithDefaultValues(result);
            return result;
        }

        for (int i = 0; i < result.length; i++) {
            result[i] = sourceFieldMatcher.group(i + 1);
        }
        return result;
    }

    @Override
    protected Column[] getOutputColumns() {
        return config.getOutputFields().stream()
                .map(
                        fieldName ->
                                PhysicalColumn.of(
                                        fieldName, BasicType.STRING_TYPE, 200, true, "", ""))
                .toArray(Column[]::new);
    }

    private void fillWithDefaultValues(Object[] result) {
        for (int i = 0; i < result.length; i++) {
            result[i] = getDefaultValue(i);
        }
    }

    private String getDefaultValue(int index) {
        List<String> defaultValues = config.getDefaultValues();
        if (defaultValues == null || defaultValues.isEmpty()) {
            return null;
        }
        return defaultValues.get(index);
    }
}
