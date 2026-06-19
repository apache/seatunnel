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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class ReplaceTransform extends AbstractCatalogSupportMapTransform {
    public static final String PLUGIN_NAME = "Replace";
    private final List<String> replaceFields = new ArrayList<>();
    private final String pattern;
    private final String replacement;
    private final boolean isRegex;
    private final boolean replaceFirst;
    private final Pattern regexPattern;
    private int[] replaceFieldIndexes;

    public ReplaceTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        validateConflictingReplaceFieldKeys(config);
        this.replaceFields.addAll(
                getRequiredOption(config, ReplaceTransformConfig.KEY_REPLACE_FIELDS));

        if (replaceFields.isEmpty()) {
            throw TransformCommonError.validationFailed(
                    String.format(
                            "Option '%s' must not be empty.",
                            ReplaceTransformConfig.KEY_REPLACE_FIELDS.key()));
        }

        this.pattern = getRequiredOption(config, ReplaceTransformConfig.KEY_PATTERN);
        this.replacement = getRequiredOption(config, ReplaceTransformConfig.KEY_REPLACEMENT);
        this.isRegex = config.get(ReplaceTransformConfig.KEY_IS_REGEX);
        this.replaceFirst = config.get(ReplaceTransformConfig.KEY_REPLACE_FIRST);
        this.regexPattern = initializeRegexPattern();
        initializeFieldIndexes();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    private void validateConflictingReplaceFieldKeys(ReadonlyConfig config) {
        Map<String, Object> sourceMap = config.getSourceMap();
        if (sourceMap.containsKey("replace_field") && sourceMap.containsKey("replace_fields")) {
            throw TransformCommonError.validationFailed(
                    "Options 'replace_field' and 'replace_fields' cannot be configured together.");
        }
    }

    private <T> T getRequiredOption(ReadonlyConfig config, Option<T> option) {
        return config.getOptional(option)
                .orElseThrow(
                        () ->
                                TransformCommonError.validationFailed(
                                        String.format("Option '%s' is required.", option.key())));
    }

    private void initializeFieldIndexes() {
        SeaTunnelRowType physicalRowType =
                inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        replaceFieldIndexes =
                replaceFields.stream()
                        .mapToInt(
                                fieldName -> {
                                    int index = physicalRowType.indexOf(fieldName, false);
                                    if (index < 0) {
                                        throw TransformCommonError.cannotFindInputFieldError(
                                                getPluginName(), fieldName);
                                    }
                                    return index;
                                })
                        .toArray();
    }

    private Pattern initializeRegexPattern() {
        if (!isRegex) {
            return null;
        }
        try {
            return Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
            throw CommonError.illegalArgument(
                    pattern,
                    String.format(
                            "Invalid regex pattern for %s transform: %s",
                            getPluginName(), e.getDescription()));
        }
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        SeaTunnelRow outputRow = inputRow.copy();

        for (int index : replaceFieldIndexes) {
            Object value = outputRow.getField(index);
            if (value == null) {
                continue;
            }
            outputRow.setField(index, applyReplacement(value.toString()));
        }
        return outputRow;
    }

    private String applyReplacement(String value) {
        if (isRegex) {
            if (replaceFirst) {
                return regexPattern.matcher(value).replaceFirst(replacement);
            }
            return regexPattern.matcher(value).replaceAll(replacement);
        }
        return value.replace(pattern, replacement);
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }
}
