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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;

import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReplaceTransform extends AbstractCatalogSupportMapTransform {
    private final List<String> replaceFields = new ArrayList<>();
    private final String pattern;
    private final String replacement;
    private final Boolean isRegex;
    private final Boolean replaceFirst;
    private int[] replaceFieldIndexes;

    public ReplaceTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.replaceFields.addAll(
                parseReplaceFields(config.get(ReplaceTransformConfig.KEY_REPLACE_FIELD)));
        this.pattern = config.get(ReplaceTransformConfig.KEY_PATTERN);
        this.replacement = config.get(ReplaceTransformConfig.KEY_REPLACEMENT);
        this.isRegex = config.get(ReplaceTransformConfig.KEY_IS_REGEX);
        this.replaceFirst = config.get(ReplaceTransformConfig.KEY_REPLACE_FIRST);
        initializeFieldIndexes();
    }

    @Override
    public String getPluginName() {
        return "Replace";
    }

    private void initializeFieldIndexes() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        replaceFieldIndexes =
                replaceFields.stream()
                        .mapToInt(
                                fieldName -> {
                                    for (int i = 0; i < columns.size(); i++) {
                                        if (columns.get(i).getName().equals(fieldName)) {
                                            return i;
                                        }
                                    }
                                    throw TransformCommonError.cannotFindInputFieldError(
                                            getPluginName(), fieldName);
                                })
                        .toArray();
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
        if (Boolean.TRUE.equals(isRegex)) {
            if (Boolean.TRUE.equals(replaceFirst)) {
                return value.replaceFirst(pattern, replacement);
            }
            return value.replaceAll(pattern, replacement);
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

    private List<String> parseReplaceFields(Object rawValue) {

        if (rawValue == null) {
            throw TransformCommonError.validationFailed(
                    String.format(
                            "Option '%s' is required and must be configured as a string or an array of strings.",
                            ReplaceTransformConfig.KEY_REPLACE_FIELD.key()));
        }

        if (rawValue instanceof String) {
            return validateReplaceFields(
                    Collections.singletonList((String) rawValue),
                    ReplaceTransformConfig.KEY_REPLACE_FIELD.key());
        }

        if (rawValue instanceof List) {
            List<String> fields = new ArrayList<>();
            for (Object field : (List<?>) rawValue) {
                if (!(field instanceof String)) {
                    throw TransformCommonError.validationFailed(
                            String.format(
                                    "Option '%s' must be configured as a string or an array of strings.",
                                    ReplaceTransformConfig.KEY_REPLACE_FIELD.key()));
                }
                fields.add((String) field);
            }
            return validateReplaceFields(fields, ReplaceTransformConfig.KEY_REPLACE_FIELD.key());
        }

        throw TransformCommonError.validationFailed(
                String.format(
                        "Option '%s' must be configured as a string or an array of strings.",
                        ReplaceTransformConfig.KEY_REPLACE_FIELD.key()));
    }

    private List<String> validateReplaceFields(List<String> fields, String optionName) {

        if (fields.isEmpty()) {
            throw TransformCommonError.validationFailed(
                    String.format("Option '%s' must not be empty.", optionName));
        }

        for (String field : fields) {
            if (field == null || field.trim().isEmpty()) {
                throw TransformCommonError.validationFailed(
                        String.format(
                                "Option '%s' must not contain blank field names.", optionName));
            }
        }

        return fields;
    }
}
