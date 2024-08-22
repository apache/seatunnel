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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import org.apache.commons.lang3.ObjectUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TablePlaceholder {
    // Placeholder ${database_name} or ${database_name:default_value}
    public static final String REPLACE_DATABASE_NAME_KEY = "database_name";
    // Placeholder ${schema_name} or ${schema_name:default_value}
    public static final String REPLACE_SCHEMA_NAME_KEY = "schema_name";
    // Placeholder ${schema_full_name} or ${schema_full_name:default_value}
    public static final String REPLACE_SCHEMA_FULL_NAME_KEY = "schema_full_name";
    // Placeholder ${table_name} or ${table_name:default_value}
    public static final String REPLACE_TABLE_NAME_KEY = "table_name";
    // Placeholder ${table_full_name} or ${table_full_name:default_value}
    public static final String REPLACE_TABLE_FULL_NAME_KEY = "table_full_name";
    // Placeholder ${primary_key} or ${primary_key:default_value}
    public static final String REPLACE_PRIMARY_KEY = "primary_key";
    // Placeholder ${unique_key} or ${unique_key:default_value}
    public static final String REPLACE_UNIQUE_KEY = "unique_key";
    // Placeholder ${field_names} or ${field_names:default_value}
    public static final String REPLACE_FIELD_NAMES_KEY = "field_names";
    public static final String NAME_DELIMITER = ".";
    public static final String FIELD_DELIMITER = ",";

    private static String replacePlaceholders(String input, String placeholderName, String value) {
        return replacePlaceholders(input, placeholderName, value, null);
    }

    private static String replacePlaceholders(
            String input, String placeholderName, String value, String defaultValue) {
        String placeholderRegex = "\\$\\{" + Pattern.quote(placeholderName) + "(:[^}]*)?\\}";
        Pattern pattern = Pattern.compile(placeholderRegex);
        Matcher matcher = pattern.matcher(input);

        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement =
                    value != null && !value.isEmpty()
                            ? value
                            : (matcher.group(1) != null
                                    ? matcher.group(1).substring(1).trim()
                                    : defaultValue);
            if (replacement == null) {
                continue;
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private static String replaceTableIdentifier(
            String placeholder, TableIdentifier identifier, String defaultValue) {
        placeholder =
                replacePlaceholders(
                        placeholder,
                        REPLACE_DATABASE_NAME_KEY,
                        identifier.getDatabaseName(),
                        defaultValue);
        placeholder =
                replacePlaceholders(
                        placeholder,
                        REPLACE_SCHEMA_NAME_KEY,
                        identifier.getSchemaName(),
                        defaultValue);
        placeholder =
                replacePlaceholders(
                        placeholder,
                        REPLACE_TABLE_NAME_KEY,
                        identifier.getTableName(),
                        defaultValue);

        List<String> fullPath = new ArrayList<>();
        if (identifier.getDatabaseName() != null) {
            fullPath.add(identifier.getDatabaseName());
        }
        if (identifier.getSchemaName() != null) {
            fullPath.add(identifier.getSchemaName());
        }
        if (!fullPath.isEmpty()) {
            placeholder =
                    replacePlaceholders(
                            placeholder,
                            REPLACE_SCHEMA_FULL_NAME_KEY,
                            String.join(NAME_DELIMITER, fullPath),
                            defaultValue);
        }

        if (identifier.getTableName() != null) {
            fullPath.add(identifier.getTableName());
        }
        if (!fullPath.isEmpty()) {
            placeholder =
                    replacePlaceholders(
                            placeholder,
                            REPLACE_TABLE_FULL_NAME_KEY,
                            String.join(NAME_DELIMITER, fullPath),
                            defaultValue);
        }
        return placeholder;
    }

    public static String replaceTableIdentifier(String placeholder, TableIdentifier identifier) {
        return replaceTableIdentifier(placeholder, identifier, "");
    }

    public static String replaceTablePrimaryKey(String placeholder, PrimaryKey primaryKey) {
        if (primaryKey != null && !primaryKey.getColumnNames().isEmpty()) {
            String pkFieldsString = String.join(FIELD_DELIMITER, primaryKey.getColumnNames());
            return replacePlaceholders(placeholder, REPLACE_PRIMARY_KEY, pkFieldsString);
        }
        return placeholder;
    }

    public static String replaceTableUniqueKey(
            String placeholder, List<ConstraintKey> constraintKeys) {
        Optional<String> ukFieldsString =
                constraintKeys.stream()
                        .filter(
                                e ->
                                        e.getConstraintType()
                                                .equals(ConstraintKey.ConstraintType.UNIQUE_KEY))
                        .findFirst()
                        .map(
                                e ->
                                        e.getColumnNames().stream()
                                                .map(f -> f.getColumnName())
                                                .collect(Collectors.joining(FIELD_DELIMITER)));
        if (ukFieldsString.isPresent()) {
            return replacePlaceholders(placeholder, REPLACE_UNIQUE_KEY, ukFieldsString.get());
        }
        return placeholder;
    }

    public static String replaceTableFieldNames(String placeholder, TableSchema schema) {
        return replacePlaceholders(
                placeholder,
                REPLACE_FIELD_NAMES_KEY,
                String.join(FIELD_DELIMITER, schema.getFieldNames()));
    }

    public static ReadonlyConfig replaceTablePlaceholder(
            ReadonlyConfig config, CatalogTable table) {
        return replaceTablePlaceholder(config, table, Collections.emptyList());
    }

    public static ReadonlyConfig replaceTablePlaceholder(
            ReadonlyConfig config, CatalogTable table, Collection<String> excludeKeys) {
        Map<String, Object> copyOnWriteData = ObjectUtils.clone(config.getSourceMap());
        for (String key : copyOnWriteData.keySet()) {
            if (excludeKeys.contains(key)) {
                continue;
            }
            Object value = copyOnWriteData.get(key);
            if (value != null) {
                if (value instanceof String) {
                    String strValue = (String) value;
                    strValue = replaceTableIdentifier(strValue, table.getTableId());
                    strValue =
                            replaceTablePrimaryKey(
                                    strValue, table.getTableSchema().getPrimaryKey());
                    strValue =
                            replaceTableUniqueKey(
                                    strValue, table.getTableSchema().getConstraintKeys());
                    strValue = replaceTableFieldNames(strValue, table.getTableSchema());
                    copyOnWriteData.put(key, strValue);
                } else if (value instanceof List) {
                    List listValue = (List) value;
                    if (listValue.size() == 1 && listValue.get(0) instanceof String) {
                        String strValue = (String) listValue.get(0);
                        if (strValue.equals("${" + REPLACE_PRIMARY_KEY + "}")) {
                            strValue =
                                    replaceTablePrimaryKey(
                                            strValue, table.getTableSchema().getPrimaryKey());
                            listValue = Arrays.asList(strValue.split(FIELD_DELIMITER));
                        } else if (strValue.equals("${" + REPLACE_UNIQUE_KEY + "}")) {
                            strValue =
                                    replaceTableUniqueKey(
                                            strValue, table.getTableSchema().getConstraintKeys());
                            listValue = Arrays.asList(strValue.split(FIELD_DELIMITER));
                        } else if (strValue.equals("${" + REPLACE_FIELD_NAMES_KEY + "}")) {
                            strValue = replaceTableFieldNames(strValue, table.getTableSchema());
                            listValue = Arrays.asList(strValue.split(FIELD_DELIMITER));
                        }
                        copyOnWriteData.put(key, listValue);
                    }
                }
            }
        }
        return ReadonlyConfig.fromMap(copyOnWriteData);
    }
}
