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

package org.apache.seatunnel.api.table.catalog.schema;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JsonUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class TableSchemaParser {

    /**
     * Parse TableSchema from schema config.
     *
     * <pre>
     *     {
     *         columns = [
     *         ...
     *         ]
     *         primaryKey = {
     *         ...
     *         }
     *         constraintKeys = [
     *         ...
     *         ]
     *     }
     * </pre>
     *
     * @param schemaConfig schema config
     * @return TableSchema
     */
    public TableSchema parse(ReadonlyConfig schemaConfig) {

        Optional<List<Map<String, Object>>> columnOption =
                schemaConfig.getOptional(TableSchemaOptions.ColumnOptions.COLUMNS);
        Optional<Map<String, Object>> fieldOption =
                schemaConfig.getOptional(TableSchemaOptions.FieldOptions.FIELDS);
        if (!columnOption.isPresent() && !fieldOption.isPresent()) {
            throw new IllegalArgumentException(
                    "Schema config can't contains both [fields] and [columns], please correct your config first");
        }
        if (columnOption.isPresent() && fieldOption.isPresent()) {
            throw new IllegalArgumentException(
                    "Schema config can't contains both [fields] and [columns], please correct your config first");
        }

        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();

        columnOption.ifPresent(
                columnsConfig ->
                        tableSchemaBuilder.columns(new ColumnParser().parse(schemaConfig)));

        fieldOption.ifPresent(
                fieldsConfig -> {
                    log.warn(
                            "The schema.fields will be deprecated, please use schema.columns instead");
                    tableSchemaBuilder.columns(new FieldParser().parse(schemaConfig));
                });

        schemaConfig
                .getOptional(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY)
                .ifPresent(
                        primaryKeyConfig ->
                                tableSchemaBuilder.primaryKey(
                                        new PrimaryKeyParser().parse(schemaConfig)));

        schemaConfig
                .getOptional(TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEYS)
                .ifPresent(
                        constraintConfig ->
                                tableSchemaBuilder.constraintKey(
                                        new ConstraintKeyParser().parse(schemaConfig)));

        // todo: validate schema
        return tableSchemaBuilder.build();
    }

    public static class FieldParser {

        public List<Column> parse(ReadonlyConfig schemaConfig) {
            JsonNode jsonNode;
            if (schemaConfig.getOptional(TableSchemaOptions.FieldOptions.FIELDS).isPresent()) {
                jsonNode =
                        JsonUtils.toJsonNode(
                                schemaConfig.get(TableSchemaOptions.FieldOptions.FIELDS));
            } else if (schemaConfig
                    .getOptional(TableSchemaOptions.FieldOptions.SCHEMA_FIELDS)
                    .isPresent()) {
                jsonNode =
                        JsonUtils.toJsonNode(
                                schemaConfig.get(TableSchemaOptions.FieldOptions.SCHEMA_FIELDS));
            } else {
                throw new IllegalArgumentException(
                        "Schema config need option [fields], please correct your config first");
            }
            Map<String, String> fieldsMap = JsonUtils.toStringMap(jsonNode);
            int fieldsNum = fieldsMap.size();
            List<Column> columns = new ArrayList<>(fieldsNum);
            for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                SeaTunnelDataType<?> dataType =
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(value);
                PhysicalColumn column = PhysicalColumn.of(key, dataType, 0, true, null, null);
                columns.add(column);
            }
            return columns;
        }
    }

    public static class ColumnParser {

        /**
         * Parse columns from columns config.
         *
         * <pre>
         *     columns = [
         *      {
         *          name = "name"
         *          type = "string"
         *          columnLength = 0
         *          nullable = true
         *          defaultValue = null
         *          comment = "name"
         *     },
         *     {
         *          name = "age"
         *          type = "int"
         *          columnLength = 0
         *          nullable = true
         *          defaultValue = null
         *          comment = "age"
         *     }
         *     ]
         * </pre>
         *
         * @param schemaConfig columns config
         * @return columns
         */
        public List<Column> parse(ReadonlyConfig schemaConfig) {
            return schemaConfig.get(TableSchemaOptions.ColumnOptions.COLUMNS).stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(
                            columnConfig -> {
                                String name =
                                        columnConfig
                                                .getOptional(TableSchemaOptions.ColumnOptions.NAME)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.columns.* config need option [name], please correct your config first"));
                                SeaTunnelDataType<?> seaTunnelDataType =
                                        columnConfig
                                                .getOptional(TableSchemaOptions.ColumnOptions.TYPE)
                                                .map(
                                                        SeaTunnelDataTypeConvertorUtil
                                                                ::deserializeSeaTunnelDataType)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.columns.* config need option [type], please correct your config first"));

                                Integer columnLength =
                                        columnConfig.get(
                                                TableSchemaOptions.ColumnOptions.COLUMN_LENGTH);
                                Boolean nullable =
                                        columnConfig.get(TableSchemaOptions.ColumnOptions.NULLABLE);
                                Object defaultValue =
                                        columnConfig.get(
                                                TableSchemaOptions.ColumnOptions.DEFAULT_VALUE);
                                String comment =
                                        columnConfig.get(TableSchemaOptions.ColumnOptions.COMMENT);
                                return PhysicalColumn.of(
                                        name,
                                        seaTunnelDataType,
                                        columnLength,
                                        nullable,
                                        defaultValue,
                                        comment);
                            })
                    .collect(Collectors.toList());
        }
    }

    public static class ConstraintKeyParser {

        public List<ConstraintKey> parse(ReadonlyConfig schemaConfig) {
            return schemaConfig.get(TableSchemaOptions.ConstraintKeyOptions.CONSTRAINT_KEYS)
                    .stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(
                            constraintKeyConfig -> {
                                String constraintName =
                                        constraintKeyConfig
                                                .getOptional(
                                                        TableSchemaOptions.ConstraintKeyOptions
                                                                .CONSTRAINT_KEY_NAME)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.constraintKeys.* config need option [constraintName], please correct your config first"));
                                ConstraintKey.ConstraintType constraintType =
                                        constraintKeyConfig
                                                .getOptional(
                                                        TableSchemaOptions.ConstraintKeyOptions
                                                                .CONSTRAINT_KEY_TYPE)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.constraintKeys.* config need option [constraintType], please correct your config first"));
                                List<ConstraintKey.ConstraintKeyColumn> columns =
                                        constraintKeyConfig
                                                .getOptional(
                                                        TableSchemaOptions.ConstraintKeyOptions
                                                                .CONSTRAINT_KEY_COLUMNS)
                                                .map(
                                                        constraintColumnMapList -> {
                                                            return constraintColumnMapList.stream()
                                                                    .map(ReadonlyConfig::fromMap)
                                                                    .map(
                                                                            constraintColumnConfig -> {
                                                                                String columnName =
                                                                                        constraintColumnConfig
                                                                                                .getOptional(
                                                                                                        TableSchemaOptions
                                                                                                                .ConstraintKeyOptions
                                                                                                                .CONSTRAINT_KEY_COLUMN_NAME)
                                                                                                .orElseThrow(
                                                                                                        () ->
                                                                                                                new IllegalArgumentException(
                                                                                                                        "schema.constraintKeys.constraintColumns.* config need option [columnName], please correct your config first"));
                                                                                ConstraintKey
                                                                                                .ColumnSortType
                                                                                        columnSortType =
                                                                                                constraintColumnConfig
                                                                                                        .get(
                                                                                                                TableSchemaOptions
                                                                                                                        .ConstraintKeyOptions
                                                                                                                        .CONSTRAINT_KEY_COLUMN_SORT_TYPE);
                                                                                return ConstraintKey
                                                                                        .ConstraintKeyColumn
                                                                                        .of(
                                                                                                columnName,
                                                                                                columnSortType);
                                                                            })
                                                                    .collect(Collectors.toList());
                                                        })
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.constraintKeys.* config need option [columns], please correct your config first"));
                                return ConstraintKey.of(constraintType, constraintName, columns);
                            })
                    .collect(Collectors.toList());
        }
    }

    public static class PrimaryKeyParser {

        /**
         * Parse primary key from primary key config.
         *
         * <pre>
         *     primaryKey {
         *          name = "primary_key"
         *          columnNames = ["name", "age"]
         *     }
         * </pre>
         *
         * @param schemaConfig schema config
         * @return primary key
         */
        public PrimaryKey parse(ReadonlyConfig schemaConfig) {
            ReadonlyConfig primaryKeyConfig =
                    ReadonlyConfig.fromMap(
                            schemaConfig.get(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY));
            String primaryKeyName =
                    primaryKeyConfig
                            .getOptional(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY_NAME)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Schema config need option [primaryKey.name], please correct your config first"));
            List<String> columns =
                    primaryKeyConfig
                            .getOptional(TableSchemaOptions.PrimaryKeyOptions.PRIMARY_KEY_COLUMNS)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Schema config need option [primaryKey.columnNames], please correct your config first"));
            return new PrimaryKey(primaryKeyName, columns);
        }
    }
}
