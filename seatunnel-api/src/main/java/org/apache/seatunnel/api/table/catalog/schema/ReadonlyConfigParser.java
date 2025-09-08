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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JsonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReadonlyConfigParser implements TableSchemaParser<ReadonlyConfig> {

    private final TableSchemaParser.ColumnParser<ReadonlyConfig> columnParser = new ColumnParser();
    private final TableSchemaParser.FieldParser<ReadonlyConfig> fieldParser = new FieldParser();
    private final TableSchemaParser.ConstraintKeyParser<ReadonlyConfig> constraintKeyParser =
            new ConstraintKeyParser();
    private final TableSchemaParser.PrimaryKeyParser<ReadonlyConfig> primaryKeyParser =
            new PrimaryKeyParser();

    @Override
    public TableSchema parse(ReadonlyConfig readonlyConfig) {
        ReadonlyConfig schemaConfig =
                readonlyConfig
                        .getOptional(ConnectorCommonOptions.SCHEMA)
                        .map(ReadonlyConfig::fromMap)
                        .orElseThrow(
                                () -> new IllegalArgumentException("Schema config can't be null"));

        if (readonlyConfig.getOptional(ConnectorCommonOptions.FIELDS).isPresent()
                && schemaConfig.getOptional(ConnectorCommonOptions.COLUMNS).isPresent()) {
            throw new IllegalArgumentException(
                    "Schema config can't contains both [fields] and [columns], please correct your config first");
        }
        TableSchema.Builder tableSchemaBuilder = TableSchema.builder();
        if (readonlyConfig.getOptional(ConnectorCommonOptions.FIELDS).isPresent()) {
            // we use readonlyConfig here to avoid flatten, this is used to solve the t.x.x as field
            // key
            tableSchemaBuilder.columns(fieldParser.parse(readonlyConfig));
        }

        if (schemaConfig.getOptional(ConnectorCommonOptions.COLUMNS).isPresent()) {
            tableSchemaBuilder.columns(columnParser.parse(schemaConfig));
        }
        if (schemaConfig.getOptional(ConnectorCommonOptions.PRIMARY_KEY).isPresent()) {
            tableSchemaBuilder.primaryKey(primaryKeyParser.parse(schemaConfig));
        }
        if (schemaConfig.getOptional(ConnectorCommonOptions.CONSTRAINT_KEYS).isPresent()) {
            tableSchemaBuilder.constraintKey(constraintKeyParser.parse(schemaConfig));
        }
        // todo: validate schema
        return tableSchemaBuilder.build();
    }

    private static class FieldParser implements TableSchemaParser.FieldParser<ReadonlyConfig> {

        @Override
        public List<Column> parse(ReadonlyConfig schemaConfig) {
            JsonNode jsonNode =
                    JsonUtils.toJsonNode(schemaConfig.get(ConnectorCommonOptions.FIELDS));
            Map<String, String> fieldsMap = JsonUtils.toStringMap(jsonNode);
            int fieldsNum = fieldsMap.size();
            List<Column> columns = new ArrayList<>(fieldsNum);
            for (Map.Entry<String, String> entry : fieldsMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                SeaTunnelDataType<?> dataType =
                        SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(key, value);
                PhysicalColumn column =
                        PhysicalColumn.of(key, dataType, null, null, true, null, null);
                columns.add(column);
            }
            return columns;
        }
    }

    private static class ColumnParser implements TableSchemaParser.ColumnParser<ReadonlyConfig> {

        @Override
        public List<Column> parse(ReadonlyConfig schemaConfig) {
            return schemaConfig.get(ConnectorCommonOptions.COLUMNS).stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(
                            columnConfig -> {
                                String name =
                                        columnConfig
                                                .getOptional(ConnectorCommonOptions.COLUMN_NAME)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.columns.* config need option [name], please correct your config first"));
                                SeaTunnelDataType<?> seaTunnelDataType =
                                        columnConfig
                                                .getOptional(ConnectorCommonOptions.TYPE)
                                                .map(
                                                        column ->
                                                                SeaTunnelDataTypeConvertorUtil
                                                                        .deserializeSeaTunnelDataType(
                                                                                name, column))
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.columns.* config need option [type], please correct your config first"));

                                Long columnLength =
                                        columnConfig.get(ConnectorCommonOptions.COLUMN_LENGTH);
                                Integer columnScale =
                                        columnConfig.get(ConnectorCommonOptions.COLUMN_SCALE);
                                Boolean nullable =
                                        columnConfig.get(ConnectorCommonOptions.NULLABLE);
                                Object defaultValue =
                                        columnConfig.get(ConnectorCommonOptions.DEFAULT_VALUE);
                                String comment =
                                        columnConfig.get(ConnectorCommonOptions.COLUMN_COMMENT);
                                return PhysicalColumn.of(
                                        name,
                                        seaTunnelDataType,
                                        columnLength,
                                        columnScale,
                                        nullable,
                                        defaultValue,
                                        comment);
                            })
                    .collect(Collectors.toList());
        }
    }

    private static class ConstraintKeyParser
            implements TableSchemaParser.ConstraintKeyParser<ReadonlyConfig> {

        @Override
        public List<ConstraintKey> parse(ReadonlyConfig schemaConfig) {
            return schemaConfig.get(ConnectorCommonOptions.CONSTRAINT_KEYS).stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(
                            constraintKeyConfig -> {
                                String constraintName =
                                        constraintKeyConfig
                                                .getOptional(
                                                        ConnectorCommonOptions.CONSTRAINT_KEY_NAME)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.constraintKeys.* config need option [constraintName], please correct your config first"));
                                ConstraintKey.ConstraintType constraintType =
                                        constraintKeyConfig
                                                .getOptional(
                                                        ConnectorCommonOptions.CONSTRAINT_KEY_TYPE)
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.constraintKeys.* config need option [constraintType], please correct your config first"));
                                List<ConstraintKey.ConstraintKeyColumn> columns =
                                        constraintKeyConfig
                                                .getOptional(
                                                        ConnectorCommonOptions
                                                                .CONSTRAINT_KEY_COLUMNS)
                                                .map(
                                                        constraintColumnMapList ->
                                                                constraintColumnMapList.stream()
                                                                        .map(
                                                                                ReadonlyConfig
                                                                                        ::fromMap)
                                                                        .map(
                                                                                constraintColumnConfig -> {
                                                                                    String
                                                                                            columnName =
                                                                                                    constraintColumnConfig
                                                                                                            .getOptional(
                                                                                                                    ConnectorCommonOptions
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
                                                                                                                    ConnectorCommonOptions
                                                                                                                            .CONSTRAINT_KEY_COLUMN_SORT_TYPE);
                                                                                    return ConstraintKey
                                                                                            .ConstraintKeyColumn
                                                                                            .of(
                                                                                                    columnName,
                                                                                                    columnSortType);
                                                                                })
                                                                        .collect(
                                                                                Collectors
                                                                                        .toList()))
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalArgumentException(
                                                                        "schema.constraintKeys.* config need option [columns], please correct your config first"));
                                return ConstraintKey.of(constraintType, constraintName, columns);
                            })
                    .collect(Collectors.toList());
        }
    }

    private static class PrimaryKeyParser
            implements TableSchemaParser.PrimaryKeyParser<ReadonlyConfig> {

        @Override
        public PrimaryKey parse(ReadonlyConfig schemaConfig) {
            ReadonlyConfig primaryKeyConfig =
                    ReadonlyConfig.fromMap(schemaConfig.get(ConnectorCommonOptions.PRIMARY_KEY));
            String primaryKeyName =
                    primaryKeyConfig
                            .getOptional(ConnectorCommonOptions.PRIMARY_KEY_NAME)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Schema config need option [primaryKey.name], please correct your config first"));
            List<String> columns =
                    primaryKeyConfig
                            .getOptional(ConnectorCommonOptions.PRIMARY_KEY_COLUMNS)
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Schema config need option [primaryKey.columnNames], please correct your config first"));
            return new PrimaryKey(primaryKeyName, columns);
        }
    }
}
