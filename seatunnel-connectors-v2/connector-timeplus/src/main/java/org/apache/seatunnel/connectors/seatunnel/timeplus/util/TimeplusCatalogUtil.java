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

package org.apache.seatunnel.connectors.seatunnel.timeplus.util;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.common.sql.template.SqlTemplate;
import org.apache.seatunnel.connectors.seatunnel.timeplus.config.TimeplusConfig;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class TimeplusCatalogUtil {

    public static final String ALL_DATABASES_QUERY =
            "SELECT name FROM system.databases ORDER BY name";

    public static final String DATABASE_QUERY =
            "SELECT name FROM system.databases " + "WHERE name = ? " + "ORDER BY name";

    public static final String TABLES_QUERY_WITH_DATABASE_QUERY =
            "SELECT name FROM system.tables " + "WHERE database = ? " + "ORDER BY name";

    /*
    There is a bug in Proton 1.5.15 for the view def of information_schema.tables
    SELECT database AS table_catalog, database AS table_schema, name AS table_name,
    multi_if(is_temporary, 4, engine LIKE '%View', 2, engine LIKE 'System%', 5, has_own_data = 0, 3, 1) AS table_type
    FROM system.tables
     */
    public static final String TABLES_QUERY_WITH_IDENTIFIER_QUERY =
            "SELECT name as TABLE_NAME FROM system.tables "
                    + "WHERE database = ? AND name = ? "
                    + "ORDER BY name";

    public static final String TABLE_SCHEMA_QUERY =
            "SELECT * "
                    + "FROM information_schema.columns "
                    + "WHERE TABLE_CATALOG = 'default' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "ORDER BY ORDINAL_POSITION";

    public static final String QUERY_TIMEPLUS_VERSION_QUERY = "SELECT version();";

    public static String randomFrontEndHost(String[] frontEndNodes) {
        if (frontEndNodes.length == 1) {
            return frontEndNodes[0].split(":")[0];
        }
        List<String> list = Arrays.asList(frontEndNodes);
        Collections.shuffle(list);
        return list.get(0).split(":")[0];
    }

    public static String getJdbcUrl(String host, String database) {
        return String.format("jdbc:proton://%s/%s", host, database);
    }

    public static String getCreateDatabaseQuery(String database, boolean ignoreIfExists) {
        return "CREATE DATABASE " + (ignoreIfExists ? "IF NOT EXISTS " : "") + database;
    }

    public static String getDropDatabaseQuery(String database, boolean ignoreIfNotExists) {
        return "DROP DATABASE " + (ignoreIfNotExists ? "IF EXISTS " : "") + database;
    }

    public static String getDropTableQuery(TablePath tablePath, boolean ignoreIfNotExists) {
        return "DROP STREAM " + (ignoreIfNotExists ? "IF EXISTS " : "") + tablePath.getFullName();
    }

    public static String getTruncateTableQuery(TablePath tablePath) {
        return "TRUNCATE STREAM " + tablePath.getFullName();
    }

    /**
     * @param createTableTemplate create table template
     * @param catalogTable catalog table
     * @param typeConverter
     * @return create table stmt
     */
    public static String getCreateTableStatement(
            String createTableTemplate,
            TablePath tablePath,
            CatalogTable catalogTable,
            TypeConverter<BasicTypeDefine> typeConverter) {

        String template = createTableTemplate;
        TableSchema tableSchema = catalogTable.getTableSchema();

        String primaryKey = "";
        if (tableSchema.getPrimaryKey() != null) {
            primaryKey =
                    tableSchema.getPrimaryKey().getColumnNames().stream()
                            .map(r -> "`" + r + "`")
                            .collect(Collectors.joining(","));
        }
        String uniqueKey = "";
        if (!tableSchema.getConstraintKeys().isEmpty()) {
            uniqueKey =
                    tableSchema.getConstraintKeys().stream()
                            .flatMap(c -> c.getColumnNames().stream())
                            .map(r -> "`" + r.getColumnName() + "`")
                            .collect(Collectors.joining(","));
        }

        // dup key
        String dupKey = "";
        if (catalogTable.getOptions() != null
                && StringUtils.isNotBlank(
                        catalogTable
                                .getOptions()
                                .get(
                                        SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY
                                                .getPlaceHolderKey()))) {
            String dupKeyColumns =
                    catalogTable
                            .getOptions()
                            .get(SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getPlaceHolderKey());
            dupKey =
                    Arrays.stream(dupKeyColumns.split(","))
                            .map(r -> "`" + r + "`")
                            .collect(Collectors.joining(","));
        }

        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getPlaceHolder(),
                primaryKey,
                tablePath.getFullName(),
                TimeplusConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getReplacePlaceHolder(),
                        primaryKey);
        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getPlaceHolder(),
                uniqueKey,
                tablePath.getFullName(),
                TimeplusConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getReplacePlaceHolder(), uniqueKey);
        SqlTemplate.canHandledByTemplateWithPlaceholder(
                template,
                SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getPlaceHolder(),
                dupKey,
                tablePath.getFullName(),
                TimeplusConfig.SAVE_MODE_CREATE_TEMPLATE.key());
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_DUPLICATE_KEY.getReplacePlaceHolder(), dupKey);
        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template, typeConverter);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(x -> TimeplusCatalogUtil.columnToTimeplusType(x, typeConverter))
                        .collect(Collectors.joining(",\n"));
        return template.replaceAll(
                        SaveModePlaceHolder.DATABASE.getReplacePlaceHolder(),
                        tablePath.getDatabaseName())
                .replaceAll(
                        SaveModePlaceHolder.TABLE_NAME.getReplacePlaceHolder(),
                        tablePath.getTableName())
                .replaceAll(
                        SaveModePlaceHolder.ROWTYPE_FIELDS.getReplacePlaceHolder(), rowTypeFields);
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template,
            TypeConverter<BasicTypeDefine> typeConverter) {
        int offset = 0;
        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        List<CreateTableParser.ColumnInfo> columnInfosInSeq =
                columnInTemplate.values().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        CreateTableParser.ColumnInfo::getStartIndex))
                        .collect(Collectors.toList());
        for (CreateTableParser.ColumnInfo columnInfo : columnInfosInSeq) {
            String col = columnInfo.getName();
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol = columnToTimeplusType(column, typeConverter);
                    String prefix = template.substring(0, columnInfo.getStartIndex() + offset);
                    String suffix = template.substring(offset + columnInfo.getEndIndex());
                    if (prefix.endsWith("`")) {
                        prefix = prefix.substring(0, prefix.length() - 1);
                        offset--;
                    }
                    if (suffix.startsWith("`")) {
                        suffix = suffix.substring(1);
                        offset--;
                    }
                    template = prefix + newCol + suffix;
                    offset += newCol.length() - columnInfo.getName().length();
                } else {
                    throw new IllegalArgumentException("Can't find column " + col + " in table.");
                }
            }
        }
        return template;
    }

    private static String columnToTimeplusType(
            Column column, TypeConverter<BasicTypeDefine> typeConverter) {
        checkNotNull(column, "The column is required.");
        if (column.isNullable()) {
            return String.format(
                    "`%s` nullable(%s)",
                    column.getName(), typeConverter.reconvert(column).getColumnType());
        } else {
            return String.format(
                    "`%s` %s", column.getName(), typeConverter.reconvert(column).getColumnType());
        }
    }
}
