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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.datatype.MaxComputeTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.CreateTableParser;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.type.TypeInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class MaxComputeCatalogUtil {

    public static String getDropTableQuery(TablePath tablePath, boolean ignoreIfNotExists) {
        return "DROP TABLE "
                + (ignoreIfNotExists ? "IF EXISTS " : "")
                + tablePath.getFullName()
                + ";";
    }

    /**
     * @param createTableTemplate create table template
     * @param catalogTable catalog table
     * @return create table stmt
     */
    public static String getCreateTableStatement(
            String createTableTemplate, TablePath tablePath, CatalogTable catalogTable) {

        String template = createTableTemplate;
        if (!createTableTemplate.trim().endsWith(";")) {
            template += ";";
        }
        TableSchema tableSchema = catalogTable.getTableSchema();

        String primaryKey = "";
        if (tableSchema.getPrimaryKey() != null) {
            List<String> fields = Arrays.asList(catalogTable.getTableSchema().getFieldNames());
            List<String> keys = tableSchema.getPrimaryKey().getColumnNames();
            keys.sort(Comparator.comparingInt(fields::indexOf));
            primaryKey = keys.stream().map(r -> "`" + r + "`").collect(Collectors.joining(","));
        }
        String uniqueKey = "";
        if (!tableSchema.getConstraintKeys().isEmpty()) {
            uniqueKey =
                    tableSchema.getConstraintKeys().stream()
                            .flatMap(c -> c.getColumnNames().stream())
                            .map(r -> "`" + r.getColumnName() + "`")
                            .collect(Collectors.joining(","));
        }

        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_PRIMARY_KEY.getReplacePlaceHolder(),
                        primaryKey);
        template =
                template.replaceAll(
                        SaveModePlaceHolder.ROWTYPE_UNIQUE_KEY.getReplacePlaceHolder(), uniqueKey);

        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(
                                x ->
                                        MaxComputeCatalogUtil.columnToMaxComputeType(
                                                x, MaxComputeTypeConverter.INSTANCE))
                        .collect(Collectors.joining(",\n"));

        return template.replaceAll(
                        SaveModePlaceHolder.DATABASE.getReplacePlaceHolder(),
                        tablePath.getDatabaseName())
                .replaceAll(
                        SaveModePlaceHolder.TABLE.getReplacePlaceHolder(), tablePath.getTableName())
                .replaceAll(
                        SaveModePlaceHolder.ROWTYPE_FIELDS.getReplacePlaceHolder(), rowTypeFields)
                .replaceAll(
                        SaveModePlaceHolder.COMMENT.getReplacePlaceHolder(),
                        Objects.isNull(catalogTable.getComment()) ? "" : catalogTable.getComment());
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template) {
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
                    String newCol =
                            columnToMaxComputeType(column, MaxComputeTypeConverter.INSTANCE);
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

    public static String columnToMaxComputeType(
            Column column, TypeConverter<BasicTypeDefine<TypeInfo>> typeConverter) {
        checkNotNull(column, "The column is required.");
        return String.format(
                "`%s` %s %s %s",
                column.getName(),
                typeConverter.reconvert(column).getColumnType(),
                column.isNullable() ? "NULL" : "NOT NULL",
                StringUtils.isEmpty(column.getComment())
                        ? ""
                        : "COMMENT '" + column.getComment() + "'");
    }
}
