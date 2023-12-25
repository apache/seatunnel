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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.api.sink.SaveModeConstants;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DorisCatalogUtil {

    public static final String ALL_DATABASES_QUERY =
            "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE CATALOG_NAME = 'internal' ORDER BY SCHEMA_NAME";

    public static final String DATABASE_QUERY =
            "SELECT SCHEMA_NAME FROM information_schema.schemata "
                    + "WHERE CATALOG_NAME = 'internal' AND SCHEMA_NAME = ? "
                    + "ORDER BY SCHEMA_NAME";

    public static final String TABLES_QUERY_WITH_DATABASE_QUERY =
            "SELECT TABLE_NAME FROM information_schema.tables "
                    + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? "
                    + "ORDER BY TABLE_NAME";

    public static final String TABLES_QUERY_WITH_IDENTIFIER_QUERY =
            "SELECT TABLE_NAME FROM information_schema.tables "
                    + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "ORDER BY TABLE_NAME";

    public static final String TABLE_SCHEMA_QUERY =
            "SELECT COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,COLUMN_TYPE,COLUMN_SIZE,"
                    + "COLUMN_KEY,NUMERIC_PRECISION,NUMERIC_SCALE,COLUMN_COMMENT "
                    + "FROM information_schema.columns "
                    + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                    + "ORDER BY ORDINAL_POSITION";

    public static String randomFrontEndHost(String[] frontEndNodes) {
        if (frontEndNodes.length == 1) {
            return frontEndNodes[0].split(":")[0];
        }
        List<String> list = Arrays.asList(frontEndNodes);
        Collections.shuffle(list);
        return list.get(0).split(":")[0];
    }

    public static String getJdbcUrl(String host, Integer port, String database) {
        return String.format("jdbc:mysql://%s:%d/%s", host, port, database);
    }

    public static String getCreateDatabaseQuery(String database, boolean ignoreIfExists) {
        return "CREATE DATABASE " + (ignoreIfExists ? "IF NOT EXISTS " : "") + database;
    }

    public static String getDropDatabaseQuery(String database, boolean ignoreIfNotExists) {
        return "DROP DATABASE " + (ignoreIfNotExists ? "IF EXISTS " : "") + database;
    }

    public static String getDropTableQuery(TablePath tablePath, boolean ignoreIfNotExists) {
        return "DROP TABLE " + (ignoreIfNotExists ? "IF EXISTS " : "") + tablePath.getFullName();
    }

    /**
     * @param createTableTemplate create table template
     * @param catalogTable catalog table
     * @return create table stmt
     */
    public static String getCreateTableStatement(
            String createTableTemplate, TablePath tablePath, CatalogTable catalogTable) {

        String template = createTableTemplate;
        TableSchema tableSchema = catalogTable.getTableSchema();

        String primaryKey = "";
        if (tableSchema.getPrimaryKey() != null) {
            primaryKey =
                    tableSchema.getPrimaryKey().getColumnNames().stream()
                            .map(r -> "`" + r + "`")
                            .collect(Collectors.joining(","));
        }
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_PRIMARY_KEY),
                        primaryKey);
        Map<String, CreateTableParser.ColumnInfo> columnInTemplate =
                CreateTableParser.getColumnList(template);
        template = mergeColumnInTemplate(columnInTemplate, tableSchema, template);

        String rowTypeFields =
                tableSchema.getColumns().stream()
                        .filter(column -> !columnInTemplate.containsKey(column.getName()))
                        .map(
                                column ->
                                        String.format(
                                                "`%s` %s %s ",
                                                column.getName(),
                                                fromSeaTunnelType(
                                                        column.getDataType(),
                                                        column.getColumnLength()),
                                                column.isNullable() ? "NULL" : "NOT NULL"))
                        .collect(Collectors.joining(",\n"));
        return template.replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.DATABASE),
                        tablePath.getDatabaseName())
                .replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.TABLE_NAME),
                        tablePath.getTableName())
                .replaceAll(
                        String.format("\\$\\{%s\\}", SaveModeConstants.ROWTYPE_FIELDS),
                        rowTypeFields);
    }

    private static String mergeColumnInTemplate(
            Map<String, CreateTableParser.ColumnInfo> columnInTemplate,
            TableSchema tableSchema,
            String template) {
        int offset = 0;
        Map<String, Column> columnMap =
                tableSchema.getColumns().stream()
                        .collect(Collectors.toMap(Column::getName, Function.identity()));
        for (String col : columnInTemplate.keySet()) {
            CreateTableParser.ColumnInfo columnInfo = columnInTemplate.get(col);
            if (StringUtils.isEmpty(columnInfo.getInfo())) {
                if (columnMap.containsKey(col)) {
                    Column column = columnMap.get(col);
                    String newCol =
                            String.format(
                                    "`%s` %s %s ",
                                    column.getName(),
                                    fromSeaTunnelType(
                                            column.getDataType(), column.getColumnLength()),
                                    column.isNullable() ? "NULL" : "NOT NULL");
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

    public static SeaTunnelDataType<?> fromDorisType(ResultSet rs) throws SQLException {

        String type = rs.getString(5).toUpperCase();
        //        int idx = type.indexOf("(");
        //        int idx2 = type.indexOf("<");
        //        if (idx != -1) {
        //            type = type.substring(0, idx);
        //        }
        //        if (idx2 != -1) {
        //            type = type.substring(0, idx2);
        //        }
        //
        //        switch (type) {
        //            case "NULL_TYPE":
        //                return BasicType.VOID_TYPE;
        //            case "BOOLEAN":
        //                return BasicType.BOOLEAN_TYPE;
        //            case "TINYINT":
        //            case "SMALLINT":
        //                return BasicType.SHORT_TYPE;
        //            case "INT":
        //                return BasicType.INT_TYPE;
        //            case "BIGINT":
        //                return BasicType.LONG_TYPE;
        //            case "FLOAT":
        //                return BasicType.FLOAT_TYPE;
        //            case "DOUBLE":
        //                return BasicType.DOUBLE_TYPE;
        //            case "DATE":
        //            case "DATEV2":
        //                return LocalTimeType.LOCAL_DATE_TYPE;
        //            case "DATETIME":
        //            case "DATETIMEV2":
        //            case "DATETIMEV3":
        //                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
        //            case "DECIMAL":
        //            case "DECIMALV2":
        //            case "DECIMALV3":
        //                int precision = rs.getInt(8);
        //                int scale = rs.getInt(9);
        //                return new DecimalType(precision, scale);
        //            case "TIME":
        //                return LocalTimeType.LOCAL_TIME_TYPE;
        //            case "CHAR":
        //            case "LARGEINT":
        //            case "VARCHAR":
        //            case "JSONB":
        //            case "STRING":
        //            case "ARRAY":
        //            case "STRUCT":
        //                return BasicType.STRING_TYPE;
        //            default:
        //                throw new CatalogException(String.format("Unsupported doris type: %s",
        // type));
        //        }
        return mapping(type);
    }

    private static String fromSeaTunnelType(SeaTunnelDataType<?> dataType, Integer columnLength) {

        switch (dataType.getSqlType()) {
            case STRING:
                if (columnLength != null && columnLength > 65533) {
                    return "STRING";
                }
                return String.format("VARCHAR(%d)", columnLength);
            case NULL:
                return "NULL_TYPE";
            case BOOLEAN:
                return "BOOLEAN";
            case SMALLINT:
                return String.format("SMALLINT(%d)", columnLength + 1);
            case INT:
                return String.format("INT(%d)", columnLength + 1);
            case BIGINT:
                return String.format("BIGINT(%d)", columnLength + 1);
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return String.format(
                        "DECIMALV3(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            case TIME:
                return "TIME";
            case DATE:
                return "DATEV2";
            case TIMESTAMP:
                return "DATETIME";
            case ROW:
                return "JSONB";
            default:
                throw new CatalogException(String.format("Unsupported doris type: %s", dataType));
        }
    }

    public static SeaTunnelDataType<?> mapping(String dorisType) {
        int idx = dorisType.indexOf("(");
        int idx2 = dorisType.indexOf("<");
        String matchCaseType = dorisType;
        if (idx != -1) {
            matchCaseType = dorisType.substring(0, idx);
        }
        if (idx2 != -1) {
            matchCaseType = dorisType.substring(0, idx2);
        }

        switch (matchCaseType) {
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "SMALLINT":
                return BasicType.SHORT_TYPE;
            case "INT":
                return BasicType.INT_TYPE;
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "FLOAT":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "DATE":
            case "DATEV2":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "DATETIME":
            case "DATETIMEV2":
            case "DATETIMEV3":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMALV3":
                int start = dorisType.indexOf("(");
                int end = dorisType.indexOf(")");
                String percScale = dorisType.substring(start + 1, end);
                String precision = percScale.split(",")[0].trim();
                String scale = percScale.split(",")[1].trim();
                return new DecimalType(Integer.parseInt(precision), Integer.parseInt(scale));
            case "TIME":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "CHAR":
            case "LARGEINT":
            case "TEXT":
            case "VARCHAR":
            case "JSONB":
            case "STRING":
            case "STRUCT":
                return BasicType.STRING_TYPE;
            case "ARRAY":
                return getArrayType(dorisType);
            case "MAP":
                //                String[] types = extractTypes(dorisType);
                //                String keyType = types[0];
                //                String valueType = types[1];
                //                return new MapType<>(mapping(keyType),mapping(valueType));
                return new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
            default:
                throw new CatalogException(
                        String.format("Unsupported doris type: %s", matchCaseType));
        }
    }

    public static String[] extractTypes(String columnType) {
        String[] types = new String[2];

        Pattern pattern = Pattern.compile("MAP<([^,]+),(.+?)>");
        Matcher matcher = pattern.matcher(columnType);

        if (matcher.matches()) {
            types[0] = matcher.group(1).trim();
            types[1] = matcher.group(2).trim();
        }

        return types;
    }

    private static ArrayType getArrayType(String columType) {
        Pattern pattern = Pattern.compile("ARRAY<([^<>]+)>");
        Matcher matcher = pattern.matcher(columType);
        String type = null;
        if (matcher.find()) {
            String typeWithArray = matcher.group(1).trim();
            Pattern elementTypePattern = Pattern.compile("\\b([^()]+)\\b");
            Matcher elementTypeMatcher = elementTypePattern.matcher(typeWithArray);

            if (elementTypeMatcher.find()) {
                type = elementTypeMatcher.group(1).trim();
            }
        }
        switch (type) {
            case "STRING":
            case "DECIMAL":
            case "LARGEINT":
                return ArrayType.STRING_ARRAY_TYPE;
            case "BIGINT":
                return ArrayType.LONG_ARRAY_TYPE;
            case "INT":
                return ArrayType.INT_ARRAY_TYPE;
            case "TINYINT":
            case "SMALLINT":
            case "BOOLEAN":
                return ArrayType.SHORT_ARRAY_TYPE;
            case "DOUBLE":
                return ArrayType.DOUBLE_ARRAY_TYPE;
            case "FLOAT":
                return ArrayType.FLOAT_ARRAY_TYPE;
            default:
                return ArrayType.STRING_ARRAY_TYPE;
        }
    }
}
