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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DorisCatalogUtil {

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

    public static String getAllDatabasesQuery() {
        return "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE CATALOG_NAME = 'internal' ORDER BY SCHEMA_NAME";
    }

    public static String getDatabaseQuery() {
        return "SELECT SCHEMA_NAME FROM information_schema.schemata "
                + "WHERE CATALOG_NAME = 'internal' AND SCHEMA_NAME = ? "
                + "ORDER BY SCHEMA_NAME";
    }

    public static String getTablesQueryWithDatabase() {
        return "SELECT TABLE_NAME FROM information_schema.tables "
                + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? "
                + "ORDER BY TABLE_NAME";
    }

    public static String getTablesQueryWithIdentifier() {
        return "SELECT TABLE_NAME FROM information_schema.tables "
                + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                + "ORDER BY TABLE_NAME";
    }

    public static String getTableSchemaQuery() {
        return "SELECT COLUMN_NAME,ORDINAL_POSITION,COLUMN_DEFAULT,IS_NULLABLE,COLUMN_TYPE,COLUMN_SIZE,"
                + "COLUMN_KEY,NUMERIC_PRECISION,NUMERIC_SCALE,COLUMN_COMMENT "
                + "FROM information_schema.columns "
                + "WHERE TABLE_CATALOG = 'internal' AND TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                + "ORDER BY ORDINAL_POSITION";
    }

    public static String getCreateDatabaseQuery(String database, boolean ignoreIfExists) {
        return "CREATE DATABASE " + (ignoreIfExists ? "IF NOT EXISTS " : "") + database;
    }

    public static String getDropDatabaseQuery(String database, boolean ignoreIfNotExists) {
        return "DROP DATABASE " + (ignoreIfNotExists ? "IF EXISTS " : "") + database;
    }

    /**
     * CREATE TABLE ${table_identifier} ( ${column_definition} ) ENGINE = ${engine_type} UNIQUE KEY
     * (${key_columns}) COMMENT ${table_comment} ${partition_info} DISTRIBUTED BY HASH
     * (${distribution_columns}) BUCKETS ${distribution_bucket} PROPERTIES ( ${properties} )
     *
     * @param createTableTemplate create table template
     * @param catalogTable catalog table
     * @param properties create table properties
     * @return create table stmt
     */
    public static String getCreateTableStatement(
            String createTableTemplate,
            TablePath tablePath,
            CatalogTable catalogTable,
            List<String> distributionCols,
            String distributionBucket,
            Map<String, String> properties) {

        String template = createTableTemplate;

        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();
        String tableIdentifier = "`" + databaseName + "`.`" + tableName + "`";
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "table_identifier"), tableIdentifier);

        TableSchema tableSchema = catalogTable.getTableSchema();

        List<Column> columns = tableSchema.getColumns();
        String columnDefinition = buildColumnDefinition(columns);
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "column_definition"), columnDefinition);

        template = template.replaceAll(String.format("\\$\\{%s\\}", "engine_type"), "OLAP");

        List<String> keys = tableSchema.getPrimaryKey().getColumnNames();
        Set<String> keySet = new HashSet<>(keys);
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "key_columns"),
                        keys.stream().map(k -> "`" + k + "`").collect(Collectors.joining(",")));

        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "table_comment"),
                        "\"" + catalogTable.getComment() + "\"");

        List<String> partitionKeys = catalogTable.getPartitionKeys();
        if (!keySet.containsAll(partitionKeys)) {
            throw new IllegalArgumentException("partition columns should all be key column");
        }
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "partition_info"),
                        partitionKeys.stream()
                                .map(k -> "`" + k + "`")
                                .collect(Collectors.joining(",")));

        if (!keySet.containsAll(distributionCols)) {
            throw new IllegalArgumentException("distribution columns should all be key column");
        }
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "distribution_columns"),
                        distributionCols.stream()
                                .map(col -> "`" + col + "`")
                                .collect(Collectors.joining(",")));
        if (!"AUTO".equalsIgnoreCase(distributionBucket)
                && Integer.parseInt(distributionBucket) < 1) {
            throw new IllegalArgumentException(
                    "distribution bucket num should be equals or greater than 1 or be auto");
        }
        template =
                template.replaceAll(
                        String.format("\\$\\{%s\\}", "distribution_bucket"),
                        distributionBucket.toUpperCase());

        String props = "";
        if (!properties.isEmpty()) {
            props =
                    properties.entrySet().stream()
                            .map(
                                    entry ->
                                            "\""
                                                    + entry.getKey()
                                                    + "\" = \""
                                                    + entry.getValue()
                                                    + "\"")
                            .collect(Collectors.joining(",\n"));
        }
        template = template.replaceAll(String.format("\\$\\{%s\\}", "properties"), props);

        template = template.replaceAll("\n\n", "\n");

        return template;
    }

    private static String buildColumnDefinition(List<Column> columns) {

        List<String> columnList = new ArrayList<>(columns.size());

        for (Column column : columns) {
            String name = column.getName();
            SeaTunnelDataType<?> dataType = column.getDataType();
            Integer columnLength = column.getColumnLength();
            String type = fromSeaTunnelType(dataType, columnLength);
            boolean nullable = column.isNullable();
            Object defaultValue = column.getDefaultValue();
            String comment = column.getComment();
            columnList.add(
                    String.format(
                            "`%s` %s%s%s%s",
                            name,
                            type,
                            nullable ? " NULL" : " NOT NULL",
                            nullable ? "" : " DEFAULT \"" + defaultValue + "\"",
                            " COMMENT \"" + comment + "\""));
        }

        return String.join(",\n", columnList);
    }

    public static SeaTunnelDataType<?> fromDorisType(ResultSet rs) throws SQLException {

        String type = rs.getString(5).toUpperCase();
        int idx = type.indexOf("(");
        if (idx != -1) {
            type = type.substring(0, idx);
        }

        switch (type) {
            case "NULL_TYPE":
                return BasicType.VOID_TYPE;
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
                int precision = rs.getInt(8);
                int scale = rs.getInt(9);
                return new DecimalType(precision, scale);
            case "TIME":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "CHAR":
            case "LARGEINT":
            case "VARCHAR":
            case "JSONB":
            case "STRING":
            case "ARRAY":
            case "MAP":
            case "STRUCT":
                return BasicType.STRING_TYPE;
            default:
                throw new CatalogException(String.format("Unsupported doris type: %s", type));
        }
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
}
