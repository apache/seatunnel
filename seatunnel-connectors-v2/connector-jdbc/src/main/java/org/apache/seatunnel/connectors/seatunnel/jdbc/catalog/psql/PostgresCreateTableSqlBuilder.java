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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_BYTEA;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresDataTypeConvertor.PG_NUMERIC;

public class PostgresCreateTableSqlBuilder {
    private List<Column> columns;
    private PrimaryKey primaryKey;
    private PostgresDataTypeConvertor postgresDataTypeConvertor;
    private String sourceCatalogName;

    public PostgresCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.postgresDataTypeConvertor = new PostgresDataTypeConvertor();
        this.sourceCatalogName = catalogTable.getCatalogName();
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE IF NOT EXISTS ")
                .append(tablePath.getSchemaAndTableName())
                .append(" (\n");

        List<String> columnSqls =
                columns.stream().map(this::buildColumnSql).collect(Collectors.toList());

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");

        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                columns ->
                                        buildColumnCommentSql(
                                                columns, tablePath.getSchemaAndTableName()))
                        .collect(Collectors.toList());

        if (!commentSqls.isEmpty()) {
            createTableSql.append("\n");
            createTableSql.append(String.join(";\n", commentSqls)).append(";");
        }

        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append(column.getName()).append(" ");

        // For simplicity, assume the column type in SeaTunnelDataType is the same as in PostgreSQL
        String columnType =
                sourceCatalogName.equals("postgres")
                        ? column.getSourceType()
                        : buildColumnType(column);
        columnSql.append(columnType);

        // Add NOT NULL if column is not nullable
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add primary key directly after the column if it is a primary key
        if (primaryKey != null && primaryKey.getColumnNames().contains(column.getName())) {
            columnSql.append(" PRIMARY KEY");
        }

        // Add default value if exists
        //        if (column.getDefaultValue() != null) {
        //            columnSql.append(" DEFAULT
        // '").append(column.getDefaultValue().toString()).append("'");
        //        }

        return columnSql.toString();
    }

    private String buildColumnType(Column column) {
        SqlType sqlType = column.getDataType().getSqlType();
        Long columnLength = column.getLongColumnLength();
        switch (sqlType) {
            case BYTES:
                return PG_BYTEA;
            case STRING:
                if (columnLength > 0 && columnLength < 10485760) {
                    return "varchar(" + columnLength + ")";
                } else {
                    return "text";
                }
            default:
                String type = postgresDataTypeConvertor.toConnectorType(column.getDataType(), null);
                if (type.equals(PG_NUMERIC)) {
                    DecimalType decimalType = (DecimalType) column.getDataType();
                    return "numeric("
                            + decimalType.getPrecision()
                            + ","
                            + decimalType.getScale()
                            + ")";
                }
                return type;
        }
    }

    private String buildColumnCommentSql(Column column, String tableName) {
        StringBuilder columnCommentSql = new StringBuilder();
        columnCommentSql.append("COMMENT ON COLUMN ").append(tableName).append(".");
        columnCommentSql
                .append(column.getName())
                .append(" IS '")
                .append(column.getComment())
                .append("'");
        return columnCommentSql.toString();
    }
}
