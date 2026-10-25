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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.yashandb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb.YashanDbTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class YashanDbCreateTableSqlBuilder {

    private final List<Column> columns;
    private final PrimaryKey primaryKey;
    private final String comment;
    protected String sourceCatalogName;
    private final String fieldIde;
    private final boolean createIndex;

    public YashanDbCreateTableSqlBuilder(CatalogTable catalogTable, boolean createIndex) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.comment = catalogTable.getComment();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.createIndex = createIndex;
    }

    public List<String> build(TablePath tablePath) {
        List<String> sqls = new ArrayList<>();
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE ")
                .append(tablePath.getSchemaAndTableName("\""))
                .append(" (\n");

        List<String> columnSqls =
                columns.stream()
                        .map(column -> CatalogUtils.getFieldIde(buildColumnSql(column), fieldIde))
                        .collect(Collectors.toList());

        // Add primary key directly in the create table statement
        if (createIndex
                && primaryKey != null
                && primaryKey.getColumnNames() != null
                && !primaryKey.getColumnNames().isEmpty()) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");
        sqls.add(createTableSql.toString());
        if (comment != null) {
            String commentSql =
                    "COMMENT ON TABLE "
                            + tablePath.getSchemaAndTableName("\"")
                            + " IS '"
                            + comment
                            + "'";
            sqls.add(commentSql);
        }
        List<String> commentSqls =
                columns.stream()
                        .filter(column -> StringUtils.isNotBlank(column.getComment()))
                        .map(
                                column ->
                                        buildColumnCommentSql(
                                                column, tablePath.getSchemaAndTableName("\"")))
                        .collect(Collectors.toList());
        sqls.addAll(commentSqls);
        return sqls;
    }

    protected String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (StringUtils.equalsIgnoreCase(DatabaseIdentifier.YASHANDB, sourceCatalogName)
                && StringUtils.isNotBlank(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = YashanDbTypeConverter.INSTANCE.reconvert(column).getColumnType();
        }
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String randomSuffix = UUID.randomUUID().toString().replace("-", "").substring(0, 4);
        String columnNamesString =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));

        String primaryKeyStr = primaryKey.getPrimaryKey();
        if (primaryKeyStr.length() > 25) {
            primaryKeyStr = primaryKeyStr.substring(0, 25);
        }

        return CatalogUtils.getFieldIde(
                "CONSTRAINT "
                        + primaryKeyStr
                        + "_"
                        + randomSuffix
                        + " PRIMARY KEY ("
                        + columnNamesString
                        + ")",
                fieldIde);
    }

    private String buildColumnCommentSql(Column column, String tableName) {
        StringBuilder columnCommentSql = new StringBuilder();
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier("COMMENT ON COLUMN ", fieldIde))
                .append(tableName)
                .append(".");
        columnCommentSql
                .append(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "\""))
                .append(CatalogUtils.quoteIdentifier(" IS '", fieldIde))
                .append(column.getComment().replace("'", "''"))
                .append("'");
        return columnCommentSql.toString();
    }
}
