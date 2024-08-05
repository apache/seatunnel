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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.iris.IrisTypeConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

public class IrisCreateTableSqlBuilder {

    private List<Column> columns;
    private PrimaryKey primaryKey;
    List<ConstraintKey> constraintKeys;
    private String sourceCatalogName;
    private String fieldIde;

    private String comment;
    private boolean createIndex;

    public IrisCreateTableSqlBuilder(CatalogTable catalogTable, boolean createIndex) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.comment = catalogTable.getComment();
        this.createIndex = createIndex;
    }

    public String build(TablePath tablePath) {
        String indexKeySql = "";
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
                && primaryKey.getColumnNames().size() > 0) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }
        if (createIndex && CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())
                        || (primaryKey != null
                                && StringUtils.equals(
                                        primaryKey.getPrimaryKey(),
                                        constraintKey.getConstraintName()))) {
                    continue;
                }
                switch (constraintKey.getConstraintType()) {
                    case UNIQUE_KEY:
                        String uniqueKeySql = buildUniqueKeySql(constraintKey);
                        columnSqls.add(uniqueKeySql);
                        break;
                    case INDEX_KEY:
                        indexKeySql = buildIndexKeySql(tablePath, constraintKey);
                        break;
                    case FOREIGN_KEY:
                        // todo: add foreign key
                        break;
                }
            }
        }
        if (StringUtils.isNotBlank(comment)) {
            createTableSql.append(" %Description '" + comment + "',\n");
        }
        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n);");
        createTableSql.append("\n" + indexKeySql);
        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        String columnType = IrisTypeConverter.INSTANCE.reconvert(column).getColumnType();
        columnSql.append(columnType);

        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        if (StringUtils.isNotBlank(column.getComment())) {
            columnSql.append(" %Description '" + column.getComment() + "'");
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String columnNamesString =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));
        return CatalogUtils.getFieldIde(" PRIMARY KEY (" + columnNamesString + ")", fieldIde);
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));
        return "UNIQUE (" + indexColumns + ")";
    }

    private String buildIndexKeySql(TablePath tablePath, ConstraintKey constraintKey) {
        // We add table name to index name to avoid name conflict
        String constraintName = tablePath.getTableName() + "_" + constraintKey.getConstraintName();
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn ->
                                        String.format(
                                                "\"%s\"",
                                                CatalogUtils.getFieldIde(
                                                        constraintKeyColumn.getColumnName(),
                                                        fieldIde)))
                        .collect(Collectors.joining(", "));

        return "CREATE INDEX "
                + constraintName
                + " ON "
                + tablePath.getSchemaAndTableName("\"")
                + "("
                + indexColumns
                + ");";
    }
}
