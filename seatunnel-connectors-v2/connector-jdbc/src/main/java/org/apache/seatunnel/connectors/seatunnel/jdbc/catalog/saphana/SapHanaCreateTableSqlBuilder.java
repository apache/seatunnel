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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.saphana;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.saphana.SapHanaTypeConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SapHanaCreateTableSqlBuilder extends AbstractJdbcCreateTableSqlBuilder {

    private final List<Column> columns;
    private final PrimaryKey primaryKey;
    private final String sourceCatalogName;
    private final String fieldIde;
    private final String comment;
    private final List<ConstraintKey> constraintKeys;

    @Getter public List<String> createIndexSqls = new ArrayList<>();
    private boolean createIndex;

    public SapHanaCreateTableSqlBuilder(CatalogTable catalogTable, boolean createIndex) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.sourceCatalogName = catalogTable.getCatalogName();
        this.fieldIde = catalogTable.getOptions().get("fieldIde");
        this.comment = catalogTable.getComment();
        constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
        this.createIndex = createIndex;
    }

    public String build(TablePath tablePath) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE ")
                .append(CatalogUtils.quoteIdentifier(tablePath.getDatabaseName(), fieldIde, "\""))
                .append(".")
                .append(CatalogUtils.quoteIdentifier(tablePath.getTableName(), fieldIde, "\""))
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

        if (createIndex && CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())
                        || (primaryKey != null
                                && (StringUtils.equals(
                                                primaryKey.getPrimaryKey(),
                                                constraintKey.getConstraintName())
                                        || primaryContainsAllConstrainKey(
                                                primaryKey, constraintKey)))) {
                    continue;
                }
                switch (constraintKey.getConstraintType()) {
                    case UNIQUE_KEY:
                        String uniqueKeySql = buildUniqueKeySql(constraintKey);
                        columnSqls.add(uniqueKeySql);
                        break;
                    case INDEX_KEY:
                    case FOREIGN_KEY:
                        break;
                }
            }
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");
        if (comment != null) {
            createTableSql.append(" COMMENT '").append(comment).append("'");
        }

        return createTableSql.toString();
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("\"").append(column.getName()).append("\" ");

        String columnType =
                StringUtils.equalsIgnoreCase(DatabaseIdentifier.SAP_HANA, sourceCatalogName)
                                && StringUtils.isNotBlank(column.getSourceType())
                        ? column.getSourceType()
                        : SapHanaTypeConverter.INSTANCE.reconvert(column).getColumnType();
        columnSql.append(columnType);

        // nullable
        if (column.isNullable()) {
            columnSql.append(" NULL");
        } else {
            columnSql.append(" NOT NULL");
        }

        if (column.getComment() != null) {
            columnSql.append(" COMMENT '").append(column.getComment()).append("'");
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));

        return String.format("PRIMARY KEY (%s)", CatalogUtils.quoteIdentifier(key, fieldIde));
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String key =
                constraintKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName.getColumnName() + "\"")
                        .collect(Collectors.joining(", "));

        return String.format("UNIQUE (%s)", CatalogUtils.quoteIdentifier(key, fieldIde));
    }
}
