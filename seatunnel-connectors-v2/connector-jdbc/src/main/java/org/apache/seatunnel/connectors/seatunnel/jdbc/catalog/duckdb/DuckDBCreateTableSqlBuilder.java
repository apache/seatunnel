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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.duckdb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeConverter;

import org.apache.commons.collections4.CollectionUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DuckDBCreateTableSqlBuilder extends AbstractJdbcCreateTableSqlBuilder {

    private final List<Column> columns;
    private final PrimaryKey primaryKey;
    private final List<ConstraintKey> constraintKeys;

    public DuckDBCreateTableSqlBuilder(CatalogTable catalogTable) {
        this.columns = catalogTable.getTableSchema().getColumns();
        this.primaryKey = catalogTable.getTableSchema().getPrimaryKey();
        this.constraintKeys = catalogTable.getTableSchema().getConstraintKeys();
    }

    public List<String> build(TablePath tablePath) {
        List<String> sqls = new ArrayList<>();

        // Build CREATE TABLE SQL
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(tablePath.getFullName()).append(" (\n");

        // Build all column definitions
        List<String> columnSqls =
                columns.stream().map(this::buildColumnSql).collect(Collectors.toList());

        // Add primary key definition
        if (primaryKey != null
                && primaryKey.getColumnNames() != null
                && !primaryKey.getColumnNames().isEmpty()) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }

        // Add constraint definitions
        if (CollectionUtils.isNotEmpty(constraintKeys)) {
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
                        columnSqls.add(buildUniqueKeySql(constraintKey));
                        break;
                    case FOREIGN_KEY:
                        // Foreign keys are not supported, ignore
                        break;
                    case INDEX_KEY:
                        // Indexes will be created separately after table creation
                        break;
                    default:
                        // Do not handle other constraint types
                        break;
                }
            }
        }

        createTableSql.append(String.join(",\n", columnSqls));
        createTableSql.append("\n)");
        sqls.add(createTableSql.toString());

        // Create indexes for constraints (after table creation)
        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (constraintKey.getConstraintType() == ConstraintKey.ConstraintType.INDEX_KEY
                        && StringUtils.isNotBlank(constraintKey.getConstraintName())) {
                    sqls.add(buildIndexSql(tablePath, constraintKey));
                }
            }
        }

        return sqls;
    }

    private String buildColumnSql(Column column) {
        StringBuilder columnSql = new StringBuilder();
        columnSql.append("    \"").append(column.getName()).append("\" ");

        // Get corresponding DuckDB column type
        String columnType = DuckDBTypeConverter.INSTANCE.reconvert(column).getColumnType();
        columnSql.append(columnType);

        // Add NOT NULL constraint
        if (!column.isNullable()) {
            columnSql.append(" NOT NULL");
        }

        // Add default value
        if (column.getDefaultValue() != null) {
            columnSql.append(" DEFAULT ").append(column.getDefaultValue());
        }

        return columnSql.toString();
    }

    private String buildPrimaryKeySql(PrimaryKey primaryKey) {
        String columnNamesString =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "\"" + columnName + "\"")
                        .collect(Collectors.joining(", "));

        return String.format("    PRIMARY KEY (%s)", columnNamesString);
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String columnNamesString =
                constraintKey.getColumnNames().stream()
                        .map(column -> "\"" + column.getColumnName() + "\"")
                        .collect(Collectors.joining(", "));

        return String.format(
                "    CONSTRAINT \"%s\" UNIQUE (%s)",
                constraintKey.getConstraintName(), columnNamesString);
    }

    private String buildIndexSql(TablePath tablePath, ConstraintKey constraintKey) {
        String columnNamesString =
                constraintKey.getColumnNames().stream()
                        .map(column -> "\"" + column.getColumnName() + "\"")
                        .collect(Collectors.joining(", "));

        return String.format(
                "CREATE INDEX \"%s\" ON %s (%s)",
                constraintKey.getConstraintName(), tablePath.getFullName(), columnNamesString);
    }
}
