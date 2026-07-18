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
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeConverter;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class DuckDBCreateTableSqlBuilder extends AbstractJdbcCreateTableSqlBuilder {

    private List<Column> columns;
    private PrimaryKey primaryKey;
    private List<ConstraintKey> constraintKeys;
    private String fieldIde;
    private String comment;
    private String sourceCatalogName;
    private final DuckDBTypeConverter typeConverter;
    private final boolean createIndex;

    private DuckDBCreateTableSqlBuilder(
            String tableName, DuckDBTypeConverter typeConverter, boolean createIndex) {
        checkNotNull(tableName, "tableName must not be null");
        this.typeConverter = typeConverter;
        this.createIndex = createIndex;
    }

    public static DuckDBCreateTableSqlBuilder builder(
            TablePath tablePath,
            CatalogTable catalogTable,
            DuckDBTypeConverter typeConverter,
            boolean createIndex) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");
        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");
        return new DuckDBCreateTableSqlBuilder(tablePath.getTableName(), typeConverter, createIndex)
                .comment(catalogTable.getComment())
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns())
                .fieldIde(catalogTable.getOptions().get("fieldIde"))
                .sourceCatalogName(catalogTable.getCatalogName());
    }

    public DuckDBCreateTableSqlBuilder addColumn(List<Column> columns) {
        this.columns = columns;
        return this;
    }

    public DuckDBCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public DuckDBCreateTableSqlBuilder fieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
        return this;
    }

    public DuckDBCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public DuckDBCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public DuckDBCreateTableSqlBuilder sourceCatalogName(String sourceCatalogName) {
        this.sourceCatalogName = sourceCatalogName;
        return this;
    }

    public List<String> build(TablePath tablePath) {
        List<String> sqls = new ArrayList<>();
        // Build CREATE TABLE SQL
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE ").append(buildTableName(tablePath)).append(" (\n");
        // Build all column definitions
        List<String> columnSqls =
                columns.stream().map(this::buildColumnSql).collect(Collectors.toList());
        // Add primary key definition
        if (createIndex
                && primaryKey != null
                && primaryKey.getColumnNames() != null
                && !primaryKey.getColumnNames().isEmpty()) {
            columnSqls.add(buildPrimaryKeySql(primaryKey));
        }
        // Add constraint definitions
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
        if (StringUtils.isNotBlank(comment)) {
            sqls.add(
                    String.format(
                            "COMMENT ON TABLE %s IS '%s'",
                            buildTableName(tablePath), comment.replace("'", "''")));
        }
        // Create indexes for constraints (after table creation)
        if (createIndex && CollectionUtils.isNotEmpty(constraintKeys)) {
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
        columnSql.append("    ").append(quoteIdentifier(column.getName())).append(" ");
        String columnType;
        if (column.getSinkType() != null) {
            columnType = column.getSinkType();
        } else if (StringUtils.equalsIgnoreCase(sourceCatalogName, typeConverter.identifier())
                && StringUtils.isNotBlank(column.getSourceType())) {
            columnType = column.getSourceType();
        } else {
            columnType = typeConverter.reconvert(column).getColumnType();
        }
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
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        return String.format("    PRIMARY KEY (%s)", columnNamesString);
    }

    private String buildUniqueKeySql(ConstraintKey constraintKey) {
        String columnNamesString =
                constraintKey.getColumnNames().stream()
                        .map(column -> quoteIdentifier(column.getColumnName()))
                        .collect(Collectors.joining(", "));
        return String.format(
                "    CONSTRAINT \"%s\" UNIQUE (%s)",
                constraintKey.getConstraintName(), columnNamesString);
    }

    private String buildIndexSql(TablePath tablePath, ConstraintKey constraintKey) {
        String columnNamesString =
                constraintKey.getColumnNames().stream()
                        .map(column -> quoteIdentifier(column.getColumnName()))
                        .collect(Collectors.joining(", "));
        return String.format(
                "CREATE INDEX \"%s\" ON %s (%s)",
                constraintKey.getConstraintName(), buildTableName(tablePath), columnNamesString);
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + CatalogUtils.getFieldIde(identifier, fieldIde) + "\"";
    }

    private String buildTableName(TablePath tablePath) {
        if (StringUtils.isNotBlank(tablePath.getSchemaName())) {
            return String.format(
                    "%s.%s",
                    quoteIdentifier(tablePath.getSchemaName()),
                    quoteIdentifier(tablePath.getTableName()));
        }
        return quoteIdentifier(tablePath.getTableName());
    }
}
