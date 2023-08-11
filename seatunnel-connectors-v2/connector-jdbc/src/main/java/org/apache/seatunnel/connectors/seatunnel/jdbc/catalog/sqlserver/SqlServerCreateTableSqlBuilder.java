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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class SqlServerCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;

    private String comment;

    private String engine;
    private String charset;
    private String collate;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    private SqlServerDataTypeConvertor sqlServerDataTypeConvertor;

    private SqlServerCreateTableSqlBuilder(String tableName) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.sqlServerDataTypeConvertor = new SqlServerDataTypeConvertor();
    }

    public static SqlServerCreateTableSqlBuilder builder(
            TablePath tablePath, CatalogTable catalogTable) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new SqlServerCreateTableSqlBuilder(tablePath.getTableName())
                .comment(catalogTable.getComment())
                // todo: set charset and collate
                .engine(null)
                .charset(null)
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns());
    }

    public SqlServerCreateTableSqlBuilder addColumn(List<Column> columns) {
        checkArgument(CollectionUtils.isNotEmpty(columns), "columns must not be empty");
        this.columns = columns;
        return this;
    }

    public SqlServerCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public SqlServerCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public SqlServerCreateTableSqlBuilder engine(String engine) {
        this.engine = engine;
        return this;
    }

    public SqlServerCreateTableSqlBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }

    public SqlServerCreateTableSqlBuilder collate(String collate) {
        this.collate = collate;
        return this;
    }

    public SqlServerCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public String build(TablePath tablePath, CatalogTable catalogTable) {
        List<String> sqls = new ArrayList<>();
        String sqlTableName = tablePath.getFullName();
        Map<String, String> columnComments = new HashMap<>();
        sqls.add(
                String.format(
                        "IF OBJECT_ID('%s', 'U') IS NULL \n"
                                + "BEGIN \n"
                                + "CREATE TABLE %s ( \n%s\n)",
                        sqlTableName,
                        sqlTableName,
                        buildColumnsIdentifySql(catalogTable.getCatalogName(), columnComments)));
        if (engine != null) {
            sqls.add("ENGINE = " + engine);
        }
        if (charset != null) {
            sqls.add("DEFAULT CHARSET = " + charset);
        }
        if (collate != null) {
            sqls.add("COLLATE = " + collate);
        }
        String sqlTableSql = String.join(" ", sqls) + ";";
        StringBuilder tableAndColumnComment = new StringBuilder();
        if (comment != null) {
            sqls.add("COMMENT = '" + comment + "'");
            tableAndColumnComment.append(
                    String.format(
                            "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s';\n",
                            tablePath.getDatabaseName(),
                            comment,
                            tablePath.getSchemaName(),
                            tablePath.getTableName()));
        }
        String columnComment =
                "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s';\n";
        columnComments.forEach(
                (fieldName, com) -> {
                    tableAndColumnComment.append(
                            String.format(
                                    columnComment,
                                    tablePath.getDatabaseName(),
                                    com,
                                    tablePath.getSchemaName(),
                                    tablePath.getTableName(),
                                    fieldName));
                });
        return String.join("\n", sqlTableSql, tableAndColumnComment.toString(), "END");
    }

    private String buildColumnsIdentifySql(String catalogName, Map<String, String> columnComments) {
        List<String> columnSqls = new ArrayList<>();
        for (Column column : columns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, catalogName, columnComments));
        }
        if (primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }
        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }
            }
        }
        return String.join(", \n", columnSqls);
    }

    private String buildColumnIdentifySql(
            Column column, String catalogName, Map<String, String> columnComments) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add(column.getName());
        String tyNameDef = "";
        if (StringUtils.equals(catalogName, "sqlserver")) {
            columnSqls.add(column.getSourceType());
        } else {
            // Column name
            SqlType dataType = column.getDataType().getSqlType();
            boolean isBytes = StringUtils.equals(dataType.name(), SqlType.BYTES.name());
            Long columnLength = column.getLongColumnLength();
            Long bitLen = column.getBitLen();
            bitLen = bitLen == -1 || bitLen <= 8 ? bitLen : bitLen >> 3;
            if (isBytes) {
                if (bitLen > 8000 || bitLen == -1) {
                    columnSqls.add(SqlServerType.VARBINARY.getName());
                } else {
                    columnSqls.add(SqlServerType.BINARY.getName());
                    tyNameDef = SqlServerType.BINARY.getName();
                }
                columnSqls.add("(" + (bitLen == -1 || bitLen > 8000 ? "max)" : bitLen + ")"));
            } else {
                // Add column type
                SqlServerType sqlServerType =
                        sqlServerDataTypeConvertor.toConnectorType(column.getDataType(), null);
                String typeName = sqlServerType.getName();
                String fieldSuffixSql = null;
                tyNameDef = typeName;
                // Add column length
                if (StringUtils.equals(SqlServerType.VARCHAR.getName(), typeName)) {
                    if (columnLength > 8000 || columnLength == -1) {
                        columnSqls.add(typeName);
                        fieldSuffixSql = "(max)";
                    } else if (columnLength > 4000) {
                        columnSqls.add(SqlServerType.VARCHAR.getName());
                        fieldSuffixSql = "(" + columnLength + ")";
                    } else {
                        columnSqls.add(SqlServerType.NVARCHAR.getName());
                        if (columnLength > 0) {
                            fieldSuffixSql = "(" + columnLength + ")";
                        }
                    }
                    columnSqls.add(fieldSuffixSql);
                } else if (StringUtils.equals(SqlServerType.DECIMAL.getName(), typeName)) {
                    columnSqls.add(typeName);
                    DecimalType decimalType = (DecimalType) column.getDataType();
                    columnSqls.add(
                            String.format(
                                    "(%d, %d)",
                                    decimalType.getPrecision(), decimalType.getScale()));
                } else {
                    columnSqls.add(typeName);
                }
            }
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }
        // default value
        //        if (column.getDefaultValue() != null) {
        //            String defaultValue = "'" + column.getDefaultValue().toString() + "'";
        //            if (StringUtils.equals(SqlServerType.BINARY.getName(), tyNameDef)
        //                    && defaultValue.contains("b'")) {
        //                String rep = defaultValue.replace("b", "").replace("'", "");
        //                defaultValue = "0x" + Integer.toHexString(Integer.parseInt(rep));
        //            } else if (StringUtils.equals(SqlServerType.BIT.getName(), tyNameDef)
        //                    && defaultValue.contains("b'")) {
        //                defaultValue = defaultValue.replace("b", "").replace("'", "");
        //            }
        //            columnSqls.add("DEFAULT " + defaultValue);
        //        }
        // comment
        if (column.getComment() != null) {
            columnComments.put(column.getName(), column.getComment());
        }

        return String.join(" ", columnSqls);
    }

    private String buildPrimaryKeySql() {
        //                        .map(columnName -> "`" + columnName + "`")
        String key = String.join(", ", primaryKey.getColumnNames());
        // add sort type
        return String.format("PRIMARY KEY (%s)", key);
    }

    private String buildConstraintKeySql(ConstraintKey constraintKey) {
        ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn -> {
                                    if (constraintKeyColumn.getSortType() == null) {
                                        return String.format(
                                                "`%s`", constraintKeyColumn.getColumnName());
                                    }
                                    return String.format(
                                            "`%s` %s",
                                            constraintKeyColumn.getColumnName(),
                                            constraintKeyColumn.getSortType().name());
                                })
                        .collect(Collectors.joining(", "));
        String keyName = null;
        switch (constraintType) {
            case KEY:
                keyName = "KEY";
                break;
            case UNIQUE_KEY:
                keyName = "UNIQUE KEY";
                break;
            case FOREIGN_KEY:
                keyName = "FOREIGN KEY";
                // todo:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported constraint type: " + constraintType);
        }
        return String.format(
                "%s `%s` (%s)", keyName, constraintKey.getConstraintName(), indexColumns);
    }
}
