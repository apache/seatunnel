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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.mysql.cj.MysqlType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class MysqlCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;

    private String comment;

    private String engine;
    private String charset;
    private String collate;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    private MysqlDataTypeConvertor mysqlDataTypeConvertor;

    private String fieldIde;

    private MysqlCreateTableSqlBuilder(String tableName) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.mysqlDataTypeConvertor = new MysqlDataTypeConvertor();
    }

    public static MysqlCreateTableSqlBuilder builder(
            TablePath tablePath, CatalogTable catalogTable) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new MysqlCreateTableSqlBuilder(tablePath.getTableName())
                .comment(catalogTable.getComment())
                // todo: set charset and collate
                .engine(null)
                .charset(null)
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns())
                .fieldIde(catalogTable.getOptions().get("fieldIde"));
    }

    public MysqlCreateTableSqlBuilder addColumn(List<Column> columns) {
        checkArgument(CollectionUtils.isNotEmpty(columns), "columns must not be empty");
        this.columns = columns;
        return this;
    }

    public MysqlCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public MysqlCreateTableSqlBuilder fieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
        return this;
    }

    public MysqlCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public MysqlCreateTableSqlBuilder engine(String engine) {
        this.engine = engine;
        return this;
    }

    public MysqlCreateTableSqlBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }

    public MysqlCreateTableSqlBuilder collate(String collate) {
        this.collate = collate;
        return this;
    }

    public MysqlCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public String build(String catalogName) {
        List<String> sqls = new ArrayList<>();
        sqls.add(
                String.format(
                        "CREATE TABLE %s (\n%s\n)",
                        CatalogUtils.quoteIdentifier(tableName, fieldIde, "`"),
                        buildColumnsIdentifySql(catalogName)));
        if (engine != null) {
            sqls.add("ENGINE = " + engine);
        }
        if (charset != null) {
            sqls.add("DEFAULT CHARSET = " + charset);
        }
        if (collate != null) {
            sqls.add("COLLATE = " + collate);
        }
        if (comment != null) {
            sqls.add("COMMENT = '" + comment + "'");
        }
        return String.join(" ", sqls) + ";";
    }

    private String buildColumnsIdentifySql(String catalogName) {
        List<String> columnSqls = new ArrayList<>();
        for (Column column : columns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, catalogName));
        }
        if (primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }
        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }
                //                columnSqls.add("\t" + buildConstraintKeySql(constraintKey));
            }
        }
        return String.join(", \n", columnSqls);
    }

    private String buildColumnIdentifySql(Column column, String catalogName) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "`"));
        boolean isSupportDef = true;
        if (StringUtils.equals(catalogName, DatabaseIdentifier.MYSQL)) {
            columnSqls.add(column.getSourceType());
        } else {
            // Column name
            getColumnName(column, columnSqls);
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }

        if (column.getComment() != null) {
            columnSqls.add("COMMENT '" + column.getComment() + "'");
        }

        return String.join(" ", columnSqls);
    }

    private void getColumnName(Column column, List<String> columnSqls) {
        SqlType dataType = column.getDataType().getSqlType();
        boolean isBytes = StringUtils.equals(dataType.name(), SqlType.BYTES.name());
        Long columnLength = column.getLongColumnLength();
        Long bitLen = column.getBitLen();
        if (isBytes) {
            getColumnNameBytes(columnSqls, bitLen);
        } else {
            getColumnNameNonBytes(column, columnSqls, columnLength);
        }
    }

    private void getColumnNameNonBytes(Column column, List<String> columnSqls, Long columnLength) {
        columnLength = columnLength == null ? Integer.MAX_VALUE : columnLength;
        if (columnLength >= 16383 && columnLength <= 65535) {
            columnSqls.add(MysqlType.TEXT.getName());
        } else if (columnLength >= 65535 && columnLength <= 16777215) {
            columnSqls.add(MysqlType.MEDIUMTEXT.getName());
        } else if (columnLength > 16777215 || columnLength == -1) {
            columnSqls.add(MysqlType.LONGTEXT.getName());
        } else {
            // Column type
            final String name =
                    mysqlDataTypeConvertor.toConnectorType(column.getDataType(), null).getName();
            if (columnLength == 0
                    && StringUtils.equalsIgnoreCase(name, MysqlType.VARCHAR.getName())) {
                columnSqls.add(MysqlType.LONGTEXT.getName());
                return;
            }
            columnSqls.add(
                    mysqlDataTypeConvertor.toConnectorType(column.getDataType(), null).getName());

            String fieSql = "";
            List<String> list = new ArrayList<>();
            list.add(MysqlType.VARCHAR.getName());
            list.add(MysqlType.CHAR.getName());
            list.add(MysqlType.BIGINT.getName());
            list.add(MysqlType.INT.getName());
            if (StringUtils.equals(name, MysqlType.DECIMAL.getName())) {
                DecimalType decimalType = (DecimalType) column.getDataType();
                fieSql =
                        String.format(
                                "(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
                columnSqls.add(fieSql);
                return;
            }
            if (list.contains(name)) {
                if (MysqlType.VARCHAR.getName().equals(name)) {
                    fieSql = "(" + column.getLongColumnLength() + ")";
                } else if (MysqlType.CHAR.getName().equals(name)) {
                    fieSql = "(" + column.getLongColumnLength() + ")";
                } else {
                    // int and bigint
                    fieSql =
                            "("
                                    + (column.getLongColumnLength().equals(0L)
                                            ? 11
                                            : column.getLongColumnLength())
                                    + ")";
                }
                columnSqls.add(fieSql);
            }
        }
    }

    private void getColumnNameBytes(List<String> columnSqls, Long bitLen) {
        bitLen = bitLen == null ? Integer.MAX_VALUE : bitLen;
        if (bitLen > 0 && bitLen <= 64) {
            columnSqls.add(MysqlType.BIT.getName());
            columnSqls.add("(" + bitLen + ")");
        } else {
            bitLen = bitLen == -1 ? bitLen : bitLen >> 3;
            if (bitLen >= 0 && bitLen <= 255) {
                columnSqls.add(MysqlType.TINYBLOB.getName());
            } else if (bitLen <= 16383) {
                columnSqls.add(MysqlType.BLOB.getName());
            } else if (bitLen <= 16777215) {
                columnSqls.add(MysqlType.MEDIUMBLOB.getName());
            } else {
                columnSqls.add(MysqlType.LONGBLOB.getName());
            }
        }
    }

    private String buildPrimaryKeySql() {
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "`" + columnName + "`")
                        .collect(Collectors.joining(", "));
        // add sort type
        return String.format("PRIMARY KEY (%s)", CatalogUtils.quoteIdentifier(key, fieldIde));
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
            case INDEX_KEY:
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
