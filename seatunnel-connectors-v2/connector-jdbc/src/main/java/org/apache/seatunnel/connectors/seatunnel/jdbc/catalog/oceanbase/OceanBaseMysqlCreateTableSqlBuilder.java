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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oceanbase;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase.OceanBaseMySqlTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase.OceanBaseMysqlType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class OceanBaseMysqlCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;

    private String comment;

    private String engine;
    private String charset;
    private String collate;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    private String fieldIde;

    private final OceanBaseMySqlTypeConverter typeConverter;
    private boolean createIndex;

    private OceanBaseMysqlCreateTableSqlBuilder(
            String tableName, OceanBaseMySqlTypeConverter typeConverter, boolean createIndex) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.typeConverter = typeConverter;
        this.createIndex = createIndex;
    }

    public static OceanBaseMysqlCreateTableSqlBuilder builder(
            TablePath tablePath,
            CatalogTable catalogTable,
            OceanBaseMySqlTypeConverter typeConverter,
            boolean createIndex) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new OceanBaseMysqlCreateTableSqlBuilder(
                        tablePath.getTableName(), typeConverter, createIndex)
                .comment(catalogTable.getComment())
                // todo: set charset and collate
                .engine(null)
                .charset(null)
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns())
                .fieldIde(catalogTable.getOptions().get("fieldIde"));
    }

    public OceanBaseMysqlCreateTableSqlBuilder addColumn(List<Column> columns) {
        checkArgument(CollectionUtils.isNotEmpty(columns), "columns must not be empty");
        this.columns = columns;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder fieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder engine(String engine) {
        this.engine = engine;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder collate(String collate) {
        this.collate = collate;
        return this;
    }

    public OceanBaseMysqlCreateTableSqlBuilder comment(String comment) {
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
        Map<String, String> columnTypeMap = new HashMap<>();
        for (Column column : columns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, catalogName, columnTypeMap));
        }
        if (createIndex && primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }
        if (createIndex && CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }
                String constraintKeyStr = buildConstraintKeySql(constraintKey, columnTypeMap);
                if (StringUtils.isNotBlank(constraintKeyStr)) {
                    columnSqls.add("\t" + constraintKeyStr);
                }
            }
        }
        return String.join(", \n", columnSqls);
    }

    private String buildColumnIdentifySql(
            Column column, String catalogName, Map<String, String> columnTypeMap) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add(CatalogUtils.quoteIdentifier(column.getName(), fieldIde, "`"));
        String type;
        if ((SqlType.TIME.equals(column.getDataType().getSqlType())
                        || SqlType.TIMESTAMP.equals(column.getDataType().getSqlType()))
                && column.getScale() != null) {
            BasicTypeDefine<OceanBaseMysqlType> typeDefine = typeConverter.reconvert(column);
            type = typeDefine.getColumnType();
        } else if (StringUtils.equals(catalogName, DatabaseIdentifier.MYSQL)
                && StringUtils.isNotBlank(column.getSourceType())) {
            type = column.getSourceType();
        } else {
            BasicTypeDefine<OceanBaseMysqlType> typeDefine = typeConverter.reconvert(column);
            type = typeDefine.getColumnType();
        }
        columnSqls.add(type);
        columnTypeMap.put(column.getName(), type);
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }

        if (column.getComment() != null) {
            columnSqls.add(
                    "COMMENT '"
                            + column.getComment().replace("'", "''").replace("\\", "\\\\")
                            + "'");
        }

        return String.join(" ", columnSqls);
    }

    private String buildPrimaryKeySql() {
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "`" + columnName + "`")
                        .collect(Collectors.joining(", "));
        // add sort type
        return String.format("PRIMARY KEY (%s)", CatalogUtils.quoteIdentifier(key, fieldIde));
    }

    private String buildConstraintKeySql(
            ConstraintKey constraintKey, Map<String, String> columnTypeMap) {
        ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn -> {
                                    String columnName = constraintKeyColumn.getColumnName();
                                    boolean withLength = false;
                                    if (columnTypeMap.containsKey(columnName)) {
                                        String columnType = columnTypeMap.get(columnName);
                                        if (columnType.endsWith("BLOB")
                                                || columnType.endsWith("TEXT")) {
                                            withLength = true;
                                        }
                                    }
                                    if (constraintKeyColumn.getSortType() == null) {
                                        return String.format(
                                                "`%s`%s",
                                                CatalogUtils.getFieldIde(columnName, fieldIde),
                                                withLength ? "(255)" : "");
                                    }
                                    return String.format(
                                            "`%s`%s %s",
                                            CatalogUtils.getFieldIde(columnName, fieldIde),
                                            withLength ? "(255)" : "",
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
