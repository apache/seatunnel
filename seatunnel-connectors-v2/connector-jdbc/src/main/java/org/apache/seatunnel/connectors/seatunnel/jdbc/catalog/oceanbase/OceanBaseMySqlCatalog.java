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
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.JdbcColumnConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase.OceanBaseMySqlTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oceanbase.OceanBaseMysqlType;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Slf4j
public class OceanBaseMySqlCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME ='%s' ORDER BY ORDINAL_POSITION ASC";

    private static final String SELECT_DATABASE_EXISTS =
            "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '%s'";

    private static final String SELECT_TABLE_EXISTS =
            "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";

    private OceanBaseMySqlTypeConverter typeConverter;

    public OceanBaseMySqlCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo, null);
        this.typeConverter = new OceanBaseMySqlTypeConverter();
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(SELECT_DATABASE_EXISTS, databaseName);
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                SELECT_TABLE_EXISTS, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected String getListDatabaseSql() {
        return "SHOW DATABASES;";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SHOW TABLES;";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getTableName(TablePath tablePath) {
        return tablePath.getTableName();
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected TableIdentifier getTableIdentifier(TablePath tablePath) {
        return TableIdentifier.of(
                catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        List<ConstraintKey> indexList =
                super.getConstraintKeys(
                        metaData,
                        tablePath.getDatabaseName(),
                        tablePath.getSchemaName(),
                        tablePath.getTableName());
        for (Iterator<ConstraintKey> it = indexList.iterator(); it.hasNext(); ) {
            ConstraintKey index = it.next();
            if (ConstraintKey.ConstraintType.UNIQUE_KEY.equals(index.getConstraintType())
                    && "PRIMARY".equals(index.getConstraintName())) {
                it.remove();
            }
        }
        return indexList;
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        // e.g. tinyint(1) unsigned
        String columnType = resultSet.getString("COLUMN_TYPE");
        // e.g. tinyint
        String dataType = resultSet.getString("DATA_TYPE").toUpperCase();
        String comment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("COLUMN_DEFAULT");
        String isNullableStr = resultSet.getString("IS_NULLABLE");
        boolean isNullable = isNullableStr.equals("YES");
        // e.g. `decimal(10, 2)` is 10
        long numberPrecision = resultSet.getInt("NUMERIC_PRECISION");
        // e.g. `decimal(10, 2)` is 2
        int numberScale = resultSet.getInt("NUMERIC_SCALE");
        // e.g. `varchar(10)` is 40
        long charOctetLength = resultSet.getLong("CHARACTER_OCTET_LENGTH");
        // e.g. `timestamp(3)` is 3
        //        int timePrecision =
        //                MySqlVersion.V_5_5.equals(version) ? 0 :
        // resultSet.getInt("DATETIME_PRECISION");
        int timePrecision = resultSet.getInt("DATETIME_PRECISION");
        Preconditions.checkArgument(!(numberPrecision > 0 && charOctetLength > 0));
        Preconditions.checkArgument(!(numberScale > 0 && timePrecision > 0));

        OceanBaseMysqlType oceanbaseMysqlType = OceanBaseMysqlType.getByName(columnType);
        boolean unsigned = columnType.toLowerCase(Locale.ROOT).contains("unsigned");

        BasicTypeDefine<OceanBaseMysqlType> typeDefine =
                BasicTypeDefine.<OceanBaseMysqlType>builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(dataType)
                        .nativeType(oceanbaseMysqlType)
                        .unsigned(unsigned)
                        .length(Math.max(charOctetLength, numberPrecision))
                        .precision(numberPrecision)
                        .scale(Math.max(numberScale, timePrecision))
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return typeConverter.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return OceanBaseMysqlCreateTableSqlBuilder.builder(
                        tablePath, table, typeConverter, createIndex)
                .build(table.getCatalogName());
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format(
                "DROP TABLE `%s`.`%s`;", tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE `%s`;", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE `%s`;", databaseName);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        try (Statement statement = defaultConnection.createStatement();
                ResultSet resultSet = statement.executeQuery(sqlQuery)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            return getCatalogTable(metaData, defaultConnection.getMetaData(), sqlQuery);
        }
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format(
                "TRUNCATE TABLE `%s`.`%s`;", tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public String getExistDataSql(TablePath tablePath) {
        return String.format(
                "SELECT * FROM `%s`.`%s` LIMIT 1;",
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static CatalogTable getCatalogTable(
            ResultSetMetaData resultSetMetaData, DatabaseMetaData databaseMetaData, String sqlQuery)
            throws SQLException {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        Map<String, String> unsupported = new LinkedHashMap<>();
        String tableName = null;
        String databaseName = null;
        String schemaName = null;
        String catalogName = "jdbc_catalog";
        try {
            tableName = resultSetMetaData.getTableName(1);
            databaseName = resultSetMetaData.getCatalogName(1);
            schemaName = resultSetMetaData.getSchemaName(1);
            catalogName = resultSetMetaData.getCatalogName(1);
        } catch (SQLException ignored) {
        }
        if (!unsupported.isEmpty()) {
            throw CommonError.getCatalogTableWithUnsupportedType("UNKNOWN", sqlQuery, unsupported);
        }
        databaseName = StringUtils.isBlank(databaseName) ? null : databaseName;
        schemaName = StringUtils.isBlank(schemaName) ? null : schemaName;
        TablePath tablePath =
                StringUtils.isBlank(tableName)
                        ? TablePath.DEFAULT
                        : TablePath.of(databaseName, schemaName, tableName);
        List<Column> columns = JdbcColumnConverter.convert(databaseMetaData, tablePath);
        ResultSet primaryKeys = databaseMetaData.getPrimaryKeys(catalogName, schemaName, tableName);
        while (primaryKeys.next()) {
            String primaryKey = primaryKeys.getString("COLUMN_NAME");
            schemaBuilder.primaryKey(
                    PrimaryKey.of(
                            primaryKey,
                            Collections.singletonList(primaryKey),
                            resultSetMetaData.isAutoIncrement(primaryKeys.getRow())));
        }
        schemaBuilder.columns(columns);
        return CatalogTable.of(
                TableIdentifier.of(catalogName, tablePath),
                schemaBuilder.build(),
                new HashMap<>(),
                new ArrayList<>(),
                "",
                catalogName);
    }
}
