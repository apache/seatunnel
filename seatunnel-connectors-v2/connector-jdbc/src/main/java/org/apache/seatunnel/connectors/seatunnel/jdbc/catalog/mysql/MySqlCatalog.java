/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlVersion;

import com.mysql.cj.MysqlType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

@Slf4j
public class MySqlCatalog extends AbstractJdbcCatalog {

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME ='%s' ORDER BY ORDINAL_POSITION ASC";

    private static final String SELECT_DATABASE_EXISTS =
            "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE SCHEMA_NAME = '%s'";

    private static final String SELECT_TABLE_EXISTS =
            "SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'";

    private static final String SELECT_TABLE_META_SQL =
            "SELECT TABLE_COMMENT, ENGINE, TABLE_COLLATION FROM INFORMATION_SCHEMA.TABLES "
                    + "WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'";

    public static final String TABLE_OPTION_ENGINE = "engine";
    public static final String TABLE_OPTION_CHARSET = "charset";
    public static final String TABLE_OPTION_COLLATE = "collate";

    private MySqlVersion version;
    private MySqlTypeConverter typeConverter;
    private boolean intTypeNarrowing = JdbcCommonOptions.INT_TYPE_NARROWING.defaultValue();

    public MySqlCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String driverClass) {
        super(catalogName, username, pwd, urlInfo, null, driverClass);
        this.version = resolveVersion();
        this.typeConverter = new MySqlTypeConverter(version, intTypeNarrowing);
    }

    public MySqlCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String driverClass,
            boolean intTypeNarrowing) {
        super(catalogName, username, pwd, urlInfo, null, driverClass);
        this.intTypeNarrowing = intTypeNarrowing;
        this.version = resolveVersion();
        this.typeConverter = new MySqlTypeConverter(version, intTypeNarrowing);
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
    protected Optional<String> getTableComment(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        String url = getUrlFromDatabaseName(database);
        String sql = String.format(SELECT_TABLE_META_SQL, database, table);
        try (PreparedStatement ps = getConnection(url).prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                String comment = rs.getString("TABLE_COMMENT");
                return Optional.ofNullable(comment != null && !comment.isEmpty() ? comment : null);
            }
        }
        return Optional.empty();
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        CatalogTable catalogTable = super.getTable(tablePath);
        readAndFillTableMetaOptions(
                tablePath.getDatabaseName(), tablePath.getTableName(), catalogTable.getOptions());
        return catalogTable;
    }

    private void readAndFillTableMetaOptions(
            String database, String tableName, Map<String, String> options) {
        String url = getUrlFromDatabaseName(database);
        String sql = String.format(SELECT_TABLE_META_SQL, database, tableName);
        try (PreparedStatement ps = getConnection(url).prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                String engine = rs.getString("ENGINE");
                if (StringUtils.isNotBlank(engine)) {
                    options.put(TABLE_OPTION_ENGINE, engine);
                }
                String collation = rs.getString("TABLE_COLLATION");
                if (StringUtils.isNotBlank(collation)) {
                    options.put(TABLE_OPTION_COLLATE, collation);
                    String charset = collation.split("_")[0];
                    options.put(TABLE_OPTION_CHARSET, charset);
                }
            }
        } catch (SQLException e) {
            log.warn(
                    "Failed to read table metadata from information_schema for {}.{}: {}",
                    database,
                    tableName,
                    e.getMessage());
        }
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
        int timePrecision =
                MySqlVersion.V_5_5.equals(version) ? 0 : resultSet.getInt("DATETIME_PRECISION");

        Preconditions.checkArgument(!(numberPrecision > 0 && charOctetLength > 0));
        Preconditions.checkArgument(!(numberScale > 0 && timePrecision > 0));

        MysqlType mysqlType = MysqlType.getByName(columnType);
        boolean unsigned = columnType.toLowerCase(Locale.ROOT).contains("unsigned");

        BasicTypeDefine<MysqlType> typeDefine =
                BasicTypeDefine.<MysqlType>builder()
                        .name(columnName)
                        .columnType(columnType)
                        .dataType(dataType)
                        .nativeType(mysqlType)
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
        return MysqlCreateTableSqlBuilder.builder(tablePath, table, typeConverter, createIndex)
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
        return CatalogUtils.getCatalogTable(
                defaultConnection, sqlQuery, new MySqlTypeMapper(typeConverter));
    }

    @Override
    protected @NonNull Properties getConnectionProperties() {
        Properties info = super.getConnectionProperties();
        if (!intTypeNarrowing) {
            // we should not use tinyint(1) as boolean type when intTypeNarrowing is false, so
            // cannot convert tinyint(1) to bit
            info.put("tinyInt1isBit", "false");
        }
        return info;
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

    private MySqlVersion resolveVersion() {
        try (Statement statement = getConnection(defaultUrl).createStatement();
                ResultSet resultSet = statement.executeQuery("SELECT VERSION()")) {
            resultSet.next();
            return MySqlVersion.parse(resultSet.getString(1));
        } catch (Exception e) {
            log.info(
                    "Failed to get mysql version, fallback to default version: {}",
                    MySqlVersion.V_5_7,
                    e);
            return MySqlVersion.V_5_7;
        }
    }
}
