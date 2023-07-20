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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class SqlServerCatalog extends AbstractJdbcCatalog {

    private static final Set<String> SYS_DATABASES = new HashSet<>(4);

    static {
        SYS_DATABASES.add("master");
        SYS_DATABASES.add("tempdb");
        SYS_DATABASES.add("model");
        SYS_DATABASES.add("msdb");
    }

    public SqlServerCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps = conn.prepareStatement("SELECT NAME FROM sys.databases")) {

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                if (!SYS_DATABASES.contains(databaseName)) {
                    databases.add(databaseName);
                }
            }

            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        String dbUrl = getUrlFromDatabaseName(databaseName);
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                "SELECT TABLE_SCHEMA, TABLE_NAME FROM "
                                        + databaseName
                                        + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")) {

            ResultSet rs = ps.executeQuery();

            List<String> tables = new ArrayList<>();

            while (rs.next()) {
                tables.add(rs.getString(1) + "." + rs.getString(2));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName())
                            .contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        String tableSql =
                StringUtils.isNotEmpty(tablePath.getTableName())
                        ? "AND tbl.name = '" + tablePath.getTableName() + "'"
                        : "";

        String columnSql =
                String.format(
                        "    SELECT tbl.name AS table_name, \n           col.name AS column_name, \n           ext.value AS comment, \n           col.column_id AS column_id, \n           types.name AS type, \n           col.max_length AS max_length, \n           col.precision AS precision, \n           col.scale AS scale, \n           col.is_nullable AS is_nullable, \n def.definition AS default_value\n     FROM sys.tables tbl \nINNER JOIN sys.columns col \n        ON tbl.object_id = col.object_id \n LEFT JOIN sys.types types \n        ON col.user_type_id = types.user_type_id \n LEFT JOIN sys.extended_properties ext \n        ON ext.major_id = col.object_id and ext.minor_id = col.column_id \n   LEFT JOIN sys.default_constraints def ON col.default_object_id = def.object_id \n    AND ext.minor_id = col.column_id \n       AND ext.name = 'MS_Description' \n     WHERE schema_name(tbl.schema_id) = '%s' \n       %s \n  ORDER BY tbl.name, col.column_id",
                        tablePath.getSchemaName(), tableSql);

        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());

            try (PreparedStatement ps = conn.prepareStatement(columnSql);
                    ResultSet resultSet = ps.executeQuery(); ) {
                TableSchema.Builder builder = TableSchema.builder();
                while (resultSet.next()) {
                    buildTable(resultSet, builder);
                }

                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "",
                        "sqlserver");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private void buildTable(ResultSet resultSet, TableSchema.Builder builder) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String sourceType = resultSet.getString("type");
        //        String typeName = resultSet.getString("DATA_TYPE").toUpperCase();
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        SeaTunnelDataType<?> type = fromJdbcType(sourceType, precision, scale);
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        if (defaultValue != null) {
            defaultValue =
                    defaultValue.toString().replace("(", "").replace("'", "").replace(")", "");
        }
        boolean isNullable = resultSet.getBoolean("is_nullable");
        long bitLen = 0;
        StringBuilder sb = new StringBuilder(sourceType);
        Pair<SqlServerType, Map<String, Object>> parse = SqlServerType.parse(sourceType);
        switch (parse.getLeft()) {
            case BINARY:
            case VARBINARY:
                // Uniform conversion to bits
                if (columnLength != -1) {
                    bitLen = columnLength * 4 * 8;
                    sourceType = sb.append("(").append(columnLength).append(")").toString();
                } else {
                    sourceType = sb.append("(").append("max").append(")").toString();
                    bitLen = columnLength;
                }
                break;
            case TIMESTAMP:
                bitLen = columnLength << 3;
                break;
            case VARCHAR:
            case NCHAR:
            case NVARCHAR:
            case CHAR:
                if (columnLength != -1) {
                    sourceType = sb.append("(").append(columnLength).append(")").toString();
                } else {
                    sourceType = sb.append("(").append("max").append(")").toString();
                }
                break;
            case DECIMAL:
            case NUMERIC:
                sourceType =
                        sb.append("(")
                                .append(precision)
                                .append(",")
                                .append(scale)
                                .append(")")
                                .toString();
                break;
            case TEXT:
                columnLength = Integer.MAX_VALUE;
                break;
            case NTEXT:
                columnLength = Integer.MAX_VALUE >> 1;
                break;
            case IMAGE:
                bitLen = Integer.MAX_VALUE * 8L;
                break;
            default:
                break;
        }
        PhysicalColumn physicalColumn =
                PhysicalColumn.of(
                        columnName,
                        type,
                        0,
                        isNullable,
                        defaultValue,
                        comment,
                        sourceType,
                        false,
                        false,
                        bitLen,
                        null,
                        columnLength);
        builder.column(physicalColumn);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, int precision, int scale) {
        Pair<SqlServerType, Map<String, Object>> pair = SqlServerType.parse(typeName);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(SqlServerDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(SqlServerDataTypeConvertor.SCALE, scale);
        return new SqlServerDataTypeConvertor().toSeaTunnelType(pair.getLeft(), dataTypeProperties);
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {

        String createTableSql =
                SqlServerCreateTableSqlBuilder.builder(tablePath, table).build(tablePath, table);
        log.info("create table sql: {}", createTableSql);
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps = conn.prepareStatement(createTableSql)) {
            System.out.println(createTableSql);
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed creating table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format(
                                        "DROP TABLE IF EXISTS %s", tablePath.getFullName()))) {
            // Will there exist concurrent drop for one table?
            return ps.execute();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed dropping table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("CREATE DATABASE `%s`", databaseName))) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed creating database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("DROP DATABASE IF EXISTS `%s`;", databaseName))) {
            return ps.execute();
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed dropping database %s in catalog %s",
                            databaseName, this.catalogName),
                    e);
        }
    }

    @SuppressWarnings("unchecked")
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        Pair<SqlServerType, Map<String, Object>> pair =
                SqlServerType.parse(metadata.getColumnTypeName(colIndex));
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(
                SqlServerDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(SqlServerDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return new SqlServerDataTypeConvertor().toSeaTunnelType(pair.getLeft(), dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", getUrlFromDatabaseName(tablePath.getDatabaseName()));
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    private String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + ";databaseName=" + databaseName + ";" + suffix;
    }

    private String getCreateTableSql(TablePath tablePath, CatalogTable table) {

        return "";
    }
}
