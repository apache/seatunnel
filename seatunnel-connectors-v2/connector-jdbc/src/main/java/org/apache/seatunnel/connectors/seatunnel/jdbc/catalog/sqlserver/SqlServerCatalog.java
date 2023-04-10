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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import org.apache.commons.lang3.tuple.Pair;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SqlServerCatalog extends AbstractJdbcCatalog {

    private static final Set<String> SYS_DATABASES = new HashSet<>(4);

    static {
        SYS_DATABASES.add("master");
        SYS_DATABASES.add("tempdb");
        SYS_DATABASES.add("model");
        SYS_DATABASES.add("msdb");
    }

    public SqlServerCatalog(
            String catalogName, String username, String pwd, JdbcUrlUtil.UrlInfo urlInfo) {
        super(catalogName, username, pwd, urlInfo);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd);
                PreparedStatement ps = conn.prepareStatement("SELECT NAME FROM SYS.DATABASES")) {

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
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        throw new UnsupportedOperationException("Unsupported create table");
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        String dbUrl = getUrlFromDatabaseName(tablePath.getDatabaseName());
        try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("DROP TABLE IF EXIST %s", tablePath.getFullName()))) {
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

    @Override
    @SuppressWarnings("unchecked")
    public SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
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
    @Override
    public Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", getUrlFromDatabaseName(tablePath.getDatabaseName()));
        options.put("table-name", tablePath.getFullName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }

    @Override
    public String getUrlFromDatabaseName(String databaseName) {
        return baseUrl + ";databaseName=" + databaseName + ";" + suffix;
    }
}
