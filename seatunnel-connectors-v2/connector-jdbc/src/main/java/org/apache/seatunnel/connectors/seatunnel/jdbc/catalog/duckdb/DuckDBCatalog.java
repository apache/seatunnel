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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MysqlCreateTableSqlBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb.DuckDBTypeMapper;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql.MySqlTypeConverter;

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
import java.util.List;
import java.util.Optional;

/** DuckDB catalog implementation. */
@Slf4j
public class DuckDBCatalog extends AbstractJdbcCatalog {

    private final DuckDBTypeConverter typeConverter;

    public DuckDBCatalog(String catalogName, JdbcUrlUtil.UrlInfo urlInfo, String defaultSchema) {
        super(catalogName, "", "", urlInfo, defaultSchema, "org.duckdb.DuckDBDriver");
        this.typeConverter = new DuckDBTypeConverter();
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return true;
    }

    @Override
    public String getTableWithConditionSql(TablePath tablePath){
        return String.format("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'",tablePath.getSchemaName(),tablePath.getTableName());
    }

    @Override
    protected List<String> getCreateTableSqls(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return DuckDBCreateTableSqlBuilder.builder(tablePath, table, typeConverter, createIndex)
                .build(tablePath);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT schema_name FROM information_schema.schemata WHERE schema_name != 'information_schema'";
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return String.format(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'",
                databaseName);
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        return rs.getString(1);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }


    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }

        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format(
                                        "DROP TABLE %s", tablePath.getSchemaAndTableName("\"")))) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to drop table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws CatalogException {
        String databaseName = tablePath.getDatabaseName();
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new CatalogException(String.format("Database %s already exists", databaseName));
        }

        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("CREATE SCHEMA \"%s\"", databaseName))) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to create database %s", databaseName), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String databaseName = tablePath.getDatabaseName();
        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        try (Connection conn = getConnection(defaultUrl);
                PreparedStatement ps =
                        conn.prepareStatement(
                                String.format("DROP SCHEMA \"%s\" CASCADE", databaseName))) {
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed to drop database %s", databaseName), e);
        }
    }
}
