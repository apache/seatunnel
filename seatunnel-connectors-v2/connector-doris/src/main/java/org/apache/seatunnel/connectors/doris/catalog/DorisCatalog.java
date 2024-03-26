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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.SQLPreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.doris.config.DorisConfig;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.util.DorisCatalogUtil;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DorisCatalog implements Catalog {

    private static final Logger LOG = LoggerFactory.getLogger(DorisCatalog.class);

    private final String catalogName;

    private final String[] frontEndNodes;

    private final Integer queryPort;

    private final String username;

    private final String password;

    private String defaultDatabase = "information_schema";

    private Connection conn;

    private DorisConfig dorisConfig;

    public DorisCatalog(
            String catalogName,
            String frontEndNodes,
            Integer queryPort,
            String username,
            String password) {
        this.catalogName = catalogName;
        this.frontEndNodes = frontEndNodes.split(",");
        this.queryPort = queryPort;
        this.username = username;
        this.password = password;
    }

    public DorisCatalog(
            String catalogName,
            String frontEndNodes,
            Integer queryPort,
            String username,
            String password,
            DorisConfig config) {
        this(catalogName, frontEndNodes, queryPort, username, password);
        this.dorisConfig = config;
    }

    public DorisCatalog(
            String catalogName,
            String frontEndNodes,
            Integer queryPort,
            String username,
            String password,
            DorisConfig config,
            String defaultDatabase) {
        this(catalogName, frontEndNodes, queryPort, username, password, config);
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        String jdbcUrl =
                DorisCatalogUtil.getJdbcUrl(
                        DorisCatalogUtil.randomFrontEndHost(frontEndNodes),
                        queryPort,
                        defaultDatabase);
        try {
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            conn.getCatalog();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed to connect url %s", jdbcUrl), e);
        }
        LOG.info("Catalog {} established connection to {} success", catalogName, jdbcUrl);
    }

    @Override
    public void close() throws CatalogException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new CatalogException("close doris catalog failed", e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.DATABASE_QUERY)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new CatalogException("check database exists failed", e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.ALL_DATABASES_QUERY)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String database = rs.getString(1);
                databases.add(database);
            }
        } catch (SQLException e) {
            throw new CatalogException("list databases failed", e);
        }
        Collections.sort(databases);
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tables = new ArrayList<>();
        try (PreparedStatement ps =
                conn.prepareStatement(DorisCatalogUtil.TABLES_QUERY_WITH_DATABASE_QUERY)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String table = rs.getString(1);
                tables.add(table);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("list tables of database [%s] failed", databaseName), e);
        }
        Collections.sort(tables);
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try (PreparedStatement ps =
                conn.prepareStatement(DorisCatalogUtil.TABLES_QUERY_WITH_IDENTIFIER_QUERY)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("check table [%s] exists failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        TableSchema.Builder builder = TableSchema.builder();
        try (PreparedStatement ps = conn.prepareStatement(DorisCatalogUtil.TABLE_SCHEMA_QUERY)) {

            List<String> keyList = new ArrayList<>();
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);
                int size = rs.getInt(6);
                boolean nullable = rs.getBoolean(4);
                String defaultVal = rs.getString(3);
                String comment = rs.getString(10);
                builder.column(
                        PhysicalColumn.of(
                                name,
                                DorisCatalogUtil.fromDorisType(rs),
                                size,
                                nullable,
                                defaultVal,
                                comment));
                if ("UNI".equalsIgnoreCase(rs.getString(7))) {
                    keyList.add(name);
                }
            }
            if (!keyList.isEmpty()) {
                builder.primaryKey(
                        PrimaryKey.of(
                                "uk_"
                                        + tablePath.getDatabaseName()
                                        + "_"
                                        + tablePath.getTableName(),
                                keyList));
            }

        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("get table [%s] failed", tablePath.getFullName()), e);
        }

        return CatalogTable.of(
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                connectorOptions(),
                Collections.emptyList(),
                StringUtils.EMPTY);
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }

        boolean tableExists = tableExists(tablePath);
        if (ignoreIfExists && tableExists) {
            LOG.info("table {} is exists, skip create", tablePath.getFullName());
            return;
        }

        if (tableExists) {
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        String stmt =
                DorisCatalogUtil.getCreateTableStatement(
                        dorisConfig.getCreateTableTemplate(), tablePath, table);

        try (Statement statement = conn.createStatement()) {
            statement.execute(stmt);
        } catch (SQLException e) {
            throw new CatalogException("create table statement execute failed", e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        String query = DorisCatalogUtil.getDropTableQuery(tablePath, ignoreIfNotExists);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        String query =
                DorisCatalogUtil.getCreateDatabaseQuery(
                        tablePath.getDatabaseName(), ignoreIfExists);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("create database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String query =
                DorisCatalogUtil.getDropDatabaseQuery(
                        tablePath.getDatabaseName(), ignoreIfNotExists);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("drop database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    private Map<String, String> connectorOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "doris");
        options.put(DorisOptions.FENODES.key(), String.join(",", frontEndNodes));
        options.put(DorisOptions.USERNAME.key(), username);
        options.put(DorisOptions.PASSWORD.key(), password);
        return options;
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            if (ignoreIfNotExists) {
                conn.createStatement().execute(DorisCatalogUtil.getTruncateTableQuery(tablePath));
            }
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed TRUNCATE TABLE in catalog %s", tablePath.getFullName()),
                    e);
        }
    }

    public boolean isExistsData(TablePath tablePath) {
        String tableName = tablePath.getFullName();
        String sql = String.format("select * from %s limit 1;", tableName);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ResultSet resultSet = ps.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            checkArgument(catalogTable.isPresent(), "CatalogTable cannot be null");
            return new SQLPreviewResult(
                    DorisCatalogUtil.getCreateTableStatement(
                            dorisConfig.getCreateTableTemplate(), tablePath, catalogTable.get()));
        } else if (actionType == ActionType.DROP_TABLE) {
            return new SQLPreviewResult(DorisCatalogUtil.getDropTableQuery(tablePath, true));
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new SQLPreviewResult(DorisCatalogUtil.getTruncateTableQuery(tablePath));
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new SQLPreviewResult(
                    DorisCatalogUtil.getCreateDatabaseQuery(tablePath.getDatabaseName(), true));
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new SQLPreviewResult(
                    DorisCatalogUtil.getDropDatabaseQuery(tablePath.getDatabaseName(), true));
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
