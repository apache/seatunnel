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
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            throw new CatalogException("", e);
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        String query = DorisCatalogUtil.getDatabaseQuery();
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new CatalogException("", e);
        }
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        String query = DorisCatalogUtil.getAllDatabasesQuery();
        List<String> databases = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String database = rs.getString(1);
                databases.add(database);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Collections.sort(databases);
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        String query = DorisCatalogUtil.getTablesQueryWithDatabase();
        List<String> tables = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, databaseName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String table = rs.getString(1);
                tables.add(table);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Collections.sort(tables);
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        String query = DorisCatalogUtil.getTablesQueryWithIdentifier();
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, tablePath.getDatabaseName());
            ps.setString(2, tablePath.getTableName());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        TableSchema.Builder builder = TableSchema.builder();
        String query = DorisCatalogUtil.getTableSchemaQuery();
        try (PreparedStatement ps = conn.prepareStatement(query)) {

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
            throw new RuntimeException(e);
        }

        String comment = "";

        return CatalogTable.of(
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                connectorOptions(),
                Collections.emptyList(),
                comment);
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
                        dorisConfig.getCreateTableTemplate(),
                        tablePath,
                        table,
                        Arrays.asList(dorisConfig.getCreateDistributionColumns().split(",")),
                        dorisConfig.getCreateDistributionBucket(),
                        dorisConfig.getCreateProperties());

        try (Statement statement = conn.createStatement()) {
            statement.execute(stmt);
        } catch (SQLException e) {
            throw new CatalogException("create table statement execute failed", e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
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
            throw new CatalogException("", e);
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
            throw new CatalogException("", e);
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
}
