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

package org.apache.seatunnel.connectors.seatunnel.hive.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SaveModePlaceHolder;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.sink.HiveSinkOptions;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;
import shade.org.apache.commons.lang3.StringEscapeUtils;

import java.io.Serializable;
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
import java.util.stream.Collectors;

@Slf4j
public class HiveJDBCCatalog implements Catalog, Serializable {

    private final String catalogName = "Hive";
    private final ReadonlyConfig config;

    private Connection connection;

    public HiveJDBCCatalog(ReadonlyConfig config) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            throw new CatalogException(e);
        }
        this.config = config;
    }

    public HiveTable getTableInformation(TablePath tablePath) {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }
        String describeFormattedTableQuery = "describe formatted " + tablePath.getFullName();
        try (PreparedStatement ps = connection.prepareStatement(describeFormattedTableQuery)) {
            ResultSet rs = ps.executeQuery();
            return generateHiveTableFromQueryResult(rs, tablePath);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("get table information [%s] failed", tablePath.getFullName()), e);
        }
    }

    @Override
    public void open() throws CatalogException {
        try {
            String jdbcUrl = config.get(HiveConfig.HIVE_JDBC_URL);
            connection = DriverManager.getConnection(jdbcUrl);
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public void close() throws CatalogException {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement("SHOW DATABASES")) {
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
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        List<String> tables = new ArrayList<>();
        String TABLES_QUERY_WITH_DATABASE_QUERY = "SHOW TABLES IN " + databaseName;
        try (PreparedStatement ps = connection.prepareStatement(TABLES_QUERY_WITH_DATABASE_QUERY)) {
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
        return listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return getTableInformation(tablePath).getCatalogTable();
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
        if (tableExists(tablePath) && ignoreIfExists) {
            return;
        }
        String template = config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        Optional<List<String>> partitionKeyOptional =
                config.getOptional(HiveSinkOptions.SAVE_MODE_PARTITION_KEYS);
        List<Column> columns = table.getTableSchema().getColumns();
        if (partitionKeyOptional.isPresent()) {
            List<String> partitionKeys = partitionKeyOptional.get();
            columns =
                    columns.stream()
                            .filter(c -> !partitionKeys.contains(c.getName()))
                            .collect(Collectors.toList());
        }
        String columnDef =
                columns.stream()
                        .map(HiveTypeConvertor::columnToHiveType)
                        .collect(Collectors.joining(",\n"));

        String ddl =
                template.replaceAll(
                                SaveModePlaceHolder.DATABASE.getReplacePlaceHolder(),
                                tablePath.getDatabaseName())
                        .replaceAll(
                                SaveModePlaceHolder.TABLE_NAME.getReplacePlaceHolder(),
                                tablePath.getTableName())
                        .replace(SaveModePlaceHolder.ROWTYPE_FIELDS.getPlaceHolder(), columnDef);
        log.info("EXECUTE DDL SQL is \n {} \n", ddl);
        try (Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        String query = getDropTableQuery(tablePath, ignoreIfNotExists);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        String query = getCreateDatabaseQuery(tablePath.getDatabaseName(), ignoreIfExists);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("create database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String query = getDropDatabaseQuery(tablePath.getDatabaseName(), ignoreIfNotExists);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("drop database [%s] failed", tablePath.getDatabaseName()), e);
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Does not support truncate table!");
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        String tableName = tablePath.getFullName();
        String sql = String.format("select * from %s limit 1;", tableName);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ResultSet resultSet = ps.executeQuery();
            return resultSet.next();
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed executeSql error %s", sql), e);
        }
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        throw new UnsupportedOperationException("Does not support executing custom SQL");
    }

    private HiveTable generateHiveTableFromQueryResult(ResultSet rs, TablePath tablePath)
            throws SQLException {
        TableSchema.Builder builder = new TableSchema.Builder();
        List<String> partitionKeys = new ArrayList<>();
        String location = "";
        String inputFormat = "";
        Map<String, String> tableParameters = new HashMap<>();
        while (rs.next()) {
            String colName = rs.getString(1).trim();
            switch (colName) {
                case "# col_name":
                    addCols(rs, builder, partitionKeys, false);
                    break;
                case "# Partition Information":
                    addCols(rs, builder, partitionKeys, true);
                    break;
                case "Location:":
                    location = rs.getString(2);
                    break;
                case "InputFormat:":
                    inputFormat = rs.getString(2);
                    break;
                case "Storage Desc Params:":
                    addTableParameters(rs, tableParameters);
                    break;
            }
        }
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(
                                "Hive", tablePath.getDatabaseName(), tablePath.getTableName()),
                        builder.build(),
                        Collections.emptyMap(),
                        partitionKeys,
                        null);
        return HiveTable.of(catalogTable, tableParameters, inputFormat, location);
    }

    private void addCols(
            ResultSet rs,
            TableSchema.Builder builder,
            List<String> partitionKeys,
            boolean isPartition)
            throws SQLException {
        while (rs.next()) {
            String name = rs.getString(1).trim();
            if (StringUtils.isEmpty(name) && builder.currentColumnSize() != 0) {
                // the currentColumnSize != 0 is to compatible different hive version.
                // some hive version's query result will be:
                // | # col_name   | data_type | comment |
                // |              | NULL      | NULL    |
                // | xxx          | xxx       | xxx     |
                // and some version won't return the blank row, like this.
                // | # col_name   | data_type | comment |
                // | xxx          | xxx       | xxx     |
                return;
            }
            String colType = rs.getString(2);
            if (StringUtils.isEmpty(colType) || "data_type".equals(colType)) {
                continue;
            }
            String comment = rs.getString(3);
            if (isPartition) {
                partitionKeys.add(name);
            }
            builder.column(
                    PhysicalColumn.of(
                            name,
                            HiveTypeConvertor.covertHiveTypeToSeaTunnelType(name, colType),
                            (Long) null,
                            true,
                            null,
                            comment));
        }
    }

    private void addTableParameters(ResultSet rs, Map<String, String> parameters)
            throws SQLException {
        while (rs.next()) {
            String key = rs.getString(2);
            if (key == null || StringUtils.isEmpty(key)) {
                return;
            }
            parameters.put(key.trim(), StringEscapeUtils.unescapeJava(rs.getString(3).trim()));
        }
    }

    private String getCreateDatabaseQuery(String database, boolean ignoreIfExists) {
        return "CREATE DATABASE " + (ignoreIfExists ? "IF NOT EXISTS " : "") + database;
    }

    private String getDropTableQuery(TablePath tablePath, boolean ignoreIfNotExists) {
        return "DROP TABLE " + (ignoreIfNotExists ? "IF EXISTS " : "") + tablePath.getFullName();
    }

    private String getDropDatabaseQuery(String database, boolean ignoreIfNotExists) {
        return "DROP DATABASE " + (ignoreIfNotExists ? "IF EXISTS " : "") + database;
    }

    public void msckRepairTable(String dbName, String tableName) {
        String query = String.format("MSCK REPAIR TABLE %s.%s", dbName, tableName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("msck repair table [%s.%s] failed", dbName, tableName), e);
        }
    }
}
