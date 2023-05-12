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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.commons.collections4.CollectionUtils;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class OracleCatalog implements Catalog {
    private static final OracleDataTypeConvertor DATA_TYPE_CONVERTOR =
            new OracleDataTypeConvertor();
    private static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "APPQOSSYS",
                            "AUDSYS",
                            "CTXSYS",
                            "DVSYS",
                            "DBSFWUSER",
                            "DBSNMP",
                            "GSMADMIN_INTERNAL",
                            "LBACSYS",
                            "MDSYS",
                            "OJVMSYS",
                            "OLAPSYS",
                            "ORDDATA",
                            "ORDSYS",
                            "OUTLN",
                            "SYS",
                            "SYSTEM",
                            "WMSYS",
                            "XDB",
                            "EXFSYS",
                            "SYSMAN"));

    private final String catalogName;
    private final String username;
    private final String pwd;
    private final String jdbcURL;

    protected final Map<String, Connection> connectionMap;

    public OracleCatalog(String catalogName, String username, String pwd, String jdbcURL) {
        this.catalogName = catalogName;
        this.username = username;
        this.pwd = pwd;
        this.jdbcURL = jdbcURL;
        this.connectionMap = new ConcurrentHashMap<>();
    }

    public Connection getConnection(String url) {
        if (connectionMap.containsKey(url)) {
            return connectionMap.get(url);
        }
        try {
            Connection connection = DriverManager.getConnection(url, username, pwd);
            connectionMap.put(url, connection);
            return connection;
        } catch (SQLException e) {
            throw new CatalogException(String.format("Failed connecting to %s via JDBC.", url), e);
        }
    }

    @Override
    public void open() throws CatalogException {
        try (Connection conn = DriverManager.getConnection(jdbcURL, username, pwd)) {
            // test connection, fail early if we cannot connect to database
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format("Failed connecting to %s via JDBC.", jdbcURL), e);
        }

        log.info("Catalog {} established connection to {}", catalogName, jdbcURL);
    }

    @Override
    public void close() throws CatalogException {
        log.info("Catalog {} closing", catalogName);
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported create table");
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported drop table");
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported create database");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException("Unsupported drop database");
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return listDatabases().get(0);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {

        try (PreparedStatement ps =
                getConnection(jdbcURL).prepareStatement("SELECT name FROM v$database")) {

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                databases.add(databaseName);
            }
            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
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
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        try (PreparedStatement ps =
                getConnection(jdbcURL)
                        .prepareStatement(
                                "SELECT OWNER, TABLE_NAME FROM ALL_TABLES\n"
                                        + "WHERE TABLE_NAME NOT LIKE 'MDRT_%'\n"
                                        + "  AND TABLE_NAME NOT LIKE 'MDRS_%'\n"
                                        + "  AND TABLE_NAME NOT LIKE 'MDXT_%'\n"
                                        + "  AND (TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%' AND IOT_NAME IS NULL)")) {

            ResultSet rs = ps.executeQuery();
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
                    continue;
                }
                tables.add(rs.getString(1) + "." + rs.getString(2));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        try {
            DatabaseMetaData metaData = getConnection(jdbcURL).getMetaData();
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

            try (PreparedStatement ps =
                    getConnection(jdbcURL)
                            .prepareStatement(
                                    String.format(
                                            "SELECT * FROM %s WHERE 1 = 0",
                                            tablePath.getSchemaAndTableName()))) {
                ResultSetMetaData tableMetaData = ps.getMetaData();
                TableSchema.Builder builder = TableSchema.builder();
                // add column
                for (int i = 1; i <= tableMetaData.getColumnCount(); i++) {
                    String columnName = tableMetaData.getColumnName(i);
                    SeaTunnelDataType<?> type = fromJdbcType(tableMetaData, i);
                    int columnDisplaySize = tableMetaData.getColumnDisplaySize(i);
                    String comment = tableMetaData.getColumnLabel(i);
                    boolean isNullable =
                            tableMetaData.isNullable(i) == ResultSetMetaData.columnNullable;
                    Object defaultValue =
                            getColumnDefaultValue(
                                            metaData,
                                            tablePath.getDatabaseName(),
                                            tablePath.getSchemaName(),
                                            tablePath.getTableName(),
                                            columnName)
                                    .orElse(null);

                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    columnName,
                                    type,
                                    columnDisplaySize,
                                    isNullable,
                                    defaultValue,
                                    comment);
                    builder.column(physicalColumn);
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
                        "");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private Optional<PrimaryKey> getPrimaryKey(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {

        // According to the Javadoc of java.sql.DatabaseMetaData#getPrimaryKeys,
        // the returned primary key columns are ordered by COLUMN_NAME, not by KEY_SEQ.
        // We need to sort them based on the KEY_SEQ value.
        ResultSet rs = metaData.getPrimaryKeys(database, schema, table);

        // seq -> column name
        List<Pair<Integer, String>> primaryKeyColumns = new ArrayList<>();
        String pkName = null;
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            // all the PK_NAME should be the same
            pkName = rs.getString("PK_NAME");
            int keySeq = rs.getInt("KEY_SEQ");
            // KEY_SEQ is 1-based index
            primaryKeyColumns.add(Pair.of(keySeq, columnName));
        }
        // initialize size
        List<String> pkFields =
                primaryKeyColumns.stream()
                        .sorted(Comparator.comparingInt(Pair::getKey))
                        .map(Pair::getValue)
                        .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(pkFields)) {
            return Optional.empty();
        }
        return Optional.of(PrimaryKey.of(pkName, pkFields));
    }

    private List<ConstraintKey> getConstraintKeys(
            DatabaseMetaData metaData, String database, String schema, String table)
            throws SQLException {
        ResultSet resultSet = metaData.getIndexInfo(database, schema, table, false, false);
        // index name -> index
        Map<String, ConstraintKey> constraintKeyMap = new HashMap<>();
        while (resultSet.next()) {
            String indexName = resultSet.getString("INDEX_NAME");
            String columnName = resultSet.getString("COLUMN_NAME");
            String unique = resultSet.getString("NON_UNIQUE");

            ConstraintKey constraintKey =
                    constraintKeyMap.computeIfAbsent(
                            indexName,
                            s -> {
                                ConstraintKey.ConstraintType constraintType =
                                        ConstraintKey.ConstraintType.KEY;
                                // 0 is unique.
                                if ("0".equals(unique)) {
                                    constraintType = ConstraintKey.ConstraintType.UNIQUE_KEY;
                                }
                                return ConstraintKey.of(
                                        constraintType, indexName, new ArrayList<>());
                            });

            ConstraintKey.ColumnSortType sortType =
                    "A".equals(resultSet.getString("ASC_OR_DESC"))
                            ? ConstraintKey.ColumnSortType.ASC
                            : ConstraintKey.ColumnSortType.DESC;
            ConstraintKey.ConstraintKeyColumn constraintKeyColumn =
                    new ConstraintKey.ConstraintKeyColumn(columnName, sortType);
            constraintKey.getColumnNames().add(constraintKeyColumn);
        }
        return new ArrayList<>(constraintKeyMap.values());
    }

    private Optional<String> getColumnDefaultValue(
            DatabaseMetaData metaData, String database, String schema, String table, String column)
            throws SQLException {
        try (ResultSet resultSet = metaData.getColumns(database, schema, table, column)) {
            while (resultSet.next()) {
                String defaultValue = resultSet.getString("COLUMN_DEF");
                return Optional.ofNullable(defaultValue);
            }
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(OracleDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(OracleDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(columnType, dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", jdbcURL);
        options.put("table-name", tablePath.getSchemaAndTableName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }
}
