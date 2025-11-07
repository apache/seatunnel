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

package org.apache.seatunnel.connectors.seatunnel.databend.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
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
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.util.DatabendUtil;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DatabendCatalog implements Catalog {
    private static final String DATABEND_DRIVER_NAME = "com.databend.jdbc.DatabendDriver";
    private final String catalogName;
    protected String defaultDatabase;
    private boolean isOpened;
    private ReadonlyConfig readonlyConfig;

    static {
        try {
            Class.forName(DATABEND_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.DRIVER_NOT_FOUND,
                    "Cannot find Databend JDBC driver",
                    e);
        }
    }

    public DatabendCatalog(ReadonlyConfig readonlyConfig, String catalogName) {
        this.catalogName = catalogName;
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        String databaseName = tablePath.getDatabaseName();
        createDatabase(databaseName, ignoreIfExists);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        String databaseName = tablePath.getDatabaseName();
        dropDatabase(databaseName, ignoreIfNotExists);
    }

    @Override
    public void open() throws CatalogException {
        if (isOpened) {
            return;
        }

        try (Connection connection = getConnection()) {
            log.info("Successfully connected to Databend");
            isOpened = true;
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to connect to Databend server: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void close() throws CatalogException {
        // Databend JDBC connections are closed after use
        isOpened = false;
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
        checkOpen();
        try (Connection connection = getConnection()) {
            try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
                while (resultSet.next()) {
                    String foundDb = resultSet.getString("table_schema");
                    if (databaseName.equalsIgnoreCase(foundDb)) {
                        return true;
                    }
                }
            }
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to check if database exists: " + e.getMessage(),
                    e);
        }
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        checkOpen();
        try (Connection connection = getConnection()) {
            List<String> databases = new ArrayList<>();
            try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
                while (resultSet.next()) {
                    String databaseName = resultSet.getString("TABLE_SCHEM");
                    databases.add(databaseName);
                }
            }
            return databases;
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to list databases: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        checkOpen();
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        try (Connection connection = getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            List<String> tables = new ArrayList<>();
            try (ResultSet resultSet =
                    metaData.getTables(null, databaseName, null, new String[] {"TABLE"})) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("TABLE_NAME");
                    tables.add(tableName);
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to list tables: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkOpen();
        try (Connection connection = getConnection()) {
            String databaseName = tablePath.getDatabaseName();
            String tableName = tablePath.getTableName();

            try (ResultSet resultSet =
                    connection
                            .getMetaData()
                            .getTables(null, databaseName, tableName, new String[] {"TABLE"})) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to check if table exists: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        checkOpen();

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        try (Connection connection = getConnection()) {
            String databaseName = tablePath.getDatabaseName();
            String tableName = tablePath.getTableName();

            // Get table schema
            List<Column> columns = new ArrayList<>();
            try (ResultSet resultSet =
                    connection.getMetaData().getColumns(null, databaseName, tableName, null)) {
                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    String typeName = resultSet.getString("TYPE_NAME");
                    int dataType = resultSet.getInt("DATA_TYPE");
                    int columnSize = resultSet.getInt("COLUMN_SIZE");
                    int decimalDigits = resultSet.getInt("DECIMAL_DIGITS");
                    String isNullable = resultSet.getString("IS_NULLABLE");
                    String remarks = resultSet.getString("REMARKS");

                    // Convert JDBC type to SeaTunnel type
                    SeaTunnelDataType<?> seaTunnelType =
                            convertDatabendType(typeName, dataType, columnSize, decimalDigits);

                    // Create column with proper nullability
                    PhysicalColumn.PhysicalColumnBuilder builder =
                            PhysicalColumn.builder()
                                    .name(columnName)
                                    .dataType(seaTunnelType)
                                    .nullable("YES".equalsIgnoreCase(isNullable));

                    if (remarks != null && !remarks.isEmpty()) {
                        builder.comment(remarks);
                    }
                    columns.add(builder.build());
                }
            }

            // Create table schema
            TableSchema tableSchema = TableSchema.builder().columns(columns).build();

            // Get table properties
            Map<String, String> properties = new HashMap<>();
            properties.put("connector", "databend");
            properties.put("url", readonlyConfig.get(DatabendOptions.URL));
            properties.put("username", readonlyConfig.get(DatabendOptions.USERNAME));
            properties.put("password", readonlyConfig.get(DatabendOptions.PASSWORD));
            properties.put("database", readonlyConfig.get(DatabendOptions.DATABASE));
            properties.put("table", readonlyConfig.get(DatabendOptions.TABLE));

            TableIdentifier tableIdentifier =
                    TableIdentifier.of(catalogName, databaseName, tableName);

            return CatalogTable.of(
                    tableIdentifier,
                    tableSchema,
                    properties,
                    Collections.emptyList(), // partitionKeys
                    null, // comment
                    "false"); // isView
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to get table metadata: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkOpen();

        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();

        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        if (tableExists(tablePath)) {
            if (ignoreIfExists) {
                return;
            }
            throw new TableAlreadyExistException(catalogName, tablePath);
        }

        String createTableSql =
                buildCreateTableSql(databaseName, tableName, table.getTableSchema());

        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to create table: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkOpen();

        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(catalogName, tablePath);
        }

        String databaseName = tablePath.getDatabaseName();
        String tableName = tablePath.getTableName();

        String dropTableSql = String.format("DROP TABLE %s.%s", databaseName, tableName);

        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(dropTableSql);
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to drop table: " + e.getMessage(),
                    e);
        }
    }

    public void createDatabase(String databaseName, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        checkOpen();

        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(catalogName, databaseName);
        }

        String createDatabaseSql = String.format("CREATE DATABASE %s", databaseName);

        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(createDatabaseSql);
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to create database: " + e.getMessage(),
                    e);
        }
    }

    public void dropDatabase(String databaseName, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkOpen();

        if (!databaseExists(databaseName)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        String dropDatabaseSql = String.format("DROP DATABASE %s", databaseName);

        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(dropDatabaseSql);
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to drop database: " + e.getMessage(),
                    e);
        }
    }

    private String buildCreateTableSql(
            String databaseName, String tableName, TableSchema tableSchema) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(databaseName).append(".").append(tableName).append(" (");

        List<Column> columns = tableSchema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            sb.append(column.getName()).append(" ");
            sb.append(toDatabendTypeString(column.getDataType()));

            if (!column.isNullable()) {
                sb.append(" NOT NULL");
            }

            if (i < columns.size() - 1) {
                sb.append(", ");
            }
        }

        sb.append(")");
        return sb.toString();
    }

    private String toDatabendTypeString(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case BOOLEAN:
                return "BOOLEAN";
            case TINYINT:
                return "TINYINT";
            case SMALLINT:
                return "SMALLINT";
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                DecimalType decimalType = (DecimalType) dataType;
                return String.format(
                        "DECIMAL(%d, %d)", decimalType.getPrecision(), decimalType.getScale());
            case BYTES:
                return "VARBINARY";
            case STRING:
                return "VARCHAR";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                LocalTimeType timeType = (LocalTimeType) dataType;
                return String.format("TIMESTAMP(%d)");
            default:
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + dataType.getSqlType());
        }
    }

    private SeaTunnelDataType<?> convertDatabendType(
            String typeName, int sqlType, int columnSize, int decimalDigits) {
        // This method should convert Databend data types to SeaTunnel data types
        // This is a simplified version, you'll need to adjust based on Databend's actual type
        // system
        typeName = typeName.toUpperCase();

        switch (typeName) {
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "INT8":
                return BasicType.BYTE_TYPE;
            case "SMALLINT":
            case "INT16":
                return BasicType.SHORT_TYPE;
            case "INT":
            case "INTEGER":
            case "INT32":
                return BasicType.INT_TYPE;
            case "BIGINT":
            case "INT64":
                return BasicType.LONG_TYPE;
            case "FLOAT":
            case "FLOAT32":
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
            case "FLOAT64":
                return BasicType.DOUBLE_TYPE;
            case "DECIMAL":
                return new DecimalType(columnSize, decimalDigits);
            case "STRING":
            case "VARCHAR":
            case "CHAR":
            case "TEXT":
                return BasicType.STRING_TYPE;
            case "DATE":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "TIMESTAMP":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "VARBINARY":
            case "BINARY":
                return BasicType.BYTE_TYPE;
            default:
                log.warn("Unsupported Databend type: {}, fallback to STRING type", typeName);
                return BasicType.STRING_TYPE;
        }
    }

    private Connection getConnection() throws SQLException {
        return DatabendUtil.createConnection(this.readonlyConfig);
    }

    private void checkOpen() {
        if (!isOpened) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.ILLEGAL_STATE,
                    "Databend catalog is not opened. Please call open() first.");
        }
    }
}
