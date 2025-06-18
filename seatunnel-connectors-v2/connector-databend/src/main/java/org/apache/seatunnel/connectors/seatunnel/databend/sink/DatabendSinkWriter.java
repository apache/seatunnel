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

package org.apache.seatunnel.connectors.seatunnel.databend.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.schema.SchemaChangeManager;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
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
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DatabendSinkWriter
        implements SinkWriter<SeaTunnelRow, Void, Void>, SupportSchemaEvolutionSinkWriter {

    private final Connection connection;
    private final Context context;
    private final CatalogTable catalogTable;
    private String insertSql;
    private final int batchSize;
    private final int executeTimeoutSec;
    private TableSchema tableSchema;
    private final TablePath sinkTablePath;
    protected TableSchemaChangeEventDispatcher tableSchemaChanger =
            new TableSchemaChangeEventDispatcher();
    private SchemaChangeManager schemaChangeManager;
    private PreparedStatement preparedStatement;
    private int batchCount = 0;
    private DatabendSinkConfig databendSinkConfig;

    public DatabendSinkWriter(
            Context context,
            Connection connection,
            CatalogTable catalogTable,
            DatabendSinkConfig databendSinkConfig,
            String customSql,
            String database,
            String table,
            int batchSize,
            int executeTimeoutSec) {
        this.context = context;
        this.connection = connection;
        this.catalogTable = catalogTable;
        this.databendSinkConfig = databendSinkConfig;
        this.batchSize = batchSize;
        this.executeTimeoutSec = executeTimeoutSec;
        this.tableSchema = catalogTable.getTableSchema();
        this.sinkTablePath = TablePath.of(database, table);

        log.info("DatabendSinkWriter constructor - catalogTable: {}", catalogTable);
        log.info("DatabendSinkWriter constructor - tableSchema: {}", tableSchema);
        log.info(
                "DatabendSinkWriter constructor - rowType: {}", catalogTable.getSeaTunnelRowType());
        log.info("DatabendSinkWriter constructor - target table path: {}", sinkTablePath);

        // if custom SQL is provided, use it directly
        if (customSql != null && !customSql.isEmpty()) {
            this.insertSql = customSql;
            log.info("Using custom SQL: {}", insertSql);
            try {
                this.schemaChangeManager = new SchemaChangeManager(databendSinkConfig);
                this.preparedStatement = connection.prepareStatement(insertSql);
                this.preparedStatement.setQueryTimeout(executeTimeoutSec);
                log.info("PreparedStatement created successfully with custom SQL");
            } catch (SQLException e) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to prepare custom statement: " + e.getMessage(),
                        e);
            }
        } else {
            // use the catalog table schema to create the target table
            SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
            if (rowType == null || rowType.getFieldNames().length == 0) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SCHEMA_NOT_FOUND,
                        "Source table schema is empty or null");
            }

            try {
                if (!tableExists(database, table)) {
                    log.info(
                            "Target table {}.{} does not exist, creating with source schema",
                            database,
                            table);
                    createTable(database, table, rowType);
                } else {
                    log.info("Target table {}.{} exists, verifying schema", database, table);
                    verifyTableSchema(database, table, rowType);
                }
            } catch (SQLException e) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to verify/create target table: " + e.getMessage(),
                        e);
            }

            this.insertSql = generateInsertSql(database, table, rowType);
            log.info("Generated insert SQL: {}", insertSql);
            try {
                this.schemaChangeManager = new SchemaChangeManager(databendSinkConfig);
                this.preparedStatement = connection.prepareStatement(insertSql);
                this.preparedStatement.setQueryTimeout(executeTimeoutSec);
                log.info("PreparedStatement created successfully");
            } catch (SQLException e) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to prepare statement: " + e.getMessage(),
                        e);
            }
        }
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        try {
            // save the current batch
            executeBatch();

            // update the table schema
            this.tableSchema = tableSchemaChanger.reset(tableSchema).apply(event);

            // update the catalog table
            schemaChangeManager.applySchemaChange(sinkTablePath, event);

            // close the old prepared statement
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    log.warn("Failed to close PreparedStatement during schema change", e);
                } finally {
                    preparedStatement = null;
                }
            }

            // update the insert SQL statement
            this.insertSql = generateInsertSql(catalogTable, tableSchema);

            this.batchCount = 0;

            log.info(
                    "Schema change applied successfully for table {}", sinkTablePath.getFullName());
        } catch (Exception e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to apply schema change: " + e.getMessage(),
                    e);
        }
    }

    /** According to the table schema, generate the insert SQL statement */
    private String generateInsertSql(CatalogTable catalogTable, TableSchema tableSchema) {
        String tableName = catalogTable.getTablePath().getFullName();

        List<String> columnNames =
                tableSchema.getColumns().stream()
                        .map(column -> "`" + column.getName() + "`")
                        .collect(Collectors.toList());

        String placeholders = String.join(", ", Collections.nCopies(columnNames.size(), "?"));

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName, String.join(", ", columnNames), placeholders);
    }

    @Override
    public void write(SeaTunnelRow row) {
        try {
            log.info("Writing row: {}", row);

            // check if row is null or empty
            if (row == null || row.getFields() == null || row.getFields().length == 0) {
                log.warn("Received empty row data, skipping");
                return;
            }

            if (preparedStatement == null) {
                log.info("PreparedStatement is null, initializing...");
                initializePreparedStatement(row);
                log.info("PreparedStatement initialized successfully");
            }

            boolean allFieldsNull = true;
            for (Object field : row.getFields()) {
                if (field != null) {
                    allFieldsNull = false;
                    break;
                }
            }

            if (allFieldsNull) {
                log.warn("All fields in row are null, skipping");
                return;
            }

            processRow(row);
            batchCount++;
            log.info("Batch count after adding row: {}", batchCount);

            if (batchCount >= batchSize) {
                log.info("Batch size {} reached, executing batch", batchSize);
                executeBatch();
                log.info("Batch executed successfully");
            }
        } catch (Exception e) {
            log.error("Failed to write row: {}", row, e);
            // tru to execute the remaining batch if any error occurs
            try {
                if (batchCount > 0) {
                    log.info("Attempting to execute remaining batch after error");
                    executeBatch();
                }
            } catch (Exception ex) {
                log.error("Failed to execute remaining batch after error", ex);
            }
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to write data to Databend: " + e.getMessage(),
                    e);
        }
    }

    private void initializePreparedStatement(SeaTunnelRow row) throws SQLException {
        log.info("Initializing PreparedStatement based on row data");

        // use sinkTablePath to get Schema
        String database = sinkTablePath.getDatabaseName();
        String table = sinkTablePath.getTableName();

        log.info("Querying target table schema for {}.{}", database, table);
        SeaTunnelRowType actualTableSchema = queryTableSchema(database, table);

        if (actualTableSchema != null) {
            log.info("Using actual table schema: {}", actualTableSchema);
            this.insertSql = generateInsertSql(database, table, actualTableSchema);
        } else {
            log.warn("Could not query table schema, using inferred schema from data");
            SeaTunnelRowType inferredRowType = inferRowTypeFromRow(row);
            log.info("Inferred row type from data: {}", inferredRowType);
            this.insertSql = generateInsertSql(database, table, inferredRowType);
        }

        log.info("Generated insert SQL from schema: {}", insertSql);

        // create PreparedStatement
        this.preparedStatement = connection.prepareStatement(insertSql);
        this.preparedStatement.setQueryTimeout(executeTimeoutSec);
        log.info("PreparedStatement initialized successfully");
    }

    private SeaTunnelRowType queryTableSchema(String database, String table) {
        try {
            connection.createStatement().execute("USE " + database);
            String describeSQL = String.format("DESCRIBE %s.%s", database, table);
            log.info("Executing describe table SQL: {}", describeSQL);

            try (PreparedStatement stmt = connection.prepareStatement(describeSQL);
                    ResultSet rs = stmt.executeQuery()) {

                List<String> fieldNames = new ArrayList<>();
                List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();

                while (rs.next()) {
                    String columnName = rs.getString("Field");
                    String columnType = rs.getString("Type");

                    fieldNames.add(columnName);
                    fieldTypes.add(convertDatabendTypeNameToSeaTunnelType(columnType));

                    log.info("Found column: {} {}", columnName, columnType);
                }

                if (!fieldNames.isEmpty()) {
                    return new SeaTunnelRowType(
                            fieldNames.toArray(new String[0]),
                            fieldTypes.toArray(new SeaTunnelDataType<?>[0]));
                }
            }
        } catch (Exception e) {
            log.warn("Failed to query table schema: {}", e.getMessage());
        }
        return null;
    }

    private SeaTunnelDataType<?> convertDatabendTypeNameToSeaTunnelType(String typeName) {
        if (typeName == null) {
            return BasicType.STRING_TYPE;
        }

        typeName = typeName.toUpperCase();

        if (typeName.contains("VARCHAR")
                || typeName.contains("STRING")
                || typeName.contains("TEXT")) {
            return BasicType.STRING_TYPE;
        } else if (typeName.contains("INT") && !typeName.contains("BIGINT")) {
            return BasicType.INT_TYPE;
        } else if (typeName.contains("BIGINT")) {
            return BasicType.LONG_TYPE;
        } else if (typeName.contains("DOUBLE") || typeName.contains("FLOAT64")) {
            return BasicType.DOUBLE_TYPE;
        } else if (typeName.contains("FLOAT") || typeName.contains("FLOAT32")) {
            return BasicType.FLOAT_TYPE;
        } else if (typeName.contains("BOOLEAN")) {
            return BasicType.BOOLEAN_TYPE;
        } else {
            return BasicType.STRING_TYPE;
        }
    }

    private SeaTunnelRowType inferRowTypeFromRow(SeaTunnelRow row) {
        Object[] fields = row.getFields();
        String[] fieldNames = new String[fields.length];
        SeaTunnelDataType<?>[] fieldTypes = new SeaTunnelDataType<?>[fields.length];

        // use the column names from the catalog table if available
        if (catalogTable != null && catalogTable.getSeaTunnelRowType() != null) {
            String[] sourceFieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            if (sourceFieldNames.length == fields.length) {
                fieldNames = sourceFieldNames;
            } else {
                log.warn(
                        "Source table field count ({}) doesn't match row field count ({}), using default column names",
                        sourceFieldNames.length,
                        fields.length);
                for (int i = 0; i < fields.length; i++) {
                    fieldNames[i] = "column_" + (i + 1);
                }
            }
        } else {
            // if catalog table is not available, throw an exception
            log.warn("No source table schema available, can't get column names");
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SCHEMA_NOT_FOUND,
                    "Source table schema is empty or null, cannot infer row type");
        }

        for (int i = 0; i < fields.length; i++) {
            Object field = fields[i];

            if (field == null) {
                fieldTypes[i] = BasicType.STRING_TYPE;
            } else if (field instanceof String) {
                fieldTypes[i] = BasicType.STRING_TYPE;
            } else if (field instanceof Integer) {
                fieldTypes[i] = BasicType.INT_TYPE;
            } else if (field instanceof Long) {
                fieldTypes[i] = BasicType.LONG_TYPE;
            } else if (field instanceof Double) {
                fieldTypes[i] = BasicType.DOUBLE_TYPE;
            } else if (field instanceof Float) {
                fieldTypes[i] = BasicType.FLOAT_TYPE;
            } else if (field instanceof Boolean) {
                fieldTypes[i] = BasicType.BOOLEAN_TYPE;
            } else {
                fieldTypes[i] = BasicType.STRING_TYPE;
            }
        }

        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    private void processRow(SeaTunnelRow row) throws SQLException {
        log.info("Processing row with {} fields", row.getFields().length);
        for (int i = 0; i < row.getFields().length; i++) {
            Object field = row.getFields()[i];
            if (field == null) {
                log.warn("Field {} is null, setting to NULL in prepared statement", i + 1);
                preparedStatement.setNull(i + 1, java.sql.Types.VARCHAR);
            } else {
                log.info(
                        "Setting parameter {}: {} ({})",
                        i + 1,
                        field,
                        field.getClass().getSimpleName());
                preparedStatement.setObject(i + 1, field);
            }
        }
        preparedStatement.addBatch();
        log.info("Added row to batch, current batch count: {}", batchCount + 1);
    }

    private void executeBatch() {
        if (batchCount > 0) {
            try {
                log.info("Executing batch of {} records", batchCount);
                int[] results = preparedStatement.executeBatch();
                int totalAffected = 0;
                for (int result : results) {
                    totalAffected += result;
                }
                log.info("Batch executed successfully, total affected rows: {}", totalAffected);
                batchCount = 0;
            } catch (SQLException e) {
                log.error("Failed to execute batch", e);
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to execute batch: " + e.getMessage(),
                        e);
            }
        } else {
            log.debug("No rows in batch to execute");
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        log.info("Preparing to commit, executing remaining batch");
        executeBatch();
        log.info("Commit prepared successfully");
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                log.info("Aborting prepared transaction");
                connection.rollback();
            }
            batchCount = 0;
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to abort transaction: " + e.getMessage(),
                    e);
        }
    }

    private String generateInsertSql(String database, String table, SeaTunnelRowType rowType) {
        String tableName = database + "." + table;
        String[] fieldNames = rowType.getFieldNames();

        List<String> columnNames = new ArrayList<>();
        for (String fieldName : fieldNames) {
            columnNames.add("`" + fieldName + "`");
        }

        String placeholders = String.join(", ", Collections.nCopies(columnNames.size(), "?"));

        return String.format(
                "INSERT INTO %s (%s) VALUES (%s)",
                tableName, String.join(", ", columnNames), placeholders);
    }

    @Override
    public void close() throws IOException {
        log.info("Closing DatabendSinkWriter");
        try {
            if (preparedStatement != null) {
                log.info("Executing final batch before closing");
                executeBatch();
                log.info("Closing PreparedStatement");
                preparedStatement.close();
            }
            if (connection != null) {
                if (!connection.getAutoCommit()) {
                    log.info("Committing transaction");
                    connection.commit();
                }
                log.info("Closing connection");
                connection.close();
            }
            log.info("DatabendSinkWriter closed successfully");
        } catch (SQLException e) {
            log.error("Failed to close DatabendSinkWriter", e);
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to close connection: " + e.getMessage(),
                    e);
        }
    }

    private boolean tableExists(String database, String table) throws SQLException {
        try (ResultSet rs =
                connection.getMetaData().getTables(null, database, table, new String[] {"TABLE"})) {
            return rs.next();
        }
    }

    private void createTable(String database, String table, SeaTunnelRowType rowType)
            throws SQLException {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql
                .append("CREATE TABLE ")
                .append(database)
                .append(".")
                .append(table)
                .append(" (");

        String[] fieldNames = rowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = rowType.getFieldTypes();
        List<String> columns = new ArrayList<>();

        for (int i = 0; i < fieldNames.length; i++) {
            String columnName = fieldNames[i];
            SeaTunnelDataType<?> dataType = fieldTypes[i];
            columns.add(String.format("`%s` %s", columnName, convertToDatabendType(dataType)));
        }

        createTableSql.append(String.join(", ", columns));
        createTableSql.append(")");

        log.info("Creating table with SQL: {}", createTableSql);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql.toString());
        }
    }

    private void verifyTableSchema(String database, String table, SeaTunnelRowType expectedRowType)
            throws SQLException {
        String[] expectedFieldNames = expectedRowType.getFieldNames();
        Map<String, String> existingColumns = new HashMap<>();

        try (ResultSet rs = connection.getMetaData().getColumns(null, database, table, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = rs.getString("TYPE_NAME");
                existingColumns.put(columnName.toLowerCase(), columnType);
            }
        }

        List<String> missingColumns = new ArrayList<>();
        for (String fieldName : expectedFieldNames) {
            if (!existingColumns.containsKey(fieldName.toLowerCase())) {
                missingColumns.add(fieldName);
            }
        }

        if (!missingColumns.isEmpty()) {
            log.info("Found missing columns in target table: {}", missingColumns);
            for (String columnName : missingColumns) {
                int columnIndex = Arrays.asList(expectedFieldNames).indexOf(columnName);
                SeaTunnelDataType<?> columnType = expectedRowType.getFieldTypes()[columnIndex];
                String databendType = convertToDatabendType(columnType);

                String alterTableSql =
                        String.format(
                                "ALTER TABLE %s.%s ADD COLUMN `%s` %s",
                                database, table, columnName, databendType);

                log.info("Executing ALTER TABLE to add column: {}", alterTableSql);
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(alterTableSql);
                    log.info(
                            "Successfully added column {} to table {}.{}",
                            columnName,
                            database,
                            table);
                } catch (SQLException e) {
                    throw new DatabendConnectorException(
                            DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                            String.format(
                                    "Failed to add column %s to table %s.%s: %s",
                                    columnName, database, table, e.getMessage()),
                            e);
                }
            }
        }
    }

    private String convertToDatabendType(SeaTunnelDataType<?> dataType) {
        switch (dataType.getSqlType()) {
            case STRING:
                return "VARCHAR";
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
                return "DECIMAL";
            case BYTES:
                return "VARBINARY";
            case DATE:
                return "DATE";
            case TIME:
                return "TIMESTAMP";
            case TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "VARCHAR"; // default use VARCHAR
        }
    }
}
