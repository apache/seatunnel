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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.schema.handler.TableSchemaChangeEventDispatcher;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.schema.SchemaChangeManager;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class DatabendSinkWriter
        implements SinkWriter<SeaTunnelRow, DatabendSinkCommitterInfo, Void>,
                SupportSchemaEvolutionSinkWriter {

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

    // CDC related fields
    // Note: In CDC mode, rawTableName and streamName are set by DatabendSinkAggregatedCommitter
    // The writer receives these values through the prepareCommit process
    private boolean isCdcMode = false;
    private String rawTableName;
    private String streamName;
    private String targetTableName;
    private PreparedStatement cdcPreparedStatement;
    private String conflictKey;
    private boolean enableDelete;

    public DatabendSinkWriter(
            Context context,
            Connection connection,
            CatalogTable catalogTable,
            DatabendSinkConfig databendSinkConfig,
            String customSql,
            String database,
            String table,
            String rawTableName,
            String streamName,
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

        // CDC mode check
        this.isCdcMode = databendSinkConfig.isCdcMode();
        if (databendSinkConfig.isCdcMode()) {
            this.rawTableName = rawTableName;
            this.streamName = streamName;
            log.info("DatabendSinkWriter initialized in CDC mode with raw table: {}", rawTableName);
        } else {
            log.info("DatabendSinkWriter initialized in traditional mode");
        }
        this.conflictKey = databendSinkConfig.getConflictKey();
        this.enableDelete = databendSinkConfig.isEnableDelete();
        this.targetTableName = table;

        log.info("DatabendSinkWriter constructor - catalogTable: {}", catalogTable);
        log.info("DatabendSinkWriter constructor - tableSchema: {}", tableSchema);
        log.info(
                "DatabendSinkWriter constructor - rowType: {}", catalogTable.getSeaTunnelRowType());
        log.info("DatabendSinkWriter constructor - target table path: {}", sinkTablePath);
        log.info("DatabendSinkWriter constructor - CDC mode: {}", isCdcMode);

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
            try {
                if (isCdcMode) {
                    // In CDC mode, we don't create tables here, it's done in AggregatedCommitter
                    // We'll get the raw table and stream names from the committer via prepareCommit
                    log.info(
                            "CDC mode enabled, table creation will be handled by AggregatedCommitter");
                } else {
                    // Traditional mode
                    initTraditionalMode(database, table);
                }
            } catch (SQLException e) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to initialize sink writer: " + e.getMessage(),
                        e);
            }
        }
    }

    private String getCurrentTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }

    private void initializeCdcPreparedStatement() throws SQLException {
        log.info("Initializing CDC PreparedStatement");

        // In CDC mode, the rawTableName should be set by the AggregatedCommitter
        // If it's not set yet, we can't proceed with CDC operations
        if (rawTableName == null || rawTableName.isEmpty()) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Raw table name not set by AggregatedCommitter. Cannot initialize CDC PreparedStatement.");
        }

        // Generate insert SQL for raw table
        String insertRawSql = generateInsertRawSql(sinkTablePath.getDatabaseName());

        // Create the PreparedStatement
        this.cdcPreparedStatement = connection.prepareStatement(insertRawSql);
        this.cdcPreparedStatement.setQueryTimeout(executeTimeoutSec);

        log.info("CDC PreparedStatement created successfully with SQL: {}", insertRawSql);
    }

    private void initTraditionalMode(String database, String table) throws SQLException {
        // use the catalog table schema to create the target table
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        if (rowType == null || rowType.getFieldNames().length == 0) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SCHEMA_NOT_FOUND,
                    "Source table schema is empty or null");
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

    private String generateInsertRawSql(String database) {
        return String.format(
                "INSERT INTO %s.%s (id, table_name, raw_data, add_time, action) VALUES (?, ?, ?, ?, ?)",
                database, rawTableName);
    }

    private void performMerge() {
        if (batchCount <= 0) {
            log.debug("No data to merge, skipping");
            return;
        }

        String mergeSql = generateMergeSql();
        log.info("Executing MERGE INTO statement: {}", mergeSql);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(mergeSql);
            log.info("Merge operation completed successfully");
            batchCount = 0; // Reset batch count after successful merge
        } catch (SQLException e) {
            log.error("Failed to execute merge operation: {}", e.getMessage(), e);
        }
    }

    String generateMergeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(
                String.format(
                        "MERGE INTO %s.%s a ", sinkTablePath.getDatabaseName(), targetTableName));
        sql.append(String.format("USING (SELECT "));

        // Add all columns from raw_data
        String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append(String.format("raw_data:%s as %s", fieldNames[i], fieldNames[i]));
        }

        sql.append(", action FROM ")
                .append(sinkTablePath.getDatabaseName())
                .append(".")
                .append(streamName)
                .append(" QUALIFY ROW_NUMBER() OVER(PARTITION BY ")
                .append(conflictKey)
                .append(" ORDER BY add_time DESC) = 1) b ");

        sql.append("ON a.").append(conflictKey).append(" = b.").append(conflictKey).append(" ");

        sql.append("WHEN MATCHED AND b.action = 'update' THEN UPDATE * ");

        if (enableDelete) {
            sql.append("WHEN MATCHED AND b.action = 'delete' THEN DELETE ");
        }

        sql.append("WHEN NOT MATCHED AND b.action!='delete' THEN INSERT *");

        return sql.toString();
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

            if (isCdcMode) {
                processCdcRow(row);
            } else {
                processTraditionalRow(row);
            }

            batchCount++;
            log.info("Batch count after adding row: {}", batchCount);

            if (batchCount >= batchSize) {
                log.info("Batch size {} reached, executing batch", batchSize);
                executeBatch();
                log.info("Batch executed successfully");
            }
        } catch (Exception e) {
            log.error("Failed to write row: {}", row, e);
            // try to execute the remaining batch if any error occurs
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

    private void processCdcRow(SeaTunnelRow row) throws SQLException {
        log.info("Processing CDC row with kind: {}", row.getRowKind());

        String action = mapRowKindToAction(row.getRowKind());
        if ("update_before".equals(action)) {
            log.debug("UPDATE_BEFORE operation detected, skipping row");
            return;
        }

        if ("delete".equals(action) && !enableDelete) {
            log.debug("DELETE operation not allowed, skipping row");
            return;
        }

        // Ensure cdcPreparedStatement is initialized
        if (cdcPreparedStatement == null) {
            log.info("CDC PreparedStatement is null, initializing...");
            initializeCdcPreparedStatement();

            // If it's still null, we need to throw an exception as we can't proceed
            if (cdcPreparedStatement == null) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to initialize CDC PreparedStatement. Raw table name might not be set by AggregatedCommitter.");
            }

            log.info("CDC PreparedStatement initialized successfully");
        }

        // Get conflict key value
        String conflictKeyValue = getConflictKeyValue(row);

        // Convert row to JSON
        String jsonData = convertRowToJson(row);

        cdcPreparedStatement.setString(1, conflictKeyValue);
        cdcPreparedStatement.setString(2, targetTableName);
        cdcPreparedStatement.setString(3, jsonData);
        cdcPreparedStatement.setTimestamp(4, java.sql.Timestamp.valueOf(LocalDateTime.now()));
        cdcPreparedStatement.setString(5, action);

        cdcPreparedStatement.addBatch();
    }

    private void processTraditionalRow(SeaTunnelRow row) throws SQLException {
        // Ensure preparedStatement is initialized
        if (preparedStatement == null) {
            log.info("PreparedStatement is null, initializing...");
            initializePreparedStatement(row);

            // If it's still null, we need to throw an exception as we can't proceed
            if (preparedStatement == null) {
                throw new DatabendConnectorException(
                        DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                        "Failed to initialize PreparedStatement.");
            }

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
    }

    private String mapRowKindToAction(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return "insert";
            case UPDATE_AFTER:
                return "update";
            case DELETE:
                return "delete";
        }
        return "update_before";
    }

    /**
     * Get the value of the conflict key field from the row. This value will be used as the ID in
     * the raw table.
     */
    private String getConflictKeyValue(SeaTunnelRow row) {
        String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
        int index = Arrays.asList(fieldNames).indexOf(conflictKey);

        if (index >= 0 && index < row.getFields().length) {
            Object value = row.getField(index);
            if (value != null) {
                return value.toString();
            }
        }

        // This should not happen in a proper CDC setup where conflict key values are always present
        // If we reach here, it indicates a data issue
        throw new IllegalArgumentException(
                "Conflict key field '" + conflictKey + "' value is null or not found in row");
    }

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String convertRowToJson(SeaTunnelRow row) {
        try {
            ObjectNode jsonNode = objectMapper.createObjectNode();
            String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            Object[] fields = row.getFields();

            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = fieldNames[i];
                Object value = fields[i];

                if (value == null) {
                    jsonNode.putNull(fieldName);
                } else if (value instanceof String) {
                    jsonNode.put(fieldName, (String) value);
                } else if (value instanceof Integer) {
                    jsonNode.put(fieldName, (Integer) value);
                } else if (value instanceof Long) {
                    jsonNode.put(fieldName, (Long) value);
                } else if (value instanceof Float) {
                    jsonNode.put(fieldName, (Float) value);
                } else if (value instanceof Double) {
                    jsonNode.put(fieldName, (Double) value);
                } else if (value instanceof Boolean) {
                    jsonNode.put(fieldName, (Boolean) value);
                } else if (value instanceof BigDecimal) {
                    jsonNode.put(fieldName, (BigDecimal) value);
                } else if (value instanceof java.sql.Timestamp) {
                    jsonNode.put(fieldName, value.toString());
                } else if (value instanceof java.sql.Date) {
                    jsonNode.put(fieldName, value.toString());
                } else if (value instanceof byte[]) {
                    jsonNode.put(fieldName, Base64.getEncoder().encodeToString((byte[]) value));
                } else {
                    jsonNode.put(fieldName, value.toString());
                }
            }

            return objectMapper.writeValueAsString(jsonNode);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert row to JSON", e);
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

    private void verifyRawTableData(String rawTableName, String database) throws SQLException {
        try (Statement stmt = connection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                "SELECT COUNT(*), COUNT(DISTINCT raw_data:id) FROM "
                                        + database
                                        + "."
                                        + rawTableName)) {
            if (rs.next()) {
                log.info(
                        "Raw table sjh {} has {} total rows, {} unique ids",
                        rawTableName,
                        rs.getInt(1),
                        rs.getInt(2));
            }
        }

        try (Statement stmt = connection.createStatement();
                ResultSet dataRs =
                        stmt.executeQuery(
                                "SELECT raw_data, action, add_time FROM "
                                        + database
                                        + "."
                                        + rawTableName
                                        + " ORDER BY add_time"); ) {
            while (dataRs.next()) {
                log.info(
                        "Raw data : {}, action: {}, time: {}",
                        dataRs.getString(1),
                        dataRs.getString(2),
                        dataRs.getTimestamp(3));
            }
        }
    }

    private void executeBatch() {
        if (batchCount > 0) {
            try {
                log.info("Executing batch of {} records", batchCount);
                if (isCdcMode) {
                    int[] results = cdcPreparedStatement.executeBatch();
                    int totalAffected = 0;
                    for (int result : results) {
                        totalAffected += result;
                    }
                    log.info(
                            "CDC batch executed successfully, total affected rows: {}",
                            totalAffected);
                    verifyRawTableData(rawTableName, sinkTablePath.getDatabaseName());
                } else {
                    int[] results = preparedStatement.executeBatch();
                    int totalAffected = 0;
                    for (int result : results) {
                        totalAffected += result;
                    }
                    log.info(
                            "Traditional batch executed successfully, total affected rows: {}",
                            totalAffected);
                }
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
    public Optional<DatabendSinkCommitterInfo> prepareCommit() throws IOException {
        log.info("Preparing to commit, executing remaining batch");
        executeBatch();
        log.info("Commit prepared successfully");
        // In the new approach, rawTableName and streamName are initialized in DatabendSink
        // We pass null values as they're not needed in the committer info
        return Optional.of(new DatabendSinkCommitterInfo(null, null));
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
            // Execute final batch before closing
            if (batchCount > 0) {
                log.info("Executing final batch before closing");
                executeBatch();
            }

            // Perform final merge in CDC mode
            if (isCdcMode) {
                log.info("Performing final merge before closing");
                performMerge();
            }

            // Close prepared statements
            if (preparedStatement != null) {
                log.info("Closing PreparedStatement");
                preparedStatement.close();
            }

            if (cdcPreparedStatement != null) {
                log.info("Closing CDC PreparedStatement");
                cdcPreparedStatement.close();
            }

            // Close connection
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

    // Package-private methods for testing
    String getConflictKey() {
        return conflictKey;
    }

    TablePath getSinkTablePath() {
        return sinkTablePath;
    }

    String getRawTableName() {
        return rawTableName;
    }

    String getStreamName() {
        return streamName;
    }

    boolean isEnableDelete() {
        return enableDelete;
    }

    CatalogTable getCatalogTable() {
        return catalogTable;
    }
}
