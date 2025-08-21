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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveFormatUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTypeConvertor;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class HiveSaveModeHandler implements SaveModeHandler, AutoCloseable {

    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;
    private final SchemaSaveMode schemaSaveMode;
    private final String createTemplate;
    private final TablePath tablePath;
    private final String dbName;
    private final String tableName;
    private final TableSchema tableSchema;
    private final List<String> partitionFields;
    private final List<String> sourceFieldNames;
    private final List<String> partitionFieldsFromSource;
    private final List<String> nonPartitionFields;

    private HiveMetaStoreProxy hiveMetaStoreProxy;

    public HiveSaveModeHandler(
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            SchemaSaveMode schemaSaveMode,
            String createTemplate) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.schemaSaveMode = schemaSaveMode;
        this.createTemplate = createTemplate;
        this.tablePath = TablePath.of(readonlyConfig.get(HiveOptions.TABLE_NAME));
        this.dbName = tablePath.getDatabaseName();
        this.tableName = tablePath.getTableName();
        this.tableSchema = catalogTable.getTableSchema();

        // Initialize partition fields and validation
        this.partitionFields = readonlyConfig.get(HiveSinkOptions.PARTITION_FIELDS);
        this.sourceFieldNames =
                tableSchema.getColumns().stream()
                        .map(org.apache.seatunnel.api.table.catalog.Column::getName)
                        .collect(Collectors.toList());

        // Validate and categorize partition fields
        validatePartitionFields();
        this.partitionFieldsFromSource =
                partitionFields.stream()
                        .filter(sourceFieldNames::contains)
                        .collect(Collectors.toList());
        this.nonPartitionFields =
                sourceFieldNames.stream()
                        .filter(field -> !partitionFieldsFromSource.contains(field))
                        .collect(Collectors.toList());
    }

    @Override
    public void open() {
        this.hiveMetaStoreProxy = HiveMetaStoreProxy.getInstance(readonlyConfig);
    }

    @Override
    public void handleSchemaSaveModeWithRestore() {
        // For Hive, we use the same logic as handleSchemaSaveMode
        handleSchemaSaveMode();
    }

    @Override
    public TablePath getHandleTablePath() {
        return tablePath;
    }

    @Override
    public Catalog getHandleCatalog() {
        // Hive doesn't use Catalog interface directly, return null
        return null;
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return schemaSaveMode;
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        // Hive uses OVERWRITE parameter for data handling
        return DataSaveMode.APPEND_DATA;
    }

    @Override
    public void close() throws Exception {
        if (hiveMetaStoreProxy != null) {
            hiveMetaStoreProxy.close();
        }
    }

    @Override
    public void handleSchemaSaveMode() {
        try {
            switch (schemaSaveMode) {
                case RECREATE_SCHEMA:
                    handleRecreateSchema();
                    break;
                case CREATE_SCHEMA_WHEN_NOT_EXIST:
                    handleCreateSchemaWhenNotExist();
                    break;
                case ERROR_WHEN_SCHEMA_NOT_EXIST:
                    handleErrorWhenSchemaNotExist();
                    break;
                case IGNORE:
                    log.info(
                            "Ignore schema save mode, skip schema handling for table {}.{}",
                            dbName,
                            tableName);
                    break;
                default:
                    throw new HiveConnectorException(
                            HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                            "Unsupported schema save mode: " + schemaSaveMode);
            }
        } catch (Exception e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Failed to handle schema save mode: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void handleDataSaveMode() {
        // For Hive, data save mode is handled by the existing OVERWRITE parameter
        // No additional data handling is needed here
        log.info(
                "Data save mode handling is managed by existing OVERWRITE parameter for table {}.{}",
                dbName,
                tableName);
    }

    private void handleRecreateSchema() throws TException {
        log.info("Recreate schema mode: dropping and recreating table {}.{}", dbName, tableName);

        // Create database if not exists
        createDatabaseIfNotExists();

        // Drop table if exists
        if (hiveMetaStoreProxy.tableExists(dbName, tableName)) {
            hiveMetaStoreProxy.dropTable(dbName, tableName);
            log.info("Dropped existing table {}.{}", dbName, tableName);
        }

        // Create table
        createTable();
    }

    private void handleCreateSchemaWhenNotExist() throws TException {
        log.info("Create schema when not exist mode for table {}.{}", dbName, tableName);

        // Create database if not exists
        createDatabaseIfNotExists();

        // Create table if not exists
        if (!hiveMetaStoreProxy.tableExists(dbName, tableName)) {
            createTable();
        }
    }

    private void handleErrorWhenSchemaNotExist() throws TException {

        // Check if database exists
        if (!hiveMetaStoreProxy.databaseExists(dbName)) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Database " + dbName + " does not exist");
        }

        if (!hiveMetaStoreProxy.tableExists(dbName, tableName)) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Table " + dbName + "." + tableName + " does not exist");
        }
    }

    private void createDatabaseIfNotExists() throws TException {
        hiveMetaStoreProxy.createDatabaseIfNotExists(dbName);
    }

    private void createTable() throws TException {
        String defaultTemplate = HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.defaultValue();
        boolean useCustomTemplate = !defaultTemplate.equals(createTemplate);

        if (useCustomTemplate) {
            createTableUsingTemplate();
        } else {
            createTableUsingAPI();
        }
    }

    private void createTableUsingAPI() throws TException {
        // Create table using Hive MetaStore API (more reliable than SQL)
        Table table = buildTableFromSchema();
        hiveMetaStoreProxy.createTableIfNotExists(table);
    }

    private void createTableUsingTemplate() throws TException {
        processCreateTemplate();
        Table table = buildTableFromSchema();
        hiveMetaStoreProxy.createTableIfNotExists(table);
    }

    private String processCreateTemplate() {
        String sql = createTemplate;
        String tableFormat = readonlyConfig.get(HiveSinkOptions.TABLE_FORMAT);
        HiveFormatUtils.validateFormat(tableFormat);

        sql = sql.replace("${database}", dbName);
        sql = sql.replace("${table}", tableName);
        sql = sql.replace("${database}", dbName);
        sql = sql.replace("${table}", tableName);
        sql = sql.replace("${table_location}", getDefaultTableLocation());
        sql = sql.replace("${rowtype_fields}", generateNonPartitionColumnDefinitions());
        sql = sql.replace("${table_format}", tableFormat);
        sql = sql.replace("${partition_by_clause}", generatePartitionByClause());
        sql =
                sql.replace(
                        "${table_properties}",
                        HiveFormatUtils.getDefaultTableProperties(tableFormat));

        return sql;
    }

    private String generatePartitionByClause() {
        if (!isPartitionedTable()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("\nPARTITIONED BY (\n");

        for (int i = 0; i < partitionFields.size(); i++) {
            String partitionField = partitionFields.get(i);
            String hiveType = getPartitionFieldType(partitionField);

            sb.append("  `").append(partitionField).append("` ").append(hiveType);
            if (i < partitionFields.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }

        sb.append(")");
        return sb.toString();
    }

    private String getPartitionFieldType(String partitionField) {
        // Check if partition field exists in source schema
        return tableSchema.getColumns().stream()
                .filter(col -> col.getName().equals(partitionField))
                .findFirst()
                .map(col -> HiveTypeConvertor.seatunnelToHiveType(col.getDataType()))
                .orElse("string"); // Default to string for new partition fields
    }

    private String getDefaultTableLocation() {
        return "/user/hive/warehouse/" + dbName + ".db/" + tableName;
    }

    private String generateColumnDefinitions() {
        // Generate all column definitions (for backward compatibility)
        return generateColumnDefinitions(tableSchema.getColumns());
    }

    private String generateNonPartitionColumnDefinitions() {
        // Generate only non-partition column definitions
        List<org.apache.seatunnel.api.table.catalog.Column> nonPartitionColumns =
                tableSchema.getColumns().stream()
                        .filter(col -> !partitionFields.contains(col.getName()))
                        .collect(Collectors.toList());
        return generateColumnDefinitions(nonPartitionColumns);
    }

    private String generateColumnDefinitions(
            List<org.apache.seatunnel.api.table.catalog.Column> columns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            org.apache.seatunnel.api.table.catalog.Column column = columns.get(i);
            String hiveType = HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
            sb.append("`").append(column.getName()).append("` ").append(hiveType);

            // Add comment
            String comment = column.getComment();
            if (comment != null && !comment.isEmpty()) {
                sb.append(" COMMENT '").append(comment.replace("'", "\\'")).append("'");
            }

            if (i < columns.size() - 1) {
                sb.append(",\n  ");
            }
        }
        return sb.toString();
    }

    private Table buildTableFromSchema() {
        Table table = new Table();
        table.setDbName(dbName);
        table.setTableName(tableName);
        table.setOwner(System.getProperty("user.name", "seatunnel"));
        table.setCreateTime((int) (System.currentTimeMillis() / 1000));
        table.setTableType("MANAGED_TABLE");

        // Set storage descriptor
        StorageDescriptor sd = new StorageDescriptor();

        // Set columns (exclude partition fields from regular columns)
        List<FieldSchema> cols = new ArrayList<>();
        tableSchema.getColumns().stream()
                .filter(column -> !partitionFields.contains(column.getName()))
                .forEach(
                        column -> {
                            String hiveType =
                                    HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
                            String comment = column.getComment();
                            cols.add(new FieldSchema(column.getName(), hiveType, comment));
                        });
        sd.setCols(cols);

        // Set table location
        String tableLocation = getDefaultTableLocation();
        sd.setLocation(tableLocation);

        // Set storage format dynamically based on configuration
        String tableFormat = readonlyConfig.get(HiveSinkOptions.TABLE_FORMAT);
        HiveFormatUtils.validateFormat(tableFormat);
        HiveFormatUtils.configureStorageDescriptor(sd, tableFormat);

        // Set SerDe name
        sd.getSerdeInfo().setName(table.getTableName());

        // Set compression and storage settings
        sd.setCompressed(HiveFormatUtils.shouldEnableCompression(tableFormat));
        sd.setStoredAsSubDirectories(false);

        table.setSd(sd);

        // Set partition keys if this is a partitioned table
        if (isPartitionedTable()) {
            List<FieldSchema> partitionKeys = new ArrayList<>();
            for (String partitionField : partitionFields) {
                String hiveType = getPartitionFieldType(partitionField);
                partitionKeys.add(new FieldSchema(partitionField, hiveType, "Partition field"));
            }
            table.setPartitionKeys(partitionKeys);
            log.info("Set partition keys for table {}.{}: {}", dbName, tableName, partitionFields);
        }

        // Set table properties
        String[] properties = HiveFormatUtils.getDefaultTableProperties(tableFormat).split(",\n  ");
        for (String property : properties) {
            String[] keyValue = property.replace("'", "").split("=");
            if (keyValue.length == 2) {
                table.putToParameters(keyValue[0], keyValue[1]);
            }
        }

        return table;
    }

    /** Validate partition fields configuration */
    private void validatePartitionFields() {
        if (partitionFields == null || partitionFields.isEmpty()) {
            log.info("No partition fields configured, creating non-partitioned table");
            return;
        }

        log.info("Configured partition fields: {}", partitionFields);
        log.info("Source table fields: {}", sourceFieldNames);

        // Check for duplicate partition fields
        Set<String> uniquePartitionFields = partitionFields.stream().collect(Collectors.toSet());
        if (uniquePartitionFields.size() != partitionFields.size()) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Duplicate partition fields found in configuration: " + partitionFields);
        }

        // Log which partition fields are from source and which are new
        List<String> fieldsFromSource =
                partitionFields.stream()
                        .filter(sourceFieldNames::contains)
                        .collect(Collectors.toList());
        List<String> newFields =
                partitionFields.stream()
                        .filter(field -> !sourceFieldNames.contains(field))
                        .collect(Collectors.toList());

        if (!fieldsFromSource.isEmpty()) {
            log.info(
                    "Partition fields from source table (will be removed from data rows): {}",
                    fieldsFromSource);
        }
        if (!newFields.isEmpty()) {
            log.info("New partition fields (should be provided in data): {}", newFields);
        }
    }

    /** Get list of partition fields that exist in source table */
    public List<String> getPartitionFieldsFromSource() {
        return partitionFieldsFromSource;
    }

    /** Get list of non-partition fields (regular table columns) */
    public List<String> getNonPartitionFields() {
        return nonPartitionFields;
    }

    /** Check if table should be partitioned */
    public boolean isPartitionedTable() {
        return partitionFields != null && !partitionFields.isEmpty();
    }
}
