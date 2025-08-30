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
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreProxy;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTableTemplateUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTypeConvertor;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HiveSaveModeHandler implements SaveModeHandler, AutoCloseable {

    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;
    private final SchemaSaveMode schemaSaveMode;
    private final TablePath tablePath;
    private final String dbName;
    private final String tableName;
    private final TableSchema tableSchema;
    private final List<String> partitionFields;

    private HiveMetaStoreProxy hiveMetaStoreProxy;

    public HiveSaveModeHandler(
            ReadonlyConfig readonlyConfig,
            CatalogTable catalogTable,
            SchemaSaveMode schemaSaveMode) {
        this.readonlyConfig = readonlyConfig;
        this.catalogTable = catalogTable;
        this.schemaSaveMode = schemaSaveMode;
        this.tablePath = TablePath.of(readonlyConfig.get(HiveOptions.TABLE_NAME));
        this.dbName = tablePath.getDatabaseName();
        this.tableName = tablePath.getTableName();
        this.tableSchema = catalogTable.getTableSchema();

        // Initialize partition fields from template if available
        this.partitionFields = extractPartitionFieldsFromConfig();
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
        log.info("Creating table {}.{} using template-based approach", dbName, tableName);
        Table table = buildTableFromTemplate();
        hiveMetaStoreProxy.createTableFromTemplate(table);
        log.info("Successfully created table {}.{}", dbName, tableName);
    }

    private List<String> extractPartitionFieldsFromConfig() {
        if (readonlyConfig.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent()) {
            String template = readonlyConfig.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
            return HiveTableTemplateUtils.extractPartitionFieldsFromTemplate(template);
        }
        return new ArrayList<>();
    }

    private Table buildTableFromTemplate() {
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
        String tableLocation = HiveTableTemplateUtils.getDefaultTableLocation(dbName, tableName);
        sd.setLocation(tableLocation);

        // Set storage format based on template or default to PARQUET
        String storageFormat = extractStorageFormatFromTemplate();
        configureStorageDescriptor(sd, storageFormat);

        // Set SerDe name
        sd.getSerdeInfo().setName(table.getTableName());

        // Set compression and storage settings
        sd.setCompressed(shouldEnableCompression(storageFormat));
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
        table.putToParameters("seatunnel.creation.mode", "template");
        table.putToParameters("seatunnel.created.time", String.valueOf(System.currentTimeMillis()));

        return table;
    }

    /** Check if table should be partitioned */
    public boolean isPartitionedTable() {
        return partitionFields != null && !partitionFields.isEmpty();
    }

    private String extractStorageFormatFromTemplate() {
        if (readonlyConfig.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent()) {
            String template = readonlyConfig.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
            // Simple extraction of storage format from template
            if (template.toUpperCase().contains("STORED AS PARQUET")) {
                return "PARQUET";
            } else if (template.toUpperCase().contains("STORED AS ORC")) {
                return "ORC";
            } else if (template.toUpperCase().contains("STORED AS TEXTFILE")) {
                return "TEXTFILE";
            }
        }
        return "PARQUET"; // Default format
    }

    private void configureStorageDescriptor(StorageDescriptor sd, String format) {
        switch (format.toUpperCase()) {
            case "PARQUET":
                sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
                sd.setOutputFormat(
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
                sd.getSerdeInfo()
                        .setSerializationLib(
                                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
                break;
            case "ORC":
                sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
                sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
                sd.getSerdeInfo().setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
                break;
            case "TEXTFILE":
                sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
                sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
                sd.getSerdeInfo()
                        .setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
                break;
            default:
                // Default to PARQUET
                sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
                sd.setOutputFormat(
                        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
                sd.getSerdeInfo()
                        .setSerializationLib(
                                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        }
    }

    private boolean shouldEnableCompression(String format) {
        return "PARQUET".equalsIgnoreCase(format) || "ORC".equalsIgnoreCase(format);
    }

    private String getPartitionFieldType(String partitionField) {
        // Check if partition field exists in source schema
        return tableSchema.getColumns().stream()
                .filter(col -> col.getName().equals(partitionField))
                .findFirst()
                .map(col -> HiveTypeConvertor.seatunnelToHiveType(col.getDataType()))
                .orElse("string"); // Default to string for new partition fields
    }
}
