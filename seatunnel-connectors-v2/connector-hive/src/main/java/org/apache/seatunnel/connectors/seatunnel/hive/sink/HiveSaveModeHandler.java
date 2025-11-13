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
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveMetaStoreCatalog;
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

    private HiveMetaStoreCatalog hiveCatalog;

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
    }

    @Override
    public void open() {
        this.hiveCatalog = HiveMetaStoreCatalog.create(readonlyConfig);
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
        return hiveCatalog;
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return schemaSaveMode;
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return readonlyConfig.get(HiveSinkOptions.DATA_SAVE_MODE);
    }

    @Override
    public void close() throws Exception {
        if (hiveCatalog != null) {
            hiveCatalog.close();
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
        } catch (HiveConnectorException e) {
            throw e;
        } catch (TException e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Failed to handle schema save mode: " + e.getMessage(),
                    e);
        }
    }

    @Override
    public void handleDataSaveMode() {
        // No-op: data cleanup is handled in AggregatedCommitter via overwrite or DROP_DATA
    }

    private void handleRecreateSchema() throws TException {
        // Do NOT create database automatically. Ensure database exists first.
        if (!hiveCatalog.databaseExists(dbName)) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Database " + dbName + " does not exist. Please create it manually.");
        }

        // Drop table if exists
        if (hiveCatalog.tableExists(dbName, tableName)) {
            // Try to drop via JDBC first
            String dropSql = String.format("DROP TABLE IF EXISTS `%s`.`%s`", dbName, tableName);
            if (!hiveCatalog.tryExecuteSqlViaJdbc(dropSql)) {
                // Fallback to Metastore Client
                hiveCatalog.dropTable(dbName, tableName);
            }
        }

        // Create table using template
        createTable();
    }

    private void handleCreateSchemaWhenNotExist() throws TException {
        if (!hiveCatalog.databaseExists(dbName)) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Database " + dbName + " does not exist. Please create it manually.");
        }

        if (!hiveCatalog.tableExists(dbName, tableName)) {
            createTable();
        }
    }

    private void handleErrorWhenSchemaNotExist() throws TException {
        if (!hiveCatalog.databaseExists(dbName)) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Database " + dbName + " does not exist");
        }

        if (!hiveCatalog.tableExists(dbName, tableName)) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED,
                    "Table " + dbName + "." + tableName + " does not exist");
        }
    }

    private void createTable() throws TException {
        // Try to create table via JDBC first if template is provided
        if (readonlyConfig.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent()) {
            String rawTemplate = readonlyConfig.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);

            // If template uses ${table_location}, qualify it based on Hadoop conf (HDFS or local)
            String defaultLoc =
                    org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveLocationUtils
                            .qualifiedDefaultLocation(readonlyConfig, dbName, tableName);
            String template =
                    rawTemplate.contains("${table_location}")
                            ? rawTemplate.replace("${table_location}", defaultLoc)
                            : rawTemplate;

            // Build complete SQL from (possibly adjusted) template
            String createTableSql =
                    HiveTableTemplateUtils.buildCreateTableSQL(
                            template, dbName, tableName, tableSchema);

            boolean jdbcSuccess = hiveCatalog.tryExecuteSqlViaJdbc(createTableSql);

            if (jdbcSuccess) {
                log.info(
                        "Successfully created table {}.{} via HiveServer2 JDBC", dbName, tableName);
                return;
            }
        }

        // Fallback to Metastore Client approach
        Table table = buildTableFromTemplate();
        hiveCatalog.createTableFromTemplate(table);
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
        if (readonlyConfig.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent()) {
            return buildTableFromCustomTemplate();
        } else {
            return buildTableFromDefaultTemplate();
        }
    }

    private Table buildTableFromDefaultTemplate() {

        Table table = new Table();
        table.setDbName(dbName);
        table.setTableName(tableName);
        table.setOwner(System.getProperty("user.name", "seatunnel"));
        table.setCreateTime((int) (System.currentTimeMillis() / 1000));
        table.setTableType("MANAGED_TABLE");

        table.setPartitionKeys(new ArrayList<>());

        // Set storage descriptor
        StorageDescriptor sd = new StorageDescriptor();

        // Initialize SerDe
        org.apache.hadoop.hive.metastore.api.SerDeInfo serdeInfo =
                new org.apache.hadoop.hive.metastore.api.SerDeInfo();
        serdeInfo.setName(table.getTableName());
        sd.setSerdeInfo(serdeInfo);

        // Set all columns as regular columns (no partitions in default template)
        List<FieldSchema> cols = new ArrayList<>();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            String hiveType =
                                    HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
                            String comment = column.getComment();
                            cols.add(new FieldSchema(column.getName(), hiveType, comment));
                        });
        sd.setCols(cols);

        // Set table location using dynamically qualified default location (HDFS if available)
        String tableLocation =
                org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveLocationUtils
                        .qualifiedDefaultLocation(readonlyConfig, dbName, tableName);
        sd.setLocation(tableLocation);

        configureStorageDescriptor(sd, "PARQUET");
        sd.setCompressed(false);
        sd.setStoredAsSubDirectories(false);

        table.setSd(sd);

        // Set table parameters
        table.putToParameters("seatunnel.creation.mode", "default_template");
        table.putToParameters("seatunnel.created.time", String.valueOf(System.currentTimeMillis()));

        return table;
    }

    private Table buildTableFromCustomTemplate() {

        Table table = new Table();
        table.setDbName(dbName);
        table.setTableName(tableName);
        table.setOwner(System.getProperty("user.name", "seatunnel"));
        table.setCreateTime((int) (System.currentTimeMillis() / 1000));

        // Determine table type from template (EXTERNAL_TABLE or MANAGED_TABLE)
        String template = readonlyConfig.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        String tableType = HiveTableTemplateUtils.extractTableTypeFromTemplate(template);
        table.setTableType(tableType);

        List<String> partitionFields = extractPartitionFieldsFromConfig();
        List<FieldSchema> partitionKeys = new ArrayList<>();
        for (String partitionField : partitionFields) {
            // Determine type from source schema if present; otherwise default to string
            String hiveType = getPartitionFieldType(partitionField);
            String comment =
                    tableSchema.getColumns().stream()
                            .filter(column -> column.getName().equals(partitionField))
                            .findFirst()
                            .map(org.apache.seatunnel.api.table.catalog.Column::getComment)
                            .orElse("Partition field");
            partitionKeys.add(new FieldSchema(partitionField, hiveType, comment));
        }
        table.setPartitionKeys(partitionKeys);

        // Set storage descriptor
        StorageDescriptor sd = new StorageDescriptor();

        // Initialize SerDe
        org.apache.hadoop.hive.metastore.api.SerDeInfo serdeInfo =
                new org.apache.hadoop.hive.metastore.api.SerDeInfo();
        serdeInfo.setName(table.getTableName());
        sd.setSerdeInfo(serdeInfo);

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

        // Set table location:
        // - If template defines LOCATION and uses ${table_location}, replace with qualified
        // default.
        // - If template defines explicit LOCATION (no variable), respect it.
        // - Else, fallback to qualified default location.
        String defaultLoc =
                org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveLocationUtils
                        .qualifiedDefaultLocation(readonlyConfig, dbName, tableName);
        String upperTpl = template != null ? template.toUpperCase() : "";
        String tableLocation;
        if (upperTpl.contains(" LOCATION ")) {
            if (template.contains("${table_location}")) {
                tableLocation = defaultLoc;
            } else {
                // Extract explicit LOCATION from template as-is
                String extractedLocation =
                        HiveTableTemplateUtils.extractLocationFromTemplate(
                                template, dbName, tableName);
                tableLocation = extractedLocation != null ? extractedLocation : defaultLoc;
            }
        } else {
            tableLocation = defaultLoc;
        }
        sd.setLocation(tableLocation);

        String storageFormat = extractStorageFormatFromTemplate();
        configureStorageDescriptor(sd, storageFormat);
        sd.setCompressed(shouldEnableCompression(storageFormat));
        sd.setStoredAsSubDirectories(false);

        table.setSd(sd);

        // Set table parameters
        table.putToParameters("seatunnel.creation.mode", "custom_template");
        table.putToParameters("seatunnel.created.time", String.valueOf(System.currentTimeMillis()));
        // Pass through the raw custom template into TBLPROPERTIES
        table.putToParameters("seatunnel.creation.template", template);
        java.util.Map<String, String> tblProps =
                HiveTableTemplateUtils.extractTblPropertiesFromTemplate(template);
        for (java.util.Map.Entry<String, String> e : tblProps.entrySet()) {
            table.putToParameters(e.getKey(), e.getValue());
        }

        return table;
    }

    // use HiveLocationUtils for location resolution (no extra helpers needed here)

    private String extractStorageFormatFromTemplate() {
        if (readonlyConfig.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent()) {
            String template = readonlyConfig.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
            if (template.toUpperCase().contains("STORED AS PARQUET")) {
                return "PARQUET";
            } else if (template.toUpperCase().contains("STORED AS ORC")) {
                return "ORC";
            } else if (template.toUpperCase().contains("STORED AS TEXTFILE")) {
                return "TEXTFILE";
            }
        }
        return "PARQUET";
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
                .orElse("string");
    }
}
