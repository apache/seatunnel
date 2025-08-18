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
import org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveTypeConvertor;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
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
    private final String createTemplate;
    private final TablePath tablePath;
    private final String dbName;
    private final String tableName;
    private final TableSchema tableSchema;

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
        } else {
            log.info("Table {}.{} already exists, skip creation", dbName, tableName);
        }
    }

    private void handleErrorWhenSchemaNotExist() throws TException {
        log.info("Error when schema not exist mode for table {}.{}", dbName, tableName);

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
        log.info("Ensured database exists: {}", dbName);
    }

    private void createTable() throws TException {
        log.info("Creating table {}.{} using Hive MetaStore API", dbName, tableName);

        // Create table using Hive MetaStore API (more reliable than SQL)
        Table table = buildTableFromSchema();
        hiveMetaStoreProxy.createTableIfNotExists(table);

        log.info("Successfully created table {}.{}", dbName, tableName);
    }

    private String processCreateTemplate() {
        // Simplified template processing - just replace basic variables
        String sql = createTemplate;
        sql = sql.replace("${database}", dbName);
        sql = sql.replace("${table}", tableName);
        sql = sql.replace("${table_location}", getDefaultTableLocation());
        sql = sql.replace("${partition_by_clause}", "");
        sql = sql.replace("${rowtype_fields}", generateColumnDefinitions());
        return sql;
    }

    private String getDefaultTableLocation() {
        return "/user/hive/warehouse/" + dbName + ".db/" + tableName;
    }

    private String generateColumnDefinitions() {
        StringBuilder sb = new StringBuilder();
        List<org.apache.seatunnel.api.table.catalog.Column> columns = tableSchema.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            org.apache.seatunnel.api.table.catalog.Column column = columns.get(i);
            String hiveType = HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
            sb.append("`").append(column.getName()).append("` ").append(hiveType);
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                sb.append(" COMMENT '").append(column.getComment().replace("'", "\\'")).append("'");
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

        // Set columns
        List<FieldSchema> cols = new ArrayList<>();
        tableSchema
                .getColumns()
                .forEach(
                        column -> {
                            String hiveType =
                                    HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
                            cols.add(
                                    new FieldSchema(
                                            column.getName(), hiveType, column.getComment()));
                        });
        sd.setCols(cols);

        // Set table location
        String tableLocation = getDefaultTableLocation();
        sd.setLocation(tableLocation);

        // Set storage format (default to Parquet)
        sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");

        // Set SerDe info
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setName(table.getTableName());
        serDeInfo.setSerializationLib(
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        sd.setSerdeInfo(serDeInfo);

        // Set compression
        sd.setCompressed(false);
        sd.setStoredAsSubDirectories(false);

        table.setSd(sd);

        // Set table properties
        table.putToParameters("parquet.compression", "SNAPPY");
        table.putToParameters("created_by", "seatunnel");

        return table;
    }
}
