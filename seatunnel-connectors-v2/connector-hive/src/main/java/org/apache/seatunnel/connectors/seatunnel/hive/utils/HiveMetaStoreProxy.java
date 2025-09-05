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

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopLoginFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy implements Catalog, Closeable, Serializable {
    private static final List<String> HADOOP_CONF_FILES = ImmutableList.of("hive-site.xml");

    private final String metastoreUri;
    private final String hadoopConfDir;
    private final String hiveSitePath;
    private final boolean kerberosEnabled;
    private final boolean remoteUserEnabled;

    private final String krb5Path;
    private final String principal;
    private final String keytabPath;
    private final String remoteUser;

    private transient HiveMetaStoreClient hiveClient;

    public HiveMetaStoreProxy(ReadonlyConfig config) {
        this.metastoreUri = config.get(HiveOptions.METASTORE_URI);
        this.hadoopConfDir = config.get(HiveConfig.HADOOP_CONF_PATH);
        this.hiveSitePath = config.get(HiveConfig.HIVE_SITE_PATH);
        this.kerberosEnabled = HiveMetaStoreProxyUtils.enableKerberos(config);
        this.remoteUserEnabled = HiveMetaStoreProxyUtils.enableRemoteUser(config);
        this.krb5Path = config.get(HdfsSourceConfigOptions.KRB5_PATH);
        this.principal = config.get(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL);
        this.keytabPath = config.get(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH);
        this.remoteUser = config.get(HdfsSourceConfigOptions.REMOTE_USER);
    }

    private synchronized HiveMetaStoreClient getClient() {
        if (hiveClient == null) {
            hiveClient = initializeClient();
        }
        return hiveClient;
    }

    private HiveMetaStoreClient initializeClient() {
        HiveConf hiveConf = buildHiveConf();
        hiveConf.setBoolean("hive.metastore.client.capability.check", false);
        hiveConf.setBoolean("hive.metastore.client.filter.enabled", false);
        hiveConf.setInt("hive.metastore.client.socket.timeout", 600);
        hiveConf.setInt("hive.metastore.client.connect.retry.delay", 5);
        hiveConf.setInt("hive.metastore.failure.retries", 3);

        try {
            if (kerberosEnabled) {
                return loginWithKerberos(hiveConf);
            }
            if (remoteUserEnabled) {
                return loginWithRemoteUser(hiveConf);
            }

            log.info("Initializing HiveMetaStoreClient with URI: {}", metastoreUri);
            HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
            log.info("Successfully initialized HiveMetaStoreClient");
            return client;

        } catch (Exception e) {
            log.error("Failed to initialize HiveMetaStoreClient: {}", e.getMessage(), e);
            String errMsg =
                    String.format(
                            "Failed to initialize HiveMetaStoreClient [uris=%s, hiveSite=%s]. "
                                    + "This may be due to version compatibility issues between client and server. "
                                    + "Error: %s",
                            metastoreUri, hiveSitePath, e.getMessage());
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errMsg, e);
        }
    }
    // Simple retry helper to mitigate transient HMS startup/connection issues in e2e
    @FunctionalInterface
    private interface SupplierEx<T> {
        T get() throws Exception;
    }

    private <T> T withRetry(SupplierEx<T> action, String desc) throws Exception {
        int attempts = 10;
        for (int i = 1; i <= attempts; i++) {
            try {
                return action.get();
            } catch (Exception e) {
                String err = (e.getMessage() == null) ? e.toString() : e.getMessage();
                if (i == attempts) {
                    log.error(
                            "HiveMetaStore operation '{}' failed after {} attempts. Final error: {}",
                            desc,
                            attempts,
                            err,
                            e);
                    throw e;
                }
                log.warn(
                        "HiveMetaStore operation '{}' failed on attempt {}/{}: {}. Will retry in {}ms...",
                        desc,
                        i,
                        attempts,
                        err,
                        3000L);
                // force re-initialize client on next try
                try {
                    this.hiveClient = null;
                } catch (Exception ignore) {
                }
                try {
                    Thread.sleep(3000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during HMS retry", ie);
                }
            }
        }
        return null; // unreachable
    }

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);
        // Avoid calling set_ugi for compatibility with older Metastore servers
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);

        if (StringUtils.isNotBlank(hadoopConfDir)) {
            for (String fileName : HADOOP_CONF_FILES) {
                Path path = Paths.get(hadoopConfDir, fileName);
                if (Files.exists(path)) {
                    try {
                        hiveConf.addResource(path.toUri().toURL());
                    } catch (IOException e) {
                        log.warn("Error adding Hadoop config {}", path, e);
                    }
                }
            }
        }
        if (StringUtils.isNotBlank(hiveSitePath)) {
            try {
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            } catch (MalformedURLException e) {
                log.warn("Invalid hiveSitePath {}", hiveSitePath, e);
            }
        }
        log.info("Hive client configuration: {}", hiveConf);
        return hiveConf;
    }

    private HiveMetaStoreClient loginWithKerberos(HiveConf hiveConf) throws Exception {
        Configuration authConf = new Configuration();
        authConf.set("hadoop.security.authentication", "kerberos");
        return HadoopLoginFactory.loginWithKerberos(
                authConf,
                krb5Path,
                principal,
                keytabPath,
                (conf, ugi) -> new HiveMetaStoreClient(hiveConf));
    }

    private HiveMetaStoreClient loginWithRemoteUser(HiveConf hiveConf) throws Exception {
        return HadoopLoginFactory.loginWithRemoteUser(
                new Configuration(), remoteUser, (conf, ugi) -> new HiveMetaStoreClient(hiveConf));
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return withRetry(
                    () -> getClient().getTable(dbName, tableName),
                    String.format("getTable %s.%s", dbName, tableName));
        } catch (Exception e) {
            String msg = String.format("Failed to get table %s.%s", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, msg, e);
        }
    }

    public void createDatabaseIfNotExists(String db) throws TException {
        try {
            // Prefer a direct getDatabase check to avoid listing all dbs (more efficient and
            // robust)
            try {
                getClient().getDatabase(db);
                log.info("Database {} already exists, skipping creation", db);
                return;
            } catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException ignored) {
                // database does not exist, will create below
            }

            Database database = new Database();
            database.setName(db);
            log.info("Creating database: {}", db);
            getClient().createDatabase(database);
            log.info("Successfully created database: {}", db);
        } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
            // concurrent creation, safe to ignore
            log.info("Database {} already exists (concurrent creation)", db);
        } catch (TException e) {
            String errorMsg = String.format("Failed to create database [%s]", db);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED, errorMsg, e);
        } catch (Exception e) {
            throw new TException("Unexpected error creating database: " + db, e);
        }
    }

    public void createTableIfNotExists(@NonNull Table tbl) throws TException {
        try {
            if (getClient().tableExists(tbl.getDbName(), tbl.getTableName())) {
                log.info(
                        "Table {}.{} already exists, skipping creation",
                        tbl.getDbName(),
                        tbl.getTableName());
                return;
            }
            log.info("Creating table: {}.{}", tbl.getDbName(), tbl.getTableName());
            getClient().createTable(tbl);
            log.info("Successfully created table: {}.{}", tbl.getDbName(), tbl.getTableName());
        } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
            log.info(
                    "Table {}.{} already exists (concurrent creation)",
                    tbl.getDbName(),
                    tbl.getTableName());
        } catch (TException e) {
            String errorMsg =
                    String.format(
                            "Failed to create table [%s.%s]", tbl.getDbName(), tbl.getTableName());
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED, errorMsg, e);
        } catch (Exception e) {
            throw new TException(
                    "Unexpected error creating table: "
                            + tbl.getDbName()
                            + "."
                            + tbl.getTableName(),
                    e);
        }
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            try {
                getClient().appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException ae) {
                log.warn("Partition {} already exists", partition);
            }
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            getClient().dropPartition(dbName, tableName, partition, false);
        }
    }

    public boolean tableExists(@NonNull String dbName, @NonNull String tableName) {
        try {
            return getClient().tableExists(dbName, tableName);
        } catch (TException e) {
            String msg = String.format("Failed to check if table %s.%s exists", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, msg, e);
        }
    }

    @Override
    public boolean databaseExists(String dbName) throws CatalogException {
        try {
            try {
                getClient().getDatabase(dbName);
                return true;
            } catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException e) {
                return false;
            }
        } catch (TException e) {
            throw new CatalogException("Failed to check if database exists: " + dbName, e);
        }
    }

    public void dropTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            getClient().dropTable(dbName, tableName, true, true);
        } catch (TException e) {
            String msg = String.format("Failed to drop table %s.%s", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED, msg, e);
        }
    }

    /**
     * Create table using template-based approach This method creates a Table object from template
     * information and uses MetaStore API
     */
    public void createTableFromTemplate(@NonNull Table table) throws TException {
        log.info("Creating table from template: {}.{}", table.getDbName(), table.getTableName());
        createTableIfNotExists(table);
        log.info("Successfully created table from template");
    }

    public static HiveMetaStoreProxy getInstance(ReadonlyConfig config) {
        return new HiveMetaStoreProxy(config);
    }

    @Override
    public void open() throws CatalogException {
        try {
            getClient();
        } catch (Exception e) {
            throw new CatalogException("Failed to open Hive catalog", e);
        }
    }

    @Override
    public String name() {
        return "hive";
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return getClient().getAllDatabases();
        } catch (TException e) {
            // 提示性增强，帮助定位 HMS 兼容问题
            log.warn(
                    "listDatabases failed via getAllDatabases(), check HMS version compatibility: {}",
                    e.getMessage());
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            if (!databaseExists(databaseName)) {
                throw new DatabaseNotExistException("hive", databaseName);
            }
            return getClient().getAllTables(databaseName);
        } catch (TException e) {
            throw new CatalogException("Failed to list tables in database: " + databaseName, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return tableExists(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            if (!tableExists(tablePath.getDatabaseName(), tablePath.getTableName())) {
                throw new TableNotExistException("hive", tablePath);
            }
            Table hiveTable = getTable(tablePath.getDatabaseName(), tablePath.getTableName());
            return convertHiveTableToCatalogTable(hiveTable);
        } catch (Exception e) {
            throw new CatalogException("Failed to get table: " + tablePath, e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try {
            if (!databaseExists(tablePath.getDatabaseName())) {
                throw new DatabaseNotExistException("hive", tablePath.getDatabaseName());
            }

            if (tableExists(tablePath.getDatabaseName(), tablePath.getTableName())) {
                if (!ignoreIfExists) {
                    throw new TableAlreadyExistException("hive", tablePath);
                }
                return;
            }

            Table hiveTable = convertCatalogTableToHiveTable(tablePath, table);
            createTableIfNotExists(hiveTable);
        } catch (TableAlreadyExistException | DatabaseNotExistException e) {
            throw e;
        } catch (Exception e) {
            throw new CatalogException("Failed to create table: " + tablePath, e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException("hive", tablePath);
        }
        if (tableExists(tablePath)) {
            dropTable(tablePath.getDatabaseName(), tablePath.getTableName());
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            createDatabaseIfNotExists(tablePath.getDatabaseName());
        } catch (TException e) {
            if (e instanceof AlreadyExistsException && !ignoreIfExists) {
                throw new DatabaseAlreadyExistException("hive", tablePath.getDatabaseName());
            }
            throw new CatalogException(
                    "Failed to create database: " + tablePath.getDatabaseName(), e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try {
            if (!databaseExists(tablePath.getDatabaseName()) && !ignoreIfNotExists) {
                throw new DatabaseNotExistException("hive", tablePath.getDatabaseName());
            }
            if (databaseExists(tablePath.getDatabaseName())) {
                getClient().dropDatabase(tablePath.getDatabaseName());
            }
        } catch (TException e) {
            throw new CatalogException(
                    "Failed to drop database: " + tablePath.getDatabaseName(), e);
        }
    }

    @Override
    public synchronized void close() throws CatalogException {
        if (Objects.nonNull(hiveClient)) {
            hiveClient.close();
        }
    }

    private CatalogTable convertHiveTableToCatalogTable(Table hiveTable) {
        List<org.apache.seatunnel.api.table.catalog.Column> columns = new ArrayList<>();

        if (hiveTable.getSd() != null && hiveTable.getSd().getCols() != null) {
            for (org.apache.hadoop.hive.metastore.api.FieldSchema field :
                    hiveTable.getSd().getCols()) {
                org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> dataType =
                        HiveTypeConvertor.covertHiveTypeToSeaTunnelType(
                                field.getName(), field.getType());
                columns.add(
                        org.apache.seatunnel.api.table.catalog.PhysicalColumn.of(
                                field.getName(), dataType, 0, true, null, field.getComment()));
            }
        }

        if (hiveTable.getPartitionKeys() != null) {
            for (org.apache.hadoop.hive.metastore.api.FieldSchema partitionKey :
                    hiveTable.getPartitionKeys()) {
                org.apache.seatunnel.api.table.type.SeaTunnelDataType<?> dataType =
                        HiveTypeConvertor.covertHiveTypeToSeaTunnelType(
                                partitionKey.getName(), partitionKey.getType());
                columns.add(
                        org.apache.seatunnel.api.table.catalog.PhysicalColumn.of(
                                partitionKey.getName(),
                                dataType,
                                0,
                                true,
                                null,
                                partitionKey.getComment()));
            }
        }

        org.apache.seatunnel.api.table.catalog.TableSchema tableSchema =
                org.apache.seatunnel.api.table.catalog.TableSchema.builder()
                        .columns(columns)
                        .build();

        org.apache.seatunnel.api.table.catalog.TableIdentifier tableId =
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        "hive", hiveTable.getDbName(), hiveTable.getTableName());

        String comment =
                hiveTable.getParameters() != null ? hiveTable.getParameters().get("comment") : null;

        return org.apache.seatunnel.api.table.catalog.CatalogTable.of(
                tableId,
                tableSchema,
                hiveTable.getParameters() != null
                        ? hiveTable.getParameters()
                        : new java.util.HashMap<>(),
                new ArrayList<>(),
                comment);
    }

    private Table convertCatalogTableToHiveTable(TablePath tablePath, CatalogTable catalogTable) {
        Table hiveTable = new Table();
        hiveTable.setDbName(tablePath.getDatabaseName());
        hiveTable.setTableName(tablePath.getTableName());
        hiveTable.setOwner(System.getProperty("user.name", "seatunnel"));
        hiveTable.setCreateTime((int) (System.currentTimeMillis() / 1000));
        hiveTable.setTableType("MANAGED_TABLE");

        org.apache.hadoop.hive.metastore.api.StorageDescriptor sd =
                new org.apache.hadoop.hive.metastore.api.StorageDescriptor();

        List<org.apache.hadoop.hive.metastore.api.FieldSchema> cols = new ArrayList<>();
        for (org.apache.seatunnel.api.table.catalog.Column column :
                catalogTable.getTableSchema().getColumns()) {
            String hiveType = HiveTypeConvertor.seatunnelToHiveType(column.getDataType());
            cols.add(
                    new org.apache.hadoop.hive.metastore.api.FieldSchema(
                            column.getName(), hiveType, column.getComment()));
        }
        sd.setCols(cols);

        sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
        sd.getSerdeInfo()
                .setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
        sd.getSerdeInfo().setName(hiveTable.getTableName());

        String defaultLocation =
                String.format(
                        "/user/hive/warehouse/%s.db/%s",
                        tablePath.getDatabaseName(), tablePath.getTableName());
        sd.setLocation(defaultLocation);

        sd.setCompressed(true);
        sd.setStoredAsSubDirectories(false);

        hiveTable.setSd(sd);
        hiveTable.setPartitionKeys(new ArrayList<>());

        java.util.Map<String, String> parameters = new java.util.HashMap<>();
        parameters.put("seatunnel.created", "true");
        parameters.put("seatunnel.created.time", String.valueOf(System.currentTimeMillis()));
        if (catalogTable.getComment() != null) {
            parameters.put("comment", catalogTable.getComment());
        }
        hiveTable.setParameters(parameters);

        return hiveTable;
    }
}
