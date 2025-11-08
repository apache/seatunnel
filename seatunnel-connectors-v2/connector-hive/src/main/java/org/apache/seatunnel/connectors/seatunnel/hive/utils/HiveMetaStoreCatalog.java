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
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * HiveMetaStoreCatalog implements the SeaTunnel Catalog interface. Provides Hive Metastore database
 * & table metadata operations with retry and security support.
 */
@Slf4j
public class HiveMetaStoreCatalog implements Catalog, Closeable, Serializable {
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
    private transient HiveConf hiveConf;
    private transient UserGroupInformation userGroupInformation;

    public HiveMetaStoreCatalog(ReadonlyConfig config) {
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

    public static HiveMetaStoreCatalog create(ReadonlyConfig config) {
        return new HiveMetaStoreCatalog(config);
    }

    public static HiveMetaStoreCatalog getInstance(ReadonlyConfig config) {
        return create(config);
    }

    private synchronized HiveMetaStoreClient getClient() {
        if (hiveClient == null) {
            hiveClient = initializeClient();
        }
        if (kerberosEnabled) {
            maybeRelogin();
        }
        return hiveClient;
    }

    private HiveMetaStoreClient initializeClient() {
        this.hiveConf = buildHiveConf();
        try {
            if (kerberosEnabled) {
                return loginWithKerberos(hiveConf);
            }
            if (remoteUserEnabled) {
                return loginWithRemoteUser(hiveConf);
            }
            return new HiveMetaStoreClient(hiveConf);
        } catch (Exception e) {
            String errMsg =
                    String.format(
                            "Failed to initialize HiveMetaStoreClient [uris=%s, hiveSite=%s]",
                            metastoreUri, hiveSitePath);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errMsg, e);
        }
    }

    /**
     * Try to execute SQL via HiveServer2 JDBC. Returns true if successful, false if HiveServer2 is
     * not available or execution failed.
     */
    public boolean tryExecuteSqlViaJdbc(String sql) {
        String jdbcUrl = getHiveServer2JdbcUrl();
        if (jdbcUrl == null) {
            return false;
        }

        Connection conn = null;
        Statement stmt = null;
        try {
            // Load Hive JDBC driver
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            // Create connection and execute SQL
            conn = DriverManager.getConnection(jdbcUrl);
            stmt = conn.createStatement();
            stmt.execute(sql);
            return true;

        } catch (ClassNotFoundException e) {
            log.debug("Hive JDBC driver not found, falling back to Metastore Client");
            return false;
        } catch (java.sql.SQLException e) {
            log.debug("Failed to execute SQL via HiveServer2 JDBC: {}", e.getMessage());
            return false;
        } finally {
            // Close resources
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (java.sql.SQLException e) {
                log.debug("Error closing JDBC resources: {}", e.getMessage());
            }
        }
    }

    /**
     * Get HiveServer2 JDBC URL from HiveConf or derive from metastore URI. Returns null if not
     * available.
     */
    private String getHiveServer2JdbcUrl() {
        if (hiveConf == null) {
            getClient();
        }

        // Try to get from hive-site.xml configuration
        String jdbcUrl = hiveConf.get("hive.server2.jdbc.url");
        if (jdbcUrl != null && !jdbcUrl.trim().isEmpty()) {
            return jdbcUrl;
        }

        // Try to derive from metastore URI
        // metastore URI format: thrift://host:9083
        // HiveServer2 JDBC URL format: jdbc:hive2://host:10000/default
        try {
            if (metastoreUri != null && metastoreUri.startsWith("thrift://")) {
                URI uri = new URI(metastoreUri);
                String host = uri.getHost();
                if (host != null) {
                    return String.format("jdbc:hive2://%s:10000/default", host);
                }
            }
        } catch (java.net.URISyntaxException e) {
            log.debug("Failed to derive HiveServer2 JDBC URL: {}", e.getMessage());
        }

        return null;
    }

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);
        hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI, false);
        hiveConf.setBoolean("hive.metastore.client.capability.check", false);
        hiveConf.setBoolean("hive.metastore.client.filter.enabled", false);
        hiveConf.setInt("hive.metastore.client.socket.timeout", 600);
        hiveConf.setInt("hive.metastore.client.connect.retry.delay", 5);
        hiveConf.setInt("hive.metastore.failure.retries", 3);

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
        log.debug("Hive client configuration initialized");
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
                (conf, ugi) -> {
                    this.userGroupInformation = ugi;
                    return new HiveMetaStoreClient(hiveConf);
                });
    }

    private HiveMetaStoreClient loginWithRemoteUser(HiveConf hiveConf) throws Exception {
        return HadoopLoginFactory.loginWithRemoteUser(
                new Configuration(), remoteUser, (conf, ugi) -> new HiveMetaStoreClient(hiveConf));
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return getClient().getTable(dbName, tableName);
        } catch (TException e) {
            String msg = String.format("Failed to get table %s.%s", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, msg, e);
        }
    }

    public void createDatabaseIfNotExists(String db) throws TException {
        try {
            try {
                getClient().getDatabase(db);
                log.debug("Database {} already exists", db);
                return;
            } catch (org.apache.hadoop.hive.metastore.api.NoSuchObjectException ignored) {
            }
            Database database = new Database();
            database.setName(db);
            log.info("Creating database {}", db);
            getClient().createDatabase(database);
        } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
            log.debug("Database {} already exists (race)", db);
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
                log.debug("Table {}.{} already exists", tbl.getDbName(), tbl.getTableName());
                return;
            }
            log.info("Creating table {}.{}", tbl.getDbName(), tbl.getTableName());
            getClient().createTable(tbl);
        } catch (org.apache.hadoop.hive.metastore.api.AlreadyExistsException e) {
            log.debug("Table {}.{} already exists (race)", tbl.getDbName(), tbl.getTableName());
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

    public void createTableFromTemplate(@NonNull Table table) throws TException {
        log.info("Create table from template {}.{}", table.getDbName(), table.getTableName());
        createTableIfNotExists(table);
    }

    @Override
    public void open() throws CatalogException {
        try {
            getClient();
        } catch (HiveConnectorException e) {
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
        } catch (TableNotExistException e) {
            throw e;
        } catch (HiveConnectorException e) {
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
        } catch (TableAlreadyExistException | DatabaseNotExistException | CatalogException e) {
            throw e;
        } catch (HiveConnectorException e) {
            throw new CatalogException("Failed to create table: " + tablePath, e);
        } catch (TException e) {
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

    private void maybeRelogin() {
        if (userGroupInformation == null) {
            return;
        }
        try {
            if (userGroupInformation.isFromKeytab()) {
                userGroupInformation.checkTGTAndReloginFromKeytab();
            }
        } catch (Exception e) {
            log.warn("Kerberos re-login for HiveMetaStore failed: {}", e.getMessage());
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
                org.apache.seatunnel.connectors.seatunnel.hive.utils.HiveLocationUtils
                        .qualifiedDefaultLocation(
                                hadoopConfDir,
                                hiveSitePath,
                                tablePath.getDatabaseName(),
                                tablePath.getTableName());
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
