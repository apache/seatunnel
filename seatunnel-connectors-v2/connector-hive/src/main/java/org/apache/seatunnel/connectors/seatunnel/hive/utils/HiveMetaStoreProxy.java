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

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);

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
            return getClient().getTable(dbName, tableName);
        } catch (TException e) {
            String msg = String.format("Failed to get table %s.%s", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, msg, e);
        }
    }

    public void createDatabaseIfNotExists(String db) throws TException {
        List<String> databases = getClient().getAllDatabases();
        if (databases.contains(db)) {
            return;
        }
        Database database = new Database();
        database.setName(db);
        getClient().createDatabase(database);
    }

    public void createTableIfNotExists(@NonNull Table tbl) throws TException {
        if (getClient().tableExists(tbl.getDbName(), tbl.getTableName())) {
            return;
        }
        try {
            getClient().createTable(tbl);
        } catch (TException e) {
            log.error(
                    "Failed to create table: {}.{}, error: {}",
                    tbl.getDbName(),
                    tbl.getTableName(),
                    e.getMessage());
            String errorMsg =
                    String.format(
                            "Failed to create table [%s.%s]", tbl.getDbName(), tbl.getTableName());
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.CREATE_HIVE_TABLE_FAILED, errorMsg, e);
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
            List<String> databases = getClient().getAllDatabases();
            return databases.contains(dbName);
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

    // ========== Catalog Interface Implementation ==========

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

    // Note: databaseExists method is already implemented above, reusing it for Catalog interface

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return getClient().getAllDatabases();
        } catch (TException e) {
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
        // This method would need to be implemented to convert Hive Table to CatalogTable
        // For now, throw UnsupportedOperationException as this requires complex conversion logic
        throw new UnsupportedOperationException(
                "getTable method needs to be implemented with proper Hive to CatalogTable conversion");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        // This method would need to be implemented to convert CatalogTable to Hive Table
        // For now, throw UnsupportedOperationException as this requires complex conversion logic
        throw new UnsupportedOperationException(
                "createTable method needs to be implemented with proper CatalogTable to Hive conversion");
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
}
