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

package org.apache.seatunnel.connectors.seatunnel.hive.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.stream.Collectors;

public class HiveCatalog implements Catalog {

    private HiveMetaStoreClient hiveMetaStoreClient;

    public HiveCatalog(Config config) {
        HiveMetaStoreProxy instance = HiveMetaStoreProxy.getInstance(config);
        this.hiveMetaStoreClient = instance.getHiveMetaStoreClient();
    }

    @Override
    public void open() throws CatalogException {}

    @Override
    public void close() {
        hiveMetaStoreClient.close();
    }

    @Override
    public String name() {
        return HiveCatalogUtils.CATALOG_NAME;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return HiveCatalogUtils.DEFAULT_DB;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return hiveMetaStoreClient.getAllDatabases();
        } catch (MetaException e) {
            throw new CatalogException(
                    String.format("Failed to list databases. %s", e.getMessage()));
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return hiveMetaStoreClient.getAllTables(databaseName);
        } catch (MetaException e) {
            throw new CatalogException(String.format("Failed to list tables. %s", e.getMessage()));
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return hiveMetaStoreClient
                    .getAllTables(tablePath.getDatabaseName())
                    .contains(tablePath.getTableName());
        } catch (MetaException e) {
            throw new CatalogException(
                    String.format("Failed to check table is exist. %s", e.getMessage()));
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            Table table =
                    hiveMetaStoreClient.getTable(
                            tablePath.getDatabaseName(), tablePath.getTableName());
            List<Column> cols =
                    table.getSd().getCols().stream()
                            .map(HiveCatalogUtils::hiveFieldSchemaToSTColumn)
                            .collect(Collectors.toList());
            List<String> partitionKeys =
                    table.getPartitionKeys().stream()
                            .map(FieldSchema::getName)
                            .collect(Collectors.toList());
            TableIdentifier tableIdentifier =
                    TableIdentifier.of(
                            HiveCatalogUtils.CATALOG_NAME, table.getDbName(), table.getTableName());
            TableSchema tableSchema = new TableSchema.Builder().columns(cols).build();
            CatalogTable catalogTable =
                    CatalogTable.of(
                            tableIdentifier,
                            tableSchema,
                            table.getParameters(),
                            partitionKeys,
                            null,
                            HiveCatalogUtils.CATALOG_NAME);
            return CatalogTable.of(tableIdentifier, catalogTable);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to get table information. %s", e.getMessage()));
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (tableExists(tablePath) && ignoreIfExists) {
            return;
        }
        try {
            Table tbl = new Table();
            tbl.setDbName(tablePath.getDatabaseName());
            tbl.setTableName(tablePath.getTableName());
            StorageDescriptor storageDescriptor = new StorageDescriptor();
            List<FieldSchema> cols =
                    table.getTableSchema().getColumns().stream()
                            .map(HiveCatalogUtils::stColumnToHiveFieldSchema)
                            .collect(Collectors.toList());
            List<FieldSchema> partitionKeys =
                    cols.stream()
                            .filter(c -> table.getPartitionKeys().contains(c.getName()))
                            .collect(Collectors.toList());
            storageDescriptor.setLocation(table.getOptions().get(HiveCatalogUtils.LOCATION));
            tbl.setPartitionKeys(partitionKeys);
            storageDescriptor.setCols(cols);
            tbl.setParameters(table.getOptions());

            tbl.setSd(storageDescriptor);
            hiveMetaStoreClient.createTable(tbl);
        } catch (TException e) {
            throw new CatalogException(String.format("Failed to create table. %s", e.getMessage()));
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            hiveMetaStoreClient.dropTable(tablePath.getDatabaseName(), tablePath.getTableName());
        } catch (TException e) {
            throw new CatalogException(String.format("Failed to drop table. %s", e.getMessage()));
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            Database db = new Database();
            db.setName(tablePath.getDatabaseName());
            hiveMetaStoreClient.createDatabase(db);
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to create database. %s", e.getMessage()));
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try {
            hiveMetaStoreClient.dropDatabase(tablePath.getDatabaseName());
        } catch (TException e) {
            throw new CatalogException(
                    String.format("Failed to drop database. %s", e.getMessage()));
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new CatalogException("Hive Catalog is not support truncate Table!");
    }

    @Override
    public void executeSql(TablePath tablePath, String sql) {
        throw new CatalogException("Hive Catalog is not support execute custom sql!");
    }
}
