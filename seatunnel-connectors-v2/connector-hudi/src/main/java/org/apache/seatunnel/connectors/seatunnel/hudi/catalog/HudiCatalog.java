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

package org.apache.seatunnel.connectors.seatunnel.hudi.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hbase.thirdparty.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.PARTITION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.RECORD_KEY_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiTableOptions.TABLE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiCatalogUtil.inferTablePath;

@Slf4j
public class HudiCatalog implements Catalog {

    private final String catalogName;
    private final org.apache.hadoop.conf.Configuration hadoopConf;
    private final String tableParentDfsPathStr;
    private final Path tableParentDfsPath;
    private FileSystem fs;

    public HudiCatalog(String catalogName, Configuration hadoopConf, String tableParentDfsPathStr) {
        this.catalogName = catalogName;
        this.hadoopConf = hadoopConf;
        this.tableParentDfsPathStr = tableParentDfsPathStr;
        this.tableParentDfsPath = new Path(tableParentDfsPathStr);
    }

    @Override
    public void open() throws CatalogException {
        fs = HadoopFSUtils.getFs(tableParentDfsPathStr, hadoopConf);
        try {
            if (!fs.exists(tableParentDfsPath)) {
                throw new CatalogException(
                        String.format(
                                "Catalog %s path %s does not exist.",
                                catalogName, tableParentDfsPathStr));
            }
        } catch (IOException e) {
            throw new CatalogException(
                    String.format(
                            "Checking catalog path %s exists exception.", tableParentDfsPathStr),
                    e);
        }

        /*if (!databaseExists(getDefaultDatabase())) {
            log.info(
                    "Creating database {} automatically because it does not exist.",
                    getDefaultDatabase());
            Path dbPath = new Path(tableParentDfsPath, getDefaultDatabase());
            try {
                fs.mkdirs(dbPath);
            } catch (IOException e) {
                throw new CatalogException(
                        String.format("Creating database %s exception.", getDefaultDatabase()), e);
            }
        }*/
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return null;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (StringUtils.isEmpty(databaseName)) {
            throw new CatalogException("Database name is null or empty.");
        }
        return listDatabases().contains(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            FileStatus[] fileStatuses = fs.listStatus(tableParentDfsPath);
            return Arrays.stream(fileStatuses)
                    .filter(FileStatus::isDirectory)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException("Listing database exception.", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }

        Path dbPath = new Path(tableParentDfsPath, databaseName);
        try {
            return Arrays.stream(fs.listStatus(dbPath))
                    .filter(FileStatus::isDirectory)
                    .map(fileStatus -> fileStatus.getPath().getName())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Listing table in database %s exception.", dbPath), e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        String basePath = inferTablePath(tableParentDfsPathStr, tablePath);
        try {
            return fs.exists(new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME))
                    && fs.exists(
                            new Path(
                                    new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME),
                                    HoodieTableConfig.HOODIE_PROPERTIES_FILE));
        } catch (IOException e) {
            throw new CatalogException(
                    "Error while checking whether table exists under path:" + basePath, e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return null;
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "Table path cannot be null");
        checkNotNull(table, "Table cannot be null");

        String tablePathStr = inferTablePath(tableParentDfsPathStr, tablePath);
        Path path = new Path(tablePathStr);
        try {
            fs = FileSystem.get(path.toUri(), hadoopConf);
            if (!fs.exists(path)) {
                HoodieTableMetaClient.PropertyBuilder hoodieTableMetaClientBuilder =
                        HoodieTableMetaClient.withPropertyBuilder()
                                .setTableType(table.getOptions().get(TABLE_TYPE.key()))
                                .setTableName(tablePath.getTableName())
                                .setPartitionFields(table.getOptions().get(PARTITION_FIELDS.key()))
                                .setRecordKeyFields(table.getOptions().get(RECORD_KEY_FIELDS.key()))
                                .setPayloadClassName(HoodieAvroPayload.class.getName());
                hoodieTableMetaClientBuilder.initTable(
                        new HadoopStorageConfiguration(hadoopConf), tablePathStr);
            }
        } catch (IOException e) {
            throw new HoodieCatalogException(
                    String.format("Failed to create table %s", tablePath.getFullName()), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new TableNotExistException(catalogName, tablePath);
            }
        }

        Path path = new Path(inferTablePath(tableParentDfsPathStr, tablePath));
        try {
            this.fs.delete(path, true);
        } catch (IOException e) {
            throw new CatalogException(String.format("Dropping table %s exception.", tablePath), e);
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException("Hudi catalog not support truncate table.");
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(tablePath.getDatabaseName())) {
            if (ignoreIfExists) {
                return;
            } else {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
            }
        }

        Path dbPath = new Path(tableParentDfsPath, tablePath.getDatabaseName());
        try {
            fs.mkdirs(dbPath);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Creating database %s exception.", tablePath.getDatabaseName()),
                    e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        // do nothing
        /*if (!databaseExists(tablePath.getDatabaseName())) {
            if (ignoreIfNotExists) {
                return;
            } else {
                throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
            }
        }

        List<String> tables = listTables(tablePath.getDatabaseName());
        if (!tables.isEmpty() && !cascade) {
            throw new DatabaseNotEmptyException(catalogName, tablePath.getDatabaseName());
        }

        if (tablePath.getDatabaseName().equals(getDefaultDatabase())) {
            throw new IllegalArgumentException(
                    "Hudi catalog doesn't support to drop the default database.");
        }

        Path dbPath = new Path(catalogPath, tablePath.getDatabaseName());
        try {
            fs.delete(dbPath, true);
        } catch (IOException e) {
            throw new CatalogException(String.format("Dropping database %s exception.", tablePath.getDatabaseName()), e);
        }*/
    }
}
