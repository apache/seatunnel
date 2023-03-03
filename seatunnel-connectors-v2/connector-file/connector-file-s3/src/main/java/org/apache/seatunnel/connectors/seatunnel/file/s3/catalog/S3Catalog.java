/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.s3.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Conf;
import org.apache.seatunnel.connectors.seatunnel.file.s3.config.S3Config;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategyFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.shade.hadoop.com.google.common.base.Preconditions.checkNotNull;

/**
 * S3 catalog implementation.
 *
 * <p>The given path directory is the database/table.
 */
public class S3Catalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3Catalog.class);

    private final String catalogName;
    private final Config s3Config;

    private String defaultDatabase;
    private FileSystem fileSystem;

    public S3Catalog(String catalogName, Config s3Config) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.s3Config = checkNotNull(s3Config, "s3Config cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        ReadStrategy readStrategy =
                ReadStrategyFactory.of(s3Config.getString(S3Config.FILE_FORMAT_TYPE.key()));
        readStrategy.setPluginConfig(s3Config);
        this.defaultDatabase = s3Config.getString(S3Config.FILE_PATH.key());
        readStrategy = ReadStrategyFactory.of(s3Config.getString(S3Config.FILE_FORMAT_TYPE.key()));
        readStrategy.setPluginConfig(s3Config);
        try {
            fileSystem =
                    FileSystem.get(readStrategy.getConfiguration(S3Conf.buildWithConfig(s3Config)));
        } catch (IOException e) {
            throw new CatalogException("Open S3Catalog failed", e);
        }
        LOGGER.info("S3Catalog {} is opened", catalogName);
    }

    @Override
    public void close() throws CatalogException {
        LOGGER.info("S3Catalog {} is closed", catalogName);
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        // check if the directory exists
        try {
            return fileSystem
                    .getFileStatus(new org.apache.hadoop.fs.Path(databaseName))
                    .isDirectory();
        } catch (FileNotFoundException e) {
            LOGGER.debug("Database {} does not exist", databaseName, e);
            return false;
        } catch (Exception ex) {
            throw new CatalogException("Check database exists failed", ex);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        // todo: Do we need to find all sub directory as database?
        if (databaseExists(defaultDatabase)) {
            return Lists.newArrayList(defaultDatabase);
        }
        return Collections.emptyList();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (databaseExists(databaseName)) {
            return Lists.newArrayList(databaseName);
        }
        return Collections.emptyList();
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        return databaseExists(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        checkNotNull(tablePath, "tablePath cannot be null");
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName());
        // todo:
        TableSchema tableSchema = TableSchema.builder().build();
        return CatalogTable.of(
                tableIdentifier, tableSchema, Collections.emptyMap(), Collections.emptyList(), "");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        createDatabase(tablePath, ignoreIfExists);
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            fileSystem.delete(new org.apache.hadoop.fs.Path(tablePath.getTableName()), true);
        } catch (IOException e) {
            throw new CatalogException("Drop table failed", e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        // todo: Do we need to set schema?
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            fileSystem.create(new org.apache.hadoop.fs.Path(tablePath.getTableName()));
        } catch (FileAlreadyExistsException e) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(catalogName, tablePath, e);
            }
        } catch (IOException e) {
            throw new CatalogException(
                    String.format(
                            "Create table %s at catalog %s failed",
                            tablePath.getTableName(), catalogName),
                    e);
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        try {
            fileSystem.delete(new org.apache.hadoop.fs.Path(tablePath.getDatabaseName()), true);
        } catch (IOException e) {
            throw new CatalogException(
                    String.format("Drop database: %s failed", tablePath.getDatabaseName()), e);
        }
    }
}
