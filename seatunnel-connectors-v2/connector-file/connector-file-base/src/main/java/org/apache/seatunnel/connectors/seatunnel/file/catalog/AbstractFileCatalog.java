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

package org.apache.seatunnel.connectors.seatunnel.file.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.LocatedFileStatus;

import lombok.SneakyThrows;

import java.io.IOException;
import java.util.List;

public abstract class AbstractFileCatalog implements Catalog {

    protected final String catalogName;
    private final HadoopFileSystemProxy hadoopFileSystemProxy;
    private final String filePath;

    protected AbstractFileCatalog(
            HadoopFileSystemProxy hadoopFileSystemProxy, String filePath, String catalogName) {
        this.catalogName = catalogName;
        this.filePath = filePath;
        this.hadoopFileSystemProxy = hadoopFileSystemProxy;
    }

    @Override
    public void open() throws CatalogException {}

    @Override
    public void close() throws CatalogException {
        if (hadoopFileSystemProxy != null) {
            try {
                hadoopFileSystemProxy.close();
            } catch (IOException e) {
                throw new CatalogException(e);
            }
        }
    }

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
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        return null;
    }

    @SneakyThrows
    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return hadoopFileSystemProxy.fileExist(filePath);
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        return null;
    }

    @SneakyThrows
    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        hadoopFileSystemProxy.createDir(filePath);
    }

    @SneakyThrows
    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        hadoopFileSystemProxy.deleteFile(filePath);
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {}

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {}

    @SneakyThrows
    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        hadoopFileSystemProxy.deleteFile(filePath);
        hadoopFileSystemProxy.createDir(filePath);
    }

    @SneakyThrows
    @Override
    public boolean isExistsData(TablePath tablePath) {
        final List<LocatedFileStatus> locatedFileStatuses =
                hadoopFileSystemProxy.listFile(filePath);
        return CollectionUtils.isNotEmpty(locatedFileStatuses);
    }
}
