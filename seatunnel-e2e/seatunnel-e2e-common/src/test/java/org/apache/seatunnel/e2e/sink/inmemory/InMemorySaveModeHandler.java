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

package org.apache.seatunnel.e2e.sink.inmemory;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class InMemorySaveModeHandler implements SaveModeHandler {

    private final CatalogTable catalogTable;

    public InMemorySaveModeHandler(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
    }

    @Override
    public void open() {}

    @Override
    public void handleSchemaSaveMode() {
        log.info("handle schema savemode with table path: {}", catalogTable.getTablePath());
    }

    @Override
    public void handleDataSaveMode() {
        log.info("handle data savemode with table path: {}", catalogTable.getTablePath());
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST;
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return DataSaveMode.APPEND_DATA;
    }

    @Override
    public TablePath getHandleTablePath() {
        return catalogTable.getTablePath();
    }

    @Override
    public Catalog getHandleCatalog() {
        return new Catalog() {
            @Override
            public void open() throws CatalogException {}

            @Override
            public void close() throws CatalogException {}

            @Override
            public String name() {
                return "InMemoryCatalog";
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

            @Override
            public boolean tableExists(TablePath tablePath) throws CatalogException {
                return false;
            }

            @Override
            public CatalogTable getTable(TablePath tablePath)
                    throws CatalogException, TableNotExistException {
                return null;
            }

            @Override
            public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
                    throws TableAlreadyExistException, DatabaseNotExistException,
                            CatalogException {}

            @Override
            public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
                    throws TableNotExistException, CatalogException {}

            @Override
            public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
                    throws DatabaseAlreadyExistException, CatalogException {}

            @Override
            public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
                    throws DatabaseNotExistException, CatalogException {}
        };
    }

    @Override
    public void close() throws Exception {}
}
