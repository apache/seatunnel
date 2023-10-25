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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Projects;
import com.aliyun.odps.Tables;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_ID;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.ENDPOINT;
import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PROJECT;

@Slf4j
public class MaxComputeCatalog implements Catalog {

    private final ReadonlyConfig readonlyConfig;
    private final String catalogName;

    private Account account;

    public MaxComputeCatalog(String catalogName, ReadonlyConfig options) {
        this.readonlyConfig = options;
        this.catalogName = catalogName;
    }

    @Override
    public void open() throws CatalogException {
        account = new AliyunAccount(readonlyConfig.get(ACCESS_ID), readonlyConfig.get(ACCESS_KEY));
    }

    @Override
    public void close() throws CatalogException {}

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return readonlyConfig.get(PROJECT);
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        Odps odps = new Odps(account);
        odps.setEndpoint(readonlyConfig.get(ENDPOINT));
        odps.setDefaultProject(readonlyConfig.get(PROJECT));
        Projects projects = odps.projects();
        try {
            return projects.exists(databaseName);
        } catch (OdpsException e) {
            throw new CatalogException("Check " + databaseName + " exist error", e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            // todo: how to get all projects
            String project = readonlyConfig.get(PROJECT);
            if (databaseExists(project)) {
                return Lists.newArrayList(project);
            }
            return Collections.emptyList();
        } catch (Exception e) {
            throw new CatalogException("listDatabases exist error", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        Odps odps = new Odps(account);
        odps.setEndpoint(readonlyConfig.get(ENDPOINT));
        odps.setDefaultProject(databaseName);

        Tables tables = odps.tables();
        List<String> tableNames = new ArrayList<>();
        tables.forEach(
                table -> {
                    tableNames.add(table.getName());
                });
        return tableNames;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        Odps odps = new Odps(account);
        odps.setEndpoint(readonlyConfig.get(ENDPOINT));
        odps.setDefaultProject(tablePath.getDatabaseName());

        Tables tables = odps.tables();
        try {
            return tables.exists(tablePath.getTableName());
        } catch (OdpsException e) {
            throw new CatalogException("tableExists" + tablePath + " error", e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
