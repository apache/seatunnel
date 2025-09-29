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

package org.apache.seatunnel.connectors.seatunnel.mongodb.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.common.exception.CommonError;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

public class MongodbCatalog implements Catalog {

    private final String catalogName;
    private final String baseUrl;
    private transient MongoClient mongoClient;
    private final String defaultDatabase;

    public MongodbCatalog(String catalogName, String baseUrl, String defaultDatabase) {
        this.catalogName = catalogName;
        this.baseUrl = baseUrl;
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    public void open() throws CatalogException {
        try {
            if (mongoClient == null) {
                mongoClient = MongoClients.create(baseUrl);
            }
        } catch (Exception e) {
            throw new CatalogException("Failed to open MongoDB Catalog: " + e.getMessage(), e);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return listDatabases().contains(databaseName);
        } catch (Exception e) {
            throw new CatalogException("Failed to check database existence: " + databaseName, e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            List<String> dbs = new ArrayList<>();
            for (String name : mongoClient.listDatabaseNames()) {
                dbs.add(name);
            }
            return dbs;
        } catch (Exception e) {
            throw new CatalogException("Failed to list databases", e);
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(name(), databaseName);
        }
        try {
            MongoDatabase db = mongoClient.getDatabase(databaseName);
            return db.listCollectionNames().into(new ArrayList<>());
        } catch (Exception e) {
            throw new CatalogException("Failed to list tables for database: " + databaseName, e);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return listTables(tablePath.getDatabaseName()).contains(tablePath.getTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        throw CommonError.unsupportedOperation(name(), "get table with tablePath ");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(name(), tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (ignoreIfExists) return;
            throw new TableAlreadyExistException(name(), tablePath);
        }
        try {
            MongoDatabase db = mongoClient.getDatabase(tablePath.getDatabaseName());
            db.createCollection(tablePath.getTableName());
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to create collection: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) return;
            throw new TableNotExistException(name(), tablePath);
        }
        try {
            MongoDatabase db = mongoClient.getDatabase(tablePath.getDatabaseName());
            db.getCollection(tablePath.getTableName()).drop();
        } catch (Exception e) {
            throw new CatalogException("Failed to drop collection: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw CommonError.unsupportedOperation(name(), "create database ");
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw CommonError.unsupportedOperation(name(), "drop database ");
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            if (!tableExists(tablePath)) {
                if (ignoreIfNotExists) {
                    return;
                }
                throw new TableNotExistException(name(), tablePath);
            }
            MongoDatabase db = mongoClient.getDatabase(tablePath.getDatabaseName());
            MongoCollection<Document> collection = db.getCollection(tablePath.getTableName());
            collection.deleteMany(new Document());
        } catch (Exception e) {
            throw new CatalogException(
                    "Failed to truncate collection: " + tablePath.getFullName(), e);
        }
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        try {
            if (!tableExists(tablePath)) {
                return false;
            }
            MongoDatabase db = mongoClient.getDatabase(tablePath.getDatabaseName());
            MongoCollection<Document> collection = db.getCollection(tablePath.getTableName());
            return collection.estimatedDocumentCount() > 0;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void close() throws CatalogException {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
