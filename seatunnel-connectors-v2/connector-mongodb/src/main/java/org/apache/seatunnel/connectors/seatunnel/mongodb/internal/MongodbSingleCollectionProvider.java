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

package org.apache.seatunnel.connectors.seatunnel.mongodb.internal;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;

import org.bson.BsonDocument;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MongodbSingleCollectionProvider implements MongodbClientProvider {

    private final String connectionString;

    private final String defaultDatabase;

    private final String defaultCollection;

    /** Optional PEM CA bundle; {@code null} creates the client from the connection string only. */
    private final String tlsCaFile;

    private MongoClient client;

    private MongoDatabase database;

    private MongoCollection<BsonDocument> collection;

    public MongodbSingleCollectionProvider(
            String connectionString, String defaultDatabase, String defaultCollection) {
        this(connectionString, defaultDatabase, defaultCollection, null);
    }

    public MongodbSingleCollectionProvider(
            String connectionString,
            String defaultDatabase,
            String defaultCollection,
            String tlsCaFile) {
        Preconditions.checkNotNull(connectionString);
        Preconditions.checkNotNull(defaultDatabase);
        Preconditions.checkNotNull(defaultCollection);
        this.connectionString = connectionString;
        this.defaultDatabase = defaultDatabase;
        this.defaultCollection = defaultCollection;
        this.tlsCaFile = tlsCaFile;
    }

    @Override
    public MongoClient getClient() {
        synchronized (this) {
            if (client == null) {
                client = MongodbClientSettingsFactory.createClient(connectionString, tlsCaFile);
            }
        }
        return client;
    }

    @Override
    public MongoDatabase getDefaultDatabase() {
        synchronized (this) {
            if (database == null) {
                database = getClient().getDatabase(defaultDatabase);
            }
        }
        return database;
    }

    @Override
    public MongoCollection<BsonDocument> getDefaultCollection() {
        synchronized (this) {
            if (collection == null) {
                collection =
                        getDefaultDatabase().getCollection(defaultCollection, BsonDocument.class);
            }
        }
        return collection;
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            log.error("Failed to close Mongo client", e);
        } finally {
            client = null;
        }
    }
}
