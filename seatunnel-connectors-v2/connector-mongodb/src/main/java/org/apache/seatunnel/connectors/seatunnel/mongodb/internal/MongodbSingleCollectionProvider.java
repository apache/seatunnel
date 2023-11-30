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

import org.bson.BsonDocument;

import com.google.common.base.Preconditions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MongodbSingleCollectionProvider implements MongodbClientProvider {

    private final String connectionString;

    private final String defaultDatabase;

    private final String defaultCollection;

    private MongoClient client;

    private MongoDatabase database;

    private MongoCollection<BsonDocument> collection;

    public MongodbSingleCollectionProvider(
            String connectionString, String defaultDatabase, String defaultCollection) {
        Preconditions.checkNotNull(connectionString);
        Preconditions.checkNotNull(defaultDatabase);
        Preconditions.checkNotNull(defaultCollection);
        this.connectionString = connectionString;
        this.defaultDatabase = defaultDatabase;
        this.defaultCollection = defaultCollection;
    }

    @Override
    public MongoClient getClient() {
        synchronized (this) {
            if (client == null) {
                client = MongoClients.create(connectionString);
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
