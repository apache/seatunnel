package org.apache.seatunnel.connectors.seatunnel.mongodb.internal;

import org.bson.Document;

import com.google.common.base.Preconditions;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;

/** A simple implementation of {@link MongoClientProvider}. */
@Slf4j
public class MongoSingleCollectionProvider implements MongoClientProvider {

    /** Connection string to MongoDB standalone instances, replica sets or sharded clusters. */
    private final String connectionString;

    /** The MongoDB defaultDatabase to write to. */
    private final String defaultDatabase;

    /**
     * The defaultCollection to write to. Must be a existing defaultCollection for MongoDB 4.2 and
     * earlier versions.
     */
    private final String defaultCollection;

    private transient MongoClient client;

    private transient MongoDatabase database;

    private transient MongoCollection<Document> collection;

    public MongoSingleCollectionProvider(
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
    public MongoCollection<Document> getDefaultCollection() {
        synchronized (this) {
            if (collection == null) {
                collection = getDefaultDatabase().getCollection(defaultCollection);
            }
        }
        return collection;
    }

    @Override
    public MongoClient recreateClient() {
        close();
        return getClient();
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
