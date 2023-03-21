package org.apache.seatunnel.connectors.seatunnel.mongodb.internal;

import com.google.common.base.Preconditions;

/** A builder class for creating {@link MongoClientProvider}. */
public class MongoColloctionProviders {

    public static Builder getBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String connectionString;

        private String database;

        private String collection;

        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder collection(String collection) {
            this.collection = collection;
            return this;
        }

        public MongoClientProvider build() {
            Preconditions.checkNotNull(connectionString, "Connection string must not be null");
            Preconditions.checkNotNull(database, "Database must not be null");
            Preconditions.checkNotNull(collection, "Collection must not be null");
            return new MongoSingleCollectionProvider(connectionString, database, collection);
        }
    }
}
