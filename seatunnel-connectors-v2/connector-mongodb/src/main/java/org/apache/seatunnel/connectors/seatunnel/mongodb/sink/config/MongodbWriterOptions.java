package org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config;

import java.io.Serializable;
import java.time.Duration;

/** mongo connector options */
public class MongodbWriterOptions implements Serializable {

    private static final long serialVersionUID = 1;
    protected final String connectString;
    protected final String database;
    protected final String collection;

    protected final boolean transactionEnable;
    protected final boolean flushOnCheckpoint;
    protected final int flushSize;
    protected final Duration flushInterval;
    protected final boolean upsertEnable;
    protected final String[] upsertKey;

    public MongodbWriterOptions(
            String connectString,
            String database,
            String collection,
            boolean transactionEnable,
            boolean flushOnCheckpoint,
            int flushSize,
            Duration flushInterval,
            boolean upsertEnable,
            String[] upsertKey) {
        this.connectString = connectString;
        this.database = database;
        this.collection = collection;
        this.transactionEnable = transactionEnable;
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.flushSize = flushSize;
        this.flushInterval = flushInterval;
        this.upsertEnable = upsertEnable;
        this.upsertKey = upsertKey;
    }

    public String getConnectString() {
        return connectString;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public boolean isTransactionEnable() {
        return transactionEnable;
    }

    public boolean getFlushOnCheckpoint() {
        return flushOnCheckpoint;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public Duration getFlushInterval() {
        return flushInterval;
    }

    public boolean isUpsertEnable() {
        return upsertEnable;
    }

    public String[] getUpsertKey() {
        return upsertKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder For {@link MongodbWriterOptions}. */
    public static class Builder {
        protected String connectString;
        protected String database;
        protected String collection;
        protected String user;
        protected String password;

        protected boolean transactionEnable;
        protected boolean flushOnCheckpoint;
        protected int flushSize;
        protected Duration flushInterval;
        protected boolean upsertEnable;
        protected String[] upsertKey;

        public Builder withConnectString(String connectString) {
            this.connectString = connectString;
            return this;
        }

        public Builder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder withCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder withTransactionEnable(boolean transactionEnable) {
            this.transactionEnable = transactionEnable;
            return this;
        }

        public Builder withFlushOnCheckpoint(boolean flushOnCheckpoint) {
            this.flushOnCheckpoint = flushOnCheckpoint;
            return this;
        }

        public Builder withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public Builder withFlushInterval(Duration flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        public Builder withUpsertEnable(boolean upsertEnable) {
            this.upsertEnable = upsertEnable;
            return this;
        }

        public Builder withUpsertKey(String[] upsertKey) {
            this.upsertKey = upsertKey;
            return this;
        }

        public MongodbWriterOptions build() {
            return new MongodbWriterOptions(
                    connectString,
                    database,
                    collection,
                    transactionEnable,
                    flushOnCheckpoint,
                    flushSize,
                    flushInterval,
                    upsertEnable,
                    upsertKey);
        }
    }
}
