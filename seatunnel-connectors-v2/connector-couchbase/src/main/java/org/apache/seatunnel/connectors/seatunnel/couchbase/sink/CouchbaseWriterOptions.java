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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import lombok.Getter;

import java.io.Serializable;

/**
 * Immutable options bag passed from {@link CouchbaseSinkFactory} to {@link CouchbaseWriter}.
 *
 * <p>All fields are final; instances are constructed via {@link Builder}.
 */
@Getter
public class CouchbaseWriterOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String connectionString;
    private final String username;
    private final String password;
    private final String bucket;
    private final String scope;
    private final String collection;
    private final int flushSize;
    private final long batchIntervalMs;
    private final boolean upsertEnable;
    private final String[] primaryKey;
    private final int retryMax;
    private final long retryInterval;

    private CouchbaseWriterOptions(Builder builder) {
        this.connectionString = builder.connectionString;
        this.username = builder.username;
        this.password = builder.password;
        this.bucket = builder.bucket;
        this.scope = builder.scope;
        this.collection = builder.collection;
        this.flushSize = builder.flushSize;
        this.batchIntervalMs = builder.batchIntervalMs;
        this.upsertEnable = builder.upsertEnable;
        this.primaryKey = builder.primaryKey;
        this.retryMax = builder.retryMax;
        this.retryInterval = builder.retryInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for {@link CouchbaseWriterOptions}. */
    public static class Builder {
        private String connectionString;
        private String username;
        private String password;
        private String bucket;
        private String scope = "_default";
        private String collection;
        private int flushSize = 1000;
        private long batchIntervalMs = 30000L;
        private boolean upsertEnable = false;
        private String[] primaryKey = new String[0];
        private int retryMax = 3;
        private long retryInterval = 1000L;

        public Builder withConnectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder withBucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder withScope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder withCollection(String collection) {
            this.collection = collection;
            return this;
        }

        public Builder withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public Builder withBatchIntervalMs(long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
            return this;
        }

        public Builder withUpsertEnable(boolean upsertEnable) {
            this.upsertEnable = upsertEnable;
            return this;
        }

        public Builder withPrimaryKey(String[] primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder withRetryMax(int retryMax) {
            this.retryMax = retryMax;
            return this;
        }

        public Builder withRetryInterval(long retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public CouchbaseWriterOptions build() {
            return new CouchbaseWriterOptions(this);
        }
    }
}
