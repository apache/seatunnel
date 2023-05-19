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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import lombok.Getter;

import java.io.Serializable;

@Getter
public class MongodbWriterOptions implements Serializable {

    private static final long serialVersionUID = 1;

    protected final String connectString;

    protected final String database;

    protected final String collection;

    protected final int flushSize;

    protected final long batchIntervalMs;

    protected final boolean upsertEnable;

    protected final String[] upsertKey;

    protected final int retryMax;

    protected final long retryInterval;

    public MongodbWriterOptions(
            String connectString,
            String database,
            String collection,
            int flushSize,
            Long batchIntervalMs,
            boolean upsertEnable,
            String[] upsertKey,
            int retryMax,
            Long retryInterval) {
        this.connectString = connectString;
        this.database = database;
        this.collection = collection;
        this.flushSize = flushSize;
        this.batchIntervalMs = batchIntervalMs;
        this.upsertEnable = upsertEnable;
        this.upsertKey = upsertKey;
        this.retryMax = retryMax;
        this.retryInterval = retryInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder For {@link MongodbWriterOptions}. */
    public static class Builder {
        protected String connectString;

        protected String database;

        protected String collection;

        protected int flushSize;

        protected long batchIntervalMs;

        protected boolean upsertEnable;

        protected String[] upsertKey;

        protected int retryMax;

        protected long retryInterval;

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

        public Builder withFlushSize(int flushSize) {
            this.flushSize = flushSize;
            return this;
        }

        public Builder withBatchIntervalMs(Long batchIntervalMs) {
            this.batchIntervalMs = batchIntervalMs;
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

        public Builder withRetryMax(int retryMax) {
            this.retryMax = retryMax;
            return this;
        }

        public Builder withRetryInterval(Long retryInterval) {
            this.retryInterval = retryInterval;
            return this;
        }

        public MongodbWriterOptions build() {
            return new MongodbWriterOptions(
                    connectString,
                    database,
                    collection,
                    flushSize,
                    batchIntervalMs,
                    upsertEnable,
                    upsertKey,
                    retryMax,
                    retryInterval);
        }
    }
}
