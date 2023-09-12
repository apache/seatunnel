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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import java.util.List;
import java.util.Objects;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.HEARTBEAT_INTERVAL_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.POLL_AWAIT_TIME_MILLIS;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions.POLL_MAX_BATCH_SIZE;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class MongodbSourceConfigProvider {

    private MongodbSourceConfigProvider() {}

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements SourceConfig.Factory<MongodbSourceConfig> {
        private String hosts;
        private String username;
        private String password;
        private List<String> databaseList;
        private List<String> collectionList;
        private String connectionOptions;
        private int batchSize = BATCH_SIZE.defaultValue();
        private int pollAwaitTimeMillis = POLL_AWAIT_TIME_MILLIS.defaultValue();
        private int pollMaxBatchSize = POLL_MAX_BATCH_SIZE.defaultValue();
        private StartupConfig startupOptions;
        private StopConfig stopOptions;
        private int heartbeatIntervalMillis = HEARTBEAT_INTERVAL_MILLIS.defaultValue();
        private int splitMetaGroupSize = 2;
        private int splitSizeMB = INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB.defaultValue();

        public Builder hosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder connectionOptions(String connectionOptions) {
            this.connectionOptions = connectionOptions;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder databaseList(List<String> databases) {
            this.databaseList = databases;
            return this;
        }

        public Builder collectionList(List<String> collections) {
            this.collectionList = collections;
            return this;
        }

        public Builder batchSize(int batchSize) {
            checkArgument(batchSize >= 0);
            this.batchSize = batchSize;
            return this;
        }

        public Builder pollAwaitTimeMillis(int pollAwaitTimeMillis) {
            checkArgument(pollAwaitTimeMillis > 0);
            this.pollAwaitTimeMillis = pollAwaitTimeMillis;
            return this;
        }

        public Builder pollMaxBatchSize(int pollMaxBatchSize) {
            checkArgument(pollMaxBatchSize > 0);
            this.pollMaxBatchSize = pollMaxBatchSize;
            return this;
        }

        public Builder startupOptions(StartupConfig startupOptions) {
            this.startupOptions = Objects.requireNonNull(startupOptions);
            if (startupOptions.getStartupMode() != StartupMode.INITIAL
                    && startupOptions.getStartupMode() != StartupMode.TIMESTAMP) {
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unsupported startup mode " + startupOptions.getStartupMode());
            }
            return this;
        }

        public Builder stopOptions(StopConfig stopOptions) {
            this.stopOptions = Objects.requireNonNull(stopOptions);
            if (stopOptions.getStopMode() != StopMode.NEVER) {
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        String.format("The %s mode is not supported.", stopOptions.getStopMode()));
            }
            return this;
        }

        public Builder heartbeatIntervalMillis(int heartbeatIntervalMillis) {
            checkArgument(heartbeatIntervalMillis >= 0);
            this.heartbeatIntervalMillis = heartbeatIntervalMillis;
            return this;
        }

        public Builder splitSizeMB(int splitSizeMB) {
            checkArgument(splitSizeMB > 0);
            this.splitSizeMB = splitSizeMB;
            return this;
        }

        public Builder splitMetaGroupSize(int splitMetaGroupSize) {
            this.splitMetaGroupSize = splitMetaGroupSize;
            return this;
        }

        public Builder validate() {
            checkNotNull(hosts, "hosts must be provided");
            return this;
        }

        @Override
        public MongodbSourceConfig create(int subtask) {
            boolean updateLookup = true;
            return new MongodbSourceConfig(
                    hosts,
                    username,
                    password,
                    databaseList,
                    collectionList,
                    connectionOptions,
                    batchSize,
                    pollAwaitTimeMillis,
                    pollMaxBatchSize,
                    updateLookup,
                    startupOptions,
                    stopOptions,
                    heartbeatIntervalMillis,
                    splitMetaGroupSize,
                    splitSizeMB);
        }
    }
}
