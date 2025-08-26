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

package org.apache.seatunnel.connectors.seatunnel.databend.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Properties;

@Slf4j
@Getter
public class DatabendSinkConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final String table;
    private final boolean autoCommit;
    private final int batchSize;
    private final int executeTimeoutSec;
    private final int interval;
    private final String conflictKey;
    private final boolean enableDelete;

    private DatabendSinkConfig(Builder builder) {
        this.url = builder.url;
        this.username = builder.username;
        this.password = builder.password;
        this.database = builder.database;
        this.table = builder.table;
        this.autoCommit = builder.autoCommit;
        this.batchSize = builder.batchSize;
        this.executeTimeoutSec = builder.executeTimeoutSec;
        this.interval = builder.interval;
        this.conflictKey = builder.conflictKey;
        this.enableDelete = builder.enableDelete;
    }

    public static DatabendSinkConfig of(ReadonlyConfig config) {
        return new Builder()
                .withUrl(config.get(DatabendOptions.URL))
                .withUsername(config.get(DatabendOptions.USERNAME))
                .withPassword(config.get(DatabendOptions.PASSWORD))
                .withDatabase(config.get(DatabendOptions.DATABASE))
                .withTable(config.get(DatabendOptions.TABLE))
                .withAutoCommit(config.get(DatabendOptions.AUTO_COMMIT))
                .withBatchSize(config.get(DatabendOptions.BATCH_SIZE))
                .withExecuteTimeoutSec(config.get(DatabendSinkOptions.EXECUTE_TIMEOUT_SEC))
                .withConflictKey(config.get(DatabendSinkOptions.CONFLICT_KEY))
                .withAllowDelete(config.get(DatabendSinkOptions.ENABLE_DELETE))
                .build();
    }

    public static class Builder {
        private String url;
        private String username;
        private String password;
        private String database;
        private String table;
        private boolean autoCommit = true;
        private int batchSize = 1000;
        private int executeTimeoutSec = 300;
        private int interval = 30;
        private String conflictKey;
        private boolean enableDelete = false;

        public Builder withUrl(String url) {
            this.url = url;
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

        public Builder withDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder withTable(String table) {
            this.table = table;
            return this;
        }

        public Builder withAutoCommit(boolean autoCommit) {
            this.autoCommit = autoCommit;
            return this;
        }

        public Builder withBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder withExecuteTimeoutSec(int executeTimeoutSec) {
            this.executeTimeoutSec = executeTimeoutSec;
            return this;
        }

        public Builder withInterval(int interval) {
            this.interval = interval;
            return this;
        }

        public Builder withConflictKey(String conflictKey) {
            this.conflictKey = conflictKey;
            return this;
        }

        public Builder withAllowDelete(boolean allowDelete) {
            this.enableDelete = allowDelete;
            return this;
        }

        public DatabendSinkConfig build() {
            return new DatabendSinkConfig(this);
        }
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        return properties;
    }

    public String getRawTableName() {
        long timestamp = System.currentTimeMillis();
        return table + "_raw_" + timestamp;
    }

    public String getStreamName() {
        long timestamp = System.currentTimeMillis();
        return table + "_stream_" + timestamp;
    }

    public Properties toProperties() {
        return getProperties();
    }

    public boolean isCdcMode() {
        return conflictKey != null && !conflictKey.isEmpty();
    }
}
