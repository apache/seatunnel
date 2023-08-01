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

import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils.buildConnectionString;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

@Getter
@EqualsAndHashCode
public class MongodbSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    private final String hosts;

    private final String username;

    private final String password;

    private final List<String> databaseList;

    private final List<String> collectionList;

    private final String connectionString;

    private final int batchSize;

    private final int pollAwaitTimeMillis;

    private final int pollMaxBatchSize;

    private final boolean updateLookup;

    private final StartupConfig startupOptions;

    private final StopConfig stopOptions;

    private final int heartbeatIntervalMillis;

    private final int splitMetaGroupSize;

    private final int splitSizeMB;

    MongodbSourceConfig(
            String hosts,
            String username,
            String password,
            List<String> databaseList,
            List<String> collectionList,
            String connectionOptions,
            int batchSize,
            int pollAwaitTimeMillis,
            int pollMaxBatchSize,
            boolean updateLookup,
            StartupConfig startupOptions,
            StopConfig stopOptions,
            int heartbeatIntervalMillis,
            int splitMetaGroupSize,
            int splitSizeMB) {
        this.hosts = checkNotNull(hosts);
        this.username = username;
        this.password = password;
        this.databaseList = databaseList;
        this.collectionList = collectionList;
        this.connectionString =
                buildConnectionString(username, password, hosts, connectionOptions)
                        .getConnectionString();
        this.batchSize = batchSize;
        this.pollAwaitTimeMillis = pollAwaitTimeMillis;
        this.pollMaxBatchSize = pollMaxBatchSize;
        this.updateLookup = updateLookup;
        this.startupOptions = startupOptions;
        this.stopOptions = stopOptions;
        this.heartbeatIntervalMillis = heartbeatIntervalMillis;
        this.splitMetaGroupSize = splitMetaGroupSize;
        this.splitSizeMB = splitSizeMB;
    }

    @Override
    public StartupConfig getStartupConfig() {
        return startupOptions;
    }

    @Override
    public StopConfig getStopConfig() {
        return stopOptions;
    }

    @Override
    public int getSplitSize() {
        return splitSizeMB;
    }

    @Override
    public boolean isExactlyOnce() {
        return true;
    }
}
