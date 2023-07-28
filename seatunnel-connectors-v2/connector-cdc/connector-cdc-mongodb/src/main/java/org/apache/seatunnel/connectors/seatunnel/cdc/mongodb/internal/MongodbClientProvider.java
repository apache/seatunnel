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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.internal;

import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum MongodbClientProvider {
    INSTANCE;

    private volatile MongoClient mongoClient;

    public MongoClient getOrCreateMongoClient(MongodbSourceConfig sourceConfig) {
        if (mongoClient == null) {
            ConnectionString connectionString =
                    new ConnectionString(sourceConfig.getConnectionString());
            log.info(
                    "Create and register mongo client {}@{}",
                    connectionString.getUsername(),
                    connectionString.getHosts());
            mongoClient = MongoClients.create(connectionString);
        }
        return mongoClient;
    }
}
