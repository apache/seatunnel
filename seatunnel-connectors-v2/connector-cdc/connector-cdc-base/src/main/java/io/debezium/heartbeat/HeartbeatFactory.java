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

package io.debezium.heartbeat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.schema.DataCollectionId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import io.debezium.util.Strings;

import java.time.Duration;

/**
 * Copied from Debezium 1.9.8.Final. A factory for creating the appropriate {@link Heartbeat}
 * implementation based on the connector type and its configured properties.
 *
 * <p>Line 66~91: If heartbeatInterval is zero, then set it 5000(Come from
 * https://github.com/apache/seatunnel/pull/6554/files#diff-3f5146dd3b9e6ed097d4dd3a08a3d0575ac8d139230f74c111b30e163c354cdc)
 */
public class HeartbeatFactory<T extends DataCollectionId> {

    private final CommonConnectorConfig connectorConfig;
    private final TopicSelector<T> topicSelector;
    private final SchemaNameAdjuster schemaNameAdjuster;
    private final HeartbeatConnectionProvider connectionProvider;
    private final HeartbeatErrorHandler errorHandler;

    public HeartbeatFactory(
            CommonConnectorConfig connectorConfig,
            TopicSelector<T> topicSelector,
            SchemaNameAdjuster schemaNameAdjuster) {
        this(connectorConfig, topicSelector, schemaNameAdjuster, null, null);
    }

    public HeartbeatFactory(
            CommonConnectorConfig connectorConfig,
            TopicSelector<T> topicSelector,
            SchemaNameAdjuster schemaNameAdjuster,
            HeartbeatConnectionProvider connectionProvider,
            HeartbeatErrorHandler errorHandler) {
        this.connectorConfig = connectorConfig;
        this.topicSelector = topicSelector;
        this.schemaNameAdjuster = schemaNameAdjuster;

        this.connectionProvider = connectionProvider;
        this.errorHandler = errorHandler;
    }

    public Heartbeat createHeartbeat() {
        Duration heartbeatInterval = connectorConfig.getHeartbeatInterval();
        if (heartbeatInterval.isZero()) {
            heartbeatInterval = Duration.ofMillis(5000);
        }

        if (connectorConfig instanceof RelationalDatabaseConnectorConfig) {
            RelationalDatabaseConnectorConfig relConfig =
                    (RelationalDatabaseConnectorConfig) connectorConfig;
            if (!Strings.isNullOrBlank(relConfig.getHeartbeatActionQuery())) {
                return new DatabaseHeartbeatImpl(
                        heartbeatInterval,
                        topicSelector.getHeartbeatTopic(),
                        connectorConfig.getLogicalName(),
                        connectionProvider.get(),
                        relConfig.getHeartbeatActionQuery(),
                        errorHandler,
                        schemaNameAdjuster);
            }
        }

        return new HeartbeatImpl(
                heartbeatInterval,
                topicSelector.getHeartbeatTopic(),
                connectorConfig.getLogicalName(),
                schemaNameAdjuster);
    }
}
