/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.postgresql;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Duration;

import static org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode.CREATE_REPLICATION_CONNECTION_FAILED;

/**
 * A factory for creating various Debezium objects
 *
 * <p>It is a hack to access package-private constructor in debezium.
 */
@Slf4j
public class PostgresObjectUtils {

    /** Create a new PostgresSchema and initialize the content of the schema. */
    public static PostgresSchema newSchema(
            PostgresConnection connection,
            PostgresConnectorConfig config,
            TypeRegistry typeRegistry,
            TopicSelector<TableId> topicSelector,
            PostgresValueConverter valueConverter)
            throws SQLException {
        PostgresSchema schema =
                new PostgresSchema(
                        config,
                        typeRegistry,
                        connection.getDefaultValueConverter(),
                        topicSelector,
                        valueConverter);
        schema.refresh(connection, false);
        return schema;
    }

    public static PostgresEventMetadataProvider newEventMetadataProvider() {
        return new PostgresEventMetadataProvider();
    }

    public static PostgresTaskContext newTaskContext(
            PostgresConnectorConfig connectorConfig,
            PostgresSchema schema,
            TopicSelector<TableId> topicSelector) {
        return new PostgresTaskContext(connectorConfig, schema, topicSelector);
    }

    // modified from
    // io.debezium.connector.postgresql.PostgresConnectorTask.createReplicationConnection.
    // pass connectorConfig instead of maxRetries and retryDelay as parameters.
    // - old: ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext,
    // boolean doSnapshot, int maxRetries, Duration retryDelay)
    // - new: ReplicationConnection createReplicationConnection(PostgresTaskContext taskContext,
    // PostgresConnection postgresConnection, boolean doSnapshot, PostgresConnectorConfig
    // connectorConfig)
    public static ReplicationConnection createReplicationConnection(
            PostgresTaskContext taskContext,
            PostgresConnection postgresConnection,
            boolean doSnapshot,
            PostgresConnectorConfig connectorConfig) {
        int maxRetries = connectorConfig.maxRetries();
        Duration retryDelay = connectorConfig.retryDelay();

        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        while (retryCount <= maxRetries) {
            try {
                log.info("Creating a new replication connection for {}", taskContext);
                return taskContext.createReplicationConnection(doSnapshot, postgresConnection);
            } catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    log.error(
                            "Too many errors connecting to server. All {} retries failed.",
                            maxRetries);
                    throw new ConnectException(ex);
                }

                log.warn(
                        "Error connecting to server; will attempt retry {} of {} after {} "
                                + "seconds. Exception message: {}",
                        retryCount,
                        maxRetries,
                        retryDelay.getSeconds(),
                        ex.getMessage());
                try {
                    metronome.pause();
                } catch (InterruptedException e) {
                    log.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new SeaTunnelRuntimeException(CREATE_REPLICATION_CONNECTION_FAILED, "" + taskContext);
    }
}
