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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.DebeziumException;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresStreamingChangeEventSource;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.snapshot.NeverSnapshotter;
import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.time.Duration;

@Slf4j
public class PostgresWalFetchTask implements FetchTask<SourceSplitBase> {
    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    private static final String CONTEXT_NAME = "postgres-cdc-connector-task";
    private ReplicationConnection replicationConnection;

    public PostgresWalFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(Context context) throws Exception {
        PostgresSourceFetchTaskContext sourceFetchContext =
                (PostgresSourceFetchTaskContext) context;
        taskRunning = true;

        PostgresConnection dataConnection =
                ((PostgresSourceFetchTaskContext) context).getDataConnection();
        PostgresConnectorConfig connectorConfig =
                ((PostgresSourceFetchTaskContext) context).getDbzConnectorConfig();
        PostgresOffsetContext offsetContext =
                ((PostgresSourceFetchTaskContext) context).getOffsetContext();

        PostgresTaskContext taskContext =
                ((PostgresSourceFetchTaskContext) context).getTaskContext();

        NeverSnapshotter snapshotter = new NeverSnapshotter();
        initReplicationSlot(
                dataConnection, connectorConfig, offsetContext, taskContext, snapshotter);

        PostgresStreamingChangeEventSource streamingChangeEventSource =
                new PostgresStreamingChangeEventSource(
                        sourceFetchContext.getDbzConnectorConfig(),
                        snapshotter,
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        Clock.SYSTEM,
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getTaskContext(),
                        replicationConnection);

        TransactionLogSplitChangeEventSourceContext changeEventSourceContext =
                new TransactionLogSplitChangeEventSourceContext();

        streamingChangeEventSource.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());
    }

    private void initReplicationSlot(
            PostgresConnection dataConnection,
            PostgresConnectorConfig connectorConfig,
            PostgresOffsetContext offsetContext,
            PostgresTaskContext taskContext,
            NeverSnapshotter snapshotter) {
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try {
                if (log.isInfoEnabled()) {
                    log.info(dataConnection.serverInfo().toString());
                }
                slotInfo =
                        dataConnection.getReplicationSlotState(
                                connectorConfig.slotName(),
                                connectorConfig.plugin().getPostgresPluginName());
            } catch (SQLException e) {
                log.warn(
                        "unable to load info of replication slot, Debezium will try to create the slot");
            }

            if (offsetContext == null) {
                log.info("No previous offset found");
                // if we have no initial offset, indicate that to Snapshotter by passing null
                snapshotter.init(connectorConfig, null, slotInfo);
            } else {
                log.info("Found previous offset {}", offsetContext);
                snapshotter.init(connectorConfig, offsetContext.asOffsetState(), slotInfo);
            }

            SlotCreationResult slotCreatedInfo = null;
            if (snapshotter.shouldStream()) {
                final boolean doSnapshot = snapshotter.shouldSnapshot();
                this.replicationConnection =
                        createReplicationConnection(
                                taskContext,
                                doSnapshot,
                                connectorConfig.maxRetries(),
                                connectorConfig.retryDelay());

                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking
                // place
                if (slotInfo == null) {
                    try {
                        slotCreatedInfo =
                                replicationConnection.createReplicationSlot().orElse(null);
                    } catch (SQLException ex) {
                        String message = "Creation of replication slot failed";
                        if (ex.getMessage().contains("already exists")) {
                            message +=
                                    "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                        }
                        throw new DebeziumException(message, ex);
                    }
                } else {
                    slotCreatedInfo = null;
                }
            }

            try {
                dataConnection.commit();
            } catch (SQLException e) {
                throw new DebeziumException(e);
            }
        } catch (Exception e) {
            throw new DebeziumException(e);
        }
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {}

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    private class TransactionLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }

    public ReplicationConnection createReplicationConnection(
            PostgresTaskContext taskContext,
            boolean doSnapshot,
            int maxRetries,
            Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(doSnapshot);
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
        return replicationConnection;
    }
}
