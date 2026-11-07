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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.config.GaussDBMppdbConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.source.reader.mppdb.MppdbReplicationStream;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.PostgresSourceFetchTaskContext;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.PostgresErrorHandler;
import io.debezium.connector.postgresql.PostgresEventDispatcher;
import io.debezium.connector.postgresql.PostgresObjectUtils;
import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.connector.postgresql.PostgresSchema;
import io.debezium.connector.postgresql.PostgresTaskContext;
import io.debezium.connector.postgresql.PostgresTopicSelector;
import io.debezium.connector.postgresql.RelationAwarePostgresSchema;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.heartbeat.DefaultHeartbeatConnectionProvider;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import io.debezium.util.LoggingContext;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;

/** PostgreSQL snapshot context extended with the GaussDB mppdb replication stream. */
@Slf4j
public final class GaussDBSourceFetchTaskContext extends PostgresSourceFetchTaskContext {

    private static final String CONTEXT_NAME = "gaussdb-cdc-connector-task";

    private final PostgresConnection dataConnection;
    private final EventMetadataProvider metadataProvider;
    private final PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder;

    /** Checkpoint-aware mppdb stream sharing the task's regular data connection. */
    private final MppdbReplicationStream mppdbStream;

    private Snapshotter snapshotter;
    private RelationAwarePostgresSchema databaseSchema;
    private PostgresOffsetContext offsetContext;
    private PostgresPartition partition;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<PostgresPartition> dispatcher;
    private PostgresEventDispatcher<TableId> pgEventDispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private PostgresErrorHandler errorHandler;
    private PostgresTaskContext taskContext;
    private SnapshotChangeEventSourceMetrics<PostgresPartition> snapshotMetrics;

    /**
     * Creates a task context for one GaussDB source split.
     *
     * @param valueConverterBuilder converter builder already resolved through {@link
     *     GaussDBPostgresConnection}; it is handed to the PostgreSQL parent so no stock Debezium
     *     connection, which would reject GaussDB's server version, is opened during construction
     */
    public GaussDBSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            PostgresConnection dataConnection,
            Collection<TableChanges.TableChange> engineHistory,
            List<CatalogTable> relationSchemaBaseline,
            PostgresConnection.PostgresValueConverterBuilder valueConverterBuilder,
            GaussDBMppdbConfig mppdbConfig) {
        super(
                sourceConfig,
                dataSourceDialect,
                dataConnection,
                engineHistory,
                relationSchemaBaseline,
                valueConverterBuilder);
        this.dataConnection = dataConnection;
        this.metadataProvider = PostgresObjectUtils.newEventMetadataProvider();
        this.valueConverterBuilder = valueConverterBuilder;
        try {
            this.mppdbStream =
                    new MppdbReplicationStream(
                            dataConnection.connection(),
                            mppdbConfig,
                            sourceConfig.getUsername(),
                            sourceConfig.getPassword());
        } catch (SQLException e) {
            // Context construction cannot propagate checked JDBC failures through the dialect SPI.
            throw new DebeziumException(
                    "Failed to initialize GaussDB mppdb stream for slot '"
                            + mppdbConfig.getSlotName()
                            + "'",
                    e);
        }
    }

    @Override
    public void configure(SourceSplitBase sourceSplit) {
        registerDatabaseHistory(sourceSplit, dataConnection);

        PostgresConnectorConfig connectorConfig = getDbzConnectorConfig();
        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        connectorConfig.getConfig().getString(SNAPSHOT_MODE));
        this.snapshotter = snapshotMode.getSnapshotter(connectorConfig.getConfig());
        this.topicSelector = PostgresTopicSelector.create(connectorConfig);
        TypeRegistry typeRegistry = dataConnection.getTypeRegistry();

        try {
            this.databaseSchema =
                    PostgresObjectUtils.newSchema(
                            dataConnection,
                            connectorConfig,
                            typeRegistry,
                            topicSelector,
                            valueConverterBuilder.build(typeRegistry));
        } catch (SQLException e) {
            throw new SeaTunnelRuntimeException(PostgresConnectorErrorCode.NEW_SCHEMA_FAILED, e);
        }

        this.taskContext =
                PostgresObjectUtils.newTaskContext(connectorConfig, databaseSchema, topicSelector);
        this.offsetContext =
                loadStartingOffsetState(
                        new PostgresOffsetContext.Loader(connectorConfig), sourceSplit);
        this.partition = new PostgresPartition(connectorConfig.getLogicalName());

        int queueSize =
                sourceSplit.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : connectorConfig.getMaxQueueSize();
        LoggingContext.PreviousContext previousContext =
                taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            try {
                if (log.isInfoEnabled()) {
                    log.info(dataConnection.serverInfo().toString());
                }
            } catch (SQLException e) {
                log.warn("Unable to load GaussDB server information", e);
            }

            if (offsetContext == null) {
                snapshotter.init(connectorConfig, null, null);
            } else {
                snapshotter.init(connectorConfig, offsetContext.asOffsetState(), null);
            }
            if (snapshotter.shouldStream()) {
                try {
                    mppdbStream.ensureSlot();
                } catch (SQLException e) {
                    throw new DebeziumException(
                            "Failed to prepare GaussDB mppdb_decoding replication slot", e);
                }
            }

            try {
                dataConnection.commit();
            } catch (SQLException e) {
                throw new DebeziumException(e);
            }

            this.queue =
                    new ChangeEventQueue.Builder<DataChangeEvent>()
                            .pollInterval(connectorConfig.getPollInterval())
                            .maxBatchSize(connectorConfig.getMaxBatchSize())
                            .maxQueueSize(queueSize)
                            .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                            .loggingContextSupplier(
                                    () -> taskContext.configureLoggingContext(CONTEXT_NAME))
                            .build();
            this.dispatcher =
                    new JdbcSourceEventDispatcher<>(
                            connectorConfig,
                            topicSelector,
                            databaseSchema,
                            queue,
                            connectorConfig.getTableFilters().dataCollectionFilter(),
                            DataChangeEvent::new,
                            metadataProvider,
                            new HeartbeatFactory<>(
                                    connectorConfig,
                                    topicSelector,
                                    schemaNameAdjuster,
                                    new DefaultHeartbeatConnectionProvider(dataConnection),
                                    null),
                            schemaNameAdjuster);
            this.pgEventDispatcher =
                    new PostgresEventDispatcher<>(
                            connectorConfig,
                            topicSelector,
                            databaseSchema,
                            queue,
                            connectorConfig.getTableFilters().dataCollectionFilter(),
                            DataChangeEvent::new,
                            metadataProvider,
                            new HeartbeatFactory<>(
                                    connectorConfig,
                                    topicSelector,
                                    schemaNameAdjuster,
                                    new DefaultHeartbeatConnectionProvider(dataConnection),
                                    null),
                            schemaNameAdjuster);
            this.snapshotMetrics =
                    new DefaultChangeEventSourceMetricsFactory()
                            .getSnapshotMetrics(taskContext, queue, metadataProvider);
            this.errorHandler = new PostgresErrorHandler(connectorConfig, queue);
        } finally {
            previousContext.restore();
        }
    }

    /** Returns the mppdb stream consumed by the incremental fetch task. */
    public MppdbReplicationStream getMppdbStream() {
        return mppdbStream;
    }

    @Override
    public PostgresConnection getDataConnection() {
        return dataConnection;
    }

    @Override
    public Snapshotter getSnapshotter() {
        return snapshotter;
    }

    @Override
    public PostgresTaskContext getTaskContext() {
        return taskContext;
    }

    @Override
    public SnapshotChangeEventSourceMetrics<PostgresPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotMetrics;
    }

    @Override
    public PostgresOffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public PostgresPartition getPartition() {
        return partition;
    }

    @Override
    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    @Override
    public PostgresSchema getDatabaseSchema() {
        return databaseSchema;
    }

    @Override
    public JdbcSourceEventDispatcher<PostgresPartition> getDispatcher() {
        return dispatcher;
    }

    @Override
    public PostgresEventDispatcher<TableId> getPgEventDispatcher() {
        return pgEventDispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    /** Returns the connection-scoped type registry used to convert mppdb text values. */
    public TypeRegistry getTypeRegistry() {
        return dataConnection.getTypeRegistry();
    }

    @Override
    public void close() {
        try {
            if (Objects.nonNull(databaseSchema)) {
                databaseSchema.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close GaussDB schema", e);
        } finally {
            mppdbStream.close();
            super.close();
        }
    }

    private PostgresOffsetContext loadStartingOffsetState(
            PostgresOffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();
        Map<String, String> stringOffset =
                Objects.requireNonNull(offset, "offset is null for the source split").getOffset();
        Map<String, Object> debeziumOffset = new HashMap<>();
        for (Map.Entry<String, String> entry : stringOffset.entrySet()) {
            if (entry.getValue() != null) {
                debeziumOffset.put(entry.getKey(), Long.parseLong(entry.getValue()));
            }
        }
        return loader.load(debeziumOffset);
    }
}
