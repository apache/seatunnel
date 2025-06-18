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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.JdbcSourceFetchTaskContext;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.exception.PostgresConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresUtils;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

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
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.connector.postgresql.spi.Snapshotter;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.TopicSelector;
import io.debezium.util.LoggingContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.debezium.connector.AbstractSourceInfo.SCHEMA_NAME_KEY;
import static io.debezium.connector.AbstractSourceInfo.TABLE_NAME_KEY;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.PLUGIN_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SLOT_NAME;
import static io.debezium.connector.postgresql.PostgresConnectorConfig.SNAPSHOT_MODE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils.PostgresConnectionUtils.newPostgresValueConverterBuilder;

@Slf4j
public class PostgresSourceFetchTaskContext extends JdbcSourceFetchTaskContext {

    private static final String CONTEXT_NAME = "postgres-cdc-connector-task";

    private final PostgresConnection dataConnection;

    @Getter private ReplicationConnection replicationConnection;

    private final EventMetadataProvider metadataProvider;

    @Getter private Snapshotter snapshotter;
    private PostgresSchema databaseSchema;
    private PostgresOffsetContext offsetContext;
    private PostgresPartition partition;
    private TopicSelector<TableId> topicSelector;
    private JdbcSourceEventDispatcher<PostgresPartition> dispatcher;
    private PostgresEventDispatcher<TableId> pgEventDispatcher;
    private ChangeEventQueue<DataChangeEvent> queue;
    private PostgresErrorHandler errorHandler;

    @Getter private PostgresTaskContext taskContext;

    private SnapshotChangeEventSourceMetrics<PostgresPartition> snapshotChangeEventSourceMetrics;

    private PostgresConnection.PostgresValueConverterBuilder postgresValueConverterBuilder;

    private Collection<TableChanges.TableChange> engineHistory;

    public PostgresSourceFetchTaskContext(
            JdbcSourceConfig sourceConfig,
            JdbcDataSourceDialect dataSourceDialect,
            PostgresConnection dataConnection,
            Collection<TableChanges.TableChange> engineHistory) {
        super(sourceConfig, dataSourceDialect);
        this.dataConnection = dataConnection;
        this.metadataProvider = PostgresObjectUtils.newEventMetadataProvider();
        this.engineHistory = engineHistory;
        this.postgresValueConverterBuilder =
                newPostgresValueConverterBuilder(
                        getDbzConnectorConfig(),
                        "postgres-source-fetch-task-context",
                        sourceConfig.getServerTimeZone());
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        super.registerDatabaseHistory(sourceSplitBase, dataConnection);

        // initial stateful objects
        final PostgresConnectorConfig connectorConfig = getDbzConnectorConfig();
        PostgresConnectorConfig.SnapshotMode snapshotMode =
                PostgresConnectorConfig.SnapshotMode.parse(
                        connectorConfig.getConfig().getString(SNAPSHOT_MODE));
        this.snapshotter = snapshotMode.getSnapshotter(connectorConfig.getConfig());

        this.topicSelector = PostgresTopicSelector.create(connectorConfig);
        final TypeRegistry typeRegistry = dataConnection.getTypeRegistry();

        try {
            this.databaseSchema =
                    PostgresObjectUtils.newSchema(
                            dataConnection,
                            connectorConfig,
                            typeRegistry,
                            topicSelector,
                            postgresValueConverterBuilder.build(typeRegistry));
        } catch (SQLException e) {
            throw new SeaTunnelRuntimeException(PostgresConnectorErrorCode.NEW_SCHEMA_FAILED, e);
        }

        this.taskContext =
                PostgresObjectUtils.newTaskContext(connectorConfig, databaseSchema, topicSelector);
        this.offsetContext =
                loadStartingOffsetState(
                        new PostgresOffsetContext.Loader(connectorConfig), sourceSplitBase);
        this.partition = new PostgresPartition(connectorConfig.getLogicalName());

        // If in the snapshot read phase and enable exactly-once, the queue needs to be set to a
        // maximum size of `Integer.MAX_VALUE` (buffered a current snapshot all data). otherwise,
        // use the configuration queue size.
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : getSourceConfig().getDbzConnectorConfig().getMaxQueueSize();

        LoggingContext.PreviousContext previousContext =
                taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            SlotState slotInfo = null;
            try {
                if (log.isInfoEnabled()) {
                    log.info(dataConnection.serverInfo().toString());
                }
                PostgresConnectorConfig.LogicalDecoder logicalDecoder =
                        PostgresConnectorConfig.LogicalDecoder.parse(
                                connectorConfig.getConfig().getString(PLUGIN_NAME));
                slotInfo =
                        dataConnection.getReplicationSlotState(
                                connectorConfig.getConfig().getString(SLOT_NAME),
                                logicalDecoder.getPostgresPluginName());
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

            if (snapshotter.shouldStream()) {
                // we need to create the slot before we start streaming if it doesn't exist
                // otherwise we can't stream back changes happening while the snapshot is taking
                // place
                if (this.replicationConnection == null) {
                    this.replicationConnection =
                            PostgresObjectUtils.createReplicationConnection(
                                    this.taskContext,
                                    dataConnection,
                                    snapshotter.shouldSnapshot(),
                                    connectorConfig);
                    try {
                        // create the slot if it doesn't exist, otherwise update slot to add new
                        // table(job restore and add table)
                        replicationConnection.createReplicationSlot().orElse(null);
                    } catch (SQLException ex) {
                        String message = "Creation of replication slot failed";
                        if (ex.getMessage().contains("already exists")) {
                            message +=
                                    "; when setting up multiple connectors for the same database host, please make sure to use a distinct replication slot name for each.";
                            log.warn(message);
                        } else {
                            throw new DebeziumException(message, ex);
                        }
                    }
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
                            // do not buffer any element, we use signal event
                            // .buffering()
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
                            schemaNameAdjuster);

            this.snapshotChangeEventSourceMetrics =
                    new DefaultChangeEventSourceMetricsFactory()
                            .getSnapshotMetrics(taskContext, queue, metadataProvider);

            this.errorHandler = new PostgresErrorHandler(connectorConfig, queue);
        } finally {
            previousContext.restore();
        }
    }

    @Override
    public PostgresSourceConfig getSourceConfig() {
        return (PostgresSourceConfig) sourceConfig;
    }

    public PostgresConnection getDataConnection() {
        return dataConnection;
    }

    public SnapshotChangeEventSourceMetrics<PostgresPartition>
            getSnapshotChangeEventSourceMetrics() {
        return snapshotChangeEventSourceMetrics;
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return (PostgresConnectorConfig) super.getDbzConnectorConfig();
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
    public TableId getTableId(SourceRecord record) {
        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String schemaName = source.getString(SCHEMA_NAME_KEY);
        String tableName = source.getString(TABLE_NAME_KEY);
        return new TableId(null, schemaName, tableName);
    }

    @Override
    public SeaTunnelRowType getSplitType(Table table) {
        return PostgresUtils.getSplitType(table);
    }

    @Override
    public JdbcSourceEventDispatcher<PostgresPartition> getDispatcher() {
        return dispatcher;
    }

    public PostgresEventDispatcher<TableId> getPgEventDispatcher() {
        return pgEventDispatcher;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return queue;
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return getDbzConnectorConfig().getTableFilters().dataCollectionFilter();
    }

    @Override
    public Offset getStreamOffset(SourceRecord sourceRecord) {
        return PostgresUtils.getLsnPosition(sourceRecord);
    }

    @Override
    public void close() {
        try {
            if (Objects.nonNull(dataConnection)) {
                this.dataConnection.close();
            }
            if (Objects.nonNull(replicationConnection)) {
                this.replicationConnection.close();
            }
        } catch (Exception e) {
            log.warn("Failed to close connection", e);
        }
    }

    /** Loads the connector's persistent offset (if present) via the given loader. */
    private PostgresOffsetContext loadStartingOffsetState(
            PostgresOffsetContext.Loader loader, SourceSplitBase split) {
        Offset offset =
                split.isSnapshotSplit()
                        ? LsnOffset.INITIAL_OFFSET
                        : split.asIncrementalSplit().getStartupOffset();
        Map<String, String> offsetStrMap =
                Objects.requireNonNull(offset, "offset is null for the sourceSplitBase")
                        .getOffset();
        // all the keys happen to be long type for PostgresOffsetContext.Loader.load
        Map<String, Object> offsetMap = new HashMap<>();
        for (String key : offsetStrMap.keySet()) {
            String value = offsetStrMap.get(key);
            if (value != null) {
                offsetMap.put(key, Long.parseLong(value));
            }
        }
        return loader.load(offsetMap);
    }
}
